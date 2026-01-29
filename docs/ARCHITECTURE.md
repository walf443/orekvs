# OREKVS アーキテクチャ

このドキュメントは、orekvs のエンジン実装の詳細によらない全体アーキテクチャを説明します。
各エンジンの詳細については、個別のドキュメント（[LSMTreeEngine.md](./LSMTreeEngine.md)、[BTreeEngine.md](./BTreeEngine.md)、[LogEngine.md](./LogEngine.md)）を参照してください。

## 概要

orekvs は、プラガブルなストレージエンジンを持つ分散キーバリューストアです。

```
┌─────────────────────────────────────────────────────────────┐
│                       クライアント                           │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ gRPC
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      gRPC サーバー                           │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐ │
│  │  KeyValue   │    │ Replication │    │    Metrics      │ │
│  │   Service   │    │   Service   │    │    Service      │ │
│  └─────────────┘    └─────────────┘    └─────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ Engine トレイト
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    ストレージエンジン                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │  Memory  │  │   Log    │  │ LSM-Tree │  │  B-Tree  │   │
│  │  Engine  │  │  Engine  │  │  Engine  │  │  Engine  │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## コア抽象化: Engine トレイト

すべてのストレージエンジンは `Engine` トレイトを実装します。これにより、サーバー層はエンジンの実装詳細を意識せずに動作できます。

### 基本操作

```rust
pub trait Engine: Send + Sync + 'static {
    // 基本 CRUD
    fn set(&self, key: String, value: String) -> Result<(), Status>;
    fn delete(&self, key: String) -> Result<(), Status>;
    fn get(&self, key: String) -> Result<String, Status>;
    fn get_with_expire_at(&self, key: String) -> Result<(String, u64), Status>;

    // TTL サポート
    fn set_with_ttl(&self, key: String, value: String, ttl_secs: u64) -> Result<(), Status>;
    fn set_with_expire_at(&self, key: String, value: String, expire_at: u64) -> Result<(), Status>;
    fn get_expire_at(&self, key: String) -> Result<(bool, u64), Status>;

    // バッチ操作
    fn batch_set(&self, items: Vec<(String, String)>) -> Result<usize, Status>;
    fn batch_get(&self, keys: Vec<String>) -> Vec<(String, String)>;
    fn batch_delete(&self, keys: Vec<String>) -> Result<usize, Status>;
    fn batch_set_with_expire_at(&self, items: Vec<(String, String, u64)>) -> Result<usize, Status>;

    // Compare-and-Set（アトミック条件付き更新）
    fn compare_and_set(
        &self,
        key: String,
        expected_value: Option<String>,
        new_value: String,
        expire_at: u64,
    ) -> Result<(bool, Option<String>), Status>;
}
```

### デフォルト実装

`Engine` トレイトは多くのメソッドにデフォルト実装を提供しており、エンジン実装者は最小限のメソッドのみを実装すれば動作します：

- `get()` → `get_with_expire_at()` を呼び出し
- `set_with_ttl()` → `set()` を呼び出し（TTL 無視）
- `batch_*` 系 → 個別操作を繰り返し呼び出し

エンジンは必要に応じてこれらをオーバーライドし、最適化された実装を提供できます。

## Holder/Wrapper パターン

各永続化エンジンは、ライフサイクル管理のために Holder/Wrapper パターンを採用しています。

```
                     ┌─────────────────────┐
                     │   Engine 実装       │
                     │   (Arc<EngineX>)    │
                     └─────────────────────┘
                              │
                              ▼
                     ┌─────────────────────┐
                     │   EngineXWrapper    │
                     │  (Engine トレイト   │
                     │    を実装)          │
                     └─────────────────────┘
                              │
                              ▼
                     ┌─────────────────────┐
                     │   EngineXHolder     │
                     │  (シャットダウン    │
                     │    ライフサイクル)  │
                     └─────────────────────┘
                              │
                              ▼
                     ┌─────────────────────┐
                     │    MyKeyValue       │
                     │   (gRPC Service)    │
                     └─────────────────────┘
```

### 目的

1. **Arc ラッピング**: 複数コンポーネント間での共有所有権
2. **グレースフルシャットダウン**: Holder がシャットダウン時のクリーンアップを管理
3. **トレイトオブジェクト**: `Box<dyn Engine>` としてサーバーに格納可能

## gRPC サービス

### KeyValue サービス

クライアント向けの主要 API を提供します。

| RPC | 説明 |
|-----|------|
| `Set` | キーに値を設定（TTL オプション） |
| `Get` | キーの値を取得 |
| `Delete` | キーを削除 |
| `BatchSet` | 複数キーを一括設定 |
| `BatchGet` | 複数キーを一括取得 |
| `BatchDelete` | 複数キーを一括削除 |
| `CompareAndSet` | アトミック条件付き更新 |
| `GetExpireAt` | キーの有効期限を取得 |
| `GetMetrics` | メトリクス取得 |
| `Promote` | フォロワーをリーダーに昇格 |

### Replication サービス

リーダー・フォロワー間のレプリケーションを管理します。

| RPC | 説明 |
|-----|------|
| `StreamWALEntries` | WAL エントリのストリーミング |
| `GetLeaderInfo` | リーダーの WAL 位置を取得 |
| `RequestSnapshot` | スナップショット転送を要求 |
| `StreamSnapshot` | スナップショットファイルのストリーミング |

## レプリケーションアーキテクチャ

### リーダー・フォロワーモデル

```
┌─────────────┐                    ┌─────────────┐
│   リーダー   │◄───── 書き込み ─────│  クライアント │
│             │                    └─────────────┘
│   ┌─────┐   │
│   │ WAL │   │
│   └─────┘   │
└──────┬──────┘
       │
       │ WAL ストリーミング
       ▼
┌─────────────┐
│  フォロワー  │◄───── 読み込み ─────┐
│             │                    │
│   ┌─────┐   │              ┌─────────────┐
│   │ WAL │   │              │  クライアント │
│   └─────┘   │              └─────────────┘
└─────────────┘
```

### 同期モード

1. **キャッチアップモード**: フォロワーが遅れている場合、WAL エントリをバッチ処理（1000エントリ/バッチ）
2. **リアルタイムモード**: 追いついた後は低レイテンシストリーミング（10ms ポーリング間隔）

### スナップショット転送

WAL ギャップが検出された場合（例: WAL がアーカイブ済み）、スナップショット転送がトリガーされます：

1. フォロワーがスナップショットを要求
2. リーダーがスナップショットロックを取得
3. SSTable ファイルを 64KB チャンクでストリーミング
4. 転送完了後、フォロワーが WAL ストリーミングを再開

## Compare-and-Set (CAS)

アトミックな条件付き更新をサポートします。

### CAS ロックストライプ

複数キーへの並行 CAS 操作を効率的に処理するため、ストライプドロックを使用します。

```
┌─────────────────────────────────────────────────────────────┐
│                     CAS Lock Stripe                         │
│  ┌────┐ ┌────┐ ┌────┐ ┌────┐        ┌────┐                │
│  │ 0  │ │ 1  │ │ 2  │ │ 3  │  ...   │255 │                │
│  └────┘ └────┘ └────┘ └────┘        └────┘                │
│    ▲                                                        │
│    │                                                        │
│    └── hash(key) % 256 で割り当て                           │
└─────────────────────────────────────────────────────────────┘
```

- 256 個のストライプ化された Mutex
- キーのハッシュ値でストライプを決定
- 同じキーへの操作はシリアライズ
- 異なるキーへの操作は並行実行可能

## データフロー

### 書き込みパス

```
クライアント
    │
    ▼
gRPC Server (KeyValue::set)
    │
    ▼
Engine::set() または Engine::set_with_ttl()
    │
    ▼
[エンジン固有の処理]
    │
    ├── WAL への書き込み（永続性保証）
    │
    └── インメモリ構造への書き込み
```

### 読み込みパス

```
クライアント
    │
    ▼
gRPC Server (KeyValue::get)
    │
    ▼
Engine::get() または Engine::get_with_expire_at()
    │
    ▼
[エンジン固有の処理]
    │
    ├── インメモリ構造をチェック
    │
    └── 永続化層をチェック（必要に応じて）
```

## TTL (Time-To-Live) サポート

### データ表現

```rust
pub struct MemValue {
    pub value: Option<String>,  // None = 削除（トゥームストーン）
    pub expire_at: u64,         // Unix タイムスタンプ、0 = 有効期限なし
}
```

### 有効期限の処理

- `expire_at > 0` かつ `expire_at <= 現在時刻` の場合、キーは期限切れ
- 読み込み時に期限切れチェックを実行
- レプリケーションでは `set_with_expire_at()` を使用して絶対時刻を保持

## 利用可能なエンジン

| エンジン | 永続性 | 用途 |
|----------|--------|------|
| Memory | なし | テスト、シングルプロセス |
| Log | あり | シンプルな永続化 |
| LSM-Tree | あり | 高スループット書き込み |
| B-Tree | あり | バランスの取れた読み書き |

## ディレクトリ構造

```
src/
├── lib.rs                    # ライブラリエクスポート
├── main.rs                   # CLI エントリーポイント
├── client/                   # クライアント実装
│   └── cli/                  # クライアント CLI コマンド
├── server/                   # gRPC サーバー
│   ├── mod.rs               # KeyValue サービス
│   └── cli.rs               # サーバー CLI
├── engine/                   # ストレージエンジン
│   ├── mod.rs               # Engine トレイト定義
│   ├── cas.rs               # CAS ロックストライプ
│   ├── wal.rs               # 共通 WAL ユーティリティ
│   ├── memory.rs            # Memory エンジン
│   ├── log.rs               # Log エンジン
│   ├── btree/               # B-Tree エンジン
│   └── lsm_tree/            # LSM-Tree エンジン
└── replication/             # レプリケーションシステム
    ├── mod.rs               # ReplicationService
    ├── service.rs           # サービス実装
    └── follower.rs          # フォロワーロジック
```

## 設計原則

1. **トレイトベースの抽象化**: `Engine` トレイトによりプラガブルな実装が可能
2. **Holder/Wrapper パターン**: Arc ラッピングとライフサイクル管理
3. **ストライプドロッキング**: CAS 操作の並行性を向上
4. **スナップショット + WAL**: レプリケーションのリカバリ戦略
