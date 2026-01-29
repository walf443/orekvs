# orekvs

Rustで実装されたKey-Valueストアです。複数のストレージエンジンを選択でき、gRPCを介してアクセスできます。

## 特徴

- **複数のストレージエンジン**: 用途に応じて選択可能
  - `memory`: インメモリストレージ（永続化なし）
  - `log`: シンプルなログ構造化ストレージ
  - `lsm-tree`: LSM-Treeベースのストレージ（書き込み最適化）
  - `btree`: ディスクベースのB-Treeストレージ（読み取り最適化）
- **gRPCインターフェース**: 高性能なRPC通信
- **TTLサポート**: キーごとに有効期限を設定可能
- **バッチ操作**: 複数キーの一括操作
- **Compare-and-Set (CAS)**: アトミックな条件付き更新
- **レプリケーション**: リーダー・フォロワー型のレプリケーション（LSM-Tree）

## ビルド

```bash
cargo build --release
```

## 使用方法

### サーバーの起動

```bash
# メモリエンジンで起動（デフォルト）
cargo run -- server

# LSM-Treeエンジンで起動
cargo run -- server --engine lsm-tree --data-dir ./data

# B-Treeエンジンで起動
cargo run -- server --engine btree --data-dir ./data

# Logエンジンで起動
cargo run -- server --engine log --data-dir ./data
```

### サーバーオプション

```
--addr <ADDR>                    リッスンアドレス [default: 127.0.0.1:50051]
--engine <ENGINE>                ストレージエンジン [default: memory]
                                 [possible values: memory, log, lsm-tree, btree]
--data-dir <DATA_DIR>            データディレクトリ [default: kv_data]
```

### クライアントコマンド

#### 基本操作

```bash
# 値の設定
cargo run -- client set mykey myvalue

# TTL付きで設定（60秒後に期限切れ）
cargo run -- client set mykey myvalue --ttl 60

# 値の取得
cargo run -- client get mykey

# 値の削除
cargo run -- client delete mykey
```

## レプリケーション

LSM-Treeエンジンはリーダー・フォロワー型のレプリケーションをサポートします。

### リーダーの起動

```bash
cargo run -- server --engine lsm-tree --data-dir ./leader_data --enable-replication
```

### フォロワーの起動

```bash
cargo run -- server --leader-addr http://127.0.0.1:50052 \
    --data-dir ./follower_data \
    --addr 127.0.0.1:50053
```

## ストレージエンジン

各エンジンの詳細は `docs/` ディレクトリを参照してください：

- [LogEngine](docs/LogEngine.md) - シンプルなログ構造化ストレージ
- [LSMTreeEngine](docs/LSMTreeEngine.md) - LSM-Treeベースのストレージ
- [BTreeEngine](docs/BTreeEngine.md) - ディスクベースのB-Treeストレージ

## テスト

```bash
cargo nextest run
```

## ライセンス

MIT License
