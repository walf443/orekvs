# LogEngine Architecture

## Overview

LogEngineは、シンプルなログ構造化ストレージエンジンです。
追記型の書き込みとインメモリインデックスを組み合わせ、実装の簡潔さを重視しています。

## Components

### 1. Data File

単一のログファイル (`log_engine.data`) にすべてのデータを格納します。

**ファイルフォーマット:**
```
Header: [magic: "OREKVS" (6B)][version: u32 (4B)]
Entry:  [timestamp: u64][key_len: u64][val_len: u64][key][value]
```

**Tombstone:**
削除はval_len = u64::MAXで表現されます。

### 2. In-Memory Index

`HashMap<String, (u64, usize)>`でキーからファイルオフセットへのマッピングを保持。

- キー: 文字列
- 値: (ファイルオフセット, バリュー長)

起動時にファイル全体をスキャンしてインデックスを再構築します。

### 3. Compaction

ログファイルが設定された容量を超えると、バックグラウンドでコンパクションを実行します。

**プロセス:**
1. 容量超過を検出
2. 新しい一時ファイルを作成
3. インデックスの有効エントリのみを新ファイルにコピー
4. アトミックにファイルをリネーム
5. インデックスを更新

## Data Flow

### 書き込み (Set)

```
1. エントリサイズが容量を超えないか確認
2. ファイル末尾に追記
3. インメモリインデックスを更新
4. 容量超過ならバックグラウンドでCompaction
```

### 読み取り (Get)

```
1. インメモリインデックスからオフセットを取得
2. ファイルからバリューを直接読み取り
3. 見つからなければNotFoundエラー
```

### 削除 (Delete)

```
1. Tombstoneエントリをファイルに追記
2. インメモリインデックスからキーを削除
3. 容量超過ならバックグラウンドでCompaction
```

## Configuration

```rust
LogEngine::new(
    data_dir: String,        // データディレクトリ
    log_capacity_bytes: u64, // ログファイルの最大サイズ
)
```

## Recovery

起動時の処理:

1. ファイルヘッダーの検証（magic bytes, version）
2. ファイル全体をスキャンしてインデックスを再構築
3. Tombstoneを検出したらインデックスから削除
4. 中断された一時ファイル (`.tmp`) をクリーンアップ

## Limitations

- **WALなし**: クラッシュ時にファイルの最後のエントリが破損する可能性
- **単一ファイル**: 大規模データには不向き
- **フルスキャンリカバリ**: 起動時間がファイルサイズに比例
- **インメモリインデックス**: メモリ使用量がキー数に比例

## Use Cases

- プロトタイピング
- 小規模データセット
- シンプルな永続化が必要な場合

## Shutdown

```rust
// グレースフルシャットダウン
// 実行中のCompactionが完了するまで待機
engine.shutdown().await;
```

## Thread Safety

- `writer`: `Arc<Mutex<File>>` で保護
- `index`: `Arc<Mutex<HashMap>>` で保護
- `is_compacting`: `AtomicBool` でCompactionの排他制御
