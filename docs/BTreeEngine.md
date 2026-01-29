# BTreeEngine Architecture

## Overview

BTreeEngineは、ディスクベースのB-Tree実装です。
読み取り性能に優れ、範囲クエリやランダムアクセスに最適化されています。

## Components

### 1. Page Manager (`src/engine/btree/page_manager.rs`)

4KBの固定サイズページを管理します。

- ページの読み書き
- メタページの管理
- ファイルI/O

### 2. Buffer Pool (`src/engine/btree/buffer_pool.rs`)

ページキャッシュとダーティページの管理を行います。

**特徴:**
- LRUベースのページ置換
- Write-behind（遅延書き込み）
- バックグラウンドフラッシュ

**設定:**
```rust
BufferPoolConfig {
    max_pages: 1024,           // キャッシュするページ数
    flush_interval_ms: 100,    // バックグラウンドフラッシュ間隔
    max_dirty_pages: 1000,     // 最大ダーティページ数
}
```

### 3. Node (`src/engine/btree/node.rs`)

B-Treeのノード実装。

**LeafNode:**
- キー・バリューペアを格納
- 隣接リーフへのポインタ（prev_leaf, next_leaf）
- 範囲スキャン用のリンク構造

**InternalNode:**
- キーと子ページIDを格納
- 子ノードへのルーティング

### 4. WAL (`src/engine/btree/wal.rs`)

クラッシュリカバリのためのWrite-Ahead Log。

**レコードタイプ:**
- `Insert` - キー・バリューの挿入
- `Delete` - キーの削除
- `Checkpoint` - チェックポイント
- `BatchBegin/BatchEnd` - バッチ操作の境界

**Group Commit:**
- 複数の書き込みをバッチ化
- 設定可能なバッチ間隔（デフォルト: 100us）

### 5. Freelist (`src/engine/btree/freelist.rs`)

削除されたページの再利用を管理します。

## Data Flow

### 書き込み (Set)

```
1. WALにInsertレコードを書き込み
2. ルートからリーフまでトラバース
3. リーフノードにエントリを挿入
4. ノードがオーバーフロー → スプリット
5. スプリットが伝播 → 必要に応じてルート分割
6. Buffer Poolにダーティページをマーク
```

### 読み取り (Get)

```
1. ルートノードから開始
2. Internal Nodeでキー比較し子ノードへ
3. LeafNodeでキーを検索
4. 見つかればバリューを返す
```

### スプリット

```
1. ノードがmax_keysを超過
2. 中央のキーで分割
3. 左半分は元のノードに、右半分は新ノードに
4. 親ノードに分割キーを挿入
5. 親もオーバーフローなら再帰的にスプリット
```

## Configuration

```rust
BTreeConfig {
    buffer_pool_pages: 1024,           // バッファプールサイズ
    enable_wal: true,                  // WAL有効化
    wal_batch_interval_micros: 100,    // WALバッチ間隔
    background_flush_interval_ms: 100, // バックグラウンドフラッシュ間隔
    max_dirty_pages: 1000,             // 最大ダーティページ数
}
```

## Recovery

起動時にWALをスキャンし、未永続化のレコードをリプレイします。

**LSN-based Recovery:**
1. メタページから`last_wal_seq`を読み取り
2. WALから`seq > last_wal_seq`のレコードを抽出
3. Insert/Deleteレコードをリプレイ
4. フラッシュして永続化
5. チェックポイント後にWALを切り詰め

## Statistics

Buffer Poolの統計情報を取得できます:

```rust
BufferPoolStats {
    hits: u64,              // キャッシュヒット数
    misses: u64,            // キャッシュミス数
    evictions: u64,         // ページ追い出し数
    flushes: u64,           // フラッシュ回数
    background_flushes: u64, // バックグラウンドフラッシュ回数
}
```

## Batch Operations

`batch_set`と`batch_delete`でバッチ操作をサポート:

```rust
// WALでバッチ境界を記録
wal.begin_batch()?;
for (key, value) in items {
    engine.set_internal(key, value, true)?;
}
wal.end_batch()?;
```
