# LSMTreeEngine Architecture

## Overview

LSMTreeEngineは、Log-Structured Merge Tree (LSM-Tree) を実装したストレージエンジンです。
書き込み性能を重視し、大量のシーケンシャル書き込みに最適化されています。

## Components

### 1. MemTable (`src/engine/lsm_tree/memtable.rs`)

インメモリの書き込みバッファ。BTreeMapを使用してキーをソート順に保持します。

- 設定可能なサイズ閾値 (`max_size_bytes`)
- 閾値を超えるとSSTableにフラッシュ
- 削除はtombstone（None値）として記録

### 2. Write-Ahead Log (WAL) (`src/engine/lsm_tree/wal.rs`)

クラッシュリカバリのための先行書き込みログ。

**特徴:**
- Group Commit対応でバッチ書き込みを最適化
- zstd圧縮によるストレージ効率化
- 設定可能なアーカイブポリシー（保持期間、最大サイズ）

**フォーマット:**
```
Header: [magic: "OREKVSWAL"][version: u32]
Block:  [flags: u8][uncompressed_size: u32][data_size: u32][compressed_data][crc32: u32]
```

### 3. SSTable (`src/engine/lsm_tree/sstable/`)

ソート済みのイミュータブルなディスク上データ構造。

**モジュール構成:**
- `reader.rs` - SSTable読み取り、バイナリサーチによるキー検索
- `writer.rs` - SSTable作成、ブロック圧縮
- `levels.rs` - レベル管理、SstableLevel trait

**ファイルフォーマット (V11):**
```
[Header: magic + version]
[Data Blocks: zstd compressed]
[Index: prefix-compressed with checksum, block-level max_expire_at]
[Bloom Filter: [len: u64][data]]
[Key Range: [min_key_len: u32][min_key][max_key_len: u32][max_key][entry_count: u64]]
[Footer: index_offset + bloom_offset + keyrange_offset + magic]
```

**最適化:**
- ブロック単位のzstd圧縮
- Bloom filterによる存在チェック高速化
- プレフィックス圧縮インデックス
- Key rangeによる範囲フィルタリング

### 4. Compaction (`src/engine/lsm_tree/compaction.rs`)

バックグラウンドでSSTableをマージし、読み取り性能を維持します。

**Leveled Compaction:**
- **Level 0**: MemTableからフラッシュされたSSTable（キー範囲が重複可能）
- **Level 1+**: キー範囲が重複しないソート済みSSTable

**Compaction トリガー:**
- Level 0のファイル数が閾値を超えた場合
- 各レベルの合計サイズが閾値を超えた場合

### 5. Bloom Filter (`src/engine/lsm_tree/bloom.rs`)

存在しないキーの読み取りを高速にスキップするための確率的データ構造。

- 設定可能な偽陽性率 (デフォルト: 1%)
- 各SSTableに埋め込み

## Data Flow

### 書き込み (Set/Delete)

```
1. WALに書き込み（耐久性保証）
2. MemTableに追加
3. MemTableが閾値超過 → SSTableにフラッシュ
4. バックグラウンドでCompaction実行
```

### 読み取り (Get)

```
1. MemTableを検索
2. Level 0のSSTableを新しい順に検索
3. Level 1以降のSSTableをバイナリサーチ
   - Bloom filterで存在チェック
   - Key rangeでスキップ判定
4. 最初に見つかった値を返す（tombstoneなら削除済み）
```

## Configuration

LSMTreeEngineはコンストラクタ関数で設定を渡します:

```rust
// 基本的な使用法
LsmTreeEngine::new(
    data_dir: String,                    // データディレクトリ
    memtable_capacity_bytes: u64,        // MemTableサイズ (例: 4 * 1024 * 1024 = 4MB)
    compaction_trigger_file_count: usize, // Compactionトリガーファイル数
)

// WAL設定付き
LsmTreeEngine::new_with_config(
    data_dir: String,
    memtable_capacity_bytes: u64,
    compaction_trigger_file_count: usize,
    wal_batch_interval_micros: u64,      // WALバッチ間隔 (例: 100 = 100us)
    wal_archive_config: WalArchiveConfig {
        retention_secs: Option<u64>,      // 保持期間 (例: Some(86400) = 24時間)
        max_size_bytes: Option<u64>,      // 最大サイズ (例: Some(1073741824) = 1GB)
    },
)
```

## Replication

LSMTreeEngineはリーダー・フォロワー型のレプリケーションをサポートします。

**コンポーネント:**
- `ReplicationService` - gRPCサービス実装
- WALストリーミング - リアルタイム同期
- Snapshotトランスファー - フォロワーが大幅に遅れた場合のフルリカバリ

**プロトコル:**
1. フォロワーがWALストリームを購読
2. リーダーが新しいWALエントリをプッシュ
3. WALギャップ検出時はSnapshotを転送
