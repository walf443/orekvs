# Orelsm 性能レポート

## 実行環境
- **OS**: Darwin (Mac)
- **ビルド**: Release (`cargo build --release`)
- **日付**: 2026-01-26 (BTree Engine 導入)

## ベンチマーク
- **ワークロード**: 10,000 Random Set / 10,000 Random Get
- **並行性**: 10 スレッド
- **データサイズ**: 小さなキー (1..100の数値文字列) と値 (約20文字)。衝突率高め。

## 結果

### 1. スループット (Requests per Second)

| エンジン | Set (req/sec) | Get (req/sec) | 備考 |
| :--- | :---: | :---: | :--- |
| **Memory** | 27,020 | 26,726 | ベースライン (メモリ上のHashMap) |
| **Log** | 221* | 25,388 | *永続性のために `sync_all()` を使用 |
| **LSM-tree** | 1,244** | 26,468*** | **Leveled Compaction + Index Cache |
| **BTree** | 24,240 | 24,930 | ディスクベースB-Tree (WAL + BufferPool) |

### 2. レイテンシ (リクエストあたりの平均)

| エンジン | Set (平均) | Get (平均) |
| :--- | :---: | :---: |
| **Memory** | 0.34 ms | 0.35 ms |
| **Log** | 45.18 ms* | 0.37 ms |
| **LSM-tree** | 8.01 ms** | 0.35 ms*** |
| **BTree** | 0.41 ms | 0.40 ms |

***LSM-tree の Get が Memory とほぼ同等** - Leveled Compaction と Index Block Cache の効果で、SSTable の検索効率が向上しています。

**BTree の特徴**:
- **Set**: Memory とほぼ同等の性能 (24,240 vs 27,020 req/sec) - インプレース更新と BufferPool の効果
- **Get**: Memory に近い性能 (24,930 vs 26,726 req/sec) - B-Tree の O(log N) 検索
- **LSM-tree との比較**: Set が約 19.5x 高速、Get は同等レベル

## 今回の最適化内容

### 1. Index Block Cache (OnceLock)

SSTable のインデックスを初回アクセス時にキャッシュし、2回目以降の解凍をスキップします。

| 指標 | Cold (初回) | Warm (キャッシュ後) | 高速化 |
|------|-------------|---------------------|--------|
| スループット | 56,000/sec | 2,000,000,000/sec | **36,000x** |
| レイテンシ | 0.018 ms | ~0 ms | 実質ゼロ |

**実装**:
```rust
pub struct MappedSSTable {
    // ...
    cached_index: OnceLock<Vec<(String, u64)>>,
}

pub fn read_index(&self) -> Result<&[(String, u64)], Status> {
    if let Some(cached) = self.cached_index.get() {
        return Ok(cached.as_slice());  // 高速パス
    }
    // 初回のみ解凍
    let index = self.decompress_index()?;
    let _ = self.cached_index.set(index);
    Ok(self.cached_index.get().unwrap().as_slice())
}
```

**メモリ使用量**: ~6KB/SSTable (100 SSTable で約 600KB)

### 2. SSTable v8: entry_count

各 SSTable のエントリ数を Key Range セクションに格納しました。

```
Key Range Section (v8):
[min_key_len: u32][min_key][max_key_len: u32][max_key][entry_count: u64]
```

**用途**:
- 概算 COUNT クエリ: O(1) で総エントリ数を推定
- 近似 Rank 計算: O(SSTables数) で順位を推定
- 統計・モニタリング機能

**Manifest にも反映**:
```json
{
  "entries": [{
    "filename": "sst_00001.data",
    "level": 1,
    "entry_count": 5000
  }]
}
```

### 3. Leveled Compaction

従来の Tiered Compaction から Leveled Compaction に変更しました。

| 特性 | Tiered Compaction | Leveled Compaction |
|------|-------------------|-------------------|
| L0 | 重複あり | 重複あり (変更なし) |
| L1+ | - | 重複なし (キー範囲で分割) |
| 読み取り | 全SSTable検索 | バイナリサーチで1ファイル特定 |
| 書き込み増幅 | 低 | 中〜高 |

**Leveled Compaction の仕組み**:
- **L0 → L1**: L0 ファイルが4つ以上になるとL1にマージ
- **Ln → Ln+1**: レベルサイズが閾値を超えると次のレベルにマージ
- **レベルサイズ**: L1=64MB, L2=640MB, L3=6.4GB... (10倍ずつ増加)

### 4. Manifest ファイル

SSTable のレベル割り当てを永続化するマニフェストファイルを導入しました。

```json
{
  "version": 1,
  "entries": [
    {"filename": "sst_00004_00000.data", "level": 1, "min_key_hex": "31", "max_key_hex": "39", "size_bytes": 1234, "entry_count": 500}
  ]
}
```

- **Atomic Update**: write-rename パターンで安全に更新
- **Recovery**: 再起動時にレベル構造を復元

### 5. Block Cache + mmap

SSTable の読み取りをメモリマップと LRU キャッシュで高速化しました。

| 方式 | 検索速度 | 高速化 |
|------|----------|--------|
| ファイルI/O直接 | 39,677/sec | 1.0x |
| mmap + Block Cache | 105,060/sec | **2.65x** |

### 6. BTree Engine (新規)

ディスクベースの B-Tree ストレージエンジンを新規実装しました。

**コンポーネント**:
- **Page**: 4KB 固定サイズページ (ヘッダー 18バイト + データ)
- **Node**: LeafNode (キー・値) と InternalNode (キー・子ポインタ)
- **BufferPool**: LRU ページキャッシュ (デフォルト 64MB)
- **WAL**: クラッシュリカバリ用 Write-Ahead Logging
- **Freelist**: 削除ページの再利用

**ファイル構造**:
```
src/engine/btree/
├── mod.rs           # BTreeEngine 本体
├── page.rs          # ページ構造・シリアライズ
├── node.rs          # ノード操作・分割
├── buffer_pool.rs   # LRU キャッシュ
├── page_manager.rs  # ディスク I/O
├── freelist.rs      # フリーリスト
├── wal.rs           # WAL 実装
└── tests.rs         # テストスイート
```

**LSM-tree との比較**:
| 項目 | BTree | LSM-tree |
|------|-------|----------|
| 書き込み | O(log N) ディスクI/O | O(1) メモリ + 非同期flush |
| 読み取り | O(log N) 単一パス | O(log N) × レベル数 |
| 空間効率 | インプレース更新 | compaction必要 |
| 書き込み増幅 | 高い（ページ単位更新） | 低い〜中 |
| 読み取り増幅 | 低い（1パス） | 高い（複数SSTable検索） |

### 7. SkipList MemTable (`crossbeam-skiplist`)

従来の `BTreeMap + Mutex` から `crossbeam-skiplist::SkipMap` に変更しました。

| 特性 | BTreeMap + Mutex | SkipList (lock-free) |
|------|------------------|----------------------|
| 読み込み | ロック必要 | ロック不要 |
| 書き込み | ロック必要 | CAS操作のみ |
| 並行読み込み | 直列化 | 完全並列 |

**マイクロベンチマーク結果**:
| 並行度 | BTreeMap+Mutex | SkipList | 高速化 |
|--------|----------------|----------|--------|
| 1 thread x2 | 5.47ms | 2.21ms | 2.48x |
| 2 threads x2 | 18.65ms | 3.23ms | 5.78x |
| 4 threads x2 | 41.01ms | 5.59ms | 7.33x |
| 8 threads x2 | 76.75ms | 12.65ms | 6.07x |

## 過去の比較 (LSM-tree)

| バージョン | Set (req/sec) | Get (req/sec) | 最適化内容 |
| :--- | :---: | :---: | :--- |
| 初期実装 | 245 | - | 基礎的なLSM-tree |
| グループコミット | 1,151 | - | 複数の書き込みを1回の `sync_all` で実行 |
| パイプラインWAL | 1,163 | 24,601 | I/O待機中のMemTable更新と読み取りの並行化 |
| SkipList + BinarySearch | 1,247 | 26,836 | ロックフリーMemTable + ブロック内バイナリサーチ |
| WAL圧縮 v2 | 1,240 | 25,634 | ブロック単位のオプショナル圧縮 |
| Leveled Compaction | 1,235 | 26,167 | レベル分割 + Manifest永続化 |
| **Index Cache + v8** | **1,244** | **26,468** | Index Block Cache + entry_count |

### WAL圧縮 v2 フォーマット

WAL v2 では、512バイト以上のブロックに対してzstd圧縮を適用します。

| 特性 | WAL v1 | WAL v2 |
|------|--------|--------|
| 圧縮 | なし | 512バイト以上で自動圧縮 |
| フォーマット | エントリ単位 | ブロック単位 |
| 後方互換性 | - | v1読み取りサポート |

**圧縮効果 (ランダム英数字データ):**
| 操作 | 値サイズ | 圧縮有無 | 書き込み性能 | ストレージ削減 |
|------|----------|----------|--------------|----------------|
| Set | 20バイト | なし | 1,240 req/sec | 0% |
| Set | 1KB | あり | 1,224 req/sec | ~24% |
| BatchSet | 20バイト | あり | 151,067 items/sec | ~64% |

## 考察

1. **読み取り性能**: LSM-tree が Memory エンジンとほぼ同等の性能を維持。Index Block Cache により同じ SSTable への繰り返しアクセスが 36,000x 高速化されています。

2. **書き込み性能**: 小規模ベンチマークでは従来とほぼ同じスループット。Leveled Compaction の書き込み増幅は大規模データで顕在化しますが、読み取り性能の向上とのトレードオフです。

3. **BTree vs LSM-tree**:
   - **書き込み**: BTree が 19.5x 高速 (24,240 vs 1,244 req/sec)。LSM-tree は `sync_all()` による同期 I/O がボトルネック。
   - **読み取り**: ほぼ同等 (24,930 vs 26,468 req/sec)。
   - **理由**: BTree は BufferPool でページをキャッシュし、WAL 書き込みのみ同期。LSM-tree はグループコミットで複数書き込みをバッチ化するが、小規模ベンチマークでは効果が限定的。

4. **スケーラビリティ**: Leveled Compaction の真価は大規模データセットで発揮されます。
   - **Tiered**: 読み取り時に全 SSTable をスキャン → O(n)
   - **Leveled**: L1+ でバイナリサーチ → O(log n)

5. **永続性と性能のトレードオフ**:
    | エンジン | 永続性 | 書き込み性能 | 読み取り性能 | 並行性 |
    | :--- | :--- | :--- | :--- | :--- |
    | Memory | なし | 最高 | 高 | 高 |
    | Log | 完全 (逐次) | 最低 | 高 | 低 |
    | LSM-tree | 完全 (グループ) | 低 | **最高** | **最高** |
    | BTree | 完全 (WAL) | **高** | 高 | 中 |

## 結論

**BTree Engine**: 新規実装したディスクベースB-Treeエンジンは、永続性を維持しながら Memory エンジンに近い性能を達成しました。特に書き込み性能で LSM-tree を大幅に上回っています (19.5x)。BufferPool による効率的なページキャッシュと、WAL による軽量な永続化が効果を発揮しています。

**LSM-tree**: Index Block Cache (OnceLock) の導入により、同じ SSTable への繰り返しアクセスが劇的に高速化されました。gRPC ベンチマークではオーバーヘッドが支配的なため顕著な差は見られませんが、マイクロベンチマークでは 36,000x の高速化を確認しています。

SSTable v8 で追加した entry_count により、将来的な統計機能（概算 COUNT、近似 Rank）の基盤が整いました。

**使い分けの指針**:
| ユースケース | 推奨エンジン | 理由 |
|-------------|-------------|------|
| 読み書きバランス | BTree | 両方で高性能 |
| 読み取り中心 | LSM-tree / BTree | 同等性能 |
| 書き込み中心 (小〜中規模) | BTree | 19.5x 高速 |
| 書き込み中心 (大規模) | LSM-tree | Compaction による空間効率 |
| 開発・テスト | Memory | 最速、永続性不要 |
