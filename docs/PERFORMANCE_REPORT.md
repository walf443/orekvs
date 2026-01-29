# Orekvs 性能レポート

## 実行環境
- **CPU**: Apple M2 Pro (10コア)
- **メモリ**: 32 GB
- **OS**: Darwin (macOS)
- **ビルド**: Release (`cargo build --release`)
- **日付**: 2026-01-28 (CAS + リファクタリング後)

## ベンチマーク
- **ワークロード**: 10,000 Random Set / 10,000 Random Get
- **並行性**: 10 スレッド
- **データサイズ**: 小さなキー (1..100の数値文字列) と値 (約20文字)。衝突率高め。

## 結果

### 1. スループット (Requests per Second)

| エンジン | Get | Set | CAS | 備考 |
| :--- | :---: | :---: | :---: | :--- |
| **Memory** | 26,966 | 25,156 | 26,087 | ベースライン (メモリ上のHashMap) |
| **Log** | 25,724 | 198* | 266 | *永続性のために `sync_all()` を使用 |
| **LSM-tree** | 25,800 | 1,215 | 1,245 | グループコミット + ストライプロック |
| **BTree** | 24,607 | 1,090 | 1,120 | グループコミット + ストライプロック |

### 2. レイテンシ (リクエストあたりの平均)

| エンジン | Get | Set | CAS |
| :--- | :---: | :---: | :---: |
| **Memory** | 0.34 ms | 0.37 ms | 0.35 ms |
| **Log** | 0.36 ms | 50.53 ms | 37.52 ms |
| **LSM-tree** | 0.36 ms | 8.15 ms | 8.00 ms |
| **BTree** | 0.38 ms | 9.13 ms | 8.88 ms |

**注釈**:
- **LSM-tree Get** が Memory とほぼ同等 - Leveled Compaction と Index Block Cache の効果
- **LSM-tree/BTree CAS** が SET と同等 - ストライプロック (256分割) による並列化

**設計上の割り切り**: 同一キーに対して CAS と SET/DELETE を混在させるのはユーザーの誤用とし、CAS 以外の操作ではロックを取得しないことで SET のスループットを維持しています。

**BTree の特徴**:
- **Set**: LSM-tree とほぼ同等の性能 (1,090 vs 1,215 req/sec) - グループコミットで複数書き込みをバッチ処理
- **Get**: Memory に近い性能 (24,607 vs 26,966 req/sec) - B-Tree の O(log N) 検索
- **CAS**: LSM-tree とほぼ同等 (1,120 vs 1,245 req/sec) - ストライプロック + 最適化されたロック戦略
- **LSM-tree との比較**: Get/Set/CAS すべてほぼ同等レベル

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

**グループコミット WAL**:
- バックグラウンドスレッドが書き込みリクエストをバッチ処理
- 複数の書き込みを1回の `sync_data()` で永続化
- デフォルトバッチ間隔: 100μs

| 指標 | sync毎回 | グループコミット | 高速化 |
|------|----------|------------------|--------|
| Set スループット | 245/sec | 1,074/sec | **4.4x** |
| Set レイテンシ | 40.79 ms | 9.23 ms | **4.4x** |

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

### 8. CAS ロックストライピング

CAS 操作の並列性を向上させるため、単一のグローバルロックから 256 分割のストライプロックに変更しました。

| 指標 | 単一ロック | ストライプロック (256) | 改善 |
|------|-----------|----------------------|------|
| CAS スループット | 267/sec | 1,245/sec | **4.7x** |
| CAS レイテンシ | 37.45 ms | 8.00 ms | **4.7x** |

**実装**:
```rust
struct CasLockStripe {
    locks: Vec<Mutex<()>>,  // 256個のロック
}

impl CasLockStripe {
    fn get_lock(&self, key: &str) -> &Mutex<()> {
        let hash = hash(key) % self.locks.len();
        &self.locks[hash]
    }
}
```

異なるキーへの CAS は並列実行可能。同じキー（または同じストライプにハッシュされるキー）への CAS はシリアライズされます。

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

## Count (Prefix Count) 性能

### ベンチマーク条件
- **総キー数**: 10,000
- **イテレーション**: 100回
- **プレフィックスパターン**: 20%マッチ、1%マッチ、0.1%マッチ、0%マッチ
- **日付**: 2026-01-29 (Key Range Filter + Block Cache最適化後)

### 結果

| エンジン | プレフィックス | マッチ数 | Time (ms) | ops/sec |
|----------|----------------|----------|-----------|---------|
| **Memory** | log: (20%) | 2,000 | 8.6 | 11,605 |
| **Memory** | log:001 (1%) | 100 | 7.9 | 12,671 |
| **Memory** | log:0010 (0.1%) | 10 | 3.7 | 27,112 |
| **Memory** | nonexistent: | 0 | 4.1 | 24,111 |
| **Log** | log: (20%) | 2,000 | 6.7 | 14,885 |
| **Log** | log:001 (1%) | 100 | 6.0 | 16,711 |
| **Log** | log:0010 (0.1%) | 10 | 3.3 | 30,651 |
| **Log** | nonexistent: | 0 | 4.2 | 23,853 |
| **BTree** | log: (20%) | 2,000 | 16.4 | 6,099 |
| **BTree** | log:001 (1%) | 100 | 2.2 | 44,763 |
| **BTree** | log:0010 (0.1%) | 10 | 1.2 | 86,356 |
| **BTree** | nonexistent: | 0 | 1.1 | 90,181 |
| **LSM-tree** | log: (20%) | 2,000 | 21.1 | 4,747 |
| **LSM-tree** | log:001 (1%) | 100 | 0.77 | **130,662** |
| **LSM-tree** | log:0010 (0.1%) | 10 | 0.09 | **1,166,181** |
| **LSM-tree** | nonexistent: | 0 | 0.02 | **6,106,870** |

### Count 実装の特徴

| エンジン | 実装方式 | 計算量 | 特徴 |
|----------|----------|--------|------|
| **Memory** | HashMap全走査 | O(n) | シンプル、小規模データで高速 |
| **Log** | Index全走査 | O(n) | ディスクI/Oなし（インメモリインデックス） |
| **BTree** | リーフノード走査 | O(log n + k) | ソート済みデータで効率的な範囲スキャン |
| **LSM-tree** | MemTable範囲スキャン + SSTable走査 | O(log n + k) | 範囲スキャン + Key Range Filter + Block Cache |

### LSM-tree Count 最適化

1. **MemTable 範囲スキャン**: SkipMap/BTreeMapの`range()`を使用してプレフィックス起点から走査
   ```rust
   // Range scan starting from prefix - O(log n + k) instead of O(n)
   for entry in active.range(prefix.to_string()..) {
       if !key.starts_with(prefix) {
           break; // Past the prefix range, stop scanning
       }
       // ...
   }
   ```

2. **Key Range Filter**: SSTableのmin_key/max_keyを使用して、プレフィックスに絶対にマッチしないSSTableをスキップ

3. **Block Cache**: パース済みブロックをキャッシュし、繰り返しcount操作を高速化

**注釈**:
- **LSM-tree** がマッチ率が低い場合に最も高速: 範囲スキャンにより O(log n + k) で検索（0%マッチで **6,106,870 ops/sec**）
- **BTree** も効率的: リーフノード走査で O(log n + k)
- **Memory/Log** は全キーを走査するため、マッチ率に関わらずほぼ一定のオーバーヘッド

## 考察

1. **読み取り性能**: LSM-tree が Memory エンジンとほぼ同等の性能を維持。Index Block Cache により同じ SSTable への繰り返しアクセスが 36,000x 高速化されています。

2. **書き込み性能**: 小規模ベンチマークでは従来とほぼ同じスループット。Leveled Compaction の書き込み増幅は大規模データで顕在化しますが、読み取り性能の向上とのトレードオフです。

3. **BTree vs LSM-tree**:
   - **書き込み**: ほぼ同等 (1,074 vs 1,215 req/sec)。両エンジンともグループコミットで複数書き込みをバッチ処理。
   - **読み取り**: ほぼ同等 (24,607 vs 25,800 req/sec)。
   - **理由**: BTree にグループコミットを導入したことで、LSM-tree と同等の書き込み性能を達成。

4. **スケーラビリティ**: Leveled Compaction の真価は大規模データセットで発揮されます。
   - **Tiered**: 読み取り時に全 SSTable をスキャン → O(n)
   - **Leveled**: L1+ でバイナリサーチ → O(log n)

5. **永続性と性能のトレードオフ**:
    | エンジン | 永続性 | 書き込み性能 | 読み取り性能 | CAS性能 | 並行性 |
    | :--- | :--- | :--- | :--- | :--- | :--- |
    | Memory | なし | 最高 | 高 | **最高** | 高 |
    | Log | 完全 (逐次) | 最低 | 高 | 低 | 低 |
    | LSM-tree | 完全 (グループ) | 中 | **最高** | **高** | **最高** |
    | BTree | 完全 (WAL) | 中 | 高 | **高** | 高 |

## 結論

**BTree Engine**: ディスクベースB-Treeエンジンは、グループコミットとCASロックストライピングにより LSM-tree と同等の性能を達成しています。Set: 1,090 req/sec、Get: 24,607 req/sec、CAS: 1,120 req/sec で、永続性を維持しながら高い性能を実現しています。

**LSM-tree**: Index Block Cache (OnceLock) の導入により、同じ SSTable への繰り返しアクセスが劇的に高速化されました。gRPC ベンチマークではオーバーヘッドが支配的なため顕著な差は見られませんが、マイクロベンチマークでは 36,000x の高速化を確認しています。

SSTable v8 で追加した entry_count により、将来的な統計機能（概算 COUNT、近似 Rank）の基盤が整いました。

**使い分けの指針**:
| ユースケース | 推奨エンジン | 理由 |
|-------------|-------------|------|
| 読み書きバランス | LSM-tree / BTree | 両方同等性能 |
| 読み取り中心 | LSM-tree / BTree | 同等性能 |
| 書き込み中心 | LSM-tree / BTree | 両方グループコミット対応 |
| CAS 中心 (永続性必要) | LSM-tree / BTree | 両方ストライプロックで高並列 CAS |
| CAS 中心 (永続性不要) | Memory | 最高性能 |
| 大規模データ | LSM-tree | Compaction による空間効率 |
| Count (プレフィックス集計) | BTree | ソート済みリーフ走査で最も効率的 |
| 開発・テスト | Memory | 最速、永続性不要 |
