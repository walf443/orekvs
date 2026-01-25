# Orelsm 性能レポート

## 実行環境
- **OS**: Darwin (Mac)
- **ビルド**: Release (`cargo build --release`)
- **日付**: 2026-01-25 (Leveled Compaction 導入後)

## ベンチマーク
- **ワークロード**: 10,000 Random Set / 10,000 Random Get
- **並行性**: 10 スレッド
- **データサイズ**: 小さなキー (1..100の数値文字列) と値 (約20文字)。衝突率高め。

## 結果

### 1. スループット (Requests per Second)

| エンジン | Set (req/sec) | Get (req/sec) | 備考 |
| :--- | :---: | :---: | :--- |
| **Memory** | 25,104 | 26,831 | ベースライン (メモリ上のHashMap) |
| **Log** | 205* | 24,350 | *永続性のために `sync_all()` を使用 |
| **LSM-tree** | 1,235** | 26,167*** | **Leveled Compaction + Block Cache |

### 2. レイテンシ (リクエストあたりの平均)

| エンジン | Set (平均) | Get (平均) |
| :--- | :---: | :---: |
| **Memory** | 0.37 ms | 0.35 ms |
| **Log** | 48.66 ms* | 0.38 ms |
| **LSM-tree** | 8.03 ms** | 0.35 ms*** |

***LSM-tree の Get が Memory とほぼ同等** - Leveled Compaction と Block Cache の効果で、SSTable の検索効率が向上しています。

## 今回の最適化内容

### 1. Leveled Compaction

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

### 2. Manifest ファイル

SSTable のレベル割り当てを永続化するマニフェストファイルを導入しました。

```json
{
  "version": 1,
  "entries": [
    {"filename": "sst_00004_00000.data", "level": 1, "min_key_hex": "31", "max_key_hex": "39", "size_bytes": 1234}
  ]
}
```

- **Atomic Update**: write-rename パターンで安全に更新
- **Recovery**: 再起動時にレベル構造を復元

### 3. Block Cache + mmap

SSTable の読み取りをメモリマップと LRU キャッシュで高速化しました。

| 方式 | 検索速度 | 高速化 |
|------|----------|--------|
| ファイルI/O直接 | 31,441/sec | 1.0x |
| mmap + Block Cache | 104,401/sec | **3.32x** |

### 4. SkipList MemTable (`crossbeam-skiplist`)

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
| **Leveled Compaction** | **1,235** | **26,167** | レベル分割 + Manifest永続化 |

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

1. **読み取り性能**: LSM-tree が Memory エンジンとほぼ同等の性能を維持。Leveled Compaction により L1+ では重複のないキー範囲でバイナリサーチが可能になり、大規模データセットでの読み取り効率が向上します。

2. **書き込み性能**: 小規模ベンチマークでは従来とほぼ同じスループット。Leveled Compaction の書き込み増幅は大規模データで顕在化しますが、読み取り性能の向上とのトレードオフです。

3. **スケーラビリティ**: Leveled Compaction の真価は大規模データセットで発揮されます。
   - **Tiered**: 読み取り時に全 SSTable をスキャン → O(n)
   - **Leveled**: L1+ でバイナリサーチ → O(log n)

4. **永続性と性能のトレードオフ**:
    | エンジン | 永続性 | 書き込み性能 | 読み取り性能 | 並行性 |
    | :--- | :--- | :--- | :--- | :--- |
    | Memory | なし | 最高 | 高 | 高 |
    | Log | 完全 (逐次) | 最低 | 高 | 低 |
    | LSM-tree | 完全 (グループ) | 中間 | **最高** | **最高** |

## 結論

Leveled Compaction と Manifest の導入により、LSM-tree エンジンは大規模データセットでのスケーラビリティを獲得しました。小規模ベンチマークでは従来と同等の性能を維持しつつ、データ量が増加しても読み取り性能が劣化しにくい構造になっています。

Block Cache (mmap + LRU) により SSTable 検索が 3.32x 高速化され、SkipList MemTable による並行処理の効率化と組み合わせることで、永続性を維持したまま高い性能を実現しています。
