# 🇮🇩 Screener Trader Indonesia — Modular Parallel v2

Screener saham IHSG dengan **7 pipeline paralel** di GitHub Actions.

## Arsitektur

```
Cron 18:00 WIB (11:00 UTC, Senin–Jumat)
         │
         ▼
┌─────────────────┐
│  ingest         │  Login Stockbit, bangun universe, download OHLCV cache
│  (~10-15 menit) │
└────────┬────────┘
         │ upload: universe_data.json + data_ohlc_cache/
         │
    ┌────┴────────────────────────────────────────────────────┐
    │              7 Job PARALEL (mesin berbeda)              │
    │                                                         │
    │  📊 intraday   📈 ara   📈 bsjp   📉 bpjs              │
    │  🌊 swing      📊 trend  🏦 position                   │
    │            (~5-10 menit, jalan bersamaan)               │
    └────┬────────────────────────────────────────────────────┘
         │ upload: *_results.json
         ▼
┌─────────────────┐
│  merge          │  Gabung semua hasil → commit ke repo
│  (~2 menit)     │
└─────────────────┘

Total waktu: ~20 menit (vs ~60 menit sebelumnya)
```

## Setup GitHub Secrets

Di repo GitHub: **Settings → Secrets and variables → Actions → New repository secret**

| Secret                  | Nilai                                                     |
|-------------------------|-----------------------------------------------------------|
| `STOCKBIT_USERNAME`     | Email/username akun Stockbit                              |
| `STOCKBIT_PASSWORD`     | Password akun Stockbit                                    |
| `STOCKBIT_PLAYER_ID`    | Device player ID Stockbit *(opsional, bisa dikosongkan)*  |

## Output Files

| File                          | Isi                                    |
|-------------------------------|----------------------------------------|
| `latest_screening.json`       | Hasil intraday (max 5 saham)           |
| `combined_screening.json`     | Semua 7 pipeline                       |
| `combined_screening_YYYYMMDD.json` | Salinan harian dengan tanggal     |

## Menjalankan Lokal

```bash
# Install dependencies
pip install -r requirements.txt

# Jalankan ingest dulu
STOCKBIT_USERNAME=xxx STOCKBIT_PASSWORD=yyy STOCKBIT_PLAYER_ID=zzz python ingest_universe.py

# Lalu tiap pipeline (atau semua sekaligus)
python run_intraday.py
python run_ara.py
python run_bsjp.py
python run_bpjs.py
python run_swing.py
python run_trend.py
python run_position.py

# Terakhir merge
python merge_outputs.py
```

## Trigger Manual

Di tab **Actions** → pilih workflow → klik **Run workflow**.

Ada pilihan `force_mode`:
- **(kosong)** — AUTO: gunakan Stockbit jika tersedia
- `YAHOO_ONLY` — Paksa mode Yahoo Finance saja
- `FULL_STOCKBIT` — Paksa mode Stockbit penuh
