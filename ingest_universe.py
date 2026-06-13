#!/usr/bin/env python3
# =============================================================================
# INGEST UNIVERSE — Step 1 dari Pipeline Paralel
# =============================================================================
# Tugas:
#   1. Login ke Stockbit API (satu kali saja)
#   2. Cek kondisi market IHSG
#   3. Bangun universe saham lengkap dari semua sumber
#   4. Pre-fetch broker signal & liquidity untuk tiap saham
#   5. Pre-download semua cache OHLCV (daily, weekly, monthly, 1h)
#   6. Simpan ke universe_data.json → di-share ke semua pipeline paralel
#
# Dipanggil oleh GitHub Actions job "ingest" sebelum 7 pipeline berjalan.
# =============================================================================

import json
import logging
import sys
import time
import gc
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict, Any

# Import semua fungsi dari modul inti
from screener_common import (
    CONFIG, NumpyEncoder, log,
    get_valid_token, invalidate_token_cache,
    get_ihsg_context,
    get_universe_mode_a, get_universe_mode_b,
    get_universe_top_gainer, get_universe_top_loser,
    get_universe_screener, get_universe_top_value,
    get_universe_guru_screener,
    check_liquidity_quality,
    get_broker_signal,
    get_daily_data, get_weekly_data, get_monthly_data, get_1h_data,
    _save_yf_cache, _get_yf_cache_path, _load_yf_cache,
    _weekly_cache, _monthly_cache,
    _normalize_yf_df,
    is_market_day, get_session_label,
)

OUTPUT_FILE = "universe_data.json"


def build_full_universe(token: Optional[str], mode: str):
    """
    Bangun universe lengkap dari semua sumber Stockbit.
    Sama persis dengan logika di run_screener() asli.
    """
    if mode == "FULL_STOCKBIT":
        universe = get_universe_mode_a(token)
    else:
        universe = get_universe_mode_b()
    return universe


def _enrich_one_stock(args: Tuple) -> Dict:
    """
    Worker function untuk ThreadPoolExecutor.
    Cek liquidity + broker signal untuk 1 saham.

    Thread-safe karena:
    - Setiap thread dapat stock_mm dict yang berbeda (tidak ada shared state)
    - token hanya DIBACA, tidak pernah ditulis
    - sb_get() menggunakan requests.get() yang thread-safe
    - Semua exception ditangkap internal → tidak pernah crash thread lain
    """
    stock_mm, token, mode = args
    ticker = stock_mm.get("ticker", "")

    try:
        if mode == "FULL_STOCKBIT":
            # --- Liquidity check (1 request ke Stockbit) ---
            liq_ok, hist_data = check_liquidity_quality(ticker, token)
            if not liq_ok or hist_data is None:
                log.debug(f"  SKIP {ticker}: liquidity gagal")
                stock_mm["_liq_ok"]        = False
                stock_mm["_hist_data"]     = None
                stock_mm["_broker_signal"] = "Neutral"
                stock_mm["_broker_score"]  = 5
                return stock_mm

            # --- Broker signal (1 request ke Stockbit, hanya jika lolos liquidity) ---
            broker_signal, broker_score = get_broker_signal(ticker, token)

        else:
            # YAHOO_ONLY mode — tidak ada request ke Stockbit sama sekali
            df_d = get_daily_data(ticker)
            if df_d is None or len(df_d) < 60:
                stock_mm["_liq_ok"]        = False
                stock_mm["_hist_data"]     = None
                stock_mm["_broker_signal"] = "Neutral"
                stock_mm["_broker_score"]  = 5
                return stock_mm

            last = df_d.iloc[-1]
            foreign_history = []
            for j in range(2, min(27, len(df_d) + 1)):
                row = df_d.iloc[-j]
                foreign_history.append({
                    "date":        str(row.get("date", "")),
                    "close":       float(row["close"]),
                    "high":        float(row["high"]),
                    "low":         float(row["low"]),
                    "open":        float(row["open"]),
                    "volume":      float(row["volume"]),
                    "value":       float(row["close"]) * float(row["volume"]),
                    "net_foreign": 0,
                })
            hist_data = {
                "pdh":          float(last["high"]),
                "pdl":          float(last["low"]),
                "pdc":          float(last["close"]),
                "pd_typical":   (float(last["high"]) + float(last["low"]) + float(last["close"])) / 3,
                "avg_range_pct": 0,
                "avg_freq":      0,
                "foreign_history": foreign_history,
            }
            liq_ok        = True
            broker_signal = "Neutral"
            broker_score  = 5

        stock_mm["_liq_ok"]        = liq_ok
        stock_mm["_hist_data"]     = hist_data
        stock_mm["_broker_signal"] = broker_signal
        stock_mm["_broker_score"]  = broker_score
        return stock_mm

    except Exception as e:
        # Tangkap semua exception agar 1 saham error tidak menghentikan thread lain
        log.debug(f"  _enrich_one_stock {ticker} exception: {e}")
        stock_mm["_liq_ok"]        = False
        stock_mm["_hist_data"]     = None
        stock_mm["_broker_signal"] = "Neutral"
        stock_mm["_broker_score"]  = 5
        return stock_mm


def prefetch_broker_and_liquidity(universe, token, mode):
    """
    Pre-fetch broker signal + liquidity untuk semua saham secara paralel.

    Menggunakan 3 thread bersamaan → request ke Stockbit 3x lebih cepat.

    CATATAN PENTING:
    - Token TIDAK di-refresh di dalam loop.
      Token Stockbit valid 24 jam. Seluruh proses selesai < 10 menit.
      Tidak ada risiko token expired di tengah jalan.
    - executor.map() menjaga urutan output = urutan input universe.
    - Setiap exception ditangkap di dalam worker → tidak ada crash.
    """
    MAX_WORKERS = 4
    total = len(universe)

    log.info(f"\n🔍 Pre-fetch liquidity + broker signal...")
    log.info(f"   {total} saham | {MAX_WORKERS} thread paralel | token valid 24 jam")

    # Bungkus argumen sebagai tuple untuk executor.map
    args_list = [(stock_mm, token, mode) for stock_mm in universe]

    # executor.map: jalan paralel, hasil TETAP urut sesuai input
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(_enrich_one_stock, args_list))

    # Hitung statistik
    liq_ok_count   = sum(1 for s in results if s.get("_liq_ok"))
    liq_fail_count = total - liq_ok_count

    log.info(f"✅ Pre-fetch selesai: {liq_ok_count} lolos liquidity, {liq_fail_count} gagal")

    return results



# ---------------------------------------------------------------------------
# KONSTANTA OHLCV DOWNLOADER — diadopsi dari IDX Batch v2 (Fast + Reliable)
# ---------------------------------------------------------------------------
_OHLCV_CHUNK_SIZE    = 100      # ↑ dari 50 → lebih sedikit request ke Yahoo
_OHLCV_SLEEP_MIN     = 0.8      # ↓ dari 2.0 → lebih agresif di kondisi normal
_OHLCV_SLEEP_MAX     = 8.0      # batas atas adaptive sleep (sinyal rate-limit)
_OHLCV_MAX_RETRY     = 2        # retry per chunk jika download gagal
_OHLCV_RETRY_BACKOFF = [5, 10]  # detik tunggu sebelum retry ke-1 dan ke-2
_OHLCV_TIMEOUT       = 45       # timeout per yf.download() request (detik)


def _bulk_extract_ticker(batch_df, yf_ticker: str):
    """
    Ekstrak sub-DataFrame 1 ticker dari hasil yf.download() multi-ticker.
    Kompatibel dengan MultiIndex yfinance >= 0.2.x.
    """
    if batch_df is None or batch_df.empty:
        return None
    cols = batch_df.columns
    if isinstance(cols, pd.MultiIndex):
        lvl0 = cols.get_level_values(0).unique().tolist()
        lvl1 = cols.get_level_values(1).unique().tolist()
        if yf_ticker in lvl0:
            sub = batch_df[yf_ticker].copy()
        elif yf_ticker in lvl1:
            sub = batch_df.xs(yf_ticker, axis=1, level=1).copy()
        else:
            return None
    else:
        sub = batch_df.copy()
    return sub.dropna(how="all") if not sub.empty else None


def _normalize_bulk_df(raw, is_intraday: bool = False) -> "Optional[pd.DataFrame]":
    """
    Standarisasi DataFrame hasil bulk download: flatten, lowercase, rename date.
    Untuk data intraday: konversi timezone ke WIB jika ada.
    """
    if raw is None or (hasattr(raw, "empty") and raw.empty):
        return None
    raw = raw.copy()
    # Flatten MultiIndex dengan benar: setelah _bulk_extract_ticker, kolom bisa
    # sisa MultiIndex dengan level kosong (misal ('open', '')). Ambil level
    # pertama yang non-kosong agar tidak menghasilkan string tuple.
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [
            next((str(lvl).lower() for lvl in c if str(lvl).strip()), str(c[0]).lower())
            for c in raw.columns
        ]
    else:
        raw.columns = [str(c).lower() for c in raw.columns]
    raw = raw.reset_index()
    date_cols = [c for c in raw.columns if "date" in c.lower() or "datetime" in c.lower()]
    if not date_cols:
        return None
    raw.rename(columns={date_cols[0]: "date"}, inplace=True)
    if "close" not in raw.columns:
        return None
    raw = raw.dropna(subset=["close"])
    if "volume" in raw.columns:
        raw = raw[raw["volume"] > 0]
    if raw.empty:
        return None
    # Format tanggal
    if is_intraday:
        dt_col = pd.to_datetime(raw["date"])
        if dt_col.dt.tz is not None:
            dt_col = dt_col.dt.tz_convert("Asia/Jakarta").dt.tz_localize(None)
        else:
            dt_col = dt_col + pd.Timedelta(hours=7)
        raw["date"] = dt_col.dt.strftime("%Y-%m-%d %H:%M:%S")
    return raw.reset_index(drop=True)


def _download_chunk_with_retry(
    chunk: list,
    interval: str,
    is_intraday: bool,
    period,
    today,
    tf_label: str,
    chunk_idx: int,
    total_chunks: int,
) -> "Optional[pd.DataFrame]":
    """
    Download 1 chunk dengan retry otomatis hingga _OHLCV_MAX_RETRY kali.
    Jika semua attempt gagal, return None (chunk dilewati, tidak crash).

    Perbedaan kunci vs versi lama:
    - threads=False  : cegah yfinance buat sub-thread sendiri (clash ThreadPoolExecutor)
    - timeout=45     : koneksi lambat tidak langsung mati
    - repair=False   : skip proses repair yang memperlambat download
    """
    common_kwargs = dict(
        interval          = interval,
        group_by          = "ticker",
        auto_adjust       = True,
        repair            = False,
        progress          = False,
        threads           = False,
        multi_level_index = True,
        timeout           = _OHLCV_TIMEOUT,
    )
    for attempt in range(_OHLCV_MAX_RETRY + 1):
        try:
            if is_intraday:
                # 1h: Yahoo Finance support hingga ~730 hari ke belakang
                # interval lain (5m, 15m, dll): max 60 hari
                days_back = 729 if interval == "1h" else 59
                start_dt  = today - timedelta(days=days_back)
                return yf.download(
                    tickers = chunk,
                    start   = start_dt.strftime("%Y-%m-%d"),
                    end     = today.strftime("%Y-%m-%d"),
                    **common_kwargs,
                )
            else:
                return yf.download(
                    tickers = chunk,
                    period  = period,
                    **common_kwargs,
                )
        except Exception as exc:
            if attempt < _OHLCV_MAX_RETRY:
                wait = _OHLCV_RETRY_BACKOFF[min(attempt, len(_OHLCV_RETRY_BACKOFF) - 1)]
                log.warning(
                    f"  [{tf_label}] Chunk {chunk_idx}/{total_chunks} "
                    f"gagal (attempt {attempt + 1}), retry {wait}s... ({exc})"
                )
                time.sleep(wait)
            else:
                log.warning(
                    f"  [{tf_label}] Chunk {chunk_idx}/{total_chunks} "
                    f"GAGAL permanen setelah {_OHLCV_MAX_RETRY + 1} attempt: {exc}"
                )
    return None


def _fetch_one_timeframe(
    tf_label: str,
    interval: str,
    period: str,
    cache_key: str,
    is_intraday: bool,
    tickers_raw: list,
    yf_to_raw: dict,
) -> dict:
    """
    Worker function untuk 1 timeframe — menangani semua saham secara chunk-by-chunk.

    Adopsi IDX Batch v2:
    - RESUME    : skip ticker yang parquet cache-nya sudah segar
    - RETRY     : tiap chunk dicoba ulang jika gagal (_OHLCV_MAX_RETRY kali)
    - ADAPTIVE SLEEP : naik jika banyak yang gagal (rate-limit), turun jika semua OK

    Dipanggil dari ThreadPoolExecutor — 4 timeframe jalan bersamaan.

    Thread-safety:
    - Parquet write: setiap file unik per ticker+tf → TIDAK ADA 2 thread write ke file sama.
    - _weekly_cache / _monthly_cache: hanya 1 thread per key yang menulis → no race condition.
    - yf.download(threads=False) → thread-safe.
    """
    today = datetime.now(tz=timezone.utc).date()

    # ── RESUME: skip ticker yang parquet cache-nya sudah ada dan segar ───────
    raw_to_yf = {v: k for k, v in yf_to_raw.items()}  # inverse map: raw_ticker → yf_ticker

    pending_raw = [t for t in tickers_raw if _load_yf_cache(t, cache_key) is None]
    skipped     = len(tickers_raw) - len(pending_raw)
    if skipped:
        log.info(f"  [{tf_label}] Resume: {skipped} sudah di-cache, {len(pending_raw)} perlu download")

    # Konversi pending ke format yf ticker
    pending_yf = [raw_to_yf.get(t, f"{t}.JK") for t in pending_raw]

    total_pending = len(pending_yf)
    if total_pending == 0:
        log.info(f"  [{tf_label}] Semua sudah di-cache, skip download")
        return {"tf": tf_label, "cache_key": cache_key, "ok": skipped, "skipped": skipped}

    chunks    = [pending_yf[i:i + _OHLCV_CHUNK_SIZE] for i in range(0, total_pending, _OHLCV_CHUNK_SIZE)]
    n_chunks  = len(chunks)
    tf_ok     = 0
    sleep_cur = _OHLCV_SLEEP_MIN  # adaptive sleep state

    log.info(f"  [{tf_label}] Download {total_pending} saham ({n_chunks} chunk, chunk_size={_OHLCV_CHUNK_SIZE})...")

    for ci, chunk in enumerate(chunks, 1):
        batch = _download_chunk_with_retry(
            chunk, interval, is_intraday, period, today,
            tf_label, ci, n_chunks,
        )

        chunk_ok = 0
        for yf_t in chunk:
            raw_t = yf_to_raw.get(yf_t, yf_t)
            try:
                sub = _bulk_extract_ticker(batch, yf_t)
                df  = _normalize_bulk_df(sub, is_intraday=is_intraday)
                if df is None or len(df) < 10:
                    continue

                if cache_key in ("1d", "1h"):
                    _save_yf_cache(raw_t, cache_key, df)
                elif cache_key == "1wk":
                    _save_yf_cache(raw_t, cache_key, df)
                    _weekly_cache[raw_t] = df
                elif cache_key == "1mo":
                    _save_yf_cache(raw_t, cache_key, df)
                    _monthly_cache[raw_t] = df

                chunk_ok += 1
            except Exception as e:
                log.debug(f"  [{tf_label}] {raw_t}: {e}")

        tf_ok += chunk_ok

        # ── Adaptive sleep ──────────────────────────────────────────────
        fail_rate = 1 - (chunk_ok / max(len(chunk), 1))
        if fail_rate > 0.3:
            sleep_cur = min(sleep_cur * 1.5, _OHLCV_SLEEP_MAX)
        elif fail_rate == 0:
            sleep_cur = max(sleep_cur * 0.9, _OHLCV_SLEEP_MIN)

        # ETA
        log.info(
            f"  [{tf_label}] Chunk {ci:>2}/{n_chunks} "
            f"→ {chunk_ok}/{len(chunk)} OK  sleep={sleep_cur:.1f}s"
        )

        if ci < n_chunks:
            time.sleep(sleep_cur)

    total_ok = tf_ok + skipped
    log.info(f"  [{tf_label}] ✓ Selesai: {tf_ok} baru + {skipped} di-cache = {total_ok}/{len(tickers_raw)}")
    return {"tf": tf_label, "cache_key": cache_key, "ok": total_ok, "skipped": skipped}


def prefetch_ohlcv(universe):
    """
    Bulk download semua data OHLCV ke local parquet cache — IDX Batch v2 logic.

    4 timeframe (1d, 1h, 1wk, 1mo) dijalankan PARALEL menggunakan ThreadPoolExecutor.
    Setiap thread menangani 1 timeframe penuh (chunk-by-chunk sequential di dalamnya).

    Optimasi vs versi lama:
    - CHUNK 50→100     : lebih sedikit request ke Yahoo Finance
    - Adaptive sleep   : 0.8s–8.0s (naik jika rate-limit, turun jika OK)
    - Retry x2         : chunk gagal diulang otomatis (backoff 5s, 10s)
    - threads=False    : cegah yfinance buat sub-thread (clash ThreadPoolExecutor)
    - timeout=45       : koneksi lambat tidak langsung mati
    - Resume           : skip ticker yang parquet cache-nya sudah segar

    Thread-safety:
    - Parquet (1d, 1h): file unik per ticker+tf → tidak ada konflik tulis.
    - _weekly_cache: hanya thread "1wk" yang menulis → no race condition.
    - _monthly_cache: hanya thread "1mo" yang menulis → no race condition.
    """
    tickers_raw = [s.get("ticker", "") for s in universe if s.get("ticker")]
    yf_tickers  = [
        f"{t}.JK" if not t.endswith(".JK") and not t.startswith("^") else t
        for t in tickers_raw
    ]
    yf_to_raw = dict(zip(yf_tickers, tickers_raw))

    total = len(tickers_raw)
    log.info(
        f"\n📥 Bulk OHLCV pre-download — {total} saham"
        f" (chunk={_OHLCV_CHUNK_SIZE}, sleep={_OHLCV_SLEEP_MIN}-{_OHLCV_SLEEP_MAX}s"
        f", retry={_OHLCV_MAX_RETRY}x, 4 timeframe paralel)"
    )

    # Konfigurasi 4 timeframe yang akan jalan bersamaan
    timeframes = [
        # (tf_label, interval, period, cache_key, is_intraday)
        ("1d",  "1d",  CONFIG.get("YF_PERIOD_DAILY", "2y"), "1d",  False),
        ("1h",  "1h",  None,  "1h",  True),   # period diabaikan, intraday pakai start/end
        ("1wk", "1wk", "max",  "1wk", False),
        ("1mo", "1mo", "max",  "1mo", False),
    ]

    results_map = {}  # tf_label → ok count

    # Jalankan 4 timeframe paralel — setiap thread = 1 timeframe penuh
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(
                _fetch_one_timeframe,
                tf_label, interval, period, cache_key, is_intraday,
                tickers_raw, yf_to_raw,
            ): tf_label
            for tf_label, interval, period, cache_key, is_intraday in timeframes
        }

        for future in as_completed(futures):
            tf_label = futures[future]
            try:
                result = future.result()
                results_map[tf_label] = result["ok"]
            except Exception as e:
                log.warning(f"  [{tf_label}] Thread error: {e}")
                results_map[tf_label] = 0

    ok_1d = results_map.get("1d",  0)
    ok_1h = results_map.get("1h",  0)
    ok_wk = results_map.get("1wk", 0)
    ok_mo = results_map.get("1mo", 0)

    log.info(f"✅ Bulk OHLCV selesai — 1d:{ok_1d} 1h:{ok_1h} 1wk:{ok_wk} 1mo:{ok_mo} (dari {total} saham)")



def main():
    start_time = time.time()
    log.info("=" * 70)
    log.info("📡 INGEST UNIVERSE — Parallel Screener Step 1")
    log.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S WIB')}")
    log.info("=" * 70)

    session_label = get_session_label()

    # --- Step 0: Token ---
    log.info("\n[STEP 0] Token management...")
    token, mode = get_valid_token()
    log.info(f"Mode aktif: {mode}")

    # --- Step 1: Market context ---
    log.info("\n[STEP 1] Market context (IHSG)...")
    market_ctx = get_ihsg_context()

    if not market_ctx["market_safe"]:
        log.warning("🛑 Market tidak aman — simpan flag dan keluar")
        output = {
            "market_safe": False,
            "market_ctx": market_ctx,
            "session_label": session_label,
            "mode": mode,
            "universe": [],
            "timestamp": datetime.now().isoformat(),
        }
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)
        log.info(f"✅ {OUTPUT_FILE} disimpan (market tidak aman)")
        return

    # --- Step 2: Universe ---
    log.info("\n[STEP 2] Build universe...")
    universe = build_full_universe(token, mode)
    if not universe:
        log.error("Universe kosong — tidak bisa lanjut")
        sys.exit(1)
    log.info(f"Universe raw: {len(universe)} saham")

    # --- Step 3: Pre-fetch broker + liquidity ---
    log.info("\n[STEP 3] Pre-fetch broker signal & liquidity...")
    enriched_universe = prefetch_broker_and_liquidity(universe, token, mode)
    log.info(f"Universe enriched: {len(enriched_universe)} saham")

    # --- Step 4: Pre-download OHLCV cache ---
    log.info("\n[STEP 4] Pre-download OHLCV cache...")
    prefetch_ohlcv(enriched_universe)

    # --- Step 5: Simpan ke JSON ---
    elapsed = (time.time() - start_time) / 60
    output = {
        "market_safe": True,
        "market_ctx": market_ctx,
        "session_label": session_label,
        "mode": mode,
        "token_available": token is not None,
        "universe": enriched_universe,
        "universe_count": len(enriched_universe),
        "timestamp": datetime.now().isoformat(),
        "elapsed_minutes": round(elapsed, 2),
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)

    log.info("\n" + "=" * 70)
    log.info(f"✅ INGEST SELESAI")
    log.info(f"   Universe: {len(enriched_universe)} saham")
    log.info(f"   Output: {OUTPUT_FILE}")
    log.info(f"   ⏱️  Waktu: {elapsed:.1f} menit")
    log.info("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("⏹️  Dihentikan oleh user")
        sys.exit(0)
    except Exception as e:
        log.error(f"💥 INGEST ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
