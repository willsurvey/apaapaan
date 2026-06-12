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
    _save_yf_cache, _get_yf_cache_path,
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
    MAX_WORKERS = 3
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


def _normalize_bulk_df(raw) -> "Optional[pd.DataFrame]":
    """Standarisasi DataFrame hasil bulk download: flatten, lowercase, rename date."""
    if raw is None or (hasattr(raw, "empty") and raw.empty):
        return None
    import pandas as pd
    raw = raw.copy()
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = [c[0].lower() for c in raw.columns]
    else:
        raw.columns = [c.lower() for c in raw.columns]
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
    return raw.reset_index(drop=True) if not raw.empty else None


def _fetch_one_chunk(
    tf_label: str,
    interval: str,
    period: str,
    cache_key: str,
    is_intraday: bool,
    chunk: list,
    chunk_idx: int,
    n_chunks: int,
    yf_to_raw: dict,
) -> dict:
    """
    Worker function: download SATU chunk untuk SATU timeframe.

    Dipanggil dari ThreadPoolExecutor — semua kombinasi (timeframe × chunk)
    jalan bersamaan, max MAX_WORKERS task serentak.

    Thread-safety:
    - Parquet write: file unik per ticker+tf → tidak ada konflik antar thread.
    - _weekly_cache[key] = df: hanya task 1wk yang tulis ke _weekly_cache,
      tiap key (ticker) berbeda antar task → CPython dict assignment atomic (GIL).
    - _monthly_cache[key] = df: sama.
    - yf.download() adalah HTTP GET → thread-safe.

    Return: {"tf": str, "ok": int}
    """
    batch = None
    try:
        if is_intraday:
            days_back = 59 if interval == "1h" else 7
            today = datetime.now(tz=timezone.utc).date()
            start_dt = today - timedelta(days=days_back)
            batch = yf.download(
                tickers=chunk,
                start=start_dt.strftime("%Y-%m-%d"),
                end=today.strftime("%Y-%m-%d"),
                interval=interval,
                group_by="ticker",
                auto_adjust=True,
                progress=False,
            )
        else:
            batch = yf.download(
                tickers=chunk,
                period=period,
                interval=interval,
                group_by="ticker",
                auto_adjust=True,
                progress=False,
            )
    except Exception as e:
        log.warning(f"  [{tf_label}] Chunk {chunk_idx}/{n_chunks} download error: {e}")
        batch = None

    chunk_ok = 0
    for yf_t in chunk:
        raw_t = yf_to_raw.get(yf_t, yf_t)
        try:
            sub = _bulk_extract_ticker(batch, yf_t)
            df  = _normalize_bulk_df(sub)
            if df is None or len(df) < 10:
                continue

            if cache_key in ("1d", "1h"):
                # Simpan ke parquet — dibaca oleh proses pipeline lain via _load_yf_cache
                _save_yf_cache(raw_t, cache_key, df)
            elif cache_key == "1wk":
                # In-memory cache (untuk proses ini) + parquet (untuk proses pipeline lain)
                _weekly_cache[raw_t] = df
                _save_yf_cache(raw_t, "1wk", df)   # ← FIX: persist lintas proses
            elif cache_key == "1mo":
                # In-memory cache (untuk proses ini) + parquet (untuk proses pipeline lain)
                _monthly_cache[raw_t] = df
                _save_yf_cache(raw_t, "1mo", df)   # ← FIX: persist lintas proses

            chunk_ok += 1
        except Exception as e:
            log.debug(f"  [{tf_label}] {raw_t}: {e}")

    log.info(f"  [{tf_label}] Chunk {chunk_idx:>2}/{n_chunks} → {chunk_ok}/{len(chunk)} OK")
    return {"tf": tf_label, "ok": chunk_ok}


def prefetch_ohlcv(universe):
    """
    Bulk download semua data OHLCV ke local cache menggunakan yf.download()
    multi-ticker (50 ticker/chunk).

    Semua kombinasi (timeframe × chunk) dijalankan sebagai task paralel
    dalam satu ThreadPoolExecutor (max_workers=8).

    Tidak ada sleep antar chunk — tidak diperlukan karena semua task sudah
    jalan bersamaan dan Yahoo Finance toleran terhadap concurrent request
    dalam skala ini.

    Thread-safety (lihat _fetch_one_chunk untuk detail):
    - Parquet write: unik per ticker+tf → no conflict.
    - _weekly_cache / _monthly_cache: key berbeda antar thread → aman di CPython.
    - yf.download(): HTTP GET → thread-safe.
    """
    CHUNK       = 50
    MAX_WORKERS = 8  # max concurrent yf.download() calls

    tickers_raw = [s.get("ticker", "") for s in universe if s.get("ticker")]
    yf_tickers  = [
        f"{t}.JK" if not t.endswith(".JK") and not t.startswith("^") else t
        for t in tickers_raw
    ]
    yf_to_raw = dict(zip(yf_tickers, tickers_raw))

    total    = len(yf_tickers)
    chunks   = [yf_tickers[i:i + CHUNK] for i in range(0, total, CHUNK)]
    n_chunks = len(chunks)

    timeframes = [
        # (tf_label, interval, period, cache_key, is_intraday)
        ("1d",  "1d",  CONFIG.get("YF_PERIOD_DAILY", "2y"), "1d",  False),
        ("1h",  "1h",  CONFIG.get("YF_PERIOD_1H",    "60d"), "1h",  True),
        ("1wk", "1wk", "max", "1wk", False),
        ("1mo", "1mo", "max", "1mo", False),
    ]

    total_tasks = len(timeframes) * n_chunks
    log.info(
        f"\n📥 Bulk OHLCV — {total} saham | {n_chunks} chunk × {len(timeframes)} timeframe"
        f" = {total_tasks} task | max_workers={MAX_WORKERS}"
    )

    # Buat semua task: setiap (timeframe × chunk) = 1 task independen
    all_tasks = []
    for tf_label, interval, period, cache_key, is_intraday in timeframes:
        for ci, chunk in enumerate(chunks, 1):
            all_tasks.append((
                tf_label, interval, period, cache_key, is_intraday,
                chunk, ci, n_chunks, yf_to_raw,
            ))

    results_map = {"1d": 0, "1h": 0, "1wk": 0, "1mo": 0}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_one_chunk, *task): task[0]  # task[0] = tf_label
            for task in all_tasks
        }
        for future in as_completed(futures):
            tf_label = futures[future]
            try:
                result = future.result()
                results_map[result["tf"]] += result["ok"]
            except Exception as e:
                log.warning(f"  Task error [{tf_label}]: {e}")

    ok_1d = results_map["1d"]
    ok_1h = results_map["1h"]
    ok_wk = results_map["1wk"]
    ok_mo = results_map["1mo"]

    log.info(
        f"✅ Bulk OHLCV selesai — "
        f"1d:{ok_1d} 1h:{ok_1h} 1wk:{ok_wk} 1mo:{ok_mo} (dari {total} saham)"
    )







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
