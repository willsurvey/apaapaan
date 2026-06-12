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
from datetime import datetime
from typing import Optional

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


def prefetch_broker_and_liquidity(universe, token, mode):
    """
    Pre-fetch broker signal + liquidity untuk semua saham.
    Hasilnya disimpan langsung ke dict tiap saham (in-place).
    Token di-refresh setiap 50 saham.
    """
    enriched = []
    for i, stock_mm in enumerate(universe):
        ticker = stock_mm.get("ticker", "")

        # Refresh token setiap 50 saham
        if i > 0 and i % 50 == 0 and mode == "FULL_STOCKBIT":
            log.info(f"🔄 TOKEN REFRESH di saham ke-{i+1}...")
            invalidate_token_cache()
            new_token, new_mode = get_valid_token()
            if new_token:
                token = new_token
            else:
                mode = new_mode
                token = None

        # --- Liquidity ---
        if mode == "FULL_STOCKBIT":
            liq_ok, hist_data = check_liquidity_quality(ticker, token)
            if not liq_ok or hist_data is None:
                log.debug(f"  SKIP {ticker}: liquidity gagal")
                stock_mm["_liq_ok"] = False
                stock_mm["_hist_data"] = None
                enriched.append(stock_mm)
                continue
        else:
            df_d = get_daily_data(ticker)
            if df_d is None or len(df_d) < 60:
                stock_mm["_liq_ok"] = False
                stock_mm["_hist_data"] = None
                enriched.append(stock_mm)
                continue
            last = df_d.iloc[-1]
            foreign_history = []
            for j in range(2, min(27, len(df_d) + 1)):
                row = df_d.iloc[-j]
                foreign_history.append({
                    "date": str(row.get("date", "")),
                    "close": float(row["close"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "open": float(row["open"]),
                    "volume": float(row["volume"]),
                    "value": float(row["close"]) * float(row["volume"]),
                    "net_foreign": 0,
                })
            hist_data = {
                "pdh": float(last["high"]),
                "pdl": float(last["low"]),
                "pdc": float(last["close"]),
                "pd_typical": (float(last["high"]) + float(last["low"]) + float(last["close"])) / 3,
                "avg_range_pct": 0,
                "avg_freq": 0,
                "foreign_history": foreign_history,
            }
            liq_ok = True

        # --- Broker Signal ---
        if mode == "FULL_STOCKBIT":
            broker_signal, broker_score = get_broker_signal(ticker, token)
        else:
            broker_signal, broker_score = "Neutral", 5

        stock_mm["_liq_ok"] = liq_ok
        stock_mm["_hist_data"] = hist_data
        stock_mm["_broker_signal"] = broker_signal
        stock_mm["_broker_score"] = broker_score
        enriched.append(stock_mm)

        if (i + 1) % 25 == 0:
            log.info(f"  [{i+1}/{len(universe)}] Pre-fetch selesai...")
        gc.collect()

    return enriched


def prefetch_ohlcv(universe):
    """
    Pre-download semua data OHLCV ke local cache (data_ohlc_cache/).
    Tiap pipeline nanti akan membaca dari cache — 0 download ulang.
    """
    log.info(f"\n📥 Pre-download OHLCV untuk {len(universe)} saham...")
    tickers_ok = 0
    tickers_fail = 0

    for i, stock_mm in enumerate(universe):
        ticker = stock_mm.get("ticker", "")
        try:
            # Daily (paling penting, dipakai semua pipeline)
            df_d = get_daily_data(ticker)

            # Weekly (Swing, Trend, Position)
            df_w = get_weekly_data(ticker)

            # Monthly (Position)
            df_m = get_monthly_data(ticker)

            # 1H (Intraday, SMC)
            df_1h = get_1h_data(ticker)

            if df_d is not None:
                tickers_ok += 1
            else:
                tickers_fail += 1

        except Exception as e:
            log.debug(f"  OHLCV {ticker}: error — {e}")
            tickers_fail += 1

        if (i + 1) % 50 == 0:
            log.info(f"  [{i+1}/{len(universe)}] OHLCV cache selesai (ok={tickers_ok}, fail={tickers_fail})")
        gc.collect()

    log.info(f"✅ OHLCV pre-fetch selesai: {tickers_ok} ok, {tickers_fail} gagal")


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
