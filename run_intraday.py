#!/usr/bin/env python3
# =============================================================================
# RUN INTRADAY — Pipeline Intraday Limit Order
# Baca universe_data.json → jalankan filter SMC + scoring → simpan hasil
# =============================================================================
import json, sys, time, gc, logging
from datetime import datetime

from screener_common import (
    CONFIG, NumpyEncoder, log,
    get_valid_token,
    calculate_accumulation_score,
    check_trend_context,
    check_smc_structure,
    calculate_entry_plan,
    calculate_final_score,
    build_output_stock,
    save_output,
)

UNIVERSE_FILE   = "universe_data.json"
OUTPUT_FILE     = "intraday_results.json"


def main():
    start = time.time()
    log.info("=" * 70)
    log.info("📊 PIPELINE INTRADAY — Baca universe_data.json")
    log.info("=" * 70)

    with open(UNIVERSE_FILE, encoding="utf-8") as f:
        data = json.load(f)

    if not data.get("market_safe"):
        log.warning("🛑 Market tidak aman — output kosong")
        with open(OUTPUT_FILE, "w") as f:
            json.dump([], f)
        return

    universe      = data["universe"]
    mode          = data["mode"]
    market_ctx    = data["market_ctx"]
    session_label = data.get("session_label", "MARKET_DAY")

    # Login fresh — setiap job GA berjalan di mesin terpisah, token tidak bisa
    # di-pass antar job. Token dibutuhkan oleh calculate_entry_plan() untuk
    # get_bid_wall() (menentukan Entry 2 level yang akurat di FULL_STOCKBIT).
    token = None
    if mode == "FULL_STOCKBIT":
        token, _ = get_valid_token()
        if token:
            log.info("✅ Token Stockbit berhasil diperoleh untuk entry plan")
        else:
            log.warning("⚠️  Token tidak tersedia — bid wall tidak akan dipakai di entry plan")

    log.info(f"Universe: {len(universe)} saham | Mode: {mode}")

    candidates = []
    summary = {
        "universe": len(universe),
        "after_liquidity": 0, "after_accumulation": 0,
        "after_trend": 0, "after_smc": 0, "after_entry": 0, "final": 0,
    }

    for i, stock_mm in enumerate(universe):
        ticker = stock_mm.get("ticker", "")

        # --- Pakai data pre-fetched dari ingest ---
        if not stock_mm.get("_liq_ok", False):
            continue
        hist_data     = stock_mm.get("_hist_data")
        broker_signal = stock_mm.get("_broker_signal", "Neutral")
        broker_score  = stock_mm.get("_broker_score", 5)
        if hist_data is None:
            continue
        if broker_score == -999:
            continue

        summary["after_liquidity"] += 1

        # --- Accumulation ---
        acc_score, acc_breakdown = calculate_accumulation_score(
            stock_mm, hist_data, broker_signal, broker_score, mode
        )
        threshold = CONFIG["ACC_THRESHOLD_MODE_A"] if mode == "FULL_STOCKBIT" else CONFIG["ACC_THRESHOLD_MODE_B"]
        if acc_score < threshold:
            continue
        summary["after_accumulation"] += 1

        # --- Trend + Candle Quality ---
        trend_ok, trend_dict = check_trend_context(ticker)
        if not trend_ok or trend_dict is None:
            continue
        gap_pct = trend_dict.get("gap_pct", 100)
        if gap_pct > CONFIG["MA_GAP_ACCEPTABLE_MAX"] * 100 and broker_signal != "Big Acc":
            continue
        _body_min = CONFIG.get("INTRADAY_CANDLE_BODY_MIN", 0.40)
        _pdh = hist_data.get("pdh", 0)
        _pdl = hist_data.get("pdl", 0)
        _pdc = hist_data.get("pdc", 0)
        if _pdh > _pdl > 0 and _pdc > 0:
            if (_pdc - _pdl) / (_pdh - _pdl) < _body_min:
                continue
        summary["after_trend"] += 1

        # --- SMC ---
        smc_ok, smc_dict = check_smc_structure(ticker)
        if not smc_ok:
            continue
        summary["after_smc"] += 1

        # --- Entry Plan ---
        entry_plan = calculate_entry_plan(ticker, hist_data, smc_dict, mode, token, stock_mm)
        if entry_plan is None:
            continue
        summary["after_entry"] += 1

        # --- Score ---
        score, tier = calculate_final_score(stock_mm, acc_breakdown, trend_dict, smc_dict, entry_plan, mode)
        if score < CONFIG.get("INTRADAY_MIN_SCORE", 50):
            continue

        log.info(f"  ✅ {ticker}: Score {score}/100 ({tier}) | RR {entry_plan['rr_str']}")
        candidates.append({
            "score": score, "tier": tier, "ticker": ticker,
            "stock_mm": stock_mm, "hist_data": hist_data,
            "broker_signal": broker_signal, "acc_breakdown": acc_breakdown,
            "trend_dict": trend_dict, "smc_dict": smc_dict, "entry_plan": entry_plan,
        })
        gc.collect()

    candidates.sort(key=lambda x: x["score"], reverse=True)
    top = candidates[:CONFIG["MAX_OUTPUT"]]
    summary["final"] = len(top)

    results = []
    for rank, c in enumerate(top, 1):
        out = build_output_stock(
            rank=rank, ticker=c["ticker"], stock_mm=c["stock_mm"],
            hist_data=c["hist_data"], broker_signal=c["broker_signal"],
            acc_breakdown=c["acc_breakdown"], trend_dict=c["trend_dict"],
            smc_dict=c["smc_dict"], entry_plan=c["entry_plan"],
            score=c["score"], tier=c["tier"], mode=mode,
        )
        results.append(out)

    # Simpan JSON partial untuk merge job
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "results": results, "summary": summary,
            "mode": mode, "market_ctx": market_ctx, "session_label": session_label,
        }, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)

    elapsed = (time.time() - start) / 60
    log.info(f"✅ Intraday selesai: {len(results)} saham | {elapsed:.1f} menit")
    log.info(f"   Disimpan ke: {OUTPUT_FILE}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"💥 ERROR: {e}")
        import traceback; traceback.print_exc()
        with open(OUTPUT_FILE, "w") as f:
            json.dump({"results": [], "error": str(e)}, f)
        sys.exit(1)
