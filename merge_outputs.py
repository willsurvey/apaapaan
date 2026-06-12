#!/usr/bin/env python3
# =============================================================================
# MERGE OUTPUTS — Step Terakhir: Gabung semua hasil pipeline
# =============================================================================
# Baca semua *_results.json → gabung → simpan:
#   latest_screening.json      (intraday only, backward compat)
#   combined_screening.json    (semua 7 pipeline)
#   combined_screening_YYYYMMDD.json (dated copy)
# =============================================================================
import json, sys, time, os
from datetime import datetime

from screener_common import (
    CONFIG, NumpyEncoder, log,
    save_output,
    save_combined_output_v3,
)

UNIVERSE_FILE = "universe_data.json"


def load_json(path, default):
    """Load JSON file dengan fallback ke default jika tidak ada / error."""
    if not os.path.exists(path):
        log.warning(f"  {path} tidak ditemukan — pakai default")
        return default
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning(f"  Gagal baca {path}: {e}")
        return default


def main():
    start = time.time()
    log.info("=" * 70)
    log.info("🔀 MERGE OUTPUTS — Gabung Semua Pipeline")
    log.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S WIB')}")
    log.info("=" * 70)

    # --- Baca metadata dari universe_data.json ---
    universe_data = load_json(UNIVERSE_FILE, {})
    market_ctx    = universe_data.get("market_ctx", {"market_safe": False})
    mode          = universe_data.get("mode", "YAHOO_ONLY")
    session_label = universe_data.get("session_label", "MARKET_DAY")

    # --- Baca hasil tiap pipeline ---
    log.info("\n[LOAD] Membaca hasil tiap pipeline...")

    intraday_data = load_json("intraday_results.json", {"results": [], "summary": {}})
    intraday_results = intraday_data.get("results", []) if isinstance(intraday_data, dict) else intraday_data
    intraday_summary = intraday_data.get("summary", {}) if isinstance(intraday_data, dict) else {}

    ara_results      = load_json("ara_results.json",      [])
    bsjp_results     = load_json("bsjp_results.json",     [])
    bpjs_results     = load_json("bpjs_results.json",     [])
    swing_results    = load_json("swing_results.json",    [])
    trend_results    = load_json("trend_results.json",    [])
    position_results = load_json("position_results.json", [])

    # Pastikan semua adalah list
    if isinstance(ara_results, dict):      ara_results      = ara_results.get("results", [])
    if isinstance(bsjp_results, dict):     bsjp_results     = bsjp_results.get("results", [])
    if isinstance(bpjs_results, dict):     bpjs_results     = bpjs_results.get("results", [])
    if isinstance(swing_results, dict):    swing_results    = swing_results.get("results", [])
    if isinstance(trend_results, dict):    trend_results    = trend_results.get("results", [])
    if isinstance(position_results, dict): position_results = position_results.get("results", [])

    log.info(f"  Intraday:  {len(intraday_results)} saham")
    log.info(f"  ARA:       {len(ara_results)} kandidat")
    log.info(f"  BSJP:      {len(bsjp_results)} kandidat")
    log.info(f"  BPJS:      {len(bpjs_results)} kandidat")
    log.info(f"  Swing:     {len(swing_results)} kandidat")
    log.info(f"  Trend:     {len(trend_results)} kandidat")
    log.info(f"  Position:  {len(position_results)} kandidat")

    # --- Simpan latest_screening.json (backward compatible) ---
    log.info("\n[SAVE] Menyimpan latest_screening.json...")
    save_output(
        results=intraday_results,
        mode=mode,
        market_ctx=market_ctx,
        screening_summary=intraday_summary,
        session_label=session_label,
    )

    # --- Simpan combined_screening.json (semua pipeline) ---
    log.info("\n[SAVE] Menyimpan combined_screening.json...")
    save_combined_output_v3(
        intraday_results=intraday_results,
        ara_results=ara_results,
        bsjp_results=bsjp_results,
        bpjs_results=bpjs_results,
        swing_results=swing_results,
        trend_results=trend_results,
        position_results=position_results,
        mode=mode,
        market_ctx=market_ctx,
        intraday_summary=intraday_summary,
        session_label=session_label,
    )

    elapsed = (time.time() - start) / 60
    log.info("\n" + "=" * 70)
    log.info("✅ MERGE SELESAI")
    log.info(f"   Output: latest_screening.json + combined_screening.json")
    log.info(f"   ⏱️  Waktu merge: {elapsed:.1f} menit")
    log.info("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"💥 ERROR MERGE: {e}")
        import traceback; traceback.print_exc()
        # Buat output darurat agar job tidak crash total
        emergency = {
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data": [],
        }
        with open("latest_screening.json", "w") as f:
            json.dump(emergency, f, indent=2)
        sys.exit(1)
