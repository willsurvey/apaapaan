#!/usr/bin/env python3
# =============================================================================
# RUN BSJP — Beli Sore Jual Pagi
# =============================================================================
import json, sys, time, logging

from screener_common import (
    CONFIG, NumpyEncoder, log,
    run_bsjp_pipeline,
)

UNIVERSE_FILE = "universe_data.json"
OUTPUT_FILE   = "bsjp_results.json"


def main():
    start = time.time()
    log.info("=" * 70)
    log.info("📈 PIPELINE BSJP — Beli Sore Jual Pagi")
    log.info("=" * 70)

    with open(UNIVERSE_FILE, encoding="utf-8") as f:
        data = json.load(f)

    if not data.get("market_safe"):
        log.warning("🛑 Market tidak aman — BSJP skip")
        with open(OUTPUT_FILE, "w") as f:
            json.dump([], f)
        return

    if not CONFIG.get("BSJP_ENABLED", True):
        log.info("⚙️ BSJP_ENABLED=False — skip")
        with open(OUTPUT_FILE, "w") as f:
            json.dump([], f)
        return

    universe = data["universe"]
    mode     = data["mode"]

    results = run_bsjp_pipeline(universe, mode)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)

    elapsed = (time.time() - start) / 60
    log.info(f"✅ BSJP selesai: {len(results)} kandidat | {elapsed:.1f} menit")
    log.info(f"   Disimpan ke: {OUTPUT_FILE}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"💥 ERROR: {e}")
        import traceback; traceback.print_exc()
        with open(OUTPUT_FILE, "w") as f:
            json.dump([], f)
        sys.exit(1)
