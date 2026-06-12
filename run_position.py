#!/usr/bin/env python3
# =============================================================================
# RUN POSITION — Position Trading (Near 52W High + OBV + Monthly)
# =============================================================================
import json, sys, time

from screener_common import (
    CONFIG, NumpyEncoder, log,
    run_position_pipeline,
)

UNIVERSE_FILE = "universe_data.json"
OUTPUT_FILE   = "position_results.json"


def main():
    start = time.time()
    log.info("=" * 70)
    log.info("📈 PIPELINE POSITION TRADING")
    log.info("=" * 70)

    with open(UNIVERSE_FILE, encoding="utf-8") as f:
        data = json.load(f)

    if not data.get("market_safe"):
        log.warning("🛑 Market tidak aman — Position skip")
        with open(OUTPUT_FILE, "w") as f:
            json.dump([], f)
        return

    if not CONFIG.get("POSITION_ENABLED", True):
        with open(OUTPUT_FILE, "w") as f:
            json.dump([], f)
        return

    universe   = data["universe"]
    mode       = data["mode"]
    market_ctx = data["market_ctx"]

    results = run_position_pipeline(universe, mode, market_ctx)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)

    elapsed = (time.time() - start) / 60
    log.info(f"✅ Position selesai: {len(results)} kandidat | {elapsed:.1f} menit")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"💥 ERROR: {e}")
        import traceback; traceback.print_exc()
        with open(OUTPUT_FILE, "w") as f:
            json.dump([], f)
        sys.exit(1)
