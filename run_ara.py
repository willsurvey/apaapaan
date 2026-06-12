#!/usr/bin/env python3
# =============================================================================
# RUN ARA — Pipeline Calon Auto Reject Atas v2
# =============================================================================
import json, sys, time, logging

from screener_common import (
    CONFIG, NumpyEncoder, log,
    get_valid_token,
    run_ara_pipeline_v2,
)

UNIVERSE_FILE = "universe_data.json"
OUTPUT_FILE   = "ara_results.json"


def main():
    start = time.time()
    log.info("=" * 70)
    log.info("🚀 PIPELINE ARA v2 — Calon Auto Reject Atas")
    log.info("=" * 70)

    with open(UNIVERSE_FILE, encoding="utf-8") as f:
        data = json.load(f)

    if not data.get("market_safe"):
        log.warning("🛑 Market tidak aman — ARA skip")
        with open(OUTPUT_FILE, "w") as f:
            json.dump([], f)
        return

    if not CONFIG.get("ARA_ENABLED", True):
        log.info("⚙️ ARA_ENABLED=False — skip")
        with open(OUTPUT_FILE, "w") as f:
            json.dump([], f)
        return

    mode = data["mode"]

    # ARA butuh token Stockbit untuk universe-nya
    # Login fresh (token tidak bisa di-pass antar job GA, tapi in-memory login ok)
    token, _ = get_valid_token()

    results = run_ara_pipeline_v2(token, mode)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)

    elapsed = (time.time() - start) / 60
    log.info(f"✅ ARA selesai: {len(results)} kandidat | {elapsed:.1f} menit")
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
