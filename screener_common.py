#!/usr/bin/env python3
# =============================================================================
# SCREENER TRADER INDONESIA - FINAL PRODUCTION VERSION
# =============================================================================
# Strategi  : Pre-market intraday, limit order
# Universe  : Market Mover Stockbit (MODE A) / Yahoo Finance 958 saham (MODE B)
# Sinyal    : Broker Accumulation + Foreign Flow + SMC Structure (LuxAlgo benar)
# Entry     : PDH/PDL/PDC + Bid Wall + Order Block zone
# Output    : latest_screening.json (max 3 saham)
# Jadwal    : Dipanggil oleh GitHub Actions setiap hari kerja jam 18:00 WIB
# =============================================================================

import os
import sys
import re
import json
import time
import base64
import logging
import warnings
import gc
import threading
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any

import numpy as np
import pandas as pd
import pandas_ta as ta
import requests
import yfinance as yf

warnings.filterwarnings("ignore")


# =============================================================================
# JSON ENCODER — Handle numpy types (bool_, int64, float64, ndarray)
# Diperlukan karena pandas/numpy menghasilkan tipe non-native yang tidak bisa
# di-serialize oleh json.dump standar (TypeError: bool is not JSON serializable)
# =============================================================================
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, (np.int8, np.int16, np.int32, np.int64)):
            return int(obj)
        if isinstance(obj, (np.float16, np.float32, np.float64)):
            if np.isnan(obj) or np.isinf(obj):
                return None
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)

# =============================================================================
# LOGGING SETUP
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("screener.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# =============================================================================
# KONFIGURASI
# =============================================================================
CONFIG = {
    # --- Mode ---
    "MODE": "AUTO",                  # AUTO | FULL_STOCKBIT | YAHOO_ONLY

    # --- File paths ---
    "SESSION_FILE": "stockbit_session.json",
    "TOKEN_FILE": "stockbit_token.txt",
    "OUTPUT_LATEST": "latest_screening.json",
    "DATA_DIR": "data_ohlc_cache",

    # --- Stockbit API ---
    "SB_BASE": "https://exodus.stockbit.com",
    "SB_TIMEOUT": 20,
    "SB_RETRY": 3,
    "SB_DELAY": 1.0,

    # --- Market Mover filter ---
    "MM_VALUE_MIN": 5_000_000_000,        # Rp 5 Miliar
    "MM_FREQ_MIN": 1000,                  # 1000 transaksi
    "MM_BOARDS": [
        "FILTER_STOCKS_TYPE_MAIN_BOARD",
        "FILTER_STOCKS_TYPE_DEVELOPMENT_BOARD",
        "FILTER_STOCKS_TYPE_ACCELERATION_BOARD",
        "FILTER_STOCKS_TYPE_NEW_ECONOMY_BOARD",
    ],

    # --- Top Gainer universe tambahan ---
    # Saham top gainer hari ini = kandidat momentum besok
    "GAINER_INCLUDE": True,              # Ambil top gainer sebagai kandidat tambahan
    "GAINER_MIN_VALUE": 3_000_000_000,   # Min Rp 3M (lebih longgar dari MM biasa)
    "GAINER_MIN_FREQ": 500,              # Min 500 transaksi
    "GAINER_MIN_CHANGE_PCT": 5.0,        # Min naik 5% hari ini
    "GAINER_MAX_CHANGE_PCT": 24.9,       # Max 24.9% (hindari ARA yang sudah terlalu tinggi)

    # --- Top Loser universe tambahan ---
    # Saham top loser hari ini = kandidat reversal / oversold bounce besok
    "LOSER_INCLUDE": True,               # Ambil top loser sebagai kandidat tambahan
    "LOSER_MIN_VALUE": 3_000_000_000,    # Min Rp 3M
    "LOSER_MIN_FREQ": 500,               # Min 500 transaksi
    "LOSER_MIN_CHANGE_PCT": -24.9,       # Batas bawah — hindari ARB
    "LOSER_MAX_CHANGE_PCT": -3.0,        # Batas atas — minimal sudah cukup turun

    # --- Stockbit Screener (Volume Explosion) ---
    "SB_SCREENER_ENABLED": True,         # Gunakan screener API Stockbit
    "SB_SCREENER_ID": "",                # Kosong = gunakan template baru, isi jika sudah punya ID
    "SB_SCREENER_VOL_MA5_MULTIPLIER": 2.0,   # Volume > VolMA5 × 2
    "SB_SCREENER_VOL_MA5_MA20_MULT": 1.5,    # VolMA5 > VolMA20 × 1.5
    "SB_SCREENER_MAX_PAGES": 20,              # Jumlah page screener yang diambil

    # --- Stockbit Screener Top Value (semua saham IHSG diurutkan by Value DESC) ---
    "SB_TOP_VALUE_ENABLED": True,
    "SB_TOP_VALUE_MAX_PAGES": 20,            # 15 page × 25 = 375 saham teratas by value

    # --- Stockbit Guru Screener ---
    # GET /screener/templates/{id}?type=TEMPLATE_TYPE_GURU
    # Format: (template_id, label, max_page)
    # max_page=5 untuk template besar (78, 79) — top 125 saham
    # max_page=2 untuk template medium
    # max_page=1 untuk template kecil (totalrows < 25)
    "SB_GURU_SCREENER_ENABLED": True,
    "SB_GURU_SCREENER_LIST": [
        # (id,  nama,                                   max_page)
        (92,  "Big Accumulation",                       20),   # Bandar Accum/Dist > 20
        (77,  "Foreign Flow Uptrend",                   20),   # Net Foreign streak ≥2
        (94,  "Bandar Bullish Reversal",                20),   # Bandar Value naik lewati MA10
        (87,  "Reversal on Bearish Trend",              20),   # Price > MA20 > MA10 + vol spike
        (88,  "Potential Reversal on Bearish Trend",    20),   # MA20 > Price > MA10 + vol spike
        (63,  "High Volume Breakout",                   20),   # Volume > 2x MA20
        (97,  "Frequency Spike",                        10),   # Frekuensi > 3x rata-rata
        (72,  "IHSG Short-term Outperformers",          20),   # 3M RS Line > 1.1
        (78,  "Daily Net Foreign Flow",                 50),   # Net Foreign Buy harian (besar)
        (79,  "1 Week Net Foreign Flow",                50),   # Net Foreign Buy 1 minggu (besar)
    ],

    # --- Liquidity check (Historical Summary 20 hari) ---
    "LIQ_VALUE_MIN_PER_DAY": 3_000_000_000,   # Rp 3 Miliar
    "LIQ_FREQ_MIN_AVG": 500,
    "LIQ_RANGE_MIN_PCT": 0.008,               # 0.8%
    "LIQ_RANGE_MAX_PCT": 0.08,                # 8%

    # --- Accumulation scoring ---
    "ACC_THRESHOLD_MODE_A": 25,
    "ACC_THRESHOLD_MODE_B": 10,

    # --- Trend context ---
    "MA_PERIOD": 50,
    "MA_GAP_IDEAL_MAX": 0.05,        # 5% ideal
    "MA_GAP_ACCEPTABLE_MAX": 0.10,   # 10% max kecuali Big Acc

    # --- SMC (LuxAlgo asli) ---
    "SMC_INTERNAL_LENGTH": 5,
    "SMC_SWING_LENGTH": 20,
    "SMC_LOOKBACK_BARS": 10,         # BOS/CHoCH harus dalam 10 bar terakhir
    "SMC_ATR_PERIOD": 200,           # LuxAlgo pakai ATR200 bukan 14 untuk volatility filter OB
    "SMC_HIGH_VOL_MULTIPLIER": 2.0,
    "SMC_FVG_MIN_GAP": 0.002,        # Minimum gap FVG 0.2% (fallback jika data kurang)

    # --- Entry / SL / TP ---
    "ENTRY_1_PCT": 0.20,
    "ENTRY_2_PCT": 0.50,
    "ENTRY_3_PCT": 0.30,
    "SL_HARD_CAP_PCT": 0.03,         # Max 3% dari average entry
    "MIN_RR": 2.0,                   # Minimum Risk:Reward 1:2

    # --- Entry Direction Detection ---
    # [BACKTEST FINDING v2 — 2026-05-07]
    # BOS Daily tanpa OB confirmation: WR 25.9%, PF 0.78x (MERUGI)
    # MOMENTUM direction di Mode B: WR 36.6%, PF 0.99x (MERUGI)
    # PULLBACK direction: WR 58.7%, PF 1.67x (MENGUNTUNGKAN)
    # → ENTRY_MOMENTUM_BOS_BONUS hanya aktif di FULL_STOCKBIT.
    # → Di Mode B (Yahoo Only): selalu PULLBACK kecuali ada IEP gap up.
    # → BOS tanpa OB Zone tidak mengubah direction (guard baru).
    "ENTRY_GAP_UP_THRESHOLD": 1.5,   # IEP change > 1.5% → prediksi gap up → entry di atas PDC
    "ENTRY_MOMENTUM_BOS_BONUS": True, # Hanya dipakai di FULL_STOCKBIT mode
    "ENTRY_BOS_REQUIRE_OB": True,     # BOS harus dikonfirmasi OB Zone untuk upgrade MOMENTUM

    # --- Intraday Quality Filters (dari backtest forward-looking 1.159 sinyal) ---
    # Candle D-1 body_pos < 0.40 → bearish ringan → skip (backtest: WR 38.9%, PF 0.73x)
    "INTRADAY_CANDLE_BODY_MIN": 0.40,
    # Score minimum: 50 (backtest: score>=50 WR 60%, score>=40 WR 57.9%)
    "INTRADAY_MIN_SCORE": 50,

    # --- Yahoo Finance ---
    "YF_PERIOD_DAILY": "max",
    "YF_PERIOD_1H": "60d",
    "YF_MIN_PRICE": 100,             # Hanya untuk MODE B
    "YF_CACHE_DAYS": 1,              # Stale setelah 1 hari

    # --- Output ---
    "MAX_OUTPUT": 5,
    "IHSG_3DAY_DOWN_STOP": True,
    "IHSG_1DAY_DOWN_WARNING": -1.5,  # %

    # --- IHSG ticker ---
    "IHSG_TICKER": "^JKSE",

    # =========================================================================
    # PIPELINE ARA (LOGIKA BARU) — Deteksi Calon Auto Reject Atas
    # =========================================================================
    # Berdasarkan analisis kuantitatif 10 saham ARA 10 April 2026.
    #
    # TEMUAN UTAMA:
    #   - Tipe 1 (Continuation) : D-1 naik >8%, vol >3x → ASPI pattern
    #   - Tipe 2 (Silent Accum) : D-1 vol spike tapi harga flat/turun → OPMS, PNSE
    #   - Tipe 3 (Out of Nowhere): Tidak ada sinyal OHLCV → CITY, DIVA, FITT, MLPT
    #
    # Korelasi fitur empiris (n=10):
    #   d1_upper_wick   : +0.555  (rejection atas = smart money beli)
    #   d1_body_pct     : -0.575  (body kecil/doji = akumulasi tersembunyi)
    #   d1_close_pos    : -0.475  (close di bawah midrange = absorption)
    #   d2_vol_ma20     : +0.425  (early accumulation D-2)
    #
    # FALSE SIGNAL RATE: ~85-95%. Gunakan sebagai WATCHLIST saja.
    # Output terpisah di kunci "logika_baru_calon_ara" dalam combined_screening.json
    # =========================================================================

    "ARA_ENABLED": True,
    "ARA_MAX_OUTPUT": 5,
    "ARA_OUTPUT_FILE": "combined_screening.json",

    # Likuiditas ARA — lebih longgar dari pipeline intraday
    # Saham ARA sering kecil/illikuid sebelum meledak
    "ARA_MIN_VALUE_D1":  500_000_000,    # Rp 500 Juta (vs Rp 3M di intraday)

    # Volume spike thresholds — berdasarkan distribusi empiris 29 saham ARA
    # Tier1: perlu konfluensi lain. Tier2: cukup kuat berdiri sendiri.
    # DIPERTAHANKAN: tidak diturunkan, ini filter precision yang baik.
    "ARA_VOL_MA20_TIER1": 3.0,           # >= 3x MA20 (moderate)
    "ARA_VOL_MA20_TIER2": 6.0,           # >= 6x MA20 (kuat)
    "ARA_VOL_MA20_D2_MIN": 1.5,          # D-2 juga ada aktivitas

    # Candle pattern D-1 — berdasarkan korelasi empiris
    "ARA_UPPER_WICK_MIN":     0.35,      # rejection upper wick (corr +0.555)
    "ARA_BODY_MAX_REJECTION": 0.45,      # body kecil = unconvincing (corr -0.575)
    "ARA_CLOSE_POS_MAX":      0.55,      # close di bawah midrange (corr -0.475)

    # Tipe 1 Continuation parameters
    "ARA_CONTINUATION_CHG_MIN": 8.0,     # D-1 naik minimal 8%
    "ARA_CONTINUATION_VOL_MIN": 3.0,     # dengan volume minimal 3x MA20

    # VCP Squeeze parameters (v2.1 — range kontraksi = tekanan terpendam)
    "ARA_SQUEEZE_STRONG": 0.70,          # range_exp < 0.70 → bonus kuat
    "ARA_SQUEEZE_MILD":   0.90,          # range_exp < 0.90 → bonus moderat

    # Mandatory Anchor Gate — minimal 2 dari 3 sinyal jangkar wajib terpenuhi
    # sebelum masuk ke scoring. Ini filter utama untuk menaikkan win-rate.
    "ARA_ANCHOR_MIN": 2,                 # min 2 anchor dari 3 yang tersedia
    "ARA_ANCHOR_VOL_MIN": 3.0,           # Anchor 1: Vol D-1 >= 3x
    "ARA_ANCHOR_GREEN_MIN": 12,          # Anchor 2: m1_last15_green >= 12
    "ARA_ANCHOR_FLAT_MAX":  4,           # [AUDIT FIX] Maksimal flat bar di sesi menit agar terbebas dari noise saham mati
    # Anchor 3: broker_signal in ["Big Acc", "Acc"] — dicek setelah fetch broker

    # Score thresholds — DINAIKAN dari 40/60 untuk meningkatkan win-rate
    "ARA_SCORE_MIN":    55,              # Naik dari 40 → 55 (lebih selektif)
    "ARA_SCORE_STRONG": 70,             # Naik dari 60 → 70 (tier STRONG lebih eksklusif)

    # Confluence guard — minimum sinyal positif
    "ARA_CONFLUENCE_MIN": 3,             # Naik dari 2 → 3 sinyal positif wajib

    # =========================================================================
    # PIPELINE BSJP (BELI SORE JUAL PAGI) — v1.0
    # =========================================================================
    # Berdasarkan backtest kuantitatif 252.480 hari perdagangan (956 saham IHSG).
    #
    # FORMULA TERBAIK (dari grid search):
    #   MASTER4: MA20+MA50+MA200 + Vol>3x + ClosePos>90% + Body>3%
    #   Win Rate spike >2% esok pagi: 64.1% | Avg spike: 7.55%
    #
    #   Tier S: Vol>5x + ClosePos>95% + Body>4% + MA20+MA50
    #   Win Rate spike >2% esok pagi: 69.9% | Avg spike: 9.85%
    #
    # UNIVERSE: Saham dari pipeline intraday yang aktif hari ini
    #   (value_today >= BSJP_MIN_VALUE_TODAY). Gratis — pakai cache YF.
    #
    # OUTPUT: key baru "bsjp_beli_sore_jual_pagi" di combined_screening.json
    # =========================================================================
    "BSJP_ENABLED":         True,
    "BSJP_MAX_OUTPUT":      5,

    # Filter universe — hanya saham yang aktif HARI INI
    "BSJP_MIN_VALUE_TODAY": 10_000_000_000,  # Rp 10 Miliar (likuid, mudah masuk)

    # Filter tambahan
    "BSJP_MIN_PRICE":       100,             # Harga minimum (hindari gorengan murah)
    "BSJP_MIN_CHG_PCT":     -5.0,            # Jangan beli yang sedang crash

    # BSJP Tier S (lebih selektif, win rate lebih tinggi)
    # Win Rate historis: spike >2% = ~70%, avg spike = ~9.85%
    "BSJP_TIER_S_VOL":      5.0,   # Backtest: 5.0+0.95+ADX25 = WR 51.5%
    "BSJP_TIER_S_CPOS":     0.95,  # KRITIS — naik dari 0.80, penyebab WR rendah sebelumnya
    "BSJP_TIER_S_BODY":     0.05,  # Body candle >= 5% (diubah dari 0.04)
    # Wajib: above_ma20 AND above_ma50

    # BSJP Tier A (lebih banyak sinyal, win rate tetap solid)
    # Win Rate historis: spike >2% = ~64%, avg spike = ~7.55%
    "BSJP_TIER_A_VOL":      4.0,   # Vol >= 4x MA20 (diubah dari 3.0)
    "BSJP_TIER_A_CPOS":     0.92,  # Tutup di >=92% dari range harian (diubah dari 0.90)
    "BSJP_TIER_A_BODY":     0.03,  # Body candle >= 3%
    "BSJP_ADX_MIN":         25,    # [BARU] ADX >= 25 wajib — tanpa ini WR drop ke 31-37%
    # Wajib: above_ma20 AND above_ma50 AND above_ma200

    # Scoring bonus (memanfaatkan data universe yang sudah ada — gratis!)
    "BSJP_BONUS_NET_FOREIGN": 10,  # Asing beli hari ini
    "BSJP_BONUS_VOL_SCREENER": 10, # Muncul di volume explosion screener
    "BSJP_BONUS_GAINER":       5,  # Saham naik hari ini (momentum)
    "BSJP_BONUS_IEP_STRONG":   5,  # IEP change > 1.5% (ekspektasi gap up)
    "BSJP_BONUS_GOOD_DAY":     5,  # Senin/Selasa (win rate lebih tinggi)

    # =========================================================================
    # PIPELINE BPJS (BELI PAGI JUAL SORE) — v1.0
    # =========================================================================
    # Berdasarkan backtest forward-looking 789.792 hari trading (956 saham IHSG).
    #
    # REALITA DATA:
    #   Baseline IHSG: Win Rate beli-pagi-jual-sore hanya 30.3% tanpa filter.
    #   Tidak ada formula OHLCV D-1 yang menghasilkan WR > 50% secara konsisten.
    #
    # DESAIN HYBRID: Pipeline ini menghasilkan WATCHLIST berbasis D-1 pattern,
    #   yang WAJIB dikonfirmasi secara real-time di pagi hari sebelum eksekusi.
    #   Tanpa konfirmasi morning, sinyal ini BUKAN perintah beli.
    #
    # FORMULA TERBAIK (dari grid search 100 kombinasi):
    #   1. Reversal Bounce: D-1 merah + vol sepi + close bawah + >MA50
    #      WR=38.6% | Profit Factor 0.90x | BUTUH morning confirmation
    #   2. Quiet Continuation: D-1 hijau solid + vol mati + close tengah
    #      WR=49.8% | Profit Factor 1.04x | Paling mendekati edge positif
    #
    # CATATAN KRITIS:
    #   Gap Down >2% di pagi hari justru sweet spot (WR 30.2%, tapi
    #   upside +3.75% vs drawdown -0.88% = risk/reward terbaik).
    #   Gap Up >5% HARUS DIHINDARI (WR hanya 11.8%, DD -6.67%).
    #
    # UNIVERSE: Sama dengan BSJP (saham aktif hari ini dari pipeline intraday)
    # OUTPUT: key baru "bpjs_beli_pagi_jual_sore" di combined_screening.json
    # =========================================================================
    "BPJS_ENABLED":         True,
    "BPJS_REVERSAL_ENABLED": False,  # [BARU] WR max 43% N=53 — tidak stabil, dinonaktifkan
    "BPJS_MAX_OUTPUT":      5,

    # Filter universe
    "BPJS_MIN_VALUE_TODAY": 10_000_000_000,  # Rp 10 Miliar (likuid)
    "BPJS_MIN_PRICE":       100,
    "BPJS_MIN_CHG_PCT":     -10.0,           # Lebih longgar: kita INCAR yang turun

    # Formula 1: Reversal Bounce
    # D-1 merah + vol sepi + close bawah + trend masih ok (>MA50)
    "BPJS_REV_BODY_MAX":    -0.02,  # Body D-1 <= -2% (merah)
    "BPJS_REV_VOL_MAX":     1.0,    # Volume D-1 < 1x MA20 (sepi)
    "BPJS_REV_CPOS_MAX":    0.30,   # Close position <= 30% (bawah)

    # Formula 2: Quiet Continuation
    # D-1 hijau solid + vol mati + close tengah + uptrend
    "BPJS_QC_BODY_MIN":     0.01,   # Body D-1 >= 1% (hijau tipis-solid)
    "BPJS_QC_BODY_MAX":     0.08,   # Body D-1 <= 8% (tidak terlalu gila)
    "BPJS_QC_VOL_MAX":      0.5,    # Volume D-1 <= 0.5x MA20 (mati)
    "BPJS_QC_CPOS_MIN":     0.30,   # Close pos >= 30%
    "BPJS_QC_CPOS_MAX":     0.70,   # Close pos <= 70% (tengah)

    # Scoring bonus
    "BPJS_BONUS_ABOVE_MA20":  5,
    "BPJS_BONUS_ABOVE_MA50":  5,
    "BPJS_BONUS_ABOVE_MA200": 5,
    "BPJS_BONUS_NET_FOREIGN": 5,
    "BPJS_BONUS_VOL_SCREENER":5,
    "BPJS_BONUS_LOW_RANGE":   5,   # Range D-1 kecil (volatilitas rendah)

    # =========================================================================
    # PIPELINE SWING TRADING — v1.0
    # =========================================================================
    # Berdasarkan backtest forward-looking 467.390 hari trading (956 saham IHSG).
    #
    # GOLDEN SETUP (dari grid search pada Strong Uptrend):
    #   Sedikit di atas MA20 (+3% s/d +8%) + Ultra Tight Squeeze
    #   (Range 5d < 35% Range 20d) + Volume Kering (Vol 5d < 50% MA20)
    #   PF=1.60x | EV=+2.14% per trade | WR=48.8% | DD=-5.5%
    #   P(sentuh +10% dalam 10 hari) = 38.7%
    #
    # KILL SWITCH: Jika IHSG < MA200, pipeline WAJIB mati total.
    #   Setup momentum TIDAK bekerja di bear market (2019: PF 0.77x,
    #   2024: PF 0.92x, 2026: PF 0.35x).
    #
    # RSI BOOST: RSI 65-80 meningkatkan PF dari 1.38x ke 1.81x.
    #
    # HOLD OPTIMAL: 10-15 hari. PF plateau setelah 15 hari.
    # EXIT: Target +10-15% (trailing stop setelah +5%), SL -6% keras.
    #
    # OUTPUT: key baru "swing_trading" di combined_screening.json
    # =========================================================================
    "SWING_ENABLED":          True,
    "SWING_MAX_OUTPUT":       5,

    # Filter universe
    "SWING_MIN_VALUE_20D":    5_000_000_000,   # Val MA20 > Rp 5 Miliar
    "SWING_MIN_PRICE":        100,

    # Stage 2 Uptrend (wajib)
    "SWING_MA50_SLOPE_MIN":   0.01,  # MA50 naik >1% per 10 hari

    # Golden Setup: Posisi vs MA20
    "SWING_DIST_MA20_MIN":    0.02,  # Diperlonggar sedikit (dari 0.03)
    "SWING_DIST_MA20_MAX":    0.10,  # Diperlonggar sedikit (dari 0.08)

    # Golden Setup: Volatility Contraction
    "SWING_SQUEEZE_MAX":      0.35,  # Range 5d < 35% dari Range 20d

    # Golden Setup: Volume Dryness
    "SWING_VOL_5D_MAX":       0.60,  # Optimal di 0.60 (dari 0.50)

    # RSI filter
    "SWING_RSI_MIN":          45,    # Setup squeeze muncul saat RSI belum overbought (dari 55)
    "SWING_RSI_MAX":          65,    # WR 52.0% di range 45-65 vs 47.7% di 55-80 (dari 80)
    "SWING_ADX_MIN":          25,    # [BARU] ADX >= 25 wajib — filter noise dramatis

    # Exit parameters
    "SWING_TARGET_PCT":       0.10,  # Target +10%
    "SWING_STOP_LOSS_PCT":    0.06,  # Stop loss -6% (keras)
    "SWING_MAX_HOLD_DAYS":    20,    # Jual berapapun setelah 20 hari (diubah dari 15)

    # Scoring bonus
    "SWING_BONUS_ABOVE_MA200":  5,
    "SWING_BONUS_RSI_SWEET":    5,   # RSI 65-80 (momentum sweet spot)
    "SWING_BONUS_TIGHT_ATR":    5,   # ATR < 3% (volatilitas rendah)
    "SWING_BONUS_NET_FOREIGN":  5,
    "SWING_BONUS_NEAR_52W_HIGH":5,   # Dekat 52-week high (<10%)

    # =========================================================================
    # PIPELINE TREND FOLLOWING — v1.0
    # =========================================================================
    # Backtest: 956 saham IHSG, TF: 1D + 1Wk.
    # FORMULA: ADX_D>=30 + RSI[50-75] + MACD>0 + Mom5>0 + WkADX>=25 + WkOBV>MA
    # WR (Max High>=10% dalam 20 hari): 54.7%
    # Kill switch: IHSG < MA200 → return []
    # Output: key "trend_following" di combined_screening.json
    # =========================================================================
    "TREND_ENABLED":              True,
    "TREND_MAX_OUTPUT":           5,
    "TREND_MIN_VALUE_20D":        1_000_000_000,  # Rp 1M value MA20
    "TREND_ADX_MIN":              30,    # ADX Daily >= 30
    "TREND_RSI_MIN":              50,    # RSI >= 50
    "TREND_RSI_MAX":              75,    # RSI <= 75
    "TREND_REQUIRE_ABOVE_MA200":  True,
    "TREND_WK_ADX_MIN":           25,    # Weekly ADX >= 25
    "TREND_WK_OBV_ABOVE_MA":      True,
    "TREND_BONUS_MOM5":           5,
    "TREND_BONUS_WK_OBV":         10,
    "TREND_BONUS_ABOVE_MA200":    5,
    "TREND_BONUS_NET_FOREIGN":    5,
    "TREND_TARGET_PCT":           0.10,
    "TREND_STOP_LOSS_PCT":        0.07,
    "TREND_MAX_HOLD_DAYS":        20,

    # =========================================================================
    # PIPELINE POSITION TRADING — v1.0
    # =========================================================================
    # Backtest: 956 saham IHSG, TF: 1Wk + 1Mo + 1D.
    # FORMULA: Near52Hi>=85% + WkRSI[55-80] + WkADX>=20 + WkOBV>MA + MoRSI>=50
    # WR (Max High>=20% dalam 60 hari): 38.7%
    # Tidak ada kill switch IHSG.
    # Output: key "position_trading" di combined_screening.json
    # =========================================================================
    "POSITION_ENABLED":              True,
    "POSITION_MAX_OUTPUT":           5,
    "POSITION_MIN_VALUE_20D":        2_000_000_000,  # Rp 2M value MA20
    "POSITION_NEAR_52W_PCT":         0.85,  # Close >= 85% dari 52W high
    "POSITION_WK_RSI_MIN":           55,
    "POSITION_WK_RSI_MAX":           80,
    "POSITION_WK_ADX_MIN":           20,
    "POSITION_WK_OBV_ABOVE_MA":      True,
    "POSITION_MO_RSI_MIN":           50,
    "POSITION_REQUIRE_ABOVE_MA200":  True,
    "POSITION_BONUS_NEAR_ATH":       10,   # Close >= 95% dari 52W high
    "POSITION_BONUS_MO_RSI":          5,
    "POSITION_BONUS_WK_OBV":         10,
    "POSITION_BONUS_NET_FOREIGN":     5,
    "POSITION_TARGET_PCT":            0.20,
    "POSITION_STOP_LOSS_PCT":         0.10,
    "POSITION_MAX_HOLD_DAYS":         60,
}

os.makedirs(CONFIG["DATA_DIR"], exist_ok=True)



# =============================================================================
# STEP 0A: TOKEN MANAGEMENT
# =============================================================================
# Skema: Direct API Login (username + password) — tanpa Playwright, tanpa session file.
#
# CARA SETUP GITHUB ACTIONS:
#   Settings → Secrets and variables → Actions → New repository secret
#     STOCKBIT_USERNAME  : username/email Stockbit
#     STOCKBIT_PASSWORD  : password Stockbit
#     STOCKBIT_PLAYER_ID : (opsional) device player ID, bisa dikosongkan
#
# URUTAN LOOKUP TOKEN (get_valid_token):
#   1. Cache in-memory    — tidak login ulang selama sesi screener berjalan
#   2. Env STOCKBIT_BEARER_TOKEN — manual override / legacy inject
#   3. Login API langsung — STOCKBIT_USERNAME + STOCKBIT_PASSWORD (utama)
#
# RESPONSE PATH: POST /login/v6/username
#   → data.login.token_data.access.token
#
# TOKEN LIFETIME: 24 jam. Cache in-memory 23 jam.
# =============================================================================

# Header HTTP untuk login request — persis seperti Chrome browser
_SB_LOGIN_URL = "https://exodus.stockbit.com/login/v6/username"
_SB_LOGIN_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
    ),
    "X-DeviceType":    "Google Chrome",
    "X-Platform":      "PC",
    "X-AppVersion":    "3.17.2",
    "Content-Type":    "application/json",
    "Accept":          "application/json",
    "Accept-Language": "ID",
    "Origin":          "https://stockbit.com",
    "Referer":         "https://stockbit.com/",
}

# In-memory token cache — hindari login ulang per-siklus
_token_cache: Optional[str] = None
_token_cache_expiry: float = 0.0


def _decode_jwt_payload(token: str) -> Optional[Dict]:
    """Decode JWT payload (middle part) tanpa verifikasi signature."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        padding = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padding))
        return payload
    except Exception:
        return None


def _is_token_valid(token: str, buffer_seconds: int = 300) -> bool:
    """
    Cek apakah JWT belum expired.
    buffer_seconds: anggap expired N detik sebelum exp sesungguhnya (default 5 menit).
    """
    if not token or len(token) < 100:
        return False
    payload = _decode_jwt_payload(token)
    if not payload:
        return False
    exp = payload.get("exp", 0)
    return time.time() < (exp - buffer_seconds)


def _get_token_exp_ts(token: str) -> float:
    """Ambil unix timestamp expiry dari JWT. Return 0 jika gagal decode."""
    payload = _decode_jwt_payload(token)
    if not payload:
        return 0.0
    return float(payload.get("exp", 0))


def _login_via_api() -> Optional[str]:
    """
    Login langsung ke Stockbit API menggunakan STOCKBIT_USERNAME + STOCKBIT_PASSWORD.
    Tidak butuh Playwright, tidak butuh session file, tidak butuh Redis.

    Membaca dari environment variables:
        STOCKBIT_USERNAME  : username/email akun Stockbit
        STOCKBIT_PASSWORD  : password akun Stockbit
        STOCKBIT_PLAYER_ID : device ID (opsional, bisa kosong)

    Retry policy:
        - 429 Too Many Requests : tunggu 10s lalu coba lagi (max 3x total)
        - Timeout / network err : coba lagi sekali setelah 3s
        - 4xx lain (401, 403)   : tidak retry, credentials salah

    Returns:
        JWT access token string jika berhasil, None jika semua upaya gagal.
    """
    global _token_cache, _token_cache_expiry

    # Gunakan cache jika masih valid — tidak login ulang tiap pemanggilan
    if _token_cache and time.time() < _token_cache_expiry and _is_token_valid(_token_cache):
        log.debug("🔄 Token dari cache in-memory")
        return _token_cache

    username  = os.environ.get("STOCKBIT_USERNAME", "").strip()
    password  = os.environ.get("STOCKBIT_PASSWORD", "").strip()
    player_id = os.environ.get("STOCKBIT_PLAYER_ID", "").strip()

    if not username or not password:
        log.warning("⚠️  STOCKBIT_USERNAME/PASSWORD tidak diset — tidak bisa login API")
        return None

    log.info(f"🔑 Login Stockbit API sebagai '{username[:3]}***'...")

    body = {
        "player_id": player_id or "",
        "user":      username,
        "password":  password,
    }

    # Retry loop: max 3 attempt, backoff pada 429
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.post(
                _SB_LOGIN_URL,
                json=body,
                headers=_SB_LOGIN_HEADERS,
                timeout=30,
            )
        except requests.exceptions.Timeout:
            log.warning(f"⚠️  Login API timeout (attempt {attempt}/{max_attempts})")
            if attempt < max_attempts:
                time.sleep(3)
            continue
        except requests.exceptions.ConnectionError as e:
            log.warning(f"⚠️  Login API connection error (attempt {attempt}): {e}")
            if attempt < max_attempts:
                time.sleep(3)
            continue
        except Exception as e:
            log.warning(f"⚠️  Login API request error (attempt {attempt}): {e}")
            if attempt < max_attempts:
                time.sleep(3)
            continue

        # --- 429: Rate limited — tunggu lalu retry ---
        if r.status_code == 429:
            wait = 15 * attempt   # 15s, 30s, 45s
            log.warning(
                f"⚠️  Login API 429 rate-limit (attempt {attempt}/{max_attempts}) "
                f"— tunggu {wait}s..."
            )
            if attempt < max_attempts:
                time.sleep(wait)
            continue

        # --- 4xx selain 429: credentials salah, tidak perlu retry ---
        if 400 <= r.status_code < 500:
            log.warning(f"⚠️  Login API HTTP {r.status_code}: {r.text[:200]}")
            return None

        # --- 5xx: server error, coba lagi ---
        if r.status_code >= 500:
            log.warning(f"⚠️  Login API HTTP {r.status_code} (attempt {attempt}) — retry...")
            if attempt < max_attempts:
                time.sleep(5)
            continue

        # --- 200: parse response ---
        try:
            data = r.json()
        except Exception:
            log.warning("⚠️  Login API response bukan JSON valid")
            return None

        # Path token: data → login → token_data → access → token
        token = (
            data
            .get("data", {})
            .get("login", {})
            .get("token_data", {})
            .get("access", {})
            .get("token", "")
        )

        if not token:
            top_keys = list(data.get("data", data).keys())[:8]
            log.warning(f"⚠️  Token tidak ditemukan di response. Keys: {top_keys}")
            log.debug(f"   Response (truncated): {r.text[:500]}")
            return None

        if not _is_token_valid(token):
            log.warning(f"⚠️  Token diterima tapi tidak valid/expired (len={len(token)})")
            return None

        # Hitung sisa waktu token untuk logging
        exp_ts = _get_token_exp_ts(token)
        remaining_hours = (exp_ts - time.time()) / 3600

        # Cache 23 jam (token Stockbit 24 jam, buffer 1 jam untuk safety)
        _token_cache        = token
        _token_cache_expiry = time.time() + (23 * 3600)

        log.info(
            f"✅ Login API berhasil — token valid {remaining_hours:.1f} jam "
            f"({len(token)} karakter)"
        )
        return token

    log.warning(f"⚠️  Login API gagal setelah {max_attempts} attempt")
    return None


def invalidate_token_cache():
    """
    Paksa login ulang pada pemanggilan get_valid_token() berikutnya.
    Berguna jika mendapat HTTP 401 di tengah proses screening.
    """
    global _token_cache, _token_cache_expiry
    _token_cache        = None
    _token_cache_expiry = 0.0
    log.info("🔄 Token cache dihapus — akan login ulang saat dipanggil lagi")


def get_valid_token() -> Tuple[Optional[str], str]:
    """
    Ambil token yang valid. Return (token, mode).
    mode: "FULL_STOCKBIT" | "YAHOO_ONLY"

    Urutan lookup:
    1. Cache in-memory          (jika belum expired — tidak login ulang)
    2. Env STOCKBIT_BEARER_TOKEN (manual override / legacy)
    3. Login API langsung       (STOCKBIT_USERNAME + STOCKBIT_PASSWORD) ← utama
    4. YAHOO_ONLY               (fallback jika semua gagal)
    """
    # --- 1. Env var override (legacy / manual inject) ---
    env_token = os.environ.get("STOCKBIT_BEARER_TOKEN", "").strip()
    if env_token and _is_token_valid(env_token):
        log.info("✅ Token dari env STOCKBIT_BEARER_TOKEN — FULL STOCKBIT MODE")
        return env_token, "FULL_STOCKBIT"

    # --- 2 & 3. Direct API login (dengan in-memory cache) ---
    api_token = _login_via_api()
    if api_token:
        log.info("✅ Token dari API login — FULL STOCKBIT MODE")
        return api_token, "FULL_STOCKBIT"

    # --- Fallback ---
    log.warning("⚠️ Semua metode auth gagal → YAHOO ONLY MODE")
    return None, "YAHOO_ONLY"


def _make_sb_headers(token: str) -> Dict:
    """
    Header HTTP lengkap untuk request ke Stockbit API.
    Meliputi sec-fetch-* dan sec-ch-ua untuk bypass basic anti-bot check.
    """
    return {
        "accept":             "application/json",
        "accept-language":    "en-US,en;q=0.9",
        "authorization":      f"Bearer {token}",
        "origin":             "https://stockbit.com",
        "priority":           "u=1, i",
        "referer":            "https://stockbit.com/",
        "sec-ch-ua":          '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-site",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/147.0.0.0 Safari/537.36"
        ),
    }


def sb_get(endpoint: str, token: str, params=None) -> Optional[Dict]:
    """
    Helper GET ke Stockbit API dengan retry.
    params bisa berupa Dict atau List[Tuple] untuk mendukung duplicate keys
    (misalnya multi-value filter_stocks).
    """
    url = f"{CONFIG['SB_BASE']}{endpoint}"
    headers = _make_sb_headers(token)

    for attempt in range(CONFIG["SB_RETRY"]):
        try:
            time.sleep(CONFIG["SB_DELAY"] + (attempt * 0.5))
            r = requests.get(url, headers=headers, params=params, timeout=CONFIG["SB_TIMEOUT"])
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 401:
                log.warning(f"401 Unauthorized pada {endpoint}")
                return None
            elif r.status_code == 429:
                log.warning(f"429 Rate limit, tunggu {5 * (attempt + 1)}s...")
                time.sleep(5 * (attempt + 1))
            else:
                log.debug(f"HTTP {r.status_code} pada {endpoint}")
        except requests.exceptions.Timeout:
            log.warning(f"Timeout pada {endpoint} (attempt {attempt + 1})")
        except Exception as e:
            log.warning(f"Error GET {endpoint}: {e}")

    return None

# =============================================================================
# STEP 1: MARKET CONTEXT (IHSG)
# =============================================================================

def get_ihsg_context() -> Dict:
    """
    Download IHSG dan analisis kondisi pasar.
    Return dict: ihsg_close, ihsg_change_pct, ihsg_trend,
                 ihsg_above_ma50, market_safe, warning
    """
    log.info("📊 Mengambil data IHSG...")
    try:
        df = yf.download(CONFIG["IHSG_TICKER"], period="3mo", interval="1d",
                         auto_adjust=True, progress=False)
        if df.empty or len(df) < 10:
            log.warning("Data IHSG kosong, lanjut tanpa market filter")
            return {"market_safe": True, "warning": None, "ihsg_trend": "UNKNOWN",
                    "ihsg_close": 0, "ihsg_change_pct": 0, "ihsg_above_ma50": True}

        df.reset_index(inplace=True)
        df.columns = [c.lower() if isinstance(c, str) else c[0].lower() for c in df.columns]

        close = df["close"].values
        current = float(close[-1])
        prev    = float(close[-2]) if len(close) >= 2 else current
        change_pct = ((current - prev) / prev * 100) if prev > 0 else 0

        # 3 hari berturut-turut turun
        three_day_down = (
            len(close) >= 4
            and close[-1] < close[-2]
            and close[-2] < close[-3]
            and close[-3] < close[-4]
        )

        # MA50
        ma50_series = pd.Series(close).rolling(50).mean()
        ma50 = float(ma50_series.iloc[-1]) if not pd.isna(ma50_series.iloc[-1]) else current
        above_ma50 = current > ma50

        # Slope MA50 (5 hari terakhir)
        ma50_prev = float(ma50_series.iloc[-6]) if len(ma50_series) >= 6 and not pd.isna(ma50_series.iloc[-6]) else ma50
        slope_positive = ma50 > ma50_prev

        if current > ma50 and slope_positive:
            trend = "BULLISH"
        elif current > ma50:
            trend = "NEUTRAL"
        else:
            trend = "BEARISH"

        warning = None
        market_safe = True

        if three_day_down and CONFIG["IHSG_3DAY_DOWN_STOP"]:
            log.warning("🛑 IHSG turun 3 hari berturut — screening dihentikan")
            market_safe = False
            warning = "IHSG turun 3 hari berturut-turut. Tidak ada output hari ini."
        elif change_pct <= CONFIG["IHSG_1DAY_DOWN_WARNING"]:
            warning = f"⚠️ IHSG turun {change_pct:.1f}% kemarin. Trading dengan sangat hati-hati."
            log.warning(warning)

        log.info(f"IHSG: {current:,.0f} | Δ {change_pct:+.2f}% | Trend: {trend} | Safe: {market_safe}")

        return {
            "ihsg_close": int(current),
            "ihsg_change_pct": round(change_pct, 2),
            "ihsg_trend": trend,
            "ihsg_above_ma50": above_ma50,
            "market_safe": market_safe,
            "warning": warning,
        }

    except Exception as e:
        log.warning(f"Error IHSG context: {e}")
        return {"market_safe": True, "warning": None, "ihsg_trend": "UNKNOWN",
                "ihsg_close": 0, "ihsg_change_pct": 0, "ihsg_above_ma50": True}


# =============================================================================
# STEP 2: UNIVERSE FILTER
# =============================================================================

def get_universe_mode_a(token: str) -> List[Dict]:
    """
    Ambil universe dari 3 sumber dan gabungkan:
    1. Market Mover Stockbit (TOP_VOLUME + TOP_VALUE + NET_FOREIGN_BUY)
    2. Top Gainer hari ini (kandidat momentum besok)
    3. Screener Volume Explosion (volume spike = akumulasi dini)

    Deduplication berdasarkan ticker — saham yang muncul di beberapa
    sumber mendapat flag tambahan dan bobot lebih tinggi di scoring.
    """
    log.info("🎯 Universe MODE A: Market Mover + Gainer + Loser + Screener + Guru...")

    mover_types = [
        "MOVER_TYPE_TOP_VOLUME",
        "MOVER_TYPE_TOP_VALUE",
        "MOVER_TYPE_NET_FOREIGN_BUY",
    ]

    all_stocks: Dict[str, Dict] = {}

    # --- Sumber 1: Market Mover ---
    for mtype in mover_types:
        params_list = [("mover_type", mtype)]
        for board in CONFIG["MM_BOARDS"]:
            params_list.append(("filter_stocks", board))

        data = sb_get("/order-trade/market-mover", token, params_list)
        if not data:
            log.warning(f"Gagal ambil {mtype}")
            continue

        movers = data.get("data", {}).get("mover_list", [])
        log.info(f"  {mtype}: {len(movers)} saham")

        for item in movers:
            code = item.get("stock_detail", {}).get("code", "")
            if not code:
                continue

            notations = item.get("stock_detail", {}).get("notations", [])
            notation_codes = [n.get("code", "") for n in notations]
            if "X" in notation_codes:
                continue

            value_raw = item.get("value", {}).get("raw", 0)
            freq_raw  = item.get("frequency", {}).get("raw", 0)

            if value_raw < CONFIG["MM_VALUE_MIN"]:
                continue
            if freq_raw < CONFIG["MM_FREQ_MIN"]:
                continue

            net_fb = item.get("net_foreign_buy", {}).get("raw", 0)
            net_fs = item.get("net_foreign_sell", {}).get("raw", 0)
            net_foreign = net_fb - net_fs

            if code not in all_stocks:
                all_stocks[code] = {
                    "ticker": code,
                    "name": item.get("stock_detail", {}).get("name", code),
                    "price": item.get("price", 0),
                    "change_pct": item.get("change", {}).get("percentage", 0),
                    "value_today": value_raw,
                    "frequency_today": freq_raw,
                    "net_foreign_today": net_foreign,
                    "iep": item.get("iepiev_detail", {}).get("iep", {}).get("raw", 0),
                    "iep_change_pct": item.get("iepiev_detail", {}).get("iep_change", {}).get("raw", 0),
                    "in_mover_types": [mtype],
                    "from_gainer": False,
                    "from_screener": False,
                    "vol_ratio_screener": 0,
                }
            else:
                if mtype not in all_stocks[code]["in_mover_types"]:
                    all_stocks[code]["in_mover_types"].append(mtype)
                if abs(net_foreign) > abs(all_stocks[code]["net_foreign_today"]):
                    all_stocks[code]["net_foreign_today"] = net_foreign

        time.sleep(0.5)

    # --- Sumber 2: Top Gainer ---
    gainer_stocks = get_universe_top_gainer(token)
    for g in gainer_stocks:
        code = g["ticker"]
        if code not in all_stocks:
            g.setdefault("from_screener", False)
            g.setdefault("from_loser", False)
            g.setdefault("from_guru", False)
            g.setdefault("vol_ratio_screener", 0)
            all_stocks[code] = g
        else:
            all_stocks[code]["from_gainer"] = True
            all_stocks[code]["in_mover_types"].append("MOVER_TYPE_TOP_GAINER")
            all_stocks[code]["change_pct"] = g["change_pct"]

    # --- Sumber 3: Top Loser ---
    loser_stocks = get_universe_top_loser(token)
    for l in loser_stocks:
        code = l["ticker"]
        if code not in all_stocks:
            l.setdefault("from_screener", False)
            l.setdefault("from_guru", False)
            l.setdefault("vol_ratio_screener", 0)
            all_stocks[code] = l
        else:
            all_stocks[code]["from_loser"] = True
            all_stocks[code]["in_mover_types"].append("MOVER_TYPE_TOP_LOSER")
            all_stocks[code]["change_pct"] = l["change_pct"]

    # --- Sumber 4: Screener Volume Explosion ---
    screener_stocks = get_universe_screener(token)
    for s in screener_stocks:
        code = s["ticker"]
        if code not in all_stocks:
            s.setdefault("from_gainer", False)
            s.setdefault("from_loser", False)
            s.setdefault("from_guru", False)
            all_stocks[code] = s
        else:
            all_stocks[code]["from_screener"] = True
            all_stocks[code]["vol_ratio_screener"] = s.get("vol_ratio_screener", 0)
            all_stocks[code]["in_mover_types"].append("SCREENER_VOLUME_EXPLOSION")

    # --- Sumber 5: Guru Screener ---
    guru_stocks = get_universe_guru_screener(token)
    for g in guru_stocks:
        code = g["ticker"]
        if code not in all_stocks:
            all_stocks[code] = g
        else:
            all_stocks[code]["from_guru"] = True
            tid = g.get("guru_template_id", "")
            label = g.get("guru_template_name", "")
            all_stocks[code]["in_mover_types"].append(f"GURU_{tid}_{label[:12]}")

    # --- Sumber 6: Top Value (375 saham teratas by value) ---
    top_value_stocks = get_universe_top_value(token)
    for tv in top_value_stocks:
        code = tv["ticker"]
        if code not in all_stocks:
            all_stocks[code] = tv
        else:
            all_stocks[code]["from_top_value"] = True
            all_stocks[code]["in_mover_types"].append("SCREENER_TOP_VALUE")
            # Update value_today kalau lebih akurat dari sumber lain
            if tv.get("value_today", 0) > 0 and all_stocks[code].get("value_today", 0) == 0:
                all_stocks[code]["value_today"] = tv["value_today"]

    result = list(all_stocks.values())
    log.info(f"✅ Universe MODE A: {len(result)} saham unik (MM + Gainer + Loser + Screener + Guru + TopValue)")
    return result


def get_universe_mode_b() -> List[Dict]:
    """
    Universe MODE B: Yahoo Finance 958 saham.
    Filter: value > Rp 5M, harga > Rp 100, data minimal 60 hari.
    """
    log.info("📋 Universe MODE B: Yahoo Finance 958 saham...")

    ticker_file = "kode_saham_958.txt"
    if not Path(ticker_file).exists():
        log.error(f"File {ticker_file} tidak ditemukan!")
        return []

    with open(ticker_file, "r", encoding="utf-8") as f:
        content = f.read()
    raw_codes = re.findall(r"['\"]([A-Z0-9]{4})['\"]", content)
    tickers = [f"{c}.JK" for c in raw_codes]
    log.info(f"Total tickers: {len(tickers)}")

    universe = []
    batch_size = 50

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        batch_str = " ".join(batch)

        try:
            data = yf.download(
                batch_str,
                period="30d",
                interval="1d",
                auto_adjust=True,
                progress=False,
                group_by="ticker",
            )

            for ticker in batch:
                try:
                    if len(batch) == 1:
                        df = data
                    else:
                        if ticker not in data.columns.get_level_values(0):
                            continue
                        df = data[ticker].dropna(how="all")

                    if df is None or len(df) < 5:
                        continue

                    df = df.dropna(subset=["Close", "Volume"])
                    if len(df) < 5:
                        continue

                    last = df.iloc[-1]
                    close = float(last["Close"])
                    volume = float(last["Volume"])
                    value = close * volume

                    if close < CONFIG["YF_MIN_PRICE"]:
                        continue
                    if value < CONFIG["MM_VALUE_MIN"]:
                        continue

                    code = ticker.replace(".JK", "")
                    universe.append({
                        "ticker": code,
                        "name": code,
                        "price": int(close),
                        "change_pct": 0,
                        "value_today": value,
                        "frequency_today": 0,
                        "net_foreign_today": 0,
                        "iep": 0,
                        "iep_change_pct": 0,
                        "in_mover_types": [],
                    })

                except Exception:
                    continue

        except Exception as e:
            log.warning(f"Batch download error: {e}")

        time.sleep(1.0)

    log.info(f"✅ Universe MODE B: {len(universe)} saham lolos filter dasar")
    return universe


def get_universe_top_gainer(token: str) -> List[Dict]:
    """
    Ambil Top Gainer hari ini sebagai kandidat tambahan universe.

    Logika: saham yang naik signifikan hari ini (5-24.9%) dengan likuiditas
    cukup adalah kandidat momentum yang mungkin berlanjut atau pullback
    ke zona entry besok.

    Dikombinasikan dengan Market Mover untuk memperluas universe.
    Saham yang sudah di-skip karena notasi X tetap di-skip.
    """
    if not CONFIG.get("GAINER_INCLUDE", True):
        return []

    log.info("📈 Ambil Top Gainer sebagai kandidat tambahan...")

    params_list = [("mover_type", "MOVER_TYPE_TOP_GAINER")]
    for board in CONFIG["MM_BOARDS"]:
        params_list.append(("filter_stocks", board))
    # Tambah Special Monitoring agar lebih lengkap tapi tetap filter X
    params_list.append(("filter_stocks", "FILTER_STOCKS_TYPE_SPECIAL_MONITORING_BOARD"))

    data = sb_get("/order-trade/market-mover", token, params_list)
    if not data:
        log.warning("Gagal ambil Top Gainer")
        return []

    movers = data.get("data", {}).get("mover_list", [])
    result = []

    for item in movers:
        code = item.get("stock_detail", {}).get("code", "")
        if not code:
            continue

        # Filter notasi berbahaya — TETAP ketat
        notations = item.get("stock_detail", {}).get("notations", [])
        notation_codes = [n.get("code", "") for n in notations]
        if "X" in notation_codes:
            continue

        change_pct = item.get("change", {}).get("percentage", 0)
        value_raw  = item.get("value", {}).get("raw", 0)
        freq_raw   = item.get("frequency", {}).get("raw", 0)

        # Filter: harus naik dalam range yang wajar
        if change_pct < CONFIG["GAINER_MIN_CHANGE_PCT"]:
            continue
        if change_pct > CONFIG["GAINER_MAX_CHANGE_PCT"]:
            continue  # Sudah terlalu tinggi, risiko profit taking besok

        # Filter likuiditas (lebih longgar dari MM biasa)
        if value_raw < CONFIG["GAINER_MIN_VALUE"]:
            continue
        if freq_raw < CONFIG["GAINER_MIN_FREQ"]:
            continue

        net_fb = item.get("net_foreign_buy", {}).get("raw", 0)
        net_fs = item.get("net_foreign_sell", {}).get("raw", 0)
        net_foreign = net_fb - net_fs

        result.append({
            "ticker": code,
            "name": item.get("stock_detail", {}).get("name", code),
            "price": item.get("price", 0),
            "change_pct": change_pct,
            "value_today": value_raw,
            "frequency_today": freq_raw,
            "net_foreign_today": net_foreign,
            "iep": item.get("iepiev_detail", {}).get("iep", {}).get("raw", 0),
            "iep_change_pct": item.get("iepiev_detail", {}).get("iep_change", {}).get("raw", 0),
            "in_mover_types": ["MOVER_TYPE_TOP_GAINER"],
            "from_gainer": True,   # Flag untuk tracking
        })

    log.info(f"  Top Gainer kandidat: {len(result)} saham lolos filter")
    return result


def get_universe_top_loser(token: str) -> List[Dict]:
    """
    Ambil Top Loser hari ini sebagai kandidat tambahan universe.

    Logika: saham yang turun signifikan (-3% s/d -24.9%) dengan likuiditas
    cukup = kandidat reversal / oversold bounce besok.
    Tetap harus lolos filter accumulation, trend, dan SMC.
    """
    if not CONFIG.get("LOSER_INCLUDE", True):
        return []

    log.info("📉 Ambil Top Loser sebagai kandidat tambahan...")

    params_list = [("mover_type", "MOVER_TYPE_TOP_LOSER")]
    for board in CONFIG["MM_BOARDS"]:
        params_list.append(("filter_stocks", board))
    params_list.append(("filter_stocks", "FILTER_STOCKS_TYPE_SPECIAL_MONITORING_BOARD"))

    data = sb_get("/order-trade/market-mover", token, params_list)
    if not data:
        log.warning("Gagal ambil Top Loser")
        return []

    movers = data.get("data", {}).get("mover_list", [])
    result = []

    for item in movers:
        code = item.get("stock_detail", {}).get("code", "")
        if not code:
            continue

        notations = item.get("stock_detail", {}).get("notations", [])
        notation_codes = [n.get("code", "") for n in notations]
        if "X" in notation_codes:
            continue

        change_pct = item.get("change", {}).get("percentage", 0)
        value_raw  = item.get("value", {}).get("raw", 0)
        freq_raw   = item.get("frequency", {}).get("raw", 0)

        if change_pct > CONFIG["LOSER_MAX_CHANGE_PCT"]:
            continue  # Belum cukup turun
        if change_pct < CONFIG["LOSER_MIN_CHANGE_PCT"]:
            continue  # ARB — terlalu dalam

        if value_raw < CONFIG["LOSER_MIN_VALUE"]:
            continue
        if freq_raw < CONFIG["LOSER_MIN_FREQ"]:
            continue

        net_fb = item.get("net_foreign_buy", {}).get("raw", 0)
        net_fs = item.get("net_foreign_sell", {}).get("raw", 0)
        net_foreign = net_fb - net_fs

        result.append({
            "ticker":            code,
            "name":              item.get("stock_detail", {}).get("name", code),
            "price":             item.get("price", 0),
            "change_pct":        change_pct,
            "value_today":       value_raw,
            "frequency_today":   freq_raw,
            "net_foreign_today": net_foreign,
            "iep":               item.get("iepiev_detail", {}).get("iep", {}).get("raw", 0),
            "iep_change_pct":    item.get("iepiev_detail", {}).get("iep_change", {}).get("raw", 0),
            "in_mover_types":    ["MOVER_TYPE_TOP_LOSER"],
            "from_loser":        True,
            "from_gainer":       False,
            "from_screener":     False,
            "from_guru":         False,
            "vol_ratio_screener": 0,
        })

    log.info(f"  Top Loser kandidat: {len(result)} saham lolos filter")
    return result


def get_universe_screener(token: str) -> List[Dict]:
    """
    Ambil universe dari Stockbit Screener — Volume Explosion:
        Volume hari ini > VolMA5 × 2  DAN  VolMA5 > VolMA20 × 1.5

    Pagination: ambil sampai SB_SCREENER_MAX_PAGES page, berhenti jika halaman kosong.
    Endpoint: POST /screener/templates
    """
    if not CONFIG.get("SB_SCREENER_ENABLED", True):
        return []

    log.info("🔍 Ambil universe dari Stockbit Screener (Volume Explosion)...")

    url = f"{CONFIG['SB_BASE']}/screener/templates"
    headers = _make_sb_headers(token)
    headers["content-type"] = "application/json"

    base_payload = {
        "name": "Volume Explosion Auto",
        "description": "",
        "ordercol": 3,
        "ordertype": "desc",
        "filters": json.dumps([
            {
                "item1": 12465,
                "item1_name": "Volume MA 5",
                "item2": "12469",
                "item2_name": "Volume",
                "multiplier": str(CONFIG["SB_SCREENER_VOL_MA5_MULTIPLIER"]),
                "operator": "<",
                "type": "compare"
            },
            {
                "item1": 12464,
                "item1_name": "Volume MA 20",
                "item2": "12465",
                "item2_name": "Volume MA 5",
                "multiplier": str(CONFIG["SB_SCREENER_VOL_MA5_MA20_MULT"]),
                "operator": "<",
                "type": "compare"
            }
        ]),
        "universe": json.dumps({"scope": "IHSG", "scopeID": "0", "name": "IHSG"}),
        "sequence": "12465,12469,12464",
        "save": "0",
        "screenerid": CONFIG.get("SB_SCREENER_ID", ""),
        "type": "TEMPLATE_TYPE_CUSTOM",
    }

    seen_codes: set = set()
    result = []
    max_pages = CONFIG.get("SB_SCREENER_MAX_PAGES", 3)

    for page in range(1, max_pages + 1):
        payload = {**base_payload, "page": page}
        try:
            time.sleep(CONFIG["SB_DELAY"])
            r = requests.post(
                url, headers=headers,
                data=json.dumps(payload),
                timeout=CONFIG["SB_TIMEOUT"]
            )
            if r.status_code != 200:
                log.warning(f"Screener API HTTP {r.status_code} (page {page})")
                break
            data = r.json()
            calcs = data.get("data", {}).get("calcs", [])
        except Exception as e:
            log.warning(f"Screener API error (page {page}): {e}")
            break

        if not calcs:
            break

        page_count = 0
        for item in calcs:
            company = item.get("company", {})
            code = company.get("symbol", "")
            if not code or code in seen_codes:
                continue

            vol_ma5 = 0
            vol_today = 0
            for r_item in item.get("results", []):
                if r_item.get("id") == 12465:
                    vol_ma5 = float(r_item.get("raw", 0))
                elif r_item.get("id") == 12469:
                    vol_today = float(r_item.get("raw", 0))

            if vol_today <= 0 or vol_ma5 <= 0:
                continue

            vol_ratio = vol_today / vol_ma5 if vol_ma5 > 0 else 0

            seen_codes.add(code)
            page_count += 1
            result.append({
                "ticker":            code,
                "name":              company.get("name", code),
                "price":             0,
                "change_pct":        0,
                "value_today":       0,
                "frequency_today":   0,
                "net_foreign_today": 0,
                "iep":               0,
                "iep_change_pct":    0,
                "in_mover_types":    ["SCREENER_VOLUME_EXPLOSION"],
                "vol_ratio_screener": round(vol_ratio, 2),
                "from_screener":     True,
                "from_gainer":       False,
                "from_loser":        False,
                "from_guru":         False,
            })

        log.info(f"  Screener page {page}: {page_count} saham baru")
        if page_count == 0:
            break  # Semua saham di halaman ini sudah duplikat — halaman berikutnya pasti sama

    log.info(f"  Screener Volume Explosion total: {len(result)} saham ({max_pages} page maks)")
    return result


def get_universe_top_value(token: str) -> List[Dict]:
    """
    Ambil universe berdasarkan Top Value — semua saham IHSG diurutkan
    berdasarkan nilai transaksi hari ini (DESC), ambil 15 page (375 saham).

    Logika: saham dengan nilai transaksi terbesar = saham paling liquid
    dan paling banyak diperdagangkan hari ini. Ini memperluas universe
    di luar Market Mover yang hanya ambil top 50 per kategori.

    Filter: Value > 1 (semua saham yang ada transaksi hari ini)
    Endpoint: POST /screener/templates
    """
    if not CONFIG.get("SB_TOP_VALUE_ENABLED", True):
        return []

    log.info("💰 Ambil universe Top Value (15 page)...")

    url = f"{CONFIG['SB_BASE']}/screener/templates"
    headers = _make_sb_headers(token)
    headers["content-type"] = "application/json"

    base_payload = {
        "name": "TOP VALUE BY wILL",
        "description": "",
        "ordercol": 2,
        "ordertype": "desc",
        "filters": json.dumps([
            {
                "item1": 13620,
                "item1_name": "Value",
                "item2": "1",
                "item2_name": "",
                "multiplier": "0",
                "operator": ">",
                "type": "basic"
            }
        ]),
        "universe": json.dumps({"scope": "IHSG", "scopeID": "0", "name": "IHSG"}),
        "sequence": "13620",
        "save": "0",
        "screenerid": "undefined",
        "type": "TEMPLATE_TYPE_CUSTOM",
    }

    seen_codes: set = set()
    result = []
    max_pages = CONFIG.get("SB_TOP_VALUE_MAX_PAGES", 15)

    for page in range(1, max_pages + 1):
        payload = {**base_payload, "page": page}
        try:
            time.sleep(CONFIG["SB_DELAY"])
            r = requests.post(
                url, headers=headers,
                data=json.dumps(payload),
                timeout=CONFIG["SB_TIMEOUT"]
            )
            if r.status_code != 200:
                log.warning(f"Top Value API HTTP {r.status_code} (page {page})")
                break
            data = r.json()
            calcs = data.get("data", {}).get("calcs", [])
        except Exception as e:
            log.warning(f"Top Value API error (page {page}): {e}")
            break

        if not calcs:
            log.debug(f"  Top Value page {page}: kosong — berhenti")
            break

        page_count = 0
        for item in calcs:
            company = item.get("company", {})
            code = company.get("symbol", "")
            if not code or code in seen_codes:
                continue

            # Ambil value dari results
            value_today = 0
            for r_item in item.get("results", []):
                if r_item.get("id") == 13620:
                    value_today = float(r_item.get("raw", 0))

            if value_today <= 0:
                continue

            seen_codes.add(code)
            page_count += 1
            result.append({
                "ticker":            code,
                "name":              company.get("name", code),
                "price":             0,
                "change_pct":        0,
                "value_today":       value_today,
                "frequency_today":   0,
                "net_foreign_today": 0,
                "iep":               0,
                "iep_change_pct":    0,
                "in_mover_types":    ["SCREENER_TOP_VALUE"],
                "vol_ratio_screener": 0,
                "from_screener":     False,
                "from_gainer":       False,
                "from_loser":        False,
                "from_guru":         False,
                "from_top_value":    True,
            })

        log.debug(f"  Top Value page {page}: {page_count} saham baru")
        if page_count == 0:
            break  # Semua saham di halaman ini sudah duplikat — halaman berikutnya pasti sama

    log.info(f"  Top Value total: {len(result)} saham ({max_pages} page maks)")
    return result


def get_universe_guru_screener(token: str) -> List[Dict]:
    """
    Ambil universe dari Stockbit Guru Screener — template preset Stockbit.

    Endpoint: GET /screener/templates/{id}?type=TEMPLATE_TYPE_GURU

    Template yang digunakan (dikonfigurasi di CONFIG["SB_GURU_SCREENER_LIST"]):
      92  — Big Accumulation          : Bandar Accum/Dist > 20, value > Rp3M
      77  — Foreign Flow Uptrend      : Net Foreign Buy streak ≥2, FF > FF MA20
      94  — Bandar Bullish Reversal   : Bandar Value naik melewati MA10
      87  — Reversal on Bearish Trend : Price > MA20 > MA10, volume spike
      88  — Potential Reversal        : MA20 > Price > MA10, volume spike
      63  — High Volume Breakout      : Volume > 2x MA20, value > Rp1M
      97  — Frequency Spike           : Frekuensi > 3x rata-rata, sideways
      72  — IHSG Outperformers        : 3M RS Line > 1.1
      78  — Daily Net Foreign Flow    : Net Foreign Buy hari ini (top 25)
      79  — 1 Week Net Foreign Flow   : Net Foreign Buy 1 minggu (top 25)

    Semua hasil digabung dengan dedup berdasarkan ticker.
    Saham tetap harus melewati pipeline filter: liquidity, accumulation, trend, SMC.
    """
    if not CONFIG.get("SB_GURU_SCREENER_ENABLED", True):
        return []

    guru_list = CONFIG.get("SB_GURU_SCREENER_LIST", [])
    if not guru_list:
        return []

    log.info(f"🎓 Ambil Guru Screener ({len(guru_list)} template)...")

    seen_codes: set = set()
    result = []
    total_requests = 0

    for entry in guru_list:
        template_id, label, max_page = entry

        for page in range(1, max_page + 1):
            data = sb_get(
                f"/screener/templates/{template_id}",
                token,
                {"type": "TEMPLATE_TYPE_GURU", "page": page},
            )
            total_requests += 1

            if not data:
                log.warning(f"  Gagal ambil Guru template {template_id} ({label}) page {page}")
                break

            calcs = data.get("data", {}).get("calcs", [])
            if not calcs:
                break   # Halaman kosong — stop pagination template ini

            count = 0
            for item in calcs:
                company = item.get("company", {})
                code = company.get("symbol", "")
                if not code or code in seen_codes:
                    continue

                seen_codes.add(code)
                count += 1
                result.append({
                    "ticker":            code,
                    "name":              company.get("name", code),
                    "price":             0,
                    "change_pct":        0,
                    "value_today":       0,
                    "frequency_today":   0,
                    "net_foreign_today": 0,
                    "iep":               0,
                    "iep_change_pct":    0,
                    "in_mover_types":    [f"GURU_{template_id}"],
                    "vol_ratio_screener": 0,
                    "from_screener":     False,
                    "from_gainer":       False,
                    "from_loser":        False,
                    "from_guru":         True,
                    "guru_template_id":  template_id,
                    "guru_template_name": label,
                })

            log.info(f"  Guru {template_id} ({label}) p{page}: {count} saham baru")
            if count == 0:
                break  # Semua saham di halaman ini sudah duplikat — halaman berikutnya pasti sama

    log.info(f"  Guru Screener total: {len(result)} saham unik ({total_requests} request)")
    return result


# =============================================================================
# STEP 3: LIQUIDITY QUALITY CHECK (Historical Summary Stockbit)
# =============================================================================

def check_liquidity_quality(ticker: str, token: str) -> Tuple[bool, Optional[Dict]]:
    """
    Cek konsistensi likuiditas 20 hari via Historical Summary.
    Return (lolos: bool, hist_data: dict dengan PDH/PDL/PDC + foreign 20 hari)
    """
    data = sb_get(
        f"/company-price-feed/historical/summary/{ticker}",
        token,
        {"period": "HS_PERIOD_DAILY", "limit": 20},
    )
    if not data:
        return False, None

    result = data.get("data", {}).get("result", [])
    if len(result) < 5:
        return False, None

    # Hari kemarin = index 0 (newest first dari API)
    yesterday = result[0]
    pdh = yesterday.get("high", 0)
    pdl = yesterday.get("low", 0)
    pdc = yesterday.get("close", 0)

    if pdh <= 0 or pdl <= 0 or pdc <= 0:
        return False, None

    # Cek konsistensi value 10 hari
    for i, day in enumerate(result[:10]):
        val = day.get("value", 0)
        if val < CONFIG["LIQ_VALUE_MIN_PER_DAY"]:
            return False, None

    # Cek average frequency
    avg_freq = sum(d.get("frequency", 0) for d in result[:10]) / 10
    if avg_freq < CONFIG["LIQ_FREQ_MIN_AVG"]:
        return False, None

    # Cek range harian (proxy spread)
    ranges = []
    for day in result[:10]:
        h = day.get("high", 0)
        l = day.get("low", 0)
        c = day.get("close", 1)
        if h > 0 and l > 0 and c > 0:
            ranges.append((h - l) / c)

    if not ranges:
        return False, None

    avg_range = sum(ranges) / len(ranges)
    if avg_range < CONFIG["LIQ_RANGE_MIN_PCT"] or avg_range > CONFIG["LIQ_RANGE_MAX_PCT"]:
        return False, None

    # Foreign flow 20 hari
    foreign_history = []
    for day in result:
        foreign_history.append({
            "date": day.get("date", ""),
            "close": day.get("close", 0),
            "high": day.get("high", 0),
            "low": day.get("low", 0),
            "open": day.get("open", 0),
            "volume": day.get("volume", 0),
            "frequency": day.get("frequency", 0),
            "value": day.get("value", 0),
            "foreign_buy": day.get("foreign_buy", 0),
            "foreign_sell": day.get("foreign_sell", 0),
            "net_foreign": day.get("net_foreign", 0),
            "change_pct": day.get("change_percentage", 0),
        })

    # PDTypical
    pd_typical = (pdh + pdl + pdc) / 3

    return True, {
        "pdh": pdh,
        "pdl": pdl,
        "pdc": pdc,
        "pd_typical": pd_typical,
        "avg_range_pct": avg_range,
        "avg_freq": avg_freq,
        "foreign_history": foreign_history,
    }


# =============================================================================
# STEP 4: ACCUMULATION SIGNAL
# =============================================================================

def get_broker_signal(ticker: str, token: str) -> Tuple[str, int]:
    """
    Ambil broker signal (bandar detector) dari Stockbit.
    Return (signal_label, score)
    signal_label: Big Acc | Acc | Neutral | Dist | Big Dist
    """
    data = sb_get(
        f"/marketdetectors/{ticker}",
        token,
        {
            "transaction_type": "TRANSACTION_TYPE_NET",
            "market_board": "MARKET_BOARD_REGULER",
            "investor_type": "INVESTOR_TYPE_ALL",
            "limit": 25,
        },
    )
    if not data:
        return "Neutral", 5

    try:
        # Coba beberapa path yang mungkin dari Stockbit API
        # Path 1 (dokumentasi awal): data.bandar_detector.avg.accdist
        accdist = (
            data.get("data", {})
            .get("bandar_detector", {})
            .get("avg", {})
            .get("accdist", "")
        )

        # Path 2: data.result.bandar_detector.avg.accdist
        if not accdist:
            accdist = (
                data.get("data", {})
                .get("result", {})
                .get("bandar_detector", {})
                .get("avg", {})
                .get("accdist", "")
            )

        # Path 3: data.bandar_detector.accdist (tanpa avg)
        if not accdist:
            accdist = (
                data.get("data", {})
                .get("bandar_detector", {})
                .get("accdist", "")
            )

        # Path 4: data.accdist langsung
        if not accdist:
            accdist = data.get("data", {}).get("accdist", "")

        if not accdist:
            # Log struktur response untuk debug jika semua path gagal
            top_keys = list((data.get("data") or data).keys())[:6]
            log.debug(f"  [{ticker}] broker response keys: {top_keys} — default Neutral")
            accdist = "Neutral"

    except Exception as e:
        log.debug(f"  [{ticker}] broker signal parse error: {e}")
        accdist = "Neutral"

    score_map = {
        "Big Acc": 35,
        "Acc":     20,
        "Neutral": 5,
        "Dist":    -999,   # SKIP
        "Big Dist":-999,   # SKIP
    }
    score = score_map.get(accdist, 5)
    log.debug(f"  [{ticker}] broker signal: {accdist} → score {score}")
    return accdist, score


def calculate_accumulation_score(
    stock_mm: Dict,
    hist_data: Dict,
    broker_signal: str,
    broker_score: int,
    mode: str,
) -> Tuple[int, Dict]:
    """
    Hitung total skor akumulasi.
    Return (total_score, breakdown_dict)
    """
    breakdown = {
        "broker": broker_score,
        "foreign_1d": 0,
        "foreign_3d": 0,
        "candle": 0,
        "volume": 0,
        "total": broker_score,
    }

    foreign_hist = hist_data.get("foreign_history", [])

    if mode == "FULL_STOCKBIT":
        # Foreign flow 1 hari kemarin
        if foreign_hist:
            net_1d = foreign_hist[0].get("net_foreign", 0)
            if net_1d > 0:
                breakdown["foreign_1d"] = 15
            elif net_1d < 0:
                breakdown["foreign_1d"] = -10

        # Foreign flow konsistensi 3 hari
        if len(foreign_hist) >= 3:
            net_3d = [foreign_hist[i].get("net_foreign", 0) for i in range(3)]
            if all(n > 0 for n in net_3d):
                breakdown["foreign_3d"] = 10
            elif all(n < 0 for n in net_3d):
                breakdown["foreign_3d"] = -15

        # Candle quality kemarin
        if foreign_hist:
            d = foreign_hist[0]
            h = d.get("high", 0)
            l = d.get("low", 0)
            c = d.get("close", 0)
            if h > 0 and l > 0 and c > 0 and (h - l) > 0:
                body_pos = (c - l) / (h - l)
                if body_pos > 0.6:
                    breakdown["candle"] = 10
                elif body_pos < 0.4:
                    breakdown["candle"] = -5

        # Volume ratio
        if len(foreign_hist) >= 20:
            vol_20 = [d.get("volume", 0) for d in foreign_hist[:20]]
            avg_vol = sum(vol_20) / len(vol_20) if vol_20 else 1
            last_vol = foreign_hist[0].get("volume", 0)
            vol_ratio = last_vol / avg_vol if avg_vol > 0 else 0
            if vol_ratio >= 2.0:
                breakdown["volume"] = 10
            elif vol_ratio >= 1.5:
                breakdown["volume"] = 5
            elif vol_ratio < 1.0:
                breakdown["volume"] = -5

    else:  # MODE B — Yahoo only
        # Net foreign dari market mover (hari ini)
        net_today = stock_mm.get("net_foreign_today", 0)
        if net_today > 0:
            breakdown["foreign_1d"] = 15
        elif net_today < 0:
            breakdown["foreign_1d"] = -10

        # Candle + volume dari historical summary
        if foreign_hist:
            d = foreign_hist[0]
            h = d.get("high", 0)
            l = d.get("low", 0)
            c = d.get("close", 0)
            if h > 0 and l > 0 and c > 0 and (h - l) > 0:
                body_pos = (c - l) / (h - l)
                if body_pos > 0.6:
                    breakdown["candle"] = 6
                elif body_pos < 0.4:
                    breakdown["candle"] = -5

            # Volume ratio dari Yahoo daily data
            if len(foreign_hist) >= 20:
                vol_20 = [x.get("volume", 0) for x in foreign_hist[:20]]
                avg_vol = sum(vol_20) / len(vol_20) if vol_20 else 1
                last_vol = foreign_hist[0].get("volume", 0)
                vol_ratio = last_vol / avg_vol if avg_vol > 0 else 0
                if vol_ratio >= 2.0:
                    breakdown["volume"] = 10
                elif vol_ratio >= 1.5:
                    breakdown["volume"] = 5
                elif vol_ratio < 1.0:
                    breakdown["volume"] = -5

    total = sum(breakdown.values()) - breakdown["total"] + breakdown["total"]
    total = (
        breakdown["broker"]
        + breakdown["foreign_1d"]
        + breakdown["foreign_3d"]
        + breakdown["candle"]
        + breakdown["volume"]
    )
    breakdown["total"] = total

    return total, breakdown


# =============================================================================
# STEP 5: TREND CONTEXT (Yahoo Finance Daily)
# =============================================================================

def _get_yf_cache_path(ticker: str, tf: str) -> str:
    return os.path.join(CONFIG["DATA_DIR"], f"{ticker.replace('.JK','').replace('^','_')}_{tf}.parquet")


def _load_yf_cache(ticker: str, tf: str) -> Optional[pd.DataFrame]:
    path = _get_yf_cache_path(ticker, tf)
    if not os.path.exists(path):
        return None
    try:
        mtime = os.path.getmtime(path)
        age_days = (time.time() - mtime) / 86400
        if age_days > CONFIG["YF_CACHE_DAYS"]:
            return None
        df = pd.read_parquet(path)
        if len(df) < 20:
            return None
        return df
    except Exception:
        return None


def _save_yf_cache(ticker: str, tf: str, df: pd.DataFrame):
    try:
        path = _get_yf_cache_path(ticker, tf)
        df.to_parquet(path, index=False)
    except Exception:
        pass


def get_daily_data(ticker: str) -> Optional[pd.DataFrame]:
    """Download atau load dari cache OHLCV daily Yahoo Finance."""
    cached = _load_yf_cache(ticker, "1d")
    if cached is not None:
        return cached

    yf_ticker = f"{ticker}.JK" if not ticker.endswith(".JK") and not ticker.startswith("^") else ticker
    try:
        df = yf.download(yf_ticker, period=CONFIG["YF_PERIOD_DAILY"], interval="1d",
                         auto_adjust=True, progress=False)
        if df.empty or len(df) < 20:
            return None

        df.reset_index(inplace=True)

        # Flatten MultiIndex columns (yfinance >= 0.2.x mengembalikan (field, ticker))
        if isinstance(df.columns, pd.MultiIndex):
            # Ambil level 0 saja (field name), buang level ticker
            df.columns = [c[0].lower() for c in df.columns]
        else:
            df.columns = [c.lower() for c in df.columns]

        # Normalize column name: 'datetime' → 'date'
        if "datetime" in df.columns and "date" not in df.columns:
            df.rename(columns={"datetime": "date"}, inplace=True)
        if "price date" in df.columns:
            df.rename(columns={"price date": "date"}, inplace=True)

        # Pastikan kolom yang dibutuhkan ada
        required = ["close", "volume"]
        for col in required:
            if col not in df.columns:
                log.debug(f"YF daily: kolom '{col}' tidak ada untuk {ticker}")
                return None

        df = df.dropna(subset=["close", "volume"])
        df = df[df["volume"] > 0].reset_index(drop=True)

        _save_yf_cache(ticker, "1d", df)
        return df

    except Exception as e:
        log.debug(f"YF daily download gagal {ticker}: {e}")
        return None


# ── Cache in-memory weekly & monthly ─────────────────────────────────────
_weekly_cache:  Dict[str, Optional[pd.DataFrame]] = {}
_monthly_cache: Dict[str, Optional[pd.DataFrame]] = {}

_WEEKLY_CHUNK  = 50
_MONTHLY_CHUNK = 50
_BULK_SLEEP    = 2.0


def _normalize_yf_df(df: pd.DataFrame, is_intraday: bool = False) -> Optional[pd.DataFrame]:
    """Standarisasi DataFrame yfinance: flatten MultiIndex, lowercase kolom, rename date."""
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0].lower() for col in df.columns]
    else:
        df.columns = [c.lower() for c in df.columns]
    df = df.reset_index()
    date_candidates = [c for c in df.columns if "date" in c or "datetime" in c]
    if not date_candidates:
        return None
    df.rename(columns={date_candidates[0]: "date"}, inplace=True)
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    return df if not df.empty else None


def get_weekly_data(ticker: str) -> Optional[pd.DataFrame]:
    """
    Fetch data OHLCV weekly untuk 1 ticker.
    Urutan lookup: 1) in-memory cache, 2) parquet disk cache, 3) download dari Yahoo.
    Parquet cache di-populate oleh ingest_universe.py (lintas proses).
    """
    # 1. In-memory cache (cepat, hanya valid dalam 1 proses)
    if ticker in _weekly_cache:
        return _weekly_cache[ticker]

    # 2. Parquet disk cache (di-populate oleh ingest_universe.py)
    cached = _load_yf_cache(ticker, "1wk")
    if cached is not None:
        _weekly_cache[ticker] = cached
        return cached

    # 3. Fallback: download langsung dari Yahoo Finance
    try:
        yf_ticker = f"{ticker}.JK" if not ticker.endswith(".JK") and not ticker.startswith("^") else ticker
        raw = yf.download(
            tickers=[yf_ticker],
            period="max",
            interval="1wk",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
        )
        if raw is None or raw.empty:
            _weekly_cache[ticker] = None
            return None
        # Extract single ticker dari group_by result
        if isinstance(raw.columns, pd.MultiIndex):
            raw = raw[yf_ticker] if yf_ticker in raw.columns.get_level_values(0) else raw
        df = _normalize_yf_df(raw)
        _weekly_cache[ticker] = df
        if df is not None:
            _save_yf_cache(ticker, "1wk", df)
        return df
    except Exception as e:
        log.debug(f"get_weekly_data {ticker}: {e}")
        _weekly_cache[ticker] = None
        return None


def get_monthly_data(ticker: str) -> Optional[pd.DataFrame]:
    """
    Fetch data OHLCV monthly untuk 1 ticker.
    Urutan lookup: 1) in-memory cache, 2) parquet disk cache, 3) download dari Yahoo.
    Parquet cache di-populate oleh ingest_universe.py (lintas proses).
    """
    # 1. In-memory cache
    if ticker in _monthly_cache:
        return _monthly_cache[ticker]

    # 2. Parquet disk cache (di-populate oleh ingest_universe.py)
    cached = _load_yf_cache(ticker, "1mo")
    if cached is not None:
        _monthly_cache[ticker] = cached
        return cached

    # 3. Fallback: download langsung dari Yahoo Finance
    try:
        yf_ticker = f"{ticker}.JK" if not ticker.endswith(".JK") and not ticker.startswith("^") else ticker
        raw = yf.download(
            tickers=[yf_ticker],
            period="max",
            interval="1mo",
            group_by="ticker",
            auto_adjust=True,
            progress=False,
        )
        if raw is None or raw.empty:
            _monthly_cache[ticker] = None
            return None
        if isinstance(raw.columns, pd.MultiIndex):
            raw = raw[yf_ticker] if yf_ticker in raw.columns.get_level_values(0) else raw
        df = _normalize_yf_df(raw)
        _monthly_cache[ticker] = df
        if df is not None:
            _save_yf_cache(ticker, "1mo", df)
        return df
    except Exception as e:
        log.debug(f"get_monthly_data {ticker}: {e}")
        _monthly_cache[ticker] = None
        return None


def check_trend_context(ticker: str) -> Tuple[bool, Optional[Dict]]:
    """
    Cek trend dengan MA50 Yahoo Finance.
    Return (lolos, trend_dict)
    """
    df = get_daily_data(ticker)
    if df is None or len(df) < 60:
        return False, None

    close = df["close"].values
    current = float(close[-1])

    # MA50
    ma50_series = pd.Series(close).rolling(50).mean()
    if pd.isna(ma50_series.iloc[-1]):
        return False, None

    ma50 = float(ma50_series.iloc[-1])

    # Close > MA50?
    if current <= ma50:
        return False, None

    # MA50 slope positif?
    if len(ma50_series.dropna()) < 6:
        return False, None
    ma50_prev = float(ma50_series.iloc[-6])
    if pd.isna(ma50_prev) or ma50 <= ma50_prev:
        return False, None

    # Gap dari MA50
    gap_pct = (current - ma50) / ma50

    return True, {
        "ma50": round(ma50, 0),
        "gap_pct": round(gap_pct * 100, 2),
        "slope": "POSITIVE",
        "df": df,   # pass forward untuk SMC
    }


# =============================================================================
# STEP 6: SMC STRUCTURE (LuxAlgo Asli - Leg-Based Trailing)
# =============================================================================

def detect_pivots_leg_based(
    highs: np.ndarray,
    lows: np.ndarray,
    length: int
) -> Tuple[List[Dict], List[Dict]]:
    """
    Deteksi pivot HIGH dan LOW menggunakan leg-based trailing,
    sesuai fungsi leg() di Pine Script LuxAlgo.

    Pine Script:
        leg(size) =>
            newLegHigh = high[size] > ta.highest(size)
            newLegLow  = low[size]  < ta.lowest(size)

    Artinya: bar di posisi [size] bar lalu adalah pivot HIGH jika high-nya
    lebih besar dari semua high dalam 'size' bar SETELAHNYA.

    Implementasi Python: scan forward, konfirmasi pivot ketika window 'length'
    bar ke depan tersedia. Tambahan: minimum distance = length bar antar pivot
    sejenis untuk menghindari cluster pivot yang terlalu rapat (noise).
    """
    n = len(highs)
    swing_highs: List[Dict] = []
    swing_lows:  List[Dict] = []

    last_sh_idx = -length  # tracking indeks pivot HIGH terakhir
    last_sl_idx = -length  # tracking indeks pivot LOW terakhir

    for i in range(n - length):
        candidate_high = float(highs[i])
        candidate_low  = float(lows[i])

        future_highs = highs[i + 1 : i + 1 + length]
        future_lows  = lows[i + 1 : i + 1 + length]

        # Pivot HIGH: kandidat > semua high dalam window setelahnya
        # DAN minimum distance 'length' bar dari pivot HIGH sebelumnya
        if (candidate_high > float(np.max(future_highs))
                and (i - last_sh_idx) >= length):
            swing_highs.append({
                "index":   i,
                "price":   candidate_high,
                "crossed": False,
            })
            last_sh_idx = i

        # Pivot LOW: kandidat < semua low dalam window setelahnya
        # DAN minimum distance 'length' bar dari pivot LOW sebelumnya
        if (candidate_low < float(np.min(future_lows))
                and (i - last_sl_idx) >= length):
            swing_lows.append({
                "index":   i,
                "price":   candidate_low,
                "crossed": False,
            })
            last_sl_idx = i

    return swing_highs, swing_lows


def detect_bos_choch(
    closes: np.ndarray,
    swing_highs: List[Dict],
    swing_lows: List[Dict],
    length: int,
    lookback_bars: int = 10,
) -> Dict:
    """
    Deteksi BOS dan CHoCH sesuai Pine Script LuxAlgo displayStructure().

    Pine Script:
        if ta.crossover(close, p_ivot.currentLevel) and not p_ivot.crossed:
            tag = trend.bias == BEARISH ? CHoCH : BOS
            p_ivot.crossed := true
            trend.bias := BULLISH

    Kunci perbedaan dari versi lama:
    - Setiap pivot hanya bisa trigger SATU event (crossed flag)
    - trend.bias di-update secara stateful (bukan dihitung ulang per bar)
    - BOS = break saat trend sudah BULLISH (konfirmasi kelanjutan)
    - CHoCH = break saat trend masih BEARISH (pembalikan)

    Kita simulasikan dari awal data, lalu ambil event dalam lookback_bars terakhir.
    """
    n = len(closes)
    result = {
        "bos_bullish":     False,
        "bos_bearish":     False,
        "choch_bullish":   False,
        "choch_bearish":   False,
        "last_swing_high": None,
        "last_swing_low":  None,
        "trend_bias":      "NEUTRAL",
    }

    if not swing_highs and not swing_lows:
        return result

    result["last_swing_high"] = swing_highs[-1]["price"] if swing_highs else None
    result["last_swing_low"]  = swing_lows[-1]["price"]  if swing_lows  else None

    # Deep copy untuk simulasi stateful — jangan ubah list asli
    sh_sim = [dict(s) for s in swing_highs]
    sl_sim = [dict(s) for s in swing_lows]

    # Inisialisasi trend bias dari pivot pertama yang ada
    # (LuxAlgo mulai dengan trend = 0 / NEUTRAL)
    trend_bias = "NEUTRAL"

    # Pointer pivot aktif (pivot paling baru yang belum di-cross)
    # Kita tracking per pivot sesuai urutan kemunculannya
    sh_ptr = 0  # index ke sh_sim yang sedang aktif untuk breakout bullish
    sl_ptr = 0  # index ke sl_sim yang sedang aktif untuk breakout bearish

    # Simulasi per bar untuk membangun state yang benar
    start_bar = max(0, n - lookback_bars)

    # Pertama, build state dari awal sampai start_bar (tanpa record event)
    for i in range(n):
        c = float(closes[i])
        record = (i >= start_bar)  # hanya record event dalam lookback window

        # Cek break bullish: close menembus swing HIGH terakhir yang belum crossed
        # Cari pivot high yang index-nya < i dan belum crossed
        for sh in sh_sim:
            if sh["index"] < i and not sh["crossed"]:
                if c > sh["price"]:
                    sh["crossed"] = True
                    if record:
                        if trend_bias == "BULLISH":
                            result["bos_bullish"] = True
                        else:
                            result["choch_bullish"] = True
                    trend_bias = "BULLISH"
                break  # hanya cek pivot HIGH terbaru yang belum crossed

        # Cek break bearish: close menembus swing LOW terakhir yang belum crossed
        for sl in sl_sim:
            if sl["index"] < i and not sl["crossed"]:
                if c < sl["price"]:
                    sl["crossed"] = True
                    if record:
                        if trend_bias == "BEARISH":
                            result["bos_bearish"] = True
                        else:
                            result["choch_bearish"] = True
                    trend_bias = "BEARISH"
                break  # hanya cek pivot LOW terbaru yang belum crossed

    result["trend_bias"] = trend_bias
    return result


def detect_order_block(
    df: pd.DataFrame,
    swing_lows: List[Dict],
    current_price: float,
) -> Optional[Dict]:
    """
    Deteksi Bullish Order Block sesuai Pine Script LuxAlgo storeOrderBlock().

    Pine Script:
        // Untuk Bullish OB (setelah BOS/CHoCH bullish):
        a_rray := parsedLows.slice(p_ivot.barIndex, bar_index)
        parsedIndex := p_ivot.barIndex + a_rray.indexof(a_rray.min())
        // OB = bar dengan parsedLow MINIMUM antara pivot LOW dan bar BOS

        // parsedLow per bar:
        //   highVolatilityBar = (high - low) >= 2 * atr(200)
        //   parsedLow = highVolatilityBar ? high : low   ← DIBALIK

        // Mitigation (HIGHLOW mode, default):
        //   bullishOB dihapus jika: low < OB.barLow

    PERUBAHAN dari versi sebelumnya:
    - ATR200 (bukan ATR14) untuk volatility filter
    - Mitigation source adalah LOW (bukan just close)
    - Range pencarian OB: dari pivot LOW ke bar BOS (bukan +30)
    - BOS proxy lebih akurat: close > swing high sebelum pivot low (bukan *1.03)
    """
    highs  = df["high"].values.astype(float)
    lows   = df["low"].values.astype(float)
    closes = df["close"].values.astype(float)
    n      = len(df)

    # ATR200 sesuai LuxAlgo (fallback ke 2% jika data kurang dari 200 bar)
    atr_period = CONFIG["SMC_ATR_PERIOD"]  # 200
    try:
        atr_series = ta.atr(df["high"], df["low"], df["close"], length=atr_period)
        atr_arr = atr_series.fillna(method="bfill").fillna(current_price * 0.02).values
    except Exception:
        atr_arr = np.full(n, current_price * 0.02)

    # Precompute parsedLow per bar (sesuai Pine Script)
    parsed_lows_arr = np.where(
        (highs - lows) >= (CONFIG["SMC_HIGH_VOL_MULTIPLIER"] * atr_arr),
        highs,   # high-vol bar: parsedLow = high (dibalik, ini adalah min dalam array parsedLows)
        lows     # normal bar: parsedLow = low
    )

    active_ob = None

    # Iterasi 5 swing low terakhir (dari terbaru ke terlama)
    for sl in reversed(swing_lows[-5:]):
        sl_idx = sl["index"]
        if sl_idx >= n - 2:
            continue

        sl_price = sl["price"]

        # Cari BOS: bar setelah swing low di mana close menembus swing high
        # yang ada sebelum swing low (proxy yang lebih akurat dari *1.03)
        # Fallback: gunakan close > sl_price * 1.01 jika tidak ada swing high referensi
        bos_idx = None
        for j in range(sl_idx + 1, n):
            if closes[j] > sl_price * 1.01:
                bos_idx = j
                break

        if bos_idx is None:
            continue

        # Cari OB: bar dengan parsed_low MINIMUM antara sl_idx dan bos_idx
        search_range = parsed_lows_arr[sl_idx : bos_idx]
        if len(search_range) == 0:
            continue

        min_idx_rel = int(np.argmin(search_range))
        ob_idx = sl_idx + min_idx_rel

        ob_high = float(highs[ob_idx])
        ob_low  = float(lows[ob_idx])

        # Mitigation: OB dihapus jika low price (bukan close) masuk ke bawah OB.low
        # Sesuai LuxAlgo HIGHLOW mode (default)
        mitigated = False
        for j in range(ob_idx + 1, n):
            if lows[j] < ob_low:
                mitigated = True
                break

        if mitigated:
            continue

        # OB aktif: harga saat ini mendekati atau di dalam zona OB
        in_zone = (
            current_price >= ob_low * 0.98
            and current_price <= ob_high * 1.02   # toleransi 2% di atas OB high
        )

        if in_zone and (active_ob is None or ob_high > active_ob["ob_high"]):
            active_ob = {
                "ob_high": ob_high,
                "ob_low":  ob_low,
                "ob_mid":  (ob_high + ob_low) / 2,
                "in_zone": True,
            }

    return active_ob


def detect_fvg(
    df: pd.DataFrame,
    current_price: float,
) -> Optional[Dict]:
    """
    Deteksi Bullish FVG sesuai Pine Script LuxAlgo drawFairValueGaps().

    Pine Script:
        bullishFVG = currentLow > last2High           // candle[0].low > candle[2].high
                  and lastClose > last2High            // candle[1].close > candle[2].high
                  and barDeltaPercent > threshold      // threshold dinamis

        threshold = ta.cum(abs(barDeltaPercent)) / bar_index * 2  // cumulative avg * 2

        Mitigation: low < FVG.bottom  (price masuk ke dalam gap dari bawah)

    PERUBAHAN dari versi sebelumnya:
    - threshold dinamis (cumulative avg perubahan %) bukan fixed 0.002
    - Mitigation: low < fvg_bottom (bukan overlap check)
    - Hanya simpan FVG yang paling baru dan aktif (overwrite jika lebih baru)
    """
    highs  = df["high"].values.astype(float)
    lows   = df["low"].values.astype(float)
    closes = df["close"].values.astype(float)
    opens  = df["open"].values.astype(float) if "open" in df.columns else closes
    n      = len(df)

    if n < 3:
        return None

    # Hitung cumulative threshold dinamis sesuai Pine Script
    # barDeltaPercent = (close - open) / open
    bar_delta_pct = np.where(opens > 0, np.abs((closes - opens) / opens), 0.0)
    cum_avg = np.cumsum(bar_delta_pct) / np.arange(1, n + 1)
    # threshold per bar = cum_avg * 2
    threshold_arr = cum_avg * 2

    active_fvg = None

    for i in range(2, n):
        # Pine Script: candle[0]=i, candle[1]=i-1, candle[2]=i-2
        c3_low   = lows[i]      # currentLow
        c2_close = closes[i-1]  # lastClose
        c1_high  = highs[i-2]   # last2High

        # Bullish FVG condition
        if c3_low <= c1_high:
            continue
        if c2_close <= c1_high:
            continue

        fvg_bottom = c1_high
        fvg_top    = c3_low
        gap_pct    = (fvg_top - fvg_bottom) / c1_high if c1_high > 0 else 0

        # Threshold dinamis (fallback ke CONFIG jika tidak cukup data)
        threshold = float(threshold_arr[i]) if i < len(threshold_arr) else CONFIG["SMC_FVG_MIN_GAP"]
        if gap_pct <= threshold:
            continue

        # Mitigation: FVG dihapus jika low bar setelahnya masuk ke bawah fvg_top
        # (price mengisi gap dari atas ke bawah)
        mitigated = False
        for j in range(i + 1, n):
            if lows[j] < fvg_top:
                mitigated = True
                break

        if mitigated:
            continue

        # Cek apakah harga saat ini di dalam atau dekat zona FVG
        in_zone = (
            current_price >= fvg_bottom * 0.98
            and current_price <= fvg_top * 1.02
        )

        if in_zone:
            # Simpan FVG terbaru yang aktif
            active_fvg = {
                "fvg_top":    float(fvg_top),
                "fvg_bottom": float(fvg_bottom),
                "fvg_mid":    float((fvg_top + fvg_bottom) / 2),
                "in_zone":    True,
            }

    return active_fvg


def get_1h_data(ticker: str) -> Optional[pd.DataFrame]:
    """Download atau load dari cache OHLCV 1H Yahoo Finance."""
    cached = _load_yf_cache(ticker, "1h")
    if cached is not None:
        return cached

    yf_ticker = f"{ticker}.JK" if not ticker.endswith(".JK") else ticker
    try:
        df = yf.download(yf_ticker, period=CONFIG["YF_PERIOD_1H"], interval="1h",
                         auto_adjust=True, progress=False)
        if df.empty or len(df) < 30:
            return None

        df.reset_index(inplace=True)

        # Flatten MultiIndex columns (yfinance >= 0.2.x)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0].lower() for c in df.columns]
        else:
            df.columns = [c.lower() for c in df.columns]

        if "datetime" in df.columns and "date" not in df.columns:
            df.rename(columns={"datetime": "date"}, inplace=True)

        # Pastikan kolom yang dibutuhkan ada
        for col in ["open", "high", "low", "close", "volume"]:
            if col not in df.columns:
                log.debug(f"YF 1H: kolom '{col}' tidak ada untuk {ticker}")
                return None

        df = df.dropna(subset=["close", "volume"])
        df = df[df["volume"] > 0].reset_index(drop=True)

        _save_yf_cache(ticker, "1h", df)
        return df

    except Exception as e:
        log.debug(f"YF 1H download gagal {ticker}: {e}")
        return None


def check_smc_structure(ticker: str) -> Tuple[bool, Optional[Dict]]:
    """
    Full SMC check pada timeframe 1H.
    Return (lolos, smc_dict)
    """
    df_1h = get_1h_data(ticker)
    if df_1h is None or len(df_1h) < 50:
        return False, None

    highs  = df_1h["high"].values.astype(float)
    lows   = df_1h["low"].values.astype(float)
    closes = df_1h["close"].values.astype(float)
    n      = len(df_1h)

    current_price = float(closes[-1])

    # Internal structure (5 bar)
    sh_int, sl_int = detect_pivots_leg_based(highs, lows, CONFIG["SMC_INTERNAL_LENGTH"])
    bos_int = detect_bos_choch(
        closes, sh_int, sl_int,
        CONFIG["SMC_INTERNAL_LENGTH"],
        CONFIG["SMC_LOOKBACK_BARS"],
    )

    # Swing structure (20 bar)
    sh_sw, sl_sw = detect_pivots_leg_based(highs, lows, CONFIG["SMC_SWING_LENGTH"])
    bos_sw = detect_bos_choch(
        closes, sh_sw, sl_sw,
        CONFIG["SMC_SWING_LENGTH"],
        CONFIG["SMC_LOOKBACK_BARS"],
    )

    # Order Block
    active_ob = detect_order_block(df_1h, sl_sw if sl_sw else sl_int, current_price)

    # FVG
    active_fvg = detect_fvg(df_1h, current_price)

    # Lolos kalau minimal SATU dari:
    has_internal_signal = (
        bos_int["bos_bullish"]
        or bos_int["choch_bullish"]
    )
    in_ob_zone  = active_ob is not None and active_ob.get("in_zone", False)
    in_fvg_zone = active_fvg is not None and active_fvg.get("in_zone", False)

    lolos = has_internal_signal or in_ob_zone or in_fvg_zone

    # Strong/Weak levels dari swing structure
    strong_low  = sl_sw[-1]["price"] if sl_sw else (sl_int[-1]["price"] if sl_int else None)
    weak_high   = sh_sw[-1]["price"] if sh_sw else (sh_int[-1]["price"] if sh_int else None)

    smc_dict = {
        "internal_bos_bullish":  bos_int["bos_bullish"],
        "internal_choch_bullish": bos_int["choch_bullish"],
        "swing_trend_bias": bos_sw["trend_bias"],
        "ob": active_ob,
        "fvg": active_fvg,
        "in_ob_zone": in_ob_zone,
        "in_fvg_zone": in_fvg_zone,
        "strong_low": float(strong_low) if strong_low else None,
        "weak_high": float(weak_high) if weak_high else None,
        "current_price_1h": current_price,
    }

    return lolos, smc_dict


# =============================================================================
# STEP 7: PRE-MARKET ENTRY CALCULATION
# =============================================================================

def get_tick_size(price: float) -> int:
    """Fraksi BEI berdasarkan harga."""
    p = float(price)
    if p < 200:    return 1
    if p < 500:    return 2
    if p < 2000:   return 5
    if p < 5000:   return 10
    return 25


def round_bei(price: float) -> int:
    """Bulatkan ke fraksi BEI terdekat."""
    tick = get_tick_size(price)
    return int(round(price / tick) * tick)


def get_bid_wall(ticker: str, token: str) -> Optional[float]:
    """Ambil level bid wall dari orderbook Stockbit."""
    data = sb_get(f"/company-price-feed/v2/orderbook/companies/{ticker}", token)
    if not data:
        return None

    try:
        bids = data.get("data", {}).get("bids", [])
        if not bids:
            return None

        # Hitung total volume per level
        bid_volumes = {}
        for bid in bids:
            price = bid.get("price", 0)
            vol   = bid.get("lot", 0)
            if price > 0:
                bid_volumes[price] = bid_volumes.get(price, 0) + vol

        if not bid_volumes:
            return None

        avg_vol = sum(bid_volumes.values()) / len(bid_volumes)
        # Bid wall: volume > 2× rata-rata
        walls = {p: v for p, v in bid_volumes.items() if v >= (2 * avg_vol)}

        if not walls:
            return None

        # Ambil bid wall terdekat dari current price (tertinggi)
        return float(max(walls.keys()))

    except Exception:
        return None


def calculate_entry_plan(
    ticker: str,
    hist_data: Dict,
    smc_data: Optional[Dict],
    mode: str,
    token: Optional[str],
    stock_mm: Dict,
) -> Optional[Dict]:
    """
    Hitung Entry 1/2/3, Average Entry, SL, TP1/TP2/TP3, RR.

    PRINSIP: Entry direction fleksibel berdasarkan prediksi arah besok.

    --- SCENARIO A: Pullback dulu baru naik (default) ---
    Semua entry di BAWAH PDC.
    Entry 1 (20%) = PDC - sedikit   → pullback ringan
    Entry 2 (50%) = area support    → pullback medium
    Entry 3 (30%) = deep pullback   → mendekati PDL

    --- SCENARIO B: Gap up / langsung naik (momentum kuat) ---
    Entry 1 di ATAS atau sama dengan PDC.
    Ini terjadi jika:
    - IEP hari ini sudah > PDC + threshold (prediksi gap up)
    - Ada internal BOS bullish 1H baru (momentum sedang kuat)
    - Saham dari Top Gainer dengan change > 10% (biasanya ada continuation)

    Entry 2 dan 3 tetap di bawah PDC sebagai fallback kalau harga pullback.
    """
    pdh = hist_data.get("pdh", 0)
    pdl = hist_data.get("pdl", 0)
    pdc = hist_data.get("pdc", 0)

    if pdh <= 0 or pdl <= 0 or pdc <= 0:
        return None

    # Range hari kemarin harus wajar
    daily_range = (pdh - pdl) / pdc
    if daily_range < 0.003:
        log.debug(f"{ticker}: range harian terlalu sempit ({daily_range:.2%}) — skip")
        return None

    range_size = pdh - pdl
    if range_size < get_tick_size(pdc) * 4:
        log.debug(f"{ticker}: range terlalu sempit ({range_size}) — skip")
        return None

    # IEP check
    iep = stock_mm.get("iep", 0)
    iep_change_pct = stock_mm.get("iep_change_pct", 0)

    if mode == "FULL_STOCKBIT" and iep > 0:
        if iep_change_pct < -2.0:
            log.debug(f"{ticker}: potensi gap down {iep_change_pct:.1f}% → skip")
            return None

    # Bid wall
    bid_wall = None
    if mode == "FULL_STOCKBIT" and token:
        bid_wall = get_bid_wall(ticker, token)

    # OB & FVG levels dari SMC
    ob_low = ob_mid = ob_high_level = fvg_bottom = fvg_top_level = None
    if smc_data:
        ob = smc_data.get("ob")
        if ob:
            ob_low        = ob.get("ob_low")
            ob_mid        = ob.get("ob_mid")
            ob_high_level = ob.get("ob_high")
        fvg = smc_data.get("fvg")
        if fvg:
            fvg_bottom    = fvg.get("fvg_bottom")
            fvg_top_level = fvg.get("fvg_top")

    # ================================================================
    # DETEKSI ENTRY DIRECTION
    # ================================================================
    entry_direction = "PULLBACK"  # default
    entry_direction_reason = ""

    # Signal 1: IEP gap up (berlaku semua mode)
    gap_up_threshold = CONFIG.get("ENTRY_GAP_UP_THRESHOLD", 1.5)
    if mode == "FULL_STOCKBIT" and iep > 0 and iep_change_pct > gap_up_threshold:
        entry_direction = "GAP_UP"
        entry_direction_reason = f"IEP gap up {iep_change_pct:.1f}%"

    # Signal 2: Internal BOS bullish baru (momentum sedang dalam)
    # [OPTIMASI v2] Hanya aktif di FULL_STOCKBIT.
    # Di Mode B: BOS dari Daily sering terlambat dan mengakibatkan masuk di puncak.
    # Backtest 1.159 sinyal: MOMENTUM WR=36.6% vs PULLBACK WR=58.7%.
    # Tambahan guard: BOS tanpa OB Zone tidak mengubah direction (WR BOS-saja=25.9%).
    bos_require_ob = CONFIG.get("ENTRY_BOS_REQUIRE_OB", True)
    bos_has_ob = smc_data.get("in_ob_zone", False) if smc_data else False
    bos_ok = (not bos_require_ob) or bos_has_ob  # BOS boleh tanpa OB hanya jika guard dimatikan
    if (mode == "FULL_STOCKBIT"
            and CONFIG.get("ENTRY_MOMENTUM_BOS_BONUS", True)
            and smc_data
            and bos_ok
            and (smc_data.get("internal_bos_bullish") or smc_data.get("internal_choch_bullish"))):
        if entry_direction == "PULLBACK":  # Hanya upgrade jika belum GAP_UP
            entry_direction = "MOMENTUM"
            entry_direction_reason = "Internal BOS/CHoCH bullish 1H + OB confirmed"

    # Signal 3: Top gainer kuat (> 10%) hari ini — hanya FULL_STOCKBIT
    # [OPTIMASI v2] Mode B tidak punya data real-time gainer yang akurat.
    change_pct_today = stock_mm.get("change_pct", 0)
    if (mode == "FULL_STOCKBIT"
            and stock_mm.get("from_gainer")
            and change_pct_today > 10):
        if entry_direction == "PULLBACK":
            entry_direction = "MOMENTUM"
            entry_direction_reason = f"Top Gainer +{change_pct_today:.1f}% hari ini"

    log.debug(f"  {ticker}: entry_direction={entry_direction} ({entry_direction_reason})")

    # ================================================================
    # ENTRY CALCULATION
    # ================================================================

    if entry_direction == "GAP_UP":
        # --- Entry 1 (20%): Di atas PDC, antisipasi langsung naik ---
        # Pakai IEP sebagai entry_1 jika tersedia
        if iep > 0:
            entry_1 = round_bei(iep)
        else:
            entry_1 = round_bei(pdc + get_tick_size(pdc))  # 1 tick di atas PDC

        # --- Entry 2 (50%): Sama dengan PDC atau bid wall tertinggi ---
        entry_2_raw = pdc
        if bid_wall and bid_wall > pdc * 0.98:
            entry_2_raw = max(entry_2_raw, bid_wall)
        entry_2 = round_bei(entry_2_raw)

        # --- Entry 3 (30%): Fallback pullback ke 50% range ---
        e3_base = pdc - range_size * 0.40
        if ob_mid and pdc * 0.85 < ob_mid < pdc:
            e3_base = ob_mid
        elif fvg_bottom and pdc * 0.85 < fvg_bottom < pdc:
            e3_base = fvg_bottom
        entry_3 = round_bei(max(e3_base, pdl + get_tick_size(pdl)))

        entry_note = {
            "entry_1_note": f"Gap up entry (IEP +{iep_change_pct:.1f}%)",
            "entry_2_note": "PDC / Bid Wall — antisipasi flat open",
            "entry_3_note": "Pullback fallback / OB zone",
        }

    elif entry_direction == "MOMENTUM":
        # --- Entry 1 (20%): Tepat di PDC (close kemarin) ---
        entry_1 = round_bei(pdc)

        # --- Entry 2 (50%): Di bawah PDC, area support / OB ---
        entry_2_raw = pdc - range_size * 0.30
        if ob_mid and (pdc - range_size * 0.50) < ob_mid < pdc:
            entry_2_raw = ob_mid
        elif fvg_bottom and (pdc - range_size * 0.50) < fvg_bottom < pdc:
            entry_2_raw = fvg_bottom
        elif bid_wall and bid_wall < pdc:
            entry_2_raw = max(entry_2_raw, bid_wall)
        entry_2 = round_bei(entry_2_raw)

        # --- Entry 3 (30%): Deep pullback ---
        e3_base = pdc - range_size * 0.55
        if ob_low and ob_low > pdl:
            e3_base = max(e3_base, ob_low)
        entry_3 = round_bei(max(e3_base, pdl + get_tick_size(pdl)))

        entry_note = {
            "entry_1_note": f"PDC entry — momentum kuat ({entry_direction_reason})",
            "entry_2_note": "Pullback ringan / OB zone",
            "entry_3_note": "Deep pullback / PDL area",
        }

    else:  # PULLBACK (default) — semua entry di bawah PDC
        # --- Entry 1 (20%): Pullback ringan ---
        entry_1_raw = pdc - range_size * 0.15
        if bid_wall and (pdc - range_size * 0.25) < bid_wall < pdc:
            entry_1_raw = max(entry_1_raw, bid_wall)
        entry_1 = round_bei(min(entry_1_raw, pdc - get_tick_size(pdc)))

        # --- Entry 2 (50%): Area support / OB / FVG ---
        entry_2_raw = pdc - range_size * 0.45
        if ob_mid and (pdc - range_size * 0.60) < ob_mid < entry_1_raw:
            entry_2_raw = ob_mid
        elif fvg_bottom and (pdc - range_size * 0.60) < fvg_bottom < entry_1_raw:
            entry_2_raw = fvg_bottom
        elif bid_wall and bid_wall < entry_1:
            entry_2_raw = max(entry_2_raw, bid_wall)
        entry_2 = round_bei(entry_2_raw)

        # --- Entry 3 (30%): Deep pullback mendekati PDL ---
        e3_candidates = [pdl + get_tick_size(pdl)]
        if ob_low and ob_low > pdl:
            e3_candidates.append(ob_low)
        if fvg_bottom and fvg_bottom > pdl and fvg_bottom < entry_2:
            e3_candidates.append(fvg_bottom)
        entry_3 = round_bei(max(e3_candidates))

        entry_note = {
            "entry_1_note": "Pullback ringan / Bid Wall area",
            "entry_2_note": "Area OB / FVG / Support",
            "entry_3_note": "Deep pullback / PDL area",
        }

    # ================================================================
    # VALIDASI URUTAN KETAT: entry_1 >= entry_2 >= entry_3 >= pdl
    # Pastikan tidak ada entry yang terbalik
    # ================================================================
    # Paksa urutan descending dengan minimum jarak 1 fraksi
    if entry_2 >= entry_1:
        entry_2 = round_bei(entry_1 - get_tick_size(entry_1) * 2)
    if entry_3 >= entry_2:
        entry_3 = round_bei(entry_2 - get_tick_size(entry_2) * 2)
    if entry_3 <= pdl:
        entry_3 = round_bei(pdl + get_tick_size(pdl))
    # Re-check entry_2 > entry_3 setelah koreksi entry_3
    if entry_2 <= entry_3:
        entry_2 = round_bei(entry_3 + get_tick_size(entry_3) * 2)
    # Re-check entry_1 > entry_2
    if entry_1 <= entry_2:
        entry_1 = round_bei(entry_2 + get_tick_size(entry_2) * 2)

    # Average entry berbobot
    avg_entry = (
        entry_1 * CONFIG["ENTRY_1_PCT"]
        + entry_2 * CONFIG["ENTRY_2_PCT"]
        + entry_3 * CONFIG["ENTRY_3_PCT"]
    )
    avg_entry = round_bei(avg_entry)

    # Untuk PULLBACK: avg_entry HARUS < PDC
    if entry_direction == "PULLBACK" and avg_entry >= pdc:
        log.debug(f"{ticker}: avg_entry {avg_entry} >= PDC {pdc} — skip")
        return None

    # ================================================================
    # SL: harus di bawah entry_3 DAN di bawah PDL
    # ================================================================
    sl_below_pdl    = round_bei(pdl - get_tick_size(pdl))
    sl_below_entry3 = round_bei(entry_3 - get_tick_size(entry_3) * 2)
    sl = min(sl_below_pdl, sl_below_entry3)
    # Hard cap: tidak lebih dari 3% di bawah avg_entry
    sl_hard_cap = round_bei(avg_entry * (1 - CONFIG["SL_HARD_CAP_PCT"]))
    sl = max(sl, sl_hard_cap)
    sl = round_bei(sl)

    if sl >= entry_3:
        sl = round_bei(entry_3 - get_tick_size(entry_3) * 3)

    risk = avg_entry - sl
    if risk <= 0:
        return None

    # ================================================================
    # TP
    # ================================================================
    tp1 = round_bei(pdh)

    if smc_data and smc_data.get("weak_high") and smc_data["weak_high"] > pdh:
        tp2 = round_bei(smc_data["weak_high"])
    else:
        tp2 = round_bei(pdh * 1.03)
    tp2 = max(tp2, round_bei(tp1 + get_tick_size(tp1) * 2))

    tp3_min = avg_entry + (risk * CONFIG["MIN_RR"])
    tp3 = round_bei(max(tp3_min, tp2 * 1.03))

    reward = tp3 - avg_entry
    rr = reward / risk if risk > 0 else 0

    if rr < CONFIG["MIN_RR"]:
        log.debug(f"{ticker}: RR {rr:.1f} < {CONFIG['MIN_RR']} — skip")
        return None

    return {
        "entry_1": entry_1,
        "entry_2": entry_2,
        "entry_3": entry_3,
        "avg_entry": avg_entry,
        "entry_zone": f"{entry_3} - {entry_1}",
        "entry_direction": entry_direction,
        "entry_direction_reason": entry_direction_reason,
        "sl": sl,
        "sl_pct_risk": round((avg_entry - sl) / avg_entry * 100, 2),
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rr": round(rr, 1),
        "rr_str": f"1:{round(rr, 1)}",
        "risk_abs": risk,
        "reward_abs": reward,
        "iep": iep,
        "iep_change_pct": round(iep_change_pct, 2),
        "bid_wall": bid_wall,
        "pdh": pdh,
        "pdl": pdl,
        "pdc": pdc,
        **entry_note,
    }


# =============================================================================
# STEP 8: FINAL SCORING & RANKING
# =============================================================================

def calculate_final_score(
    stock_mm: Dict,
    acc_breakdown: Dict,
    trend_dict: Dict,
    smc_dict: Optional[Dict],
    entry_plan: Dict,
    mode: str,
) -> Tuple[int, str]:
    """
    Hitung confidence score 0-100.
    Return (score, tier: HIGH|MODERATE|LOW)
    """
    score = 0

    if mode == "FULL_STOCKBIT":
        # Broker signal (0-35)
        broker_score = acc_breakdown.get("broker", 5)
        score += max(0, broker_score)

        # Foreign flow (0-25)
        f1d = acc_breakdown.get("foreign_1d", 0)
        f3d = acc_breakdown.get("foreign_3d", 0)
        foreign_score = max(0, min(25, f1d + f3d))
        score += foreign_score

        # SMC (0-20)
        smc_score = 0
        if smc_dict:
            if smc_dict.get("internal_choch_bullish"):
                smc_score += 15
            elif smc_dict.get("internal_bos_bullish"):
                smc_score += 10
            if smc_dict.get("in_ob_zone"):
                smc_score += 5
            if smc_dict.get("in_fvg_zone"):
                smc_score += 5
        score += min(20, smc_score)

        # Candle + Volume (0-10)
        cv_score = max(0, min(10, acc_breakdown.get("candle", 0) + acc_breakdown.get("volume", 0)))
        score += cv_score

        # Orderbook bonus (0-10)
        if entry_plan.get("bid_wall"):
            score += 10

    else:  # MODE B
        # SMC (0-40)
        smc_score = 0
        if smc_dict:
            if smc_dict.get("internal_choch_bullish"):
                smc_score += 30
            elif smc_dict.get("internal_bos_bullish"):
                smc_score += 20
            if smc_dict.get("in_ob_zone"):
                smc_score += 10
            if smc_dict.get("in_fvg_zone"):
                smc_score += 10
        score += min(40, smc_score)

        # Volume (0-35)
        vol_score = max(0, min(35, acc_breakdown.get("volume", 0) * 3))
        score += vol_score

        # Trend strength (0-25)
        gap_pct = trend_dict.get("gap_pct", 10)
        if gap_pct <= 3:
            trend_score = 25
        elif gap_pct <= 5:
            trend_score = 15
        elif gap_pct <= 10:
            trend_score = 5
        else:
            trend_score = 0
        score += trend_score

    score = min(100, score)

    if score >= 70:
        tier = "HIGH"
    elif score >= 40:
        tier = "MODERATE"
    else:
        tier = "LOW"

    return score, tier


# =============================================================================
# STEP 9: OUTPUT GENERATOR
# =============================================================================

def build_signals_list(
    stock_mm: Dict,
    acc_breakdown: Dict,
    broker_signal: str,
    smc_dict: Optional[Dict],
    trend_dict: Dict,
    mode: str,
) -> List[str]:
    signals = []

    if broker_signal in ("Big Acc", "Acc"):
        signals.append(f"✅ Broker Signal: {broker_signal}")

    net_1d = acc_breakdown.get("foreign_1d", 0)
    net_3d = acc_breakdown.get("foreign_3d", 0)
    net_today = stock_mm.get("net_foreign_today", 0)

    if net_today > 0:
        signals.append(f"✅ Net Foreign Buy hari ini: Rp{net_today/1e9:.1f}B")
    elif net_1d > 0:
        signals.append("✅ Net Foreign Buy kemarin")
    if net_3d > 0:
        signals.append("✅ Net Foreign Buy 3 hari berturut")

    if smc_dict:
        if smc_dict.get("internal_choch_bullish"):
            signals.append("✅ Internal CHoCH Bullish (1H)")
        elif smc_dict.get("internal_bos_bullish"):
            signals.append("✅ Internal BOS Bullish (1H)")
        if smc_dict.get("in_ob_zone"):
            signals.append("✅ Harga di zona Order Block aktif")
        if smc_dict.get("in_fvg_zone"):
            signals.append("✅ Harga di zona FVG aktif")
        trend_bias = smc_dict.get("swing_trend_bias", "NEUTRAL")
        if trend_bias == "BULLISH":
            signals.append("✅ Swing Structure: BULLISH")

    gap_pct = trend_dict.get("gap_pct", 0)
    if gap_pct <= 5:
        signals.append(f"✅ Pullback ke MA50 ({gap_pct:.1f}% di atas MA50)")

    return signals


def build_output_stock(
    rank: int,
    ticker: str,
    stock_mm: Dict,
    hist_data: Dict,
    broker_signal: str,
    acc_breakdown: Dict,
    trend_dict: Dict,
    smc_dict: Optional[Dict],
    entry_plan: Dict,
    score: int,
    tier: str,
    mode: str,
) -> Dict:
    signals = build_signals_list(
        stock_mm, acc_breakdown, broker_signal, smc_dict, trend_dict, mode
    )

    warnings_list = []
    if entry_plan.get("iep_change_pct", 0) > 2:
        warnings_list.append(f"⚠️ Potensi GAP UP {entry_plan['iep_change_pct']:.1f}% — entry_1 disesuaikan ke IEP")
    if entry_plan.get("entry_direction") == "GAP_UP":
        warnings_list.append(f"⚠️ Entry 1 DI ATAS close — prediksi gap up. Jika tidak gap, skip entry_1.")
    if stock_mm.get("from_gainer") and not stock_mm.get("from_screener"):
        warnings_list.append(f"⚠️ Sumber: Top Gainer hari ini. Pastikan ada konfirmasi teknikal sebelum entry.")

    entry_direction = entry_plan.get("entry_direction", "PULLBACK")
    entry_direction_label = {
        "PULLBACK":  "⚠️ WATCHLIST ONLY: Tunggu pantulan di area OB, jangan pasang antrian beli buta (blind limit order) karena risiko whipsaw pagi sangat tinggi!",
        "MOMENTUM":  "⚠️ WATCHLIST ONLY: Tunggu konfirmasi pantulan di area OB, hindari entry langsung pagi hari karena volatilitas tinggi!",
        "GAP_UP":    "⚡ Gap Up entry — entry di atas PDC jika gap terkonfirmasi (pantau volume)",
    }.get(entry_direction, "")

    return {
        "rank": rank,
        "ticker": ticker,
        "company": stock_mm.get("name", ticker),
        "mode": mode,
        "universe_sources": stock_mm.get("in_mover_types", []),

        "market_data": {
            "close": hist_data.get("pdc", stock_mm.get("price", 0)),
            "change_pct": round(stock_mm.get("change_pct", 0), 2),
            "value_today": stock_mm.get("value_today", 0),
            "frequency_today": stock_mm.get("frequency_today", 0),
            "iep": entry_plan.get("iep", 0),
            "iep_change_pct": entry_plan.get("iep_change_pct", 0),
        },

        "accumulation": {
            "broker_signal": broker_signal,
            "net_foreign_today": stock_mm.get("net_foreign_today", 0),
            "net_foreign_3d": acc_breakdown.get("foreign_3d", 0),
            "acc_score": acc_breakdown.get("total", 0),
            "score_breakdown": acc_breakdown,
        },

        "trend": {
            "ma50": trend_dict.get("ma50", 0),
            "gap_from_ma50_pct": trend_dict.get("gap_pct", 0),
            "ma50_slope": trend_dict.get("slope", "POSITIVE"),
        },

        "smc": {
            "internal_structure": (
                "CHoCH_BULLISH" if smc_dict and smc_dict.get("internal_choch_bullish")
                else "BOS_BULLISH" if smc_dict and smc_dict.get("internal_bos_bullish")
                else "NONE"
            ),
            "swing_trend_bias": smc_dict.get("swing_trend_bias", "NEUTRAL") if smc_dict else "N/A",
            "ob_zone": (
                f"{int(smc_dict['ob']['ob_low'])} - {int(smc_dict['ob']['ob_high'])}"
                if smc_dict and smc_dict.get("ob") else None
            ),
            "fvg_zone": (
                f"{int(smc_dict['fvg']['fvg_bottom'])} - {int(smc_dict['fvg']['fvg_top'])}"
                if smc_dict and smc_dict.get("fvg") else None
            ),
            "strong_low": smc_dict.get("strong_low") if smc_dict else None,
            "weak_high":  smc_dict.get("weak_high")  if smc_dict else None,
        },

        "entry_plan": {
            "whipsaw_risk": "HIGH",
            "execution_note": "⚠️ WATCHLIST ONLY: Abaikan limit order statis. Tunggu pantulan di area OB sebelum entry, jangan pasang antrian beli karena risiko whipsaw pagi sangat tinggi!",
            "entry_direction": entry_direction,
            "entry_direction_label": entry_direction_label,
            "entry_direction_reason": entry_plan.get("entry_direction_reason", ""),
            "entry_1": entry_plan["entry_1"],
            "entry_1_pct": int(CONFIG["ENTRY_1_PCT"] * 100),
            "entry_1_note": entry_plan.get("entry_1_note", ""),
            "entry_2": entry_plan["entry_2"],
            "entry_2_pct": int(CONFIG["ENTRY_2_PCT"] * 100),
            "entry_2_note": entry_plan.get("entry_2_note", ""),
            "entry_3": entry_plan["entry_3"],
            "entry_3_pct": int(CONFIG["ENTRY_3_PCT"] * 100),
            "entry_3_note": entry_plan.get("entry_3_note", ""),
            "average_entry": entry_plan["avg_entry"],
            "entry_zone": entry_plan["entry_zone"],
            "sl": entry_plan["sl"],
            "sl_pct_risk": entry_plan["sl_pct_risk"],
            "sl_note": "Cut loss disarankan menggunakan Closing Basis (akhir sesi/harian) untuk menghindari sapuan ekor candle (whipsaw) di pagi hari.",
            "tp1": entry_plan["tp1"],
            "tp1_note": "PDH / Target psikologis intraday",
            "tp2": entry_plan["tp2"],
            "tp2_note": "Weak High / Supply Zone 1H",
            "tp3": entry_plan["tp3"],
            "tp3_note": f"RR minimum 1:{CONFIG['MIN_RR']}",
            "rr_ratio": entry_plan["rr_str"],
            "risk_pct": 2,
            "risk_note": "Max 2% dari total portofolio",
        },

        "scoring": {
            "confidence_score": score,
            "tier": tier,
        },

        "signals": signals,
        "warnings": warnings_list,
        "updated_at": datetime.now().strftime("%H.%M WIB"),
    }


def save_output(results: List[Dict], mode: str, market_ctx: Dict, screening_summary: Dict, session_label: str = "MARKET_DAY"):
    """Simpan ke latest_screening.json dan watchlist_YYYYMMDD.json"""
    today = datetime.now().strftime("%Y-%m-%d")
    today_str = datetime.now().strftime("%Y%m%d")

    output = {
        "status": "success" if results else "no_signal",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S WIB"),
        "date": today,
        "mode": mode,
        "session_label": session_label,
        "session_warning": (
            "⚠️ Screening dijalankan di akhir pekan — gunakan sebagai referensi persiapan, bukan sinyal eksekusi langsung."
            if "PRE_MARKET_WEEKEND" in session_label else None
        ),
        "mode_warning": None if mode == "FULL_STOCKBIT" else "⚠️ TOKEN TIDAK TERSEDIA — Data terbatas (Yahoo Finance only)",
        "total_saham": len(results),

        "market_context": market_ctx,
        "screening_summary": screening_summary,

        "config": {
            "strategy": "Pre-Market Intraday — Limit Order",
            "data_sources": ["Stockbit API", "Yahoo Finance"],
            "filters": [
                "Market Mover (Stockbit) / Yahoo 958 tickers",
                "Liquidity consistency 20 hari",
                "Broker Accumulation Signal",
                "Foreign Flow Analysis",
                "Trend: MA50 Daily",
                "SMC Structure: BOS/CHoCH + OB + FVG (1H)",
                f"RR minimum 1:{CONFIG['MIN_RR']}",
            ],
            "max_output": CONFIG["MAX_OUTPUT"],
            "min_rr": CONFIG["MIN_RR"],
        },

        "data": results,
    }

    # Simpan latest
    with open(CONFIG["OUTPUT_LATEST"], "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    log.info(f"✅ Tersimpan: {CONFIG['OUTPUT_LATEST']}")

    # Simpan dated
    dated_filename = f"watchlist_{today_str}.json"
    with open(dated_filename, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    log.info(f"✅ Tersimpan: {dated_filename}")


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

def is_market_day() -> bool:
    """Cek apakah hari ini hari kerja (Senin-Jumat)."""
    today = datetime.now()
    return today.weekday() < 5  # 0=Mon, 4=Fri


def get_session_label() -> str:
    """
    Return label sesi screening berdasarkan hari.
    - Hari kerja  : 'MARKET_DAY'
    - Akhir pekan : 'PRE_MARKET_WEEKEND (Saturday/Sunday)'
    Screening tetap jalan di kedua kondisi — output diberi flag berbeda.
    """
    if is_market_day():
        return "MARKET_DAY"
    day_name = datetime.now().strftime("%A")
    return f"PRE_MARKET_WEEKEND ({day_name})"


def run_screener():
    """
    Main function — dipanggil oleh GitHub Actions setiap hari kerja 18:00 WIB.
    """
    start_time = time.time()

    log.info("=" * 70)
    log.info("🚀 SCREENER TRADER INDONESIA — FINAL PRODUCTION")
    log.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S WIB')}")
    log.info("=" * 70)

    # Label sesi — weekend tetap jalan untuk persiapan, output diberi flag berbeda
    session_label = get_session_label()
    if not is_market_day():
        log.info(f"📅 {session_label} — screening tetap jalan untuk persiapan market")

    # ----------------------------------------------------------------
    # STEP 0: Token & Mode
    # ----------------------------------------------------------------
    log.info("\n[STEP 0] Token management...")
    token, mode = get_valid_token()

    if os.environ.get("FORCE_MODE") == "YAHOO_ONLY":
        mode = "YAHOO_ONLY"
        token = None
        log.info("⚙️ Override: YAHOO_ONLY mode")

    log.info(f"Mode aktif: {mode}")

    # ----------------------------------------------------------------
    # STEP 1: Market Context
    # ----------------------------------------------------------------
    log.info("\n[STEP 1] Market context (IHSG)...")
    market_ctx = get_ihsg_context()

    if not market_ctx["market_safe"]:
        log.warning("🛑 Market tidak aman — output kosong")
        save_output([], mode, market_ctx, {"stopped": "market_unsafe"}, session_label)
        save_combined_output(
            intraday_results=[],
            ara_results=[],
            bsjp_results=[],
            bpjs_results=[],
            swing_results=[],
            trend_results=[],
            position_results=[],
            mode=mode,
            market_ctx=market_ctx,
            intraday_summary={"stopped": "market_unsafe"},
            session_label=session_label,
        )
        return

    # ----------------------------------------------------------------
    # STEP 2: Universe
    # ----------------------------------------------------------------
    log.info("\n[STEP 2] Universe filter...")
    if mode == "FULL_STOCKBIT":
        universe = get_universe_mode_a(token)
    else:
        universe = get_universe_mode_b()

    if not universe:
        log.error("Universe kosong — keluar")
        save_output([], mode, market_ctx, {"error": "empty_universe"}, session_label)
        return

    log.info(f"Universe: {len(universe)} saham")

    # ----------------------------------------------------------------
    # SCREENING PIPELINE
    # ----------------------------------------------------------------
    candidates = []
    summary = {
        "universe": len(universe),
        "after_liquidity": 0,
        "after_accumulation": 0,
        "after_trend": 0,
        "after_smc": 0,
        "after_entry": 0,
        "final": 0,
    }

    for i, stock_mm in enumerate(universe):
        ticker = stock_mm["ticker"]
        log.info(f"  [{i+1}/{len(universe)}] {ticker}...")

        # ----------------------------------------------------------------
        # TOKEN REFRESH — setiap 50 saham untuk menghindari token stale
        # pada universe besar (hingga 972 saham).
        #
        # Cara kerja:
        #   - i == 0 dilewati (token baru saja diambil di STEP 0)
        #   - Setiap kelipatan 50 (i=50,100,150,...): invalidate cache
        #     lalu login ulang. Token baru menggantikan variable lokal
        #     `token` sehingga semua fungsi downstream (check_liquidity,
        #     get_broker_signal, calculate_entry_plan) otomatis memakai
        #     token segar.
        #   - Jika login gagal (mode menjadi YAHOO_ONLY): pipeline
        #     dilanjutkan dengan mode yang sudah ter-degradasi —
        #     tidak abort, konsisten dengan desain awal screener.
        #   - Logika screener, filter, scoring — tidak diubah sama sekali.
        # ----------------------------------------------------------------
        if i > 0 and i % 50 == 0 and mode == "FULL_STOCKBIT":
            log.info(
                f"\n{'='*60}\n"
                f"🔄 TOKEN REFRESH — saham ke-{i+1} dari {len(universe)}\n"
                f"   Invalidate cache dan login ulang ke Stockbit API...\n"
                f"{'='*60}"
            )
            invalidate_token_cache()
            new_token, new_mode = get_valid_token()
            if new_token:
                token = new_token
                log.info(f"✅ Token baru diperoleh — lanjut screening (mode={new_mode})")
            else:
                log.warning(
                    f"⚠️  Login ulang GAGAL di iterasi ke-{i+1}. "
                    f"Melanjutkan dengan mode={new_mode}. "
                    f"Saham berikutnya akan diproses tanpa Stockbit API."
                )
                mode = new_mode
                token = None

        # --- STEP 3: Liquidity ---
        if mode == "FULL_STOCKBIT":
            liq_ok, hist_data = check_liquidity_quality(ticker, token)
            if not liq_ok or hist_data is None:
                log.debug(f"    {ticker}: FAIL liquidity")
                continue
        else:
            # MODE B: buat hist_data dari Yahoo Finance
            df_d = get_daily_data(ticker)
            if df_d is None or len(df_d) < 60:
                continue
            last = df_d.iloc[-1]
            pdh = float(last["high"])
            pdl = float(last["low"])
            pdc = float(last["close"])
            
            # Generate foreign_history from df_d (newest first, starting from yesterday = iloc[-2])
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
                "pdh": pdh,
                "pdl": pdl,
                "pdc": pdc,
                "pd_typical": (pdh + pdl + pdc) / 3,
                "avg_range_pct": 0,
                "avg_freq": 0,
                "foreign_history": foreign_history,
            }

        summary["after_liquidity"] += 1

        # --- STEP 4: Accumulation ---
        if mode == "FULL_STOCKBIT":
            broker_signal, broker_score = get_broker_signal(ticker, token)
            if broker_score == -999:
                log.debug(f"    {ticker}: SKIP — Broker {broker_signal}")
                continue
        else:
            broker_signal, broker_score = "Neutral", 5

        acc_score, acc_breakdown = calculate_accumulation_score(
            stock_mm, hist_data, broker_signal, broker_score, mode
        )

        threshold = (
            CONFIG["ACC_THRESHOLD_MODE_A"]
            if mode == "FULL_STOCKBIT"
            else CONFIG["ACC_THRESHOLD_MODE_B"]
        )
        if acc_score < threshold:
            log.debug(f"    {ticker}: FAIL accumulation (score={acc_score})")
            continue

        summary["after_accumulation"] += 1

        # --- STEP 5: Trend ---
        trend_ok, trend_dict = check_trend_context(ticker)
        if not trend_ok or trend_dict is None:
            log.debug(f"    {ticker}: FAIL trend")
            continue

        # Gap terlalu jauh? (kecuali Big Acc)
        gap_pct = trend_dict.get("gap_pct", 100)
        if gap_pct > CONFIG["MA_GAP_ACCEPTABLE_MAX"] * 100 and broker_signal != "Big Acc":
            log.debug(f"    {ticker}: FAIL trend — gap {gap_pct:.1f}% terlalu jauh")
            continue

        # [OPTIMASI v2] Filter candle quality D-1 sebelum masuk SMC.
        # Candle bearish ringan (body_pos < 0.40): backtest WR=38.9%, PF=0.73x (merugi).
        # Menggunakan data hist_data yang sudah ada (0 overhead).
        _body_min = CONFIG.get("INTRADAY_CANDLE_BODY_MIN", 0.40)
        _pdh = hist_data.get("pdh", 0)
        _pdl = hist_data.get("pdl", 0)
        _pdc = hist_data.get("pdc", 0)
        if _pdh > _pdl > 0 and _pdc > 0:
            _body_pos = (_pdc - _pdl) / (_pdh - _pdl)
            if _body_pos < _body_min:
                log.debug(
                    f"    {ticker}: FAIL candle quality "
                    f"(body_pos={_body_pos:.2f} < {_body_min})"
                )
                continue

        summary["after_trend"] += 1

        # --- STEP 6: SMC ---
        smc_ok, smc_dict = check_smc_structure(ticker)
        if not smc_ok:
            log.debug(f"    {ticker}: FAIL SMC")
            continue

        summary["after_smc"] += 1

        # --- STEP 7: Entry Plan ---
        entry_plan = calculate_entry_plan(
            ticker, hist_data, smc_dict, mode, token, stock_mm
        )
        if entry_plan is None:
            log.debug(f"    {ticker}: FAIL entry plan (RR < {CONFIG['MIN_RR']})")
            continue

        summary["after_entry"] += 1

        # --- STEP 8: Score ---
        score, tier = calculate_final_score(
            stock_mm, acc_breakdown, trend_dict, smc_dict, entry_plan, mode
        )

        # [OPTIMASI v2] Threshold dinaikkan 40→50.
        # Backtest: score>=50 WR=60.0% PF=1.72x vs score>=40 WR=57.9% PF=1.65x.
        _min_score = CONFIG.get("INTRADAY_MIN_SCORE", 50)
        if score < _min_score:
            log.debug(f"    {ticker}: FAIL score ({score} < {_min_score})")
            continue

        log.info(f"    ✅ {ticker}: Score {score}/100 ({tier}) | RR {entry_plan['rr_str']}")

        candidates.append({
            "score": score,
            "tier": tier,
            "ticker": ticker,
            "stock_mm": stock_mm,
            "hist_data": hist_data,
            "broker_signal": broker_signal,
            "acc_breakdown": acc_breakdown,
            "trend_dict": trend_dict,
            "smc_dict": smc_dict,
            "entry_plan": entry_plan,
        })

        gc.collect()

    # ----------------------------------------------------------------
    # STEP 9: Sort + Build Output
    # ----------------------------------------------------------------
    candidates.sort(key=lambda x: x["score"], reverse=True)
    top_candidates = candidates[:CONFIG["MAX_OUTPUT"]]
    summary["final"] = len(top_candidates)

    results = []
    for rank, c in enumerate(top_candidates, 1):
        stock_out = build_output_stock(
            rank=rank,
            ticker=c["ticker"],
            stock_mm=c["stock_mm"],
            hist_data=c["hist_data"],
            broker_signal=c["broker_signal"],
            acc_breakdown=c["acc_breakdown"],
            trend_dict=c["trend_dict"],
            smc_dict=c["smc_dict"],
            entry_plan=c["entry_plan"],
            score=c["score"],
            tier=c["tier"],
            mode=mode,
        )
        results.append(stock_out)

    save_output(results, mode, market_ctx, summary, session_label)

    elapsed = (time.time() - start_time) / 60
    log.info("\n" + "=" * 70)
    log.info(f"✅ SCREENING INTRADAY SELESAI — {len(results)} saham output")
    log.info(f"   Universe: {summary['universe']}")
    log.info(f"   Lolos Liquidity: {summary['after_liquidity']}")
    log.info(f"   Lolos Accumulation: {summary['after_accumulation']}")
    log.info(f"   Lolos Trend: {summary['after_trend']}")
    log.info(f"   Lolos SMC: {summary['after_smc']}")
    log.info(f"   Lolos Entry Plan: {summary['after_entry']}")
    log.info(f"   Final Output: {summary['final']}")
    log.info(f"   ⏱️  Waktu intraday: {elapsed:.1f} menit")
    log.info("=" * 70)

    # ----------------------------------------------------------------
    # PIPELINE ARA (LOGIKA BARU) — dijalankan setelah intraday selesai
    # Berbagi token, mode, market_ctx, dan semua utilitas dengan intraday.
    # Tidak mengganggu output latest_screening.json yang sudah disimpan.
    # ----------------------------------------------------------------
    ara_results = []
    if CONFIG.get("ARA_ENABLED", True):
        ara_results = run_ara_pipeline(token, mode)

    # ----------------------------------------------------------------
    # PIPELINE BSJP (BELI SORE JUAL PAGI) — dijalankan setelah ARA selesai
    # Memanfaatkan `universe` yang sudah dikumpulkan pipeline intraday.
    # Data OHLCV sudah di-cache oleh pipeline intraday — 0 download tambahan.
    # Tidak menyentuh pipeline intraday maupun ARA sama sekali.
    # ----------------------------------------------------------------
    bsjp_results = []
    if CONFIG.get("BSJP_ENABLED", True):
        bsjp_results = run_bsjp_pipeline(universe, mode)

    # ----------------------------------------------------------------
    # PIPELINE BPJS (BELI PAGI JUAL SORE) — dijalankan setelah BSJP selesai
    # Memanfaatkan `universe` yang sudah dikumpulkan pipeline intraday.
    # Data OHLCV sudah di-cache — 0 download tambahan.
    # Menghasilkan WATCHLIST, bukan sinyal eksekusi langsung.
    # Tidak menyentuh pipeline intraday, ARA, maupun BSJP.
    # ----------------------------------------------------------------
    bpjs_results = []
    if CONFIG.get("BPJS_ENABLED", True):
        bpjs_results = run_bpjs_pipeline(universe, mode)

    # ----------------------------------------------------------------
    # PIPELINE SWING TRADING — dijalankan setelah BPJS selesai
    # Memanfaatkan `universe` dan cache OHLCV yang sudah ada.
    # Mencari setup VCP (Volatility Contraction) pada Stage 2 Uptrend.
    # Kill switch: otomatis mati jika IHSG < MA200.
    # Tidak menyentuh pipeline intraday, ARA, BSJP, maupun BPJS.
    # ----------------------------------------------------------------
    swing_results = []
    if CONFIG.get("SWING_ENABLED", True):
        swing_results = run_swing_pipeline(universe, mode, market_ctx)

    # ----------------------------------------------------------------
    # PIPELINE TREND FOLLOWING — dijalankan setelah Swing selesai
    # Memanfaatkan `universe` dan cache OHLCV/Weekly yang sudah ada.
    # ----------------------------------------------------------------
    trend_results = []
    if CONFIG.get("TREND_ENABLED", True):
        trend_results = run_trend_pipeline(universe, mode, market_ctx)

    # ----------------------------------------------------------------
    # PIPELINE POSITION TRADING — dijalankan setelah Trend selesai
    # Memanfaatkan `universe` dan cache OHLCV/Weekly/Monthly yang sudah ada.
    # ----------------------------------------------------------------
    position_results = []
    if CONFIG.get("POSITION_ENABLED", True):
        position_results = run_position_pipeline(universe, mode, market_ctx)

    # Simpan output gabungan (combined_screening.json)
    save_combined_output(
        intraday_results=results,
        ara_results=ara_results,
        bsjp_results=bsjp_results,
        bpjs_results=bpjs_results,
        swing_results=swing_results,
        trend_results=trend_results,
        position_results=position_results,
        mode=mode,
        market_ctx=market_ctx,
        intraday_summary=summary,
        session_label=session_label,
    )

    elapsed_total = (time.time() - start_time) / 60
    log.info("\n" + "=" * 70)
    log.info(f"✅ SCREENING TOTAL SELESAI")
    log.info(f"   Intraday output:   {len(results)} saham → latest_screening.json")
    log.info(f"   ARA kandidat:      {len(ara_results)} saham → combined_screening.json")
    log.info(f"   BSJP kandidat:     {len(bsjp_results)} saham → combined_screening.json")
    log.info(f"   BPJS watchlist:    {len(bpjs_results)} saham → combined_screening.json")
    log.info(f"   SWING watchlist:   {len(swing_results)} saham → combined_screening.json")
    log.info(f"   TREND watchlist:   {len(trend_results)} saham → combined_screening.json")
    log.info(f"   POSITION watchlist:{len(position_results)} saham → combined_screening.json")
    log.info(f"   ⏱️  Total waktu: {elapsed_total:.1f} menit")
    log.info("=" * 70)




# =============================================================================
# COMPATIBILITY SHIMS — run_screener() memanggil fungsi-fungsi ini secara
# langsung. Shims ini meneruskan panggilan ke versi v2 yang baru.
# Tidak ada logika di sini — murni redirect.
# =============================================================================

def compute_ara_features(df: pd.DataFrame) -> Optional[Dict]:
    """Shim: redirect ke compute_ara_features_v2 (tanpa minute data)."""
    return compute_ara_features_v2(df, df_minute=None)


def score_ara_candidate(feat: Dict) -> Tuple[int, List[str], List[str]]:
    """Shim: redirect ke score_ara_candidate_v2, strip pattern_type dari return."""
    score, _pattern, pos, neg = score_ara_candidate_v2(feat)
    return score, pos, neg


def check_ara_liquidity_yahoo(df: pd.DataFrame) -> Tuple[bool, str]:
    """Shim: redirect ke check_ara_liquidity_v2."""
    return check_ara_liquidity_v2(df)


def get_ara_universe(token: Optional[str], mode: str) -> Dict[str, Dict]:
    """Shim: redirect ke get_ara_universe_v2."""
    return get_ara_universe_v2(token, mode)


def run_ara_pipeline(token: Optional[str], mode: str) -> List[Dict]:
    """
    Shim: redirect ke run_ara_pipeline_v2.
    Dipanggil oleh run_screener() (kode lama) — diteruskan ke implementasi v2.
    """
    return run_ara_pipeline_v2(token, mode)


def save_combined_output(
    intraday_results: List[Dict],
    ara_results: List[Dict],
    mode: str,
    market_ctx: Dict,
    intraday_summary: Dict,
    session_label: str = "MARKET_DAY",
    bsjp_results: Optional[List[Dict]] = None,
    bpjs_results: Optional[List[Dict]] = None,
    swing_results: Optional[List[Dict]] = None,
    trend_results: Optional[List[Dict]] = None,
    position_results: Optional[List[Dict]] = None,
):
    """
    Shim: redirect ke save_combined_output_v3.
    Dipanggil oleh run_screener() — diteruskan ke implementasi v3 yang
    menambahkan semua pipeline keys tanpa mengubah key lama.
    """
    save_combined_output_v3(
        intraday_results=intraday_results,
        ara_results=ara_results,
        bsjp_results=bsjp_results or [],
        bpjs_results=bpjs_results or [],
        swing_results=swing_results or [],
        trend_results=trend_results or [],
        position_results=position_results or [],
        mode=mode,
        market_ctx=market_ctx,
        intraday_summary=intraday_summary,
        session_label=session_label,
    )




# =============================================================================
# PIPELINE ARA v2 — LOGIKA BARU FINAL (REVISED berdasarkan analisis 23 saham ARA)
# =============================================================================
#
# TEMUAN EMPIRIS (n=23 saham ARA, 14-15 April 2026):
#   - Type 3 "Out of Nowhere" = 48% (TIDAK bisa dideteksi dari OHLCV)
#   - Type 1 "Continuation"   =  9% (D-1 naik >8%, vol >3x MA20)
#   - Type 2 "Silent Accum"   =  9% (vol spike, harga flat, wick tinggi)
#   - Mixed                   = 35% (sinyal lemah, sebagian bisa terdeteksi)
#
# SATU SINYAL UNIVERSALNYA (22/22 saham ARA):
#   - last_15_green >= 10/14 bar terakhir sesi D-1 = tidak ada tekanan jual
#
# SCORING: Daily (0-70) + Minute (0-20) + Stockbit bonus (0-10) = max 100
#
# FALSE SIGNAL RATE: ~85-92% (Type 1/2), ~95%+ (Mixed/Type3)
# Gunakan HANYA sebagai watchlist. Konfirmasi manual wajib.
# =============================================================================


def compute_ara_features_v2(
    df_daily: pd.DataFrame,
    df_minute: Optional[pd.DataFrame] = None,
) -> Optional[Dict]:
    """
    Hitung semua fitur prediktif ARA dari data OHLCV daily + minute (opsional).

    Perspektif: df_daily.iloc[-1] = D-0 (close hari ini, sudah final jam 18:00 WIB).
    Screener jalan sore/malam sehingga D-0 = hari ini, D-1 = kemarin.

    FITUR DAILY (berdasarkan korelasi empiris n=23 saham ARA):
      d1_close_pos       : median=0.60 (tutup di atas midrange, tapi tidak selalu)
      d1_upper_wick      : median=0.29 (rejection moderat di atas)
      d1_body_pct        : median=0.40 (candle kecil-medium)
      d1_range_expansion : median=0.80 (sering MENYEMPIT sebelum ARA!)
      d1_vol_ratio_ma20  : median=1.30 (banyak ARA vol spike TIDAK ada di D-0)

    FITUR MINUTE (satu sinyal universal — 22/22 saham):
      m1_last15_green >= 10/14 bar hijau di akhir sesi D-0

    Parameters:
        df_daily  : DataFrame sorted ascending, kolom: date, open, high, low, close, volume
        df_minute : DataFrame 1-minute OHLCV untuk D-0 (opsional, bisa None)

    Returns:
        Dict fitur atau None jika data tidak cukup (<55 bar daily).
    """
    if df_daily is None or len(df_daily) < 55:
        return None

    n = len(df_daily)
    i = n - 1   # index D-0: baris terakhir = close hari ini (sudah final jam 18:00 WIB)
    d1 = df_daily.iloc[i]       # D-0: hari ini
    d2 = df_daily.iloc[i - 1]   # D-1: kemarin
    d3 = df_daily.iloc[i - 2] if i >= 2 else d2  # D-2: 2 hari lalu

    # ---- Volume MA (dihitung dari bar SEBELUM D-0, hindari look-ahead bias) ----
    # FIX vol_slice_ma5: end index diperbaiki dari i-1 → i
    # Sebelumnya :i-1 hanya menghasilkan 4 bar (off-by-one), seharusnya 5 bar (D-1 s/d D-5)
    vol_slice_ma20 = df_daily["volume"].iloc[max(0, i - 21):i]
    vol_slice_ma5  = df_daily["volume"].iloc[max(0, i - 6):i]   # FIX: was :i-1
    vol_ma20_pre = float(vol_slice_ma20.mean()) if len(vol_slice_ma20) >= 5 else 0.0
    vol_ma5_pre  = float(vol_slice_ma5.mean())  if len(vol_slice_ma5)  >= 3 else 0.0

    if vol_ma20_pre <= 0:
        return None

    # ---- D-1 candle metrics ----
    d1_h = float(d1["high"])
    d1_l = float(d1["low"])
    d1_o = float(d1["open"])
    d1_c = float(d1["close"])
    d1_v = float(d1["volume"])

    d1_range       = d1_h - d1_l + 1e-6
    d1_body        = abs(d1_c - d1_o)
    d1_body_pct    = d1_body / d1_range
    d1_upper_wick  = (d1_h - max(d1_o, d1_c)) / d1_range
    d1_lower_wick  = (min(d1_o, d1_c) - d1_l) / d1_range
    d1_close_pos   = (d1_c - d1_l) / d1_range
    d1_bullish     = d1_c > d1_o
    d1_change      = (d1_c - float(d2["close"])) / (float(d2["close"]) + 1e-6) * 100
    d1_vol_r_ma20  = d1_v / vol_ma20_pre
    d1_vol_r_ma5   = d1_v / (vol_ma5_pre + 1e-6)
    d1_value_rp    = d1_c * d1_v * 100   # IDX: lot × 100 lembar

    # ---- Range expansion D-0 vs 5 hari sebelumnya ----
    # FIX: end index diperbaiki dari i-1 → i
    # Sebelumnya range(max(0,i-6), i-1) hanya 4 bar, missing D-1 sepenuhnya
    ranges_5d = []
    for j in range(max(0, i - 6), i):   # FIX: was i-1
        r = df_daily.iloc[j]
        rc = float(r["close"])
        if rc > 0:
            ranges_5d.append((float(r["high"]) - float(r["low"])) / rc)
    avg_range_5d  = float(np.mean(ranges_5d)) if ranges_5d else 0.01
    d1_range_pct  = d1_range / (d1_c + 1e-6)
    d1_range_exp  = d1_range_pct / avg_range_5d if avg_range_5d > 0 else 1.0

    # ---- D-2 metrics ----
    d2_range   = float(d2["high"]) - float(d2["low"]) + 1e-6
    d2_body_pct = abs(float(d2["close"]) - float(d2["open"])) / d2_range
    d2_change  = (float(d2["close"]) - float(d3["close"])) / (float(d3["close"]) + 1e-6) * 100
    d2_vol_r_ma20 = float(d2["volume"]) / vol_ma20_pre

    # ---- MA context ----
    ma20_arr = df_daily["close"].iloc[max(0, i - 21):i].astype(float)
    ma50_arr = df_daily["close"].iloc[max(0, i - 51):i].astype(float)
    ma20 = float(ma20_arr.mean()) if len(ma20_arr) >= 5 else d1_c
    ma50 = float(ma50_arr.mean()) if len(ma50_arr) >= 10 else ma20
    above_ma20 = d1_c > ma20
    above_ma50 = d1_c > ma50

    # ---- Up-streak sebelum D-1 ----
    up_streak = 0
    for j in range(i - 1, max(i - 10, 1), -1):
        if float(df_daily.iloc[j]["close"]) > float(df_daily.iloc[j - 1]["close"]):
            up_streak += 1
        else:
            break

    # ---- 5-day price trend ----
    price_5d_ago = float(df_daily.iloc[max(0, i - 5)]["close"])
    trend_5d_pct = (d1_c - price_5d_ago) / (price_5d_ago + 1e-6) * 100

    # ---- D-1 date string ----
    try:
        d1_date_str = d1["date"].strftime("%Y-%m-%d") if hasattr(d1["date"], "strftime") else str(d1["date"])[:10]
    except Exception:
        d1_date_str = ""

    # ---- Minute-derived features (opsional) ----
    m1_last15_green    = None
    m1_close_vs_vwap   = None
    m1_close_pos_intra = None
    m1_high_timing     = None
    m1_n_bars          = None

    if df_minute is not None and len(df_minute) >= 15:
        try:
            dm = df_minute.sort_values("date").reset_index(drop=True)
            n_m = len(dm)
            m1_n_bars = n_m

            dm_h = dm["high"].astype(float)
            dm_l = dm["low"].astype(float)
            dm_c = dm["close"].astype(float)
            dm_v = dm["volume"].astype(float)

            # Intraday close position
            d1_high_m  = float(dm_h.max())
            d1_low_m   = float(dm_l.min())
            d1_close_m = float(dm_c.iloc[-1])
            intra_range = d1_high_m - d1_low_m + 1e-6
            m1_close_pos_intra = (d1_close_m - d1_low_m) / intra_range

            # VWAP
            typical = (dm_h + dm_l + dm_c) / 3
            total_vol_m = float(dm_v.sum())
            if total_vol_m > 0:
                vwap = float((typical * dm_v).sum()) / total_vol_m
                m1_close_vs_vwap = (d1_close_m - vwap) / (vwap + 1e-6) * 100
            else:
                m1_close_vs_vwap = 0.0

            # High timing (fraksi bar di mana high terjadi)
            high_bar_local_idx = int(dm_h.idxmax()) - int(dm.index[0])
            m1_high_timing = high_bar_local_idx / (n_m + 1e-6)

            # Last 15 bars: hitung bar hijau (close >= prev close)
            last_15 = dm_c.iloc[-15:].values
            green_count = sum(
                1 for k in range(1, len(last_15))
                if last_15[k] >= last_15[k - 1]
            )
            m1_last15_green = green_count  # dari 14 possible comparisons

            # [AUDIT FIX] Deteksi real active green vs flat/illiquid bars
            m1_flat_count = sum(
                1 for k in range(1, len(last_15))
                if last_15[k] == last_15[k - 1]
            )
            m1_strict_green = sum(
                1 for k in range(1, len(last_15))
                if last_15[k] > last_15[k - 1]
            )

        except Exception as e_m:
            log.debug(f"Minute feature error: {e_m}")

    return {
        # D-1 identifiers
        "d1_date":             d1_date_str,
        "d1_close":            round(d1_c, 2),
        "d1_open":             round(d1_o, 2),
        "d1_high":             round(d1_h, 2),
        "d1_low":              round(d1_l, 2),

        # D-1 fitur utama
        "d1_change":           round(d1_change, 2),
        "d1_body_pct":         round(d1_body_pct, 3),
        "d1_upper_wick":       round(d1_upper_wick, 3),
        "d1_lower_wick":       round(d1_lower_wick, 3),
        "d1_close_pos":        round(d1_close_pos, 3),
        "d1_bullish":          bool(d1_bullish),
        "d1_vol_ratio_ma20":   round(d1_vol_r_ma20, 2),
        "d1_vol_ratio_ma5":    round(d1_vol_r_ma5, 2),
        "d1_value_rp":         round(d1_value_rp),
        "d1_range_expansion":  round(d1_range_exp, 2),

        # D-2 fitur
        "d2_change":           round(d2_change, 2),
        "d2_body_pct":         round(d2_body_pct, 3),
        "d2_vol_ratio_ma20":   round(d2_vol_r_ma20, 2),

        # Context
        "above_ma20":          bool(above_ma20),
        "above_ma50":          bool(above_ma50),
        "ma20":                round(ma20, 0),
        "ma50":                round(ma50, 0),
        "up_streak":           int(up_streak),
        "trend_5d_pct":        round(trend_5d_pct, 2),
        "vol_ma20_pre":        round(vol_ma20_pre, 0),
        "avg_range_5d_pct":    round(avg_range_5d * 100, 3),

        # Minute-derived features (None jika tidak tersedia)
        "m1_last15_green":     m1_last15_green,
        "m1_flat_count":       m1_flat_count if df_minute is not None else None,
        "m1_strict_green":     m1_strict_green if df_minute is not None else None,
        "m1_close_vs_vwap":    round(m1_close_vs_vwap, 2) if m1_close_vs_vwap is not None else None,
        "m1_close_pos_intra":  round(m1_close_pos_intra, 3) if m1_close_pos_intra is not None else None,
        "m1_high_timing":      round(m1_high_timing, 3) if m1_high_timing is not None else None,
        "m1_n_bars":           m1_n_bars,
    }


def score_ara_candidate_v2(feat: Dict) -> Tuple[int, str, List[str], List[str]]:
    """
    Hitung ARA score v2 (0-100).

    STRUKTUR SKOR:
      Daily  (0-70 poin) — volume spike, candle pattern, trend context
      Minute (0-20 poin) — closing momentum confirmation (dari m1_* fields)
      [Stockbit bonus +0–10 diterapkan SETELAH fungsi ini di run_ara_pipeline_v2]

    THRESHOLD:
      >= 60 → STRONG  (~85% false signal rate)
      >= 40 → MODERATE (~90% false signal rate)
      <  40 → buang

    MINIMUM KONFLUENSI:
      < 2 sinyal positif → score di-cap 30 (mencegah false positive single-signal)

    Returns: (score, pattern_type, reasons_positive, reasons_negative)
    """
    score = 0
    pos: List[str] = []
    neg: List[str] = []

    # Ekstrak fields
    d1_vol  = feat.get("d1_vol_ratio_ma20", 0.0)
    d1_wick = feat.get("d1_upper_wick", 0.0)
    d1_body = feat.get("d1_body_pct", 1.0)
    d1_pos  = feat.get("d1_close_pos", 0.5)
    d1_chg  = feat.get("d1_change", 0.0)
    d2_vol  = feat.get("d2_vol_ratio_ma20", 0.0)
    d2_chg  = feat.get("d2_change", 0.0)
    trend5d = feat.get("trend_5d_pct", 0.0)
    up_str  = feat.get("up_streak", 0)
    rng_exp = feat.get("d1_range_expansion", 1.0)

    # Minute features (None jika data tidak tersedia)
    m1_green  = feat.get("m1_last15_green")
    m1_vwap   = feat.get("m1_close_vs_vwap")
    m1_cpos   = feat.get("m1_close_pos_intra")
    m1_timing = feat.get("m1_high_timing")

    # ==========================================================
    # DAILY SCORING (0-70 poin)
    # ==========================================================

    # Komponen 1: Volume Spike D-1
    # Tier 2 (>=6x): anomali kuat
    # Tier 1 (3-6x): moderat (tidak stack dengan tier 2)
    vol_t2 = CONFIG.get("ARA_VOL_MA20_TIER2", 6.0)
    vol_t1 = CONFIG.get("ARA_VOL_MA20_TIER1", 3.0)
    if d1_vol >= vol_t2:
        score += 25
        pos.append(f"Volume D-1 ekstrem: {d1_vol:.1f}x MA20 (tier-2 ≥{vol_t2:.0f}x)")
    elif d1_vol >= vol_t1:
        score += 12
        pos.append(f"Volume D-1 spike: {d1_vol:.1f}x MA20 (tier-1 ≥{vol_t1:.0f}x)")

    # Komponen 2: Volume D-2 (early accumulation)
    vol_d2_min = CONFIG.get("ARA_VOL_MA20_D2_MIN", 1.5)
    if d2_vol >= vol_d2_min:
        score += 8
        pos.append(f"Volume D-2 elevated: {d2_vol:.1f}x MA20 (akumulasi dini)")

    # Komponen 3: Continuation (D-1 naik besar + vol besar — KEDUANYA wajib)
    cont_chg = CONFIG.get("ARA_CONTINUATION_CHG_MIN", 8.0)
    cont_vol = CONFIG.get("ARA_CONTINUATION_VOL_MIN", 3.0)
    if d1_chg >= cont_chg and d1_vol >= cont_vol:
        score += 10   # stacks di atas vol spike
        pos.append(f"Continuation D-1: +{d1_chg:.1f}% vol {d1_vol:.1f}x MA20")

    # Komponen 4: Candle patterns — Silent Accumulation signals
    wick_min = CONFIG.get("ARA_UPPER_WICK_MIN", 0.35)
    body_max = CONFIG.get("ARA_BODY_MAX_REJECTION", 0.40)
    pos_max  = CONFIG.get("ARA_CLOSE_POS_MAX", 0.50)

    if d1_wick >= wick_min:
        score += 10
        pos.append(f"Upper wick rejection: {d1_wick:.2f} (smart money absorption)")

    if d1_body <= body_max:
        score += 8
        pos.append(f"Body kecil D-1: {d1_body:.2f} (candle akumulasi)")

    if d1_pos <= pos_max:
        score += 8
        pos.append(f"Close pos rendah: {d1_pos:.2f} (buyers absorbed selling)")

    # Komponen 5: D-2 momentum
    if d2_chg >= 5.0 and d2_vol >= 1.5:
        score += 8
        pos.append(f"D-2 early signal: +{d2_chg:.1f}% vol {d2_vol:.1f}x")
    elif d2_chg >= 3.0 and d2_vol >= 2.0:
        score += 5
        pos.append(f"D-2 moderate: +{d2_chg:.1f}% vol {d2_vol:.1f}x")

    # Komponen 6: Range expansion DAN Squeeze (VCP pattern)
    # Squeeze (kontraksi) diberi bonus karena empiris 44.8% ARA punya range_exp < 0.8
    # Bonus squeeze HANYA menambah, bukan menggantikan sinyal utama (precision-first)
    squeeze_strong = CONFIG.get("ARA_SQUEEZE_STRONG", 0.70)
    squeeze_mild   = CONFIG.get("ARA_SQUEEZE_MILD",   0.90)
    if rng_exp >= 2.0:
        score += 5
        pos.append(f"Range expansion: {rng_exp:.1f}x avg-5d")
    elif rng_exp >= 1.5:
        score += 3
        pos.append(f"Range expansion moderat: {rng_exp:.1f}x avg-5d")
    elif rng_exp < squeeze_strong:
        # VCP Squeeze kuat — harga terjepit sebelum ledakan
        score += 8
        pos.append(f"VCP Squeeze kuat D-1: {rng_exp:.2f}x (coiling — tekanan terpendam)")
    elif rng_exp < squeeze_mild:
        # Squeeze moderat
        score += 4
        pos.append(f"VCP Squeeze moderat D-1: {rng_exp:.2f}x (range menyempit)")

    # Komponen 7: Context
    if feat.get("above_ma20"):
        score += 4
        pos.append("Harga di atas MA20")

    if 3.0 <= trend5d <= 25.0:
        score += 4
        pos.append(f"5d trend sehat: +{trend5d:.1f}%")

    # ==========================================================
    # MINUTE SCORING (0-20 poin, capped)
    # Sinyal universalnya: last_15_green >= 12 (n=22 saham ARA)
    # ==========================================================
    minute_score = 0
    m1_flat = feat.get("m1_flat_count")

    if m1_green is not None:
        # [AUDIT FIX] Jika flat bar terlalu dominan (>= 5), kurangi skornya
        # karena itu indikasi saham tidak aktif, bukan buying momentum!
        if m1_flat is not None and m1_flat >= CONFIG.get("ARA_ANCHOR_FLAT_MAX", 4):
            neg.append(f"Closing flat bias: {m1_flat}/14 bar flat — indikasi tidak aktif")
            minute_score -= 10
        elif m1_green >= 12:
            minute_score += 10
            pos.append(f"Closing momentum kuat: {m1_green}/14 bar hijau (sinyal universal ARA)")
        elif m1_green >= 10:
            minute_score += 5
            pos.append(f"Closing momentum OK: {m1_green}/14 bar hijau")
        else:
            neg.append(f"Closing momentum lemah: {m1_green}/14 bar hijau")
    else:
        # Tidak ada data menit — catat sebagai kekurangan sinyal
        neg.append("Data menit tidak tersedia — sinyal universal ARA tidak terkonfirmasi")

    if m1_cpos is not None and m1_cpos >= 0.70:
        minute_score += 5
        pos.append(f"Close intraday kuat: {m1_cpos:.2f} (mendekati high harian)")

    if m1_vwap is not None:
        if m1_vwap >= 2.0:
            minute_score += 5
            pos.append(f"Close di atas VWAP: +{m1_vwap:.1f}%")
        elif m1_vwap > 0:
            minute_score += 2
            pos.append(f"Close sedikit di atas VWAP: +{m1_vwap:.1f}%")
        elif m1_vwap < -2.0:
            neg.append(f"Close di bawah VWAP: {m1_vwap:.1f}%")

    if m1_timing is not None:
        if m1_timing >= 0.75:
            minute_score += 5
            pos.append(f"High intraday di akhir sesi ({m1_timing:.0%})")
        elif m1_timing <= 0.25:
            neg.append(f"High intraday di awal sesi ({m1_timing:.0%}) — profit taking sore")

    score += min(20, minute_score)

    # ==========================================================
    # PENALTIES (v2.1 — diperbaiki untuk hindari membuang pola shakeout)
    # ==========================================================
    # PERBAIKAN: Hanya penalti jika harga TURUN DAN volume TINGGI sekaligus.
    # Harga turun + vol rendah = shakeout/manipulasi bandar, BUKAN dumping.
    # Harga turun + vol tinggi = distribusi nyata = bahaya nyata.
    if d1_chg <= -5.0 and d1_vol >= vol_t1:
        score -= 20
        neg.append(f"Dumping nyata: D-1 turun {d1_chg:.1f}% disertai volume {d1_vol:.1f}x — distribusi bandar")
    elif d1_chg <= -5.0 and d1_vol < vol_t1:
        # Ini shakeout — catat tapi tidak penalti besar
        neg.append(f"Pola shakeout: D-1 turun {d1_chg:.1f}% tanpa volume (mungkin manipulasi sebelum ARA)")

    if up_str >= 5 and d1_vol < 2.0:
        score -= 8
        neg.append(f"Naik {up_str} hari berturut tanpa konfirmasi volume")

    if trend5d > 40.0:
        score -= 5
        neg.append(f"5d trend terlalu cepat: +{trend5d:.1f}%")

    # ==========================================================
    # MINIMUM KONFLUENSI GUARD (v2.1 — dinaikkan dari 2 ke 3)
    # ==========================================================
    confluence_min = CONFIG.get("ARA_CONFLUENCE_MIN", 3)
    if len(pos) < confluence_min:
        score = min(score, 40)
        neg.append(f"Kurang konfluensi (min {confluence_min} sinyal diperlukan, dapat {len(pos)})")

    # ==========================================================
    # PATTERN TYPE
    # ==========================================================
    if d1_chg >= cont_chg and d1_vol >= cont_vol:
        pattern_type = "CONTINUATION"
    elif d1_vol >= vol_t1 and d1_wick >= wick_min and d1_body <= body_max:
        pattern_type = "SILENT_ACCUMULATION"
    elif d1_vol >= vol_t1:
        pattern_type = "VOLUME_SPIKE"
    elif m1_green is not None and m1_green >= 12:
        pattern_type = "CLOSING_SQUEEZE"
    else:
        pattern_type = "OUT_OF_NOWHERE"

    return max(0, min(100, score)), pattern_type, pos, neg


def calculate_ara_entry_range(
    feat: Dict,
    smc_data: Optional[Dict] = None,
    pattern_type: str = "",
) -> Dict:
    """
    Hitung zona entry ideal untuk calon ARA.

    DIPERBARUI berdasarkan backtest rigorusi 3.680 first-day ARA:
    - 77% kejadian: Open D+1 Gap Up dari harga ARA
    - 64% kejadian: Gap Up > 2% dari harga ARA
    - Median gap: +3.70% dari harga ARA

    Implikasi: Limit Order di PDC (harga kemarin) akan TERTINGGAL di 77% kejadian.
    Strategi yang valid:
      - Tipe 1 (Continuation): pasang Limit Order di ARA price + 5% buffer
        agar match saat pre-opening meski gap besar.
      - Tipe 2 (Quiet Accum): pasang Limit Order di ARA price atau sedikit
        di bawahnya (gap lebih kecil, peluang fill lebih tinggi).

    entry_lower = ARA price (harga kunci maksimum kemarin)
    entry_upper = ARA price * 1.08 (buffer gap-up Tipe 1)
    """
    pdc  = feat["d1_close"]
    pdh  = feat["d1_high"]

    # Hitung harga ARA sesungguhnya (dibulatkan ke fraksi BEI)
    tick = get_tick_size(pdc)
    if pdc < 200:       ara_limit = 0.35
    elif pdc <= 5000:   ara_limit = 0.25
    else:               ara_limit = 0.20
    raw_ara   = pdc * (1 + ara_limit)
    ara_price = int(raw_ara // tick) * tick

    is_continuation = "Continuation" in pattern_type or "Tipe 1" in pattern_type

    if is_continuation:
        # Tipe 1: gap besar (median +3.70%). Pasang limit agresif di atas ARA.
        entry_lower_raw = ara_price
        entry_upper_raw = min(ara_price * 1.08, pdc * (1 + ara_limit * 1.2))
        entry_strategy  = (
            f"Pasang Limit Order pukul 19:00 di {round_bei(ara_price):,}–{round_bei(ara_price * 1.05):,} "
            f"(harga ARA + buffer). Tipe Continuation cenderung Gap Up >2% di pagi hari. "
            f"Jangan kejar jika open sudah di atas zona ini."
        )
    else:
        # Tipe 2/3: gap lebih kecil (median +1.72%). Limit Order di ARA price.
        entry_lower_raw = pdc * 0.99     # sedikit di bawah PDC sebagai backup fill
        entry_upper_raw = ara_price
        entry_strategy  = (
            f"Pasang Limit Order pukul 19:00 di {round_bei(pdc):,}–{round_bei(ara_price):,} "
            f"(PDC hingga harga ARA). Tipe Quiet Accumulation gap lebih kecil — "
            f"peluang fill order lebih tinggi dibanding Tipe Continuation."
        )

    ob_note = ""
    if smc_data and smc_data.get("ob"):
        ob = smc_data["ob"]
        ob_high = float(ob.get("ob_high", 0))
        if pdc < ob_high < entry_upper_raw:
            entry_upper_raw = ob_high
            ob_note = " (batas atas: OB 1H aktif)"

    entry_lower = round_bei(entry_lower_raw)
    entry_upper = round_bei(entry_upper_raw)

    if entry_upper <= entry_lower:
        entry_upper = round_bei(entry_lower * 1.05)

    range_pct = (entry_upper - entry_lower) / (pdc + 1e-6) * 100

    return {
        "entry_lower":      entry_lower,
        "entry_upper":      entry_upper,
        "entry_range":      f"{entry_lower:,} \u2013 {entry_upper:,}",
        "entry_note":       entry_strategy + ob_note,
        "entry_range_pct":  round(range_pct, 1),
        "ara_price_calc":   ara_price,
        "ara_limit_pct":    round(ara_limit * 100, 0),
        "gap_risk_note": (
            f"Backtest: 64% saham ARA Gap Up >2% di pagi hari. "
            f"Median gap: +3.70% dari harga ARA. Limit Order berisiko tidak fill."
        ),
    }


def calculate_ara_target(feat: Dict, pattern_type: str = "") -> Dict:
    """
    Hitung estimasi target harga calon ARA.

    DIPERBARUI berdasarkan backtest rigorusi 2.213 kunci ARA:
    - High D+1 vs Harga ARA: sempat naik >5% di 84.5% kejadian
    - Median High D+1: +16.77% dari harga ARA
    - KUNCI: Close D+1 profit hanya 48.9% (hampir coin flip)

    IMPLIKASI KRITIS:
    Jangan hold saham sampai close D+1.
    Target keluar adalah PAGI HARI (09:00-09:30) saat market baru buka
    dan retail masih dalam euforia. Ini adalah edge sesungguhnya.

    Strategi exit berdasarkan tipe:
      - Tipe 1 (Continuation): target sell di gap open atau 15 menit pertama
      - Tipe 2 (Quiet): target sell lebih sabar, bisa tunggu hingga 09:30
      - 23.5% kemungkinan saham lanjut ARA lagi (hold 1 hari extra jika konfirmasi)
    """
    pdc  = feat["d1_close"]
    pdh  = feat["d1_high"]
    pdl  = feat["d1_low"]

    # Hitung harga ARA sesungguhnya
    tick = get_tick_size(pdc)
    if pdc < 200:       ara_limit = 0.35
    elif pdc <= 5000:   ara_limit = 0.25
    else:               ara_limit = 0.20
    ara_price = int((pdc * (1 + ara_limit)) // tick) * tick

    # Target berdasarkan harga ARA (bukan PDC)
    # Karena entry realistis kita adalah di sekitar ARA price
    assumed_entry = ara_price * 1.015  # asumsi fill 1.5% di atas ARA

    # ATR normalization
    d1_range_pct = (pdh - pdl) / (pdc + 1e-6)
    vol_adj = min(1.3, max(0.8, d1_range_pct / 0.06))

    # Target berbasis backtest High D+1 (median +16.77%, P25=+5%, P75=+24%)
    # Konservatif: jual di 30 menit pertama (tidak menunggu peak)
    target_morning_exit  = round_bei(assumed_entry * (1 + 0.05 * vol_adj))  # +5% (P25 high D+1)
    target_base          = round_bei(assumed_entry * (1 + 0.12 * vol_adj))  # +12% (moderat)
    target_optimistic    = round_bei(assumed_entry * (1 + 0.20))            # +20% (hampir full ARA lagi)

    # Pastikan urutan logis
    if target_morning_exit <= ara_price:
        target_morning_exit = round_bei(ara_price * 1.03)
    if target_base <= target_morning_exit:
        target_base = round_bei(target_morning_exit * 1.07)
    if target_optimistic <= target_base:
        target_optimistic = round_bei(target_base * 1.06)

    m_pct  = round((target_morning_exit - assumed_entry) / (assumed_entry + 1e-6) * 100, 0)
    b_pct  = round((target_base - assumed_entry) / (assumed_entry + 1e-6) * 100, 0)
    op_pct = round((target_optimistic - assumed_entry) / (assumed_entry + 1e-6) * 100, 0)

    is_continuation = "Continuation" in pattern_type or "Tipe 1" in pattern_type
    if is_continuation:
        exit_guide = (
            "JUAL DI PAGI HARI (09:00-09:15 WIB). Tipe Continuation cenderung "
            "Gap Up besar — euforia retail terjadi di menit-menit pertama. "
            "Jangan tunggu. 48.9% saham balik turun sebelum close."
        )
    else:
        exit_guide = (
            "JUAL DI PAGI HARI (09:00-09:30 WIB). Backtest menunjukkan High D+1 "
            "median +16.77% dari harga ARA. Setelah 09:30, momentum sering berbalik. "
            "Jika masih ARA lagi (23.5% peluang), hold dengan trailing stop."
        )

    return {
        "target_morning_exit":     target_morning_exit,
        "target_base":             target_base,
        "target_optimistic":       target_optimistic,
        "estimated_target_pct":    f"+{int(m_pct)}% hingga +{int(b_pct)}% dari harga beli",
        "target_morning_exit_pct": m_pct,
        "target_base_pct":         b_pct,
        "target_optimistic_pct":   op_pct,
        "assumed_entry":           round_bei(assumed_entry),
        "ara_price_ref":           ara_price,
        "exit_timing_guide":       exit_guide,
        "target_note": (
            f"Pagi: {target_morning_exit:,} (~+{int(m_pct)}%) | "
            f"Moderat: {target_base:,} (~+{int(b_pct)}%) | "
            f"Full: {target_optimistic:,} (~+{int(op_pct)}%) | "
            f"JUAL PAGI, bukan hold sampai close."
        ),
        # Backtest stats untuk transparansi
        "backtest_high_d1_median":  "+16.77% dari harga ARA (n=2213)",
        "backtest_profit_morning":  "84.5% sempat naik >5% di D+1",
        "backtest_close_d1_profit": "hanya 48.9% (hampir coin flip — JANGAN hold)",
        "backtest_continued_ara":   "23.5% lanjut ARA hari berikutnya",
    }


def build_ara_reason_beginner(
    ticker: str,
    pattern_type: str,
    feat: Dict,
    score: int,
    broker_signal: str,
) -> str:
    """
    Penjelasan ARA dalam bahasa sederhana tanpa jargon berat.
    Template berbeda per pattern_type.
    """
    d1_vol = feat.get("d1_vol_ratio_ma20", 0)
    d1_chg = feat.get("d1_change", 0)

    if pattern_type == "CONTINUATION":
        reason = (
            f"{ticker} sudah naik {d1_chg:.0f}% kemarin dengan volume "
            f"{d1_vol:.0f}x lebih besar dari biasanya. Ada pembeli besar yang "
            f"masuk agresif. Jika momentum berlanjut, saham ini bisa naik lebih jauh besok."
        )
    elif pattern_type == "SILENT_ACCUMULATION":
        reason = (
            f"{ticker} diam-diam banyak ditransaksikan kemarin "
            f"(volume {d1_vol:.0f}x dari biasanya) meski harga tidak bergerak banyak. "
            f"Ini pola sebelum harga melonjak tajam — tapi bisa juga tidak terjadi apa-apa "
            f"jika tidak ada berita pendukung."
        )
    elif pattern_type == "VOLUME_SPIKE":
        reason = (
            f"{ticker} mengalami lonjakan volume kemarin ({d1_vol:.0f}x dari biasanya) "
            f"tanpa pola candle yang jelas. Konfirmasi dengan berita atau sinyal broker "
            f"sebelum masuk."
        )
    elif pattern_type == "CLOSING_SQUEEZE":
        reason = (
            f"{ticker} menunjukkan tekanan beli di akhir sesi kemarin — "
            f"harga terus naik di menit-menit terakhir. Sinyal lemah, perlu konfirmasi tambahan."
        )
    else:
        reason = (
            f"{ticker} tidak menunjukkan sinyal teknikal yang jelas kemarin. "
            f"Kandidat spekulatif murni — kemungkinan dipicu berita yang belum terlihat. "
            f"Risiko sangat tinggi."
        )

    if broker_signal in ("Big Acc", "Acc"):
        reason += f" ✅ Broker signal: {broker_signal}."

    if score >= 70:
        reason += " Skor kepercayaan: TINGGI."
    elif score >= 50:
        reason += " Skor kepercayaan: SEDANG."
    else:
        reason += " Skor kepercayaan: RENDAH — watchlist saja."

    return reason


def build_ara_risk_warning(
    ticker: str,
    feat: Dict,
    pattern_type: str,
    score: int,
) -> str:
    """
    Peringatan risiko spesifik per saham.
    Dikombinasikan dari: likuiditas, posisi MA, volume, pola, dan skor.
    """
    parts = []
    d1_vol   = feat.get("d1_vol_ratio_ma20", 0)
    d1_chg   = feat.get("d1_change", 0)
    d1_value = feat.get("d1_value_rp", 0)
    above_ma20 = feat.get("above_ma20", True)
    above_ma50 = feat.get("above_ma50", True)

    if d1_value < 1_000_000_000:
        parts.append("⚠️ SAHAM KECIL (value <Rp1M/hari) — spread bisa lebar, sulit exit saat panik")
    elif d1_value < 5_000_000_000:
        parts.append("⚠️ Likuiditas sedang — batasi posisi")

    if d1_vol < 1.5:
        parts.append("⚠️ Volume D-1 tidak konfirmasi — risiko false signal sangat tinggi")

    if not above_ma20:
        parts.append("⚠️ Harga di bawah MA20 — tren jangka pendek masih turun")

    if not above_ma50:
        parts.append("⚠️ Harga di bawah MA50 — tren jangka menengah masih bearish")

    if pattern_type in ("OUT_OF_NOWHERE", "CLOSING_SQUEEZE"):
        parts.append("⚠️ SPEKULATIF TINGGI — tidak ada sinyal OHLCV yang kuat")

    if d1_chg < -3.0:
        parts.append(f"⚠️ Harga turun {d1_chg:.1f}% kemarin — bukan pola akumulasi ideal")

    if score < 50:
        parts.append("⚠️ Skor rendah — kemungkinan false signal sangat tinggi")

    # Warning universal — selalu ada
    parts.append(
        "⚠️ FALSE SIGNAL RATE ~85-90%. "
        "Konfirmasi dengan broker flow, berita, dan analisis manual sebelum beli."
    )

    # Batasan posisi
    if d1_value < 1_000_000_000:
        max_pos = "max Rp500rb–1jt"
    elif score >= 70:
        max_pos = "max 2% dari portofolio"
    else:
        max_pos = "max 1% dari portofolio"

    parts.append(f"📌 Batasi posisi: {max_pos}.")

    return " | ".join(parts)


def get_minute_data_for_ara(ticker: str, d1_date_str: str) -> Optional[pd.DataFrame]:
    """
    Ambil data 1-minute OHLCV untuk tanggal D-0 (hari ini) dari Yahoo Finance.
    Cache di parquet. Return None jika data tidak cukup (<15 bar).
    """
    cache_path = _get_yf_cache_path(ticker, "1m_d1")

    if os.path.exists(cache_path):
        try:
            age_days = (time.time() - os.path.getmtime(cache_path)) / 86400
            if age_days <= CONFIG["YF_CACHE_DAYS"]:
                df = pd.read_parquet(cache_path)
                if df is not None and len(df) >= 15:
                    return df
        except Exception:
            pass

    yf_ticker = f"{ticker}.JK" if not ticker.endswith(".JK") else ticker
    try:
        df = yf.download(yf_ticker, period="5d", interval="1m",
                         auto_adjust=True, progress=False)
        if df is None or (hasattr(df, "empty") and df.empty) or len(df) < 15:
            return None

        df.reset_index(inplace=True)

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [c[0].lower() for c in df.columns]
        else:
            df.columns = [c.lower() for c in df.columns]

        if "datetime" in df.columns and "date" not in df.columns:
            df.rename(columns={"datetime": "date"}, inplace=True)

        for col in ["open", "high", "low", "close", "volume"]:
            if col not in df.columns:
                return None

        df = df.dropna(subset=["close", "volume"])
        df = df[df["volume"] > 0].reset_index(drop=True)
        df["date"] = pd.to_datetime(df["date"])

        # Filter ke tanggal D-0 (hari ini)
        if d1_date_str:
            try:
                target_date = pd.to_datetime(d1_date_str).date()
                df_d0 = df[df["date"].dt.date == target_date].copy()
                if len(df_d0) >= 15:
                    _save_yf_cache(ticker, "1m_d1", df_d0)
                    return df_d0
            except Exception:
                pass

        # Fallback: ambil hari TERAKHIR yang tersedia (= D-0, hari ini)
        # FIX: was dates[-2] (kemarin) — seharusnya dates[-1] (hari ini/D-0)
        # FIX: was len(dates) >= 2 — cukup >= 1 karena kita ambil hari terakhir
        dates = sorted(df["date"].dt.date.unique())
        if len(dates) >= 1:                                      # FIX: was >= 2
            df_d0 = df[df["date"].dt.date == dates[-1]].copy()  # FIX: was dates[-2]
            if len(df_d0) >= 15:
                _save_yf_cache(ticker, "1m_d1", df_d0)
                return df_d0

        return None

    except Exception as e:
        log.debug(f"Minute data gagal {ticker}: {e}")
        return None


def check_ara_liquidity_v2(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Cek liquidity untuk ARA — lebih longgar dari intraday pipeline.
    Threshold: value avg 5 hari >= Rp 500 juta, harga >= Rp 25, data >= 25 bar.
    """
    if df is None or len(df) < 25:
        n = len(df) if df is not None else 0
        return False, f"Data hanya {n} bar (butuh ≥25)"

    last_close = float(df["close"].iloc[-1])
    if last_close < 25:
        return False, f"Harga terlalu rendah: Rp{last_close:.0f}"

    recent = df.tail(5)
    values = recent["close"].astype(float) * recent["volume"].astype(float) * 100
    avg_value = float(values.mean())

    min_val = CONFIG.get("ARA_MIN_VALUE_D1", 500_000_000)
    if avg_value < min_val:
        return False, f"Value 5d avg Rp{avg_value/1e9:.3f}B < min Rp{min_val/1e9:.2f}B"

    nonzero = (recent["volume"] > 0).sum()
    if nonzero < 3:
        return False, f"Volume 0 di {5 - nonzero} dari 5 hari terakhir"

    return True, "OK"


def get_ara_universe_v2(token: Optional[str], mode: str) -> Dict[str, Dict]:
    """
    Bangun universe untuk ARA v2.

    Sumber:
      1. Volume Explosion Screener (3 page) — vol spike kemarin
      2. Guru 92 — Big Accumulation
      3. Guru 94 — Bandar Bullish Reversal
      4. Guru 77 — Foreign Flow Uptrend
      5. Top Value screener (4 page, ~100 saham teratas by value)

    Return: Dict[ticker -> metadata]
    """
    universe: Dict[str, Dict] = {}

    if mode != "FULL_STOCKBIT" or not token:
        log.warning("ARA universe kosong: butuh FULL_STOCKBIT mode")
        return universe

    # Sumber 1: Volume Explosion
    log.info("  [ARA v2 Universe] Screener Volume Explosion (3 page)...")
    vol_url  = f"{CONFIG['SB_BASE']}/screener/templates"
    vol_hdrs = _make_sb_headers(token)
    vol_hdrs["content-type"] = "application/json"
    vol_base = {
        "name": "ARA Vol Explosion v2", "description": "",
        "ordercol": 3, "ordertype": "desc",
        "filters": json.dumps([
            {"item1": 12465, "item1_name": "Volume MA 5", "item2": "12469",
             "item2_name": "Volume", "multiplier": "2", "operator": "<", "type": "compare"},
            {"item1": 12464, "item1_name": "Volume MA 20", "item2": "12465",
             "item2_name": "Volume MA 5", "multiplier": "1.5", "operator": "<", "type": "compare"},
        ]),
        "universe": json.dumps({"scope": "IHSG", "scopeID": "0", "name": "IHSG"}),
        "sequence": "12465,12469,12464", "save": "0", "screenerid": "",
        "type": "TEMPLATE_TYPE_CUSTOM",
    }
    seen: set = set()
    for page in range(1, 4):
        try:
            time.sleep(CONFIG["SB_DELAY"])
            r = requests.post(vol_url, headers=vol_hdrs,
                              data=json.dumps({**vol_base, "page": page}),
                              timeout=CONFIG["SB_TIMEOUT"])
            if r.status_code != 200:
                break
            calcs = r.json().get("data", {}).get("calcs", [])
            if not calcs:
                break
            cnt = 0
            for item in calcs:
                code = item.get("company", {}).get("symbol", "")
                if not code or code in seen:
                    continue
                seen.add(code)
                cnt += 1
                universe[code] = {
                    "ticker": code,
                    "name":   item.get("company", {}).get("name", code),
                    "in_mover_types": ["SCREENER_VOL_EXPLOSION"],
                }
            log.info(f"    Vol Explosion page {page}: {cnt} saham baru")
        except Exception as e:
            log.warning(f"    Vol Explosion error page {page}: {e}")
            break

    # Sumber 2-5: Guru Screener
    # TAMBAH: Guru 97 (Frequency Spike) — frekuensi transaksi meledak adalah
    # tanda ritel FOMO atau bandar akumulasi diam-diam tanpa perlu vol besar.
    for guru_id, guru_label, guru_tag in [
        (92, "Big Accumulation",       "GURU_92"),
        (94, "Bandar Bullish Reversal", "GURU_94"),
        (77, "Foreign Flow Uptrend",   "GURU_77"),
        (97, "Frequency Spike",        "GURU_97"),  # v2.1: tambah sinyal frekuensi
    ]:
        log.info(f"  [ARA v2 Universe] Guru {guru_id} ({guru_label})...")
        data = sb_get(f"/screener/templates/{guru_id}", token,
                      {"type": "TEMPLATE_TYPE_GURU", "page": 1})
        if not data:
            continue
        calcs = data.get("data", {}).get("calcs", [])
        cnt = 0
        for item in calcs:
            code = item.get("company", {}).get("symbol", "")
            if not code:
                continue
            if code not in universe:
                universe[code] = {
                    "ticker": code,
                    "name":   item.get("company", {}).get("name", code),
                    "in_mover_types": [guru_tag],
                }
                cnt += 1
            else:
                if guru_tag not in universe[code]["in_mover_types"]:
                    universe[code]["in_mover_types"].append(guru_tag)
        log.info(f"    +{cnt} saham baru (total: {len(universe)})")

    # Sumber 5: Top Value (4 page)
    log.info("  [ARA v2 Universe] Top Value (4 page)...")
    tv_hdrs = _make_sb_headers(token)
    tv_hdrs["content-type"] = "application/json"
    tv_base = {
        "name": "ARA Top Value", "description": "",
        "ordercol": 2, "ordertype": "desc",
        "filters": json.dumps([
            {"item1": 13620, "item1_name": "Value", "item2": "1000000000",
             "item2_name": "", "multiplier": "0", "operator": ">", "type": "basic"}
        ]),
        "universe": json.dumps({"scope": "IHSG", "scopeID": "0", "name": "IHSG"}),
        "sequence": "13620", "save": "0", "screenerid": "undefined",
        "type": "TEMPLATE_TYPE_CUSTOM",
    }
    seen_tv: set = set()
    for page in range(1, 5):
        try:
            time.sleep(CONFIG["SB_DELAY"])
            r = requests.post(vol_url, headers=tv_hdrs,
                              data=json.dumps({**tv_base, "page": page}),
                              timeout=CONFIG["SB_TIMEOUT"])
            if r.status_code != 200:
                break
            calcs = r.json().get("data", {}).get("calcs", [])
            if not calcs:
                break
            cnt = 0
            for item in calcs:
                code = item.get("company", {}).get("symbol", "")
                if not code or code in seen_tv:
                    continue
                seen_tv.add(code)
                if code not in universe:
                    universe[code] = {
                        "ticker": code,
                        "name":   item.get("company", {}).get("name", code),
                        "in_mover_types": ["SCREENER_TOP_VALUE"],
                    }
                    cnt += 1
                else:
                    if "SCREENER_TOP_VALUE" not in universe[code]["in_mover_types"]:
                        universe[code]["in_mover_types"].append("SCREENER_TOP_VALUE")
            log.info(f"    Top Value page {page}: {cnt} saham baru")
        except Exception as e:
            log.warning(f"    Top Value error page {page}: {e}")
            break

    log.info(f"  ARA v2 Universe final: {len(universe)} saham unik")
    return universe


def run_ara_pipeline_v2(token: Optional[str], mode: str) -> List[Dict]:
    """
    Pipeline deteksi calon ARA v2 — FINAL IMPLEMENTATION.

    LANGKAH:
      1. Bangun universe dari Stockbit API (~20-100 kandidat)
      2. Download data Daily Yahoo Finance
      3. Liquidity check (longgar: Rp500jt/hari)
      4. Feature engineering: Daily (wajib) + Minute/1M (opsional)
      5. Scoring: Daily (0-70) + Minute (0-20)
      6. Broker signal bonus dari Stockbit (+0 s/d +10)
      7. Build output JSON dengan semua field baru
      8. Sort by score DESC, trim ke ARA_MAX_OUTPUT terbaik

    HANYA berjalan dalam FULL_STOCKBIT mode.
    Yahoo-only mode: return [] (ARA butuh broker signal Stockbit).

    Returns: List of Dict, max ARA_MAX_OUTPUT items, sorted by score DESC.
    """
    if not CONFIG.get("ARA_ENABLED", True):
        log.info("Pipeline ARA v2 di-skip (ARA_ENABLED=False)")
        return []

    log.info("\n" + "=" * 70)
    log.info("🎯 PIPELINE ARA v2 — Dual Timeframe (Daily + Minute)")
    log.info("   Scoring: Daily(0-70) + Minute(0-20) + Stockbit bonus(0-10)")
    log.info("=" * 70)

    # Step 1: Universe
    log.info("[ARA v2 Step 1] Bangun universe...")
    ara_universe = get_ara_universe_v2(token, mode)
    if not ara_universe:
        log.warning("ARA v2 universe kosong — pipeline dihentikan")
        return []

    candidates = []
    total = len(ara_universe)

    for idx, (code, stock_info) in enumerate(ara_universe.items()):
        log.info(f"  [ARA v2 {idx+1}/{total}] {code}...")

        try:
            # Step 2: Daily data
            df_daily = get_daily_data(code)
            if df_daily is None or len(df_daily) < 55:
                log.debug(f"    {code}: data harian tidak cukup")
                continue

            # Step 3: Liquidity check
            liq_ok, liq_msg = check_ara_liquidity_v2(df_daily)
            if not liq_ok:
                log.debug(f"    {code}: FAIL liquidity — {liq_msg}")
                continue

            # Step 4a: Features dari Daily saja (quick pass)
            feat = compute_ara_features_v2(df_daily, df_minute=None)
            if feat is None:
                log.debug(f"    {code}: feature computation gagal")
                continue

            # Quick pre-filter v2.1 — lebih cerdas dari sekadar cek vol
            # Logika baru: skip HANYA jika vol D-1 DAN D-2 benar-benar mati
            # DAN tidak ada indikasi aktivitas lain yang perlu dicek minute-nya.
            # Tujuan: hindari membuang pola shakeout atau Out-of-Nowhere yang berpotensi
            # dikonfirmasi oleh closing squeeze di data menit.
            d1_vol_quick = feat.get("d1_vol_ratio_ma20", 0)
            d2_vol_quick = feat.get("d2_vol_ratio_ma20", 0)
            d3_vol_quick = feat.get("d2_vol_ratio_ma20", 0)  # proxy D-3 (fallback)
            # Hanya skip jika benar-benar mati total: D-1 < 0.5x DAN D-2 < 0.7x
            # (lebih ketat dari sebelumnya: dulu D-2 < 1.0x, sekarang 0.7x)
            if d1_vol_quick < 0.5 and d2_vol_quick < 0.7:
                log.debug(
                    f"    {code}: pre-filter skip — vol benar-benar mati "
                    f"(D-1={d1_vol_quick:.2f}x, D-2={d2_vol_quick:.2f}x)"
                )
                continue

            # Step 4b: Minute data untuk D-1 (best-effort, tidak wajib)
            df_minute_d1 = get_minute_data_for_ara(code, feat.get("d1_date", ""))
            if df_minute_d1 is not None and len(df_minute_d1) >= 15:
                feat_full = compute_ara_features_v2(df_daily, df_minute=df_minute_d1)
                if feat_full is not None:
                    feat = feat_full
                    log.debug(f"    {code}: minute data OK ({len(df_minute_d1)} bar)")
            else:
                log.debug(f"    {code}: minute data tidak tersedia, Daily-only scoring")

            # Step 5: Scoring
            score, pattern_type, reasons_pos, reasons_neg = score_ara_candidate_v2(feat)

            if score < CONFIG.get("ARA_SCORE_MIN", 55):
                log.debug(f"    {code}: score {score} < {CONFIG.get('ARA_SCORE_MIN',55)} — skip")
                continue

            # Step 6: Broker signal bonus + Anchor Gate check
            broker_signal = "N/A"
            anchor_bandar  = False  # Anchor 3: broker signal
            if mode == "FULL_STOCKBIT" and token:
                try:
                    bs, _ = get_broker_signal(code, token)
                    broker_signal = bs
                    if bs == "Big Acc":
                        score = min(100, score + 10)
                        reasons_pos.append("Broker signal: Big Accumulation (+10 poin)")
                        anchor_bandar = True
                    elif bs == "Acc":
                        score = min(100, score + 5)
                        reasons_pos.append("Broker signal: Accumulation (+5 poin)")
                        anchor_bandar = True
                    elif bs in ("Dist", "Big Dist"):
                        score = max(0, score - 15)
                        reasons_neg.append(f"Broker signal: {bs} (-15 poin)")
                except Exception as e_bs:
                    log.debug(f"    {code}: broker signal error: {e_bs}")

            # ================================================================
            # MANDATORY ANCHOR GATE (v2.1 — kunci utama peningkatan win-rate)
            # Kandidat WAJIB memenuhi minimal 2 dari 3 sinyal jangkar:
            #   Anchor 1 — Volume D-1 >= 3x MA20 (vol spike kuat)
            #   Anchor 2 — Closing momentum >= 12/14 bar hijau (sinyal universal)
            #   Anchor 3 — Broker signal = Acc atau Big Acc (konfirmasi bandar)
            # Tanpa 2 anchor, saham dibuang meski score cukup.
            # Ini secara sengaja melepas Tipe 3 tanpa konfirmasi bandar.
            # ================================================================
            anchor_vol     = feat.get("d1_vol_ratio_ma20", 0) >= CONFIG.get("ARA_ANCHOR_VOL_MIN", 3.0)
            
            # [AUDIT FIX] Anchor closing tidak boleh dipenuhi oleh flat bar (saham sepi / pre-closing bias)
            m1_flat_val = feat.get("m1_flat_count")
            anchor_closing = (
                feat.get("m1_last15_green") is not None
                and feat.get("m1_last15_green") >= CONFIG.get("ARA_ANCHOR_GREEN_MIN", 12)
                and (m1_flat_val is None or m1_flat_val < CONFIG.get("ARA_ANCHOR_FLAT_MAX", 4))
            )
            anchors_met = sum([anchor_vol, anchor_closing, anchor_bandar])
            anchor_min  = CONFIG.get("ARA_ANCHOR_MIN", 2)

            if anchors_met < anchor_min:
                log.debug(
                    f"    {code}: FAIL Anchor Gate "
                    f"({anchors_met}/{anchor_min} anchor terpenuhi: "
                    f"vol={anchor_vol}, closing={anchor_closing}, bandar={anchor_bandar})"
                )
                continue

            log.debug(
                f"    {code}: PASS Anchor Gate "
                f"({anchors_met}/3: vol={anchor_vol}, closing={anchor_closing}, bandar={anchor_bandar})"
            )

            if score < CONFIG.get("ARA_SCORE_MIN", 55):
                log.debug(f"    {code}: score {score} setelah broker adj — skip")
                continue

            # Step 7: Build output fields
            # SMC context untuk entry range (best-effort)
            smc_ctx = None
            try:
                smc_ok, smc_ctx = check_smc_structure(code)
            except Exception:
                pass

            entry_data  = calculate_ara_entry_range(feat, smc_data=smc_ctx, pattern_type=pattern_type)
            target_data = calculate_ara_target(feat, pattern_type=pattern_type)
            reason_txt  = build_ara_reason_beginner(code, pattern_type, feat, score, broker_signal)
            warning_txt = build_ara_risk_warning(code, feat, pattern_type, score)

            score_tier = "STRONG" if score >= CONFIG.get("ARA_SCORE_STRONG", 60) else "MODERATE"

            log.info(
                f"    ✅ {code}: score={score} ({score_tier}) | "
                f"{pattern_type} | confluences={len(reasons_pos)} | "
                f"entry={entry_data['entry_range']} | "
                f"target={target_data['estimated_target_pct']}"
            )

            candidates.append({
                # Identifikasi
                "ticker":   code,
                "company":  stock_info.get("name", code),
                "type":     "CALON_ARA",

                # Scoring
                "score":            score,
                "score_tier":       score_tier,
                "pattern_type":     pattern_type,
                "confluence_count": len(reasons_pos),

                # ===== FIELD BARU (approved di Prompt 1) =====
                "entry_range":              entry_data["entry_range"],
                "entry_lower":              entry_data["entry_lower"],
                "entry_upper":              entry_data["entry_upper"],
                "entry_note":               entry_data["entry_note"],
                "entry_range_pct":          entry_data["entry_range_pct"],
                "ara_price_ref":            entry_data.get("ara_price_calc", 0),
                "ara_limit_pct":            entry_data.get("ara_limit_pct", 0),
                "gap_risk_note":            entry_data.get("gap_risk_note", ""),
                "estimated_target_pct":     target_data["estimated_target_pct"],
                "target_morning_exit":      target_data["target_morning_exit"],
                "target_base":              target_data["target_base"],
                "target_optimistic":        target_data["target_optimistic"],
                "target_morning_exit_pct":  target_data["target_morning_exit_pct"],
                "target_base_pct":          target_data["target_base_pct"],
                "target_optimistic_pct":    target_data["target_optimistic_pct"],
                "target_note":              target_data["target_note"],
                "assumed_entry":            target_data["assumed_entry"],
                "exit_timing_guide":        target_data["exit_timing_guide"],
                "reason_beginner_friendly": reason_txt,
                "risk_warning":             warning_txt,

                # Backtest stats (transparansi penuh)
                "d1p_backtest_stats": {
                    "high_d1_median":     target_data["backtest_high_d1_median"],
                    "profit_morning":     target_data["backtest_profit_morning"],
                    "close_d1_profit":    target_data["backtest_close_d1_profit"],
                    "continued_ara_pct":  target_data["backtest_continued_ara"],
                    "sample_size":        "n=2.213 kunci ARA pertama pada saham liquid",
                },

                # Broker
                "broker_signal": broker_signal,

                # ===== FIELD TEKNIKAL (untuk backtest & audit) =====
                "d1_date":            feat["d1_date"],
                "d1_close":           feat["d1_close"],
                "d1_open":            feat["d1_open"],
                "d1_high":            feat["d1_high"],
                "d1_low":             feat["d1_low"],
                "d1_change_pct":      feat["d1_change"],
                "d1_vol_ratio_ma20":  feat["d1_vol_ratio_ma20"],
                "d1_vol_ratio_ma5":   feat["d1_vol_ratio_ma5"],
                "d1_body_pct":        feat["d1_body_pct"],
                "d1_upper_wick":      feat["d1_upper_wick"],
                "d1_lower_wick":      feat["d1_lower_wick"],
                "d1_close_pos":       feat["d1_close_pos"],
                "d1_bullish":         feat["d1_bullish"],
                "d1_range_expansion": feat["d1_range_expansion"],
                "d1_value_rp":        round(feat["d1_value_rp"]),
                "d2_change_pct":      feat["d2_change"],
                "d2_body_pct":        feat["d2_body_pct"],
                "d2_vol_ratio_ma20":  feat["d2_vol_ratio_ma20"],

                # Minute features (None jika tidak tersedia)
                "m1_last15_green":    feat.get("m1_last15_green"),
                "m1_close_vs_vwap":   feat.get("m1_close_vs_vwap"),
                "m1_close_pos_intra": feat.get("m1_close_pos_intra"),
                "m1_high_timing":     feat.get("m1_high_timing"),
                "m1_n_bars":          feat.get("m1_n_bars"),
                "m1_data_available":  feat.get("m1_last15_green") is not None,

                # Context
                "above_ma20":     feat["above_ma20"],
                "above_ma50":     feat["above_ma50"],
                "ma20":           feat["ma20"],
                "ma50":           feat["ma50"],
                "up_streak_days": feat["up_streak"],
                "trend_5d_pct":   feat["trend_5d_pct"],

                # Reasoning (transparan untuk audit)
                "signals_positive": reasons_pos,
                "signals_negative": reasons_neg,

                # Sumber universe
                "universe_sources": stock_info.get("in_mover_types", []),

                # Warning universal
                "warning": (
                    "⚠️ FALSE SIGNAL RATE TINGGI (~85-90%). "
                    "Pipeline ini mendeteksi Tipe 1 (Continuation) dan Tipe 2 (Silent Accumulation). "
                    "Tipe 3 (Out of Nowhere, ~48% ARA nyata) TIDAK BISA dideteksi dari OHLCV. "
                    "Gunakan sebagai WATCHLIST saja. "
                    "Konfirmasi dengan broker flow, news, dan analisis manual sebelum eksekusi."
                ),
                "generated_at": datetime.now().strftime("%H:%M WIB"),
            })

        except Exception as e_loop:
            log.warning(f"    {code}: error — {e_loop}")
            continue

        gc.collect()

    # Step 8: Sort & trim
    candidates.sort(
        key=lambda x: (
            x["score"],
            x["confluence_count"],
            1 if x.get("m1_data_available") else 0,
        ),
        reverse=True,
    )
    max_out = CONFIG.get("ARA_MAX_OUTPUT", 5)
    result  = candidates[:max_out]

    log.info(f"\n✅ ARA v2 selesai: {len(result)} kandidat final (dari {len(candidates)} lolos scoring)")
    for r in result:
        log.info(
            f"   → {r['ticker']}: score={r['score']} | {r['pattern_type']} | "
            f"entry={r['entry_range']} | target={r['estimated_target_pct']}"
        )
    return result


def save_combined_output_v2(
    intraday_results: List[Dict],
    ara_results: List[Dict],
    mode: str,
    market_ctx: Dict,
    intraday_summary: Dict,
    session_label: str = "MARKET_DAY",
):
    """
    Simpan output gabungan ke combined_screening.json (v2).

    Struktur JSON output:
    {
      "logika_lama_intraday"  : [...],   # pipeline intraday (tidak berubah)
      "logika_baru_calon_ara" : [...],   # pipeline ARA v2 dengan field baru
      "meta"                  : {...},   # metadata + disclaimer
      "market_context"        : {...},
      "screening_summary"     : {...},   # funnel filter intraday
      "config_ara"            : {...},   # config ARA yang dipakai saat run
    }

    Juga simpan dated copy: combined_screening_YYYYMMDD.json
    """
    today     = datetime.now().strftime("%Y-%m-%d")
    today_str = datetime.now().strftime("%Y%m%d")

    output = {
        "logika_lama_intraday":  intraday_results,
        "logika_baru_calon_ara": ara_results,
        "bsjp_beli_sore_jual_pagi": [],
        "bpjs_beli_pagi_jual_sore": [],
        "swing_trading":            [],
        "trend_following":          [],
        "position_trading":         [],

        "meta": {
            "status":           "success" if (intraday_results or ara_results) else "no_signal",
            "generated_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S WIB"),
            "date":             today,
            "mode":             mode,
            "session_label":    session_label,
            "pipeline_version": "v2.0",
            "session_warning": (
                "⚠️ Screening akhir pekan — referensi persiapan saja, bukan sinyal eksekusi."
                if "PRE_MARKET_WEEKEND" in session_label else None
            ),
            "mode_warning": (
                None if mode == "FULL_STOCKBIT"
                else "⚠️ TOKEN TIDAK TERSEDIA — ARA pipeline tidak berjalan"
            ),
            "intraday_count": len(intraday_results),
            "ara_count":      len(ara_results),
            "ara_disclaimer": (
                "Pipeline ARA v2: deteksi Tipe 1 (Continuation) & Tipe 2 (Silent Accumulation). "
                "Tipe 3 (Out of Nowhere, ~48% dari ARA nyata) tidak terdeteksi dari OHLCV. "
                "Precision: 8-15%. Backtest wajib sebelum live trading."
            ),
            "scoring_breakdown": {
                "daily_max": 70, "minute_max": 20, "stockbit_bonus": 10, "total_max": 100,
            },
        },

        "market_context":    market_ctx,
        "screening_summary": intraday_summary,

        "config_ara": {
            "vol_ma20_tier1":       CONFIG.get("ARA_VOL_MA20_TIER1", 3.0),
            "vol_ma20_tier2":       CONFIG.get("ARA_VOL_MA20_TIER2", 6.0),
            "vol_ma20_d2_min":      CONFIG.get("ARA_VOL_MA20_D2_MIN", 1.5),
            "upper_wick_min":       CONFIG.get("ARA_UPPER_WICK_MIN", 0.35),
            "body_max_rejection":   CONFIG.get("ARA_BODY_MAX_REJECTION", 0.40),
            "close_pos_max":        CONFIG.get("ARA_CLOSE_POS_MAX", 0.50),
            "continuation_chg_min": CONFIG.get("ARA_CONTINUATION_CHG_MIN", 8.0),
            "continuation_vol_min": CONFIG.get("ARA_CONTINUATION_VOL_MIN", 3.0),
            "min_value_d1_rp":      CONFIG.get("ARA_MIN_VALUE_D1", 500_000_000),
            "score_min":            CONFIG.get("ARA_SCORE_MIN", 40),
            "score_strong":         CONFIG.get("ARA_SCORE_STRONG", 60),
            "max_output":           CONFIG.get("ARA_MAX_OUTPUT", 5),
            "minute_data_enabled":  True,
            "minute_closing_bars":  15,
        },
    }

    combined_path = CONFIG.get("ARA_OUTPUT_FILE", "combined_screening.json")
    with open(combined_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)
    log.info(f"✅ Tersimpan: {combined_path}")

    dated_path = f"combined_screening_{today_str}.json"
    with open(dated_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)
    log.info(f"✅ Tersimpan: {dated_path}")


# =============================================================================
# PIPELINE BSJP (BELI SORE JUAL PAGI) — v1.0
# =============================================================================
# Fungsi utama: run_bsjp_pipeline(universe, mode)
#
# Input  : universe (List[Dict]) — saham aktif hari ini dari pipeline intraday
# Output : List[Dict] — kandidat BSJP terurut by score, max BSJP_MAX_OUTPUT
#
# Prinsip desain:
#   1. TIDAK menyentuh pipeline intraday maupun ARA
#   2. Memanfaatkan get_daily_data() yang sudah di-cache (0 download tambahan)
#   3. Memanfaatkan field universe (value_today, net_foreign, iep_change, dll)
#   4. Formula berdasarkan backtest kuantitatif 252.480 hari perdagangan
# =============================================================================

def _bsjp_compute_ohlcv_features(ticker: str) -> Optional[Dict]:
    """
    Ambil data OHLCV harian (dari cache YF) dan hitung fitur teknikal BSJP.
    Return None jika data tidak cukup atau tidak memenuhi syarat dasar.

    Fitur yang dihitung (semua dari data historis, bukan real-time):
      - vol_ratio20  : volume hari ini vs rata-rata MA20
      - body_pct     : kekuatan candle (close - open) / open
      - close_pos    : posisi close di dalam range (0=low, 1=high)
      - above_ma20/50/200 : posisi harga terhadap MA utama
      - ma20_slope   : arah tren MA20 (positif = naik)
      - atr_pct      : volatilitas normal (ATR14 / close)
    """
    df = get_daily_data(ticker)
    if df is None or len(df) < 60:
        return None

    # Pastikan kolom lowercase
    df.columns = [c.lower() if isinstance(c, str) else str(c) for c in df.columns]

    try:
        # Volume dan value
        if "volume" not in df.columns:
            return None

        vol_ma20 = df["volume"].rolling(20).mean().iloc[-1]
        if pd.isna(vol_ma20) or vol_ma20 == 0:
            return None
        vol_today = float(df["volume"].iloc[-1])
        vol_ratio20 = vol_today / vol_ma20

        # Candle anatomy hari ini
        open_  = float(df["open"].iloc[-1])
        high   = float(df["high"].iloc[-1])
        low    = float(df["low"].iloc[-1])
        close  = float(df["close"].iloc[-1])
        range_ = high - low

        if range_ == 0 or open_ == 0:
            return None

        body_pct   = (close - open_) / open_
        close_pos  = (close - low) / range_
        upper_wick = (high - max(open_, close)) / range_

        # Moving averages
        ma20  = df["close"].rolling(20).mean().iloc[-1]
        ma50  = df["close"].rolling(50).mean().iloc[-1]
        ma200 = df["close"].rolling(200).mean().iloc[-1]

        above_ma20  = (not pd.isna(ma20))  and close > ma20
        above_ma50  = (not pd.isna(ma50))  and close > ma50
        above_ma200 = (not pd.isna(ma200)) and close > ma200

        # Slope MA20 (tren 5 hari terakhir)
        ma20_5d_ago = df["close"].rolling(20).mean().iloc[-6] if len(df) >= 26 else ma20
        ma20_slope  = (ma20 - ma20_5d_ago) / ma20_5d_ago if (ma20_5d_ago and ma20_5d_ago != 0) else 0

        # ATR14 sebagai proxy volatilitas
        tr = pd.Series(np.maximum(
            df["high"] - df["low"],
            np.maximum(
                (df["high"] - df["close"].shift(1)).abs(),
                (df["low"]  - df["close"].shift(1)).abs()
            )
        ))
        atr14   = tr.rolling(14).mean().iloc[-1]
        atr_pct = atr14 / close if close > 0 else 0

        # Nilai tukar hari ini (untuk filter tambahan di pemanggil)
        val_ma20 = (df["close"] * df["volume"]).rolling(20).mean().iloc[-1]

        # ADX via pandas_ta (sudah diimport sebagai `ta` di baris 29)
        adx_df  = ta.adx(df["high"], df["low"], df["close"], length=14)
        adx_val = float(adx_df["ADX_14"].iloc[-1]) if (
            adx_df is not None and not adx_df.empty and
            not pd.isna(adx_df["ADX_14"].iloc[-1])
        ) else 0.0

        return {
            "vol_ratio20":  round(vol_ratio20, 2),
            "body_pct":     round(body_pct, 4),
            "close_pos":    round(close_pos, 4),
            "upper_wick":   round(upper_wick, 4),
            "above_ma20":   above_ma20,
            "above_ma50":   above_ma50,
            "above_ma200":  above_ma200,
            "ma20_slope":   round(float(ma20_slope), 6),
            "atr_pct":      round(float(atr_pct), 4),
            "val_ma20":     float(val_ma20) if not pd.isna(val_ma20) else 0,
            "close":        close,
            "high":         high,
            "low":          low,
            "open":         open_,
            "adx":          round(adx_val, 1),   # [BARU]
        }
    except Exception as e:
        log.debug(f"    BSJP feature error {ticker}: {e}")
        return None


def _bsjp_score(feat_ohlcv: Dict, stock_mm: Dict) -> Tuple[int, str, List[str], List[str]]:
    """
    Hitung skor BSJP dan tentukan tier.

    Return: (score, tier, signals_positive, signals_negative)
      - tier : "S" (premium) | "A" (solid) | "NONE" (tidak lolos)
      - score: 0-100
    """
    score = 0
    signals_pos = []
    signals_neg = []

    vol_ratio20 = feat_ohlcv["vol_ratio20"]
    body_pct    = feat_ohlcv["body_pct"]
    close_pos   = feat_ohlcv["close_pos"]
    above_ma20  = feat_ohlcv["above_ma20"]
    above_ma50  = feat_ohlcv["above_ma50"]
    above_ma200 = feat_ohlcv["above_ma200"]
    ma20_slope  = feat_ohlcv["ma20_slope"]

    # ── ADX GATE (wajib) ────────────────────────────────────────────────────
    # Backtest: tanpa ADX filter → WR Tier S 37.6%, Tier A 31.5%
    # Dengan ADX>=25 → WR Tier S 51.5%, Tier A 43.1%
    adx = feat_ohlcv.get("adx", 0.0)
    if adx < CONFIG.get("BSJP_ADX_MIN", 25):
        return 0, "NONE", [], [
            f"ADX terlalu lemah ({adx:.1f} < {CONFIG.get('BSJP_ADX_MIN', 25)}) "
            f"— tren tidak cukup kuat untuk BSJP"
        ]
    # ── AKHIR ADX GATE ──────────────────────────────────────────────────────

    # ---- TENTUKAN TIER TEKNIKAL ----
    tier_s = (
        vol_ratio20 >= CONFIG["BSJP_TIER_S_VOL"]  and
        close_pos   >= CONFIG["BSJP_TIER_S_CPOS"] and
        body_pct    >= CONFIG["BSJP_TIER_S_BODY"] and
        above_ma20 and above_ma50 and above_ma200
    )
    tier_a = (
        vol_ratio20 >= CONFIG["BSJP_TIER_A_VOL"]  and
        close_pos   >= CONFIG["BSJP_TIER_A_CPOS"] and
        body_pct    >= CONFIG["BSJP_TIER_A_BODY"] and
        above_ma20 and above_ma50 and above_ma200
    )

    if not (tier_s or tier_a):
        return 0, "NONE", signals_pos, signals_neg

    tier = "S" if tier_s else "A"

    # ---- BASE SCORE dari fitur teknikal ----
    # Volume (max 30 poin)
    if vol_ratio20 >= 8.0:
        score += 30; signals_pos.append(f"Volume ledakan ekstrem {vol_ratio20:.1f}x MA20")
    elif vol_ratio20 >= 5.0:
        score += 24; signals_pos.append(f"Volume spike kuat {vol_ratio20:.1f}x MA20")
    elif vol_ratio20 >= 3.0:
        score += 16; signals_pos.append(f"Volume spike {vol_ratio20:.1f}x MA20")
    else:
        score += 8;  signals_pos.append(f"Volume di atas rata-rata {vol_ratio20:.1f}x")

    # Candle strength (max 25 poin)
    if body_pct >= 0.07:
        score += 25; signals_pos.append(f"Candle hijau sangat kuat ({body_pct*100:.1f}%)")
    elif body_pct >= 0.05:
        score += 20; signals_pos.append(f"Candle hijau kuat ({body_pct*100:.1f}%)")
    elif body_pct >= 0.04:
        score += 16; signals_pos.append(f"Candle hijau solid ({body_pct*100:.1f}%)")
    else:
        score += 10; signals_pos.append(f"Candle hijau ({body_pct*100:.1f}%)")

    # Close position — tutup di puncak (max 20 poin)
    if close_pos >= 0.98:
        score += 20; signals_pos.append("Tutup di puncak absolut — tidak ada upper wick")
    elif close_pos >= 0.95:
        score += 17; signals_pos.append(f"Tutup sangat dekat puncak ({close_pos*100:.0f}%)")
    elif close_pos >= 0.90:
        score += 13; signals_pos.append(f"Tutup dekat puncak ({close_pos*100:.0f}%)")
    else:
        score += 8;  signals_pos.append(f"Tutup di {close_pos*100:.0f}% dari range")

    # Trend (max 10 poin)
    trend_score = 0
    if above_ma20:  trend_score += 3
    if above_ma50:  trend_score += 3
    if above_ma200: trend_score += 4
    score += trend_score
    if above_ma20 and above_ma50 and above_ma200:
        signals_pos.append("Uptrend penuh: di atas MA20, MA50, MA200")
    elif above_ma20 and above_ma50:
        signals_pos.append("Di atas MA20 & MA50 (tren menengah bagus)")
    else:
        signals_neg.append("Belum di atas semua MA — tren tidak sempurna")

    # MA slope positif (bonus 3 poin)
    if ma20_slope > 0.002:
        score += 3; signals_pos.append("MA20 sedang naik — tren menguat")

    # ---- BONUS dari data universe real-time ----
    net_foreign   = stock_mm.get("net_foreign_today", 0)
    from_screener = stock_mm.get("from_screener", False)
    from_gainer   = stock_mm.get("from_gainer", False)
    iep_chg       = stock_mm.get("iep_change_pct", 0) or 0
    today_dow     = datetime.now().weekday()  # 0=Senin, 1=Selasa

    if net_foreign > 0:
        score += CONFIG["BSJP_BONUS_NET_FOREIGN"]
        signals_pos.append(f"Asing beli bersih hari ini (Rp {net_foreign/1e9:.1f}B)")
    elif net_foreign < 0:
        signals_neg.append("Asing jual bersih hari ini — hati-hati")

    if from_screener:
        score += CONFIG["BSJP_BONUS_VOL_SCREENER"]
        signals_pos.append("Masuk screener volume explosion Stockbit")

    if from_gainer:
        score += CONFIG["BSJP_BONUS_GAINER"]
        signals_pos.append("Saham naik signifikan hari ini (momentum)")

    if iep_chg > 1.5:
        score += CONFIG["BSJP_BONUS_IEP_STRONG"]
        signals_pos.append(f"IEP naik {iep_chg:.1f}% — ekspektasi gap up besok")

    if today_dow in (0, 1):  # Senin atau Selasa
        score += CONFIG["BSJP_BONUS_GOOD_DAY"]
        day_name = "Senin" if today_dow == 0 else "Selasa"
        signals_pos.append(f"Hari {day_name} — win rate BSJP historis tertinggi")

    return score, tier, signals_pos, signals_neg


def run_bsjp_pipeline(universe: List[Dict], mode: str) -> List[Dict]:
    """
    Pipeline BSJP (Beli Sore Jual Pagi) — v1.0

    Menerima universe yang sudah dikumpulkan pipeline intraday.
    Memanfaatkan get_daily_data() yang sudah di-cache — 0 download tambahan.

    Alur:
      1. Filter: value_today >= BSJP_MIN_VALUE_TODAY
      2. Filter: change_pct >= BSJP_MIN_CHG_PCT (hindari yang crash)
      3. Filter: price >= BSJP_MIN_PRICE
      4. Hitung fitur OHLCV (dari cache)
      5. Terapkan formula Tier S / Tier A
      6. Scoring + bonus dari data universe
      7. Sort by score, ambil top BSJP_MAX_OUTPUT
    """
    log.info("\n" + "=" * 70)
    log.info("📈 BSJP PIPELINE — Beli Sore Jual Pagi v1.0")
    log.info(f"   Universe input: {len(universe)} saham dari pipeline intraday")
    log.info("=" * 70)

    min_value = CONFIG["BSJP_MIN_VALUE_TODAY"]
    min_price = CONFIG["BSJP_MIN_PRICE"]
    min_chg   = CONFIG["BSJP_MIN_CHG_PCT"]

    candidates = []
    filtered_value = 0
    filtered_price = 0
    filtered_chg   = 0
    processed      = 0
    no_data        = 0

    for stock_mm in universe:
        ticker    = stock_mm.get("ticker", "")
        val_today = stock_mm.get("value_today", 0) or 0
        price     = stock_mm.get("price", 0) or 0
        chg_pct   = stock_mm.get("change_pct", 0) or 0

        # ---- Filter 1: Likuiditas aktif hari ini ----
        if val_today < min_value:
            filtered_value += 1
            continue

        # ---- Filter 2: Harga minimum ----
        if price < min_price:
            filtered_price += 1
            continue

        # ---- Filter 3: Tidak sedang crash ----
        if chg_pct < min_chg:
            filtered_chg += 1
            log.debug(f"    BSJP {ticker}: SKIP crash ({chg_pct:+.1f}%)")
            continue

        processed += 1

        # ---- Ambil fitur OHLCV (dari cache) ----
        feat = _bsjp_compute_ohlcv_features(ticker)
        if feat is None:
            no_data += 1
            log.debug(f"    BSJP {ticker}: No OHLCV data")
            continue

        # ---- Score ----
        score, tier, sig_pos, sig_neg = _bsjp_score(feat, stock_mm)
        if tier == "NONE":
            log.debug(f"    BSJP {ticker}: FAIL formula (vol={feat['vol_ratio20']:.1f}x, "
                      f"cpos={feat['close_pos']:.2f}, body={feat['body_pct']*100:.1f}%)")
            continue

        log.info(f"    ✅ BSJP {ticker}: Tier {tier} | Score {score} | "
                 f"Vol {feat['vol_ratio20']:.1f}x | Body {feat['body_pct']*100:.1f}% | "
                 f"ClosePos {feat['close_pos']*100:.0f}%")

        # ---- Hitung estimasi entry dan target ----
        close_price = feat["close"]
        tick = get_tick_size(close_price)

        # Entry: beli di range akhir sesi (harga sekarang ± 0.5 ATR)
        atr_rp    = feat["atr_pct"] * close_price
        entry_low  = round_bei(max(close_price - atr_rp * 0.3, close_price * 0.99))
        entry_high = round_bei(close_price)

        # Target: estimasi gap up besok pagi (berdasarkan win rate historis)
        if tier == "S":
            target_pct  = "+2% s/d +10% di pagi hari"
            win_rate_str = "~70% berdasarkan backtest 1.796 kejadian historis"
        else:
            target_pct  = "+2% s/d +8% di pagi hari"
            win_rate_str = "~64% berdasarkan backtest 3.330 kejadian historis"

        # Stop loss: jika open besok gap down melewati low hari ini
        stop_loss = round_bei(feat["low"] * 0.99)

        candidates.append({
            "ticker":         ticker,
            "company":        stock_mm.get("name", ticker),
            "tier":           tier,
            "score":          score,

            # Data hari ini
            "market_data": {
                "close":         close_price,
                "change_pct":    round(chg_pct, 2),
                "value_today":   val_today,
                "net_foreign":   stock_mm.get("net_foreign_today", 0),
                "iep":           stock_mm.get("iep", 0),
                "iep_change_pct":stock_mm.get("iep_change_pct", 0),
            },

            # Fitur teknikal BSJP
            "bsjp_features": {
                "vol_ratio20":  feat["vol_ratio20"],
                "body_pct":     round(feat["body_pct"] * 100, 2),
                "close_pos_pct":round(feat["close_pos"] * 100, 1),
                "above_ma20":   feat["above_ma20"],
                "above_ma50":   feat["above_ma50"],
                "above_ma200":  feat["above_ma200"],
                "atr_pct":      round(feat["atr_pct"] * 100, 2),
            },

            # Rencana trading
            "entry_plan": {
                "strategy":     "Beli di sesi akhir (14:30-15:45 WIB)",
                "entry_range":  f"{entry_low:,} – {entry_high:,}",
                "entry_note":   "Limit order di harga saat ini atau sedikit di bawahnya",
                "stop_loss":    stop_loss,
                "stop_note":    "Jika gap down besok, cut di bawah low hari ini",
                "target_pct":   target_pct,
                "exit_strategy":"Jual di pre-opening (08:45-08:55) atau awal sesi pagi",
            },

            # Konteks sinyal
            "universe_context": {
                "in_mover_types":   stock_mm.get("in_mover_types", []),
                "from_gainer":      stock_mm.get("from_gainer", False),
                "from_screener":    stock_mm.get("from_screener", False),
                "from_top_value":   stock_mm.get("from_top_value", False),
            },

            # Reasoning transparan
            "signals_positive":  sig_pos,
            "signals_negative":  sig_neg,
            "win_probability":   win_rate_str,

            # Disclaimer
            "disclaimer": (
                f"Tier {tier} BSJP. Strategi hold overnight — ada risiko gap down. "
                "Selalu gunakan stop loss. Pastikan Anda bisa monitor pre-opening besok."
            ),
        })

    # Sort by score (tertinggi dulu), ambil top N
    candidates.sort(key=lambda x: x["score"], reverse=True)
    top = candidates[:CONFIG["BSJP_MAX_OUTPUT"]]

    # Tambahkan rank
    for rank, c in enumerate(top, 1):
        c["rank"] = rank

    log.info(f"\n📊 BSJP Summary:")
    log.info(f"   Input universe     : {len(universe)} saham")
    log.info(f"   Filter value <10B  : {filtered_value}")
    log.info(f"   Filter price <100  : {filtered_price}")
    log.info(f"   Filter crash       : {filtered_chg}")
    log.info(f"   Diproses (OHLCV)   : {processed}")
    log.info(f"   No data / sedikit  : {no_data}")
    log.info(f"   Lolos formula BSJP : {len(candidates)}")
    log.info(f"   Final output       : {len(top)} saham")
    log.info("=" * 70)

    return top


# =============================================================================
# PIPELINE BPJS (BELI PAGI JUAL SORE) — v1.0
# =============================================================================
# Fungsi utama: run_bpjs_pipeline(universe, mode)
#
# Input  : universe (List[Dict]) — saham aktif hari ini dari pipeline intraday
# Output : List[Dict] — WATCHLIST BPJS terurut by score, max BPJS_MAX_OUTPUT
#
# Prinsip desain:
#   1. TIDAK menyentuh pipeline intraday, ARA, maupun BSJP
#   2. Memanfaatkan get_daily_data() yang sudah di-cache (0 download tambahan)
#   3. Menghasilkan WATCHLIST (bukan sinyal eksekusi langsung)
#   4. Formula berdasarkan grid search 100 kombinasi pada 789.792 hari trading
#   5. WAJIB dikonfirmasi secara real-time pagi hari sebelum eksekusi
# =============================================================================

def _bpjs_compute_d1_features(ticker: str) -> Optional[Dict]:
    """
    Ambil data OHLCV harian (dari cache YF) dan hitung fitur D-1 untuk BPJS.
    Return None jika data tidak cukup.

    Fitur yang dihitung:
      - d1_body    : kekuatan candle D-1 (close - open) / open
      - d1_cpos    : posisi close D-1 di dalam range (0=low, 1=high)
      - d1_vol_r   : volume D-1 vs rata-rata MA20
      - d1_chg     : perubahan harga D-1 vs D-2
      - d1_range_pct : range D-1 / close (ukuran volatilitas)
      - d1_uwick   : upper wick ratio
      - above_ma20/50/200 : posisi harga terhadap MA utama
      - d2_chg     : perubahan harga D-2 vs D-3
      - atr_pct    : ATR14 / close (volatilitas normal)
    """
    df = get_daily_data(ticker)
    if df is None or len(df) < 60:
        return None

    df.columns = [c.lower() if isinstance(c, str) else str(c) for c in df.columns]

    try:
        if "volume" not in df.columns:
            return None

        # Screener berjalan setelah market close (18:00 WIB).
        # df.iloc[-1] = close HARI INI → ini adalah D-1 untuk beli BESOK PAGI.
        # df.iloc[-2] = close kemarin → D-2.
        # BUG FIX: sebelumnya pakai iloc[-2] sebagai D-1 (salah, itu 2 hari lalu).
        if len(df) < 3:
            return None

        d1 = df.iloc[-1]   # Hari ini (= D-1 untuk beli besok)
        d2 = df.iloc[-2]   # Kemarin (= D-2)

        # Volume MA20 — dihitung pada bar D-1 (hari ini = iloc[-1])
        vol_ma20 = df["volume"].rolling(20).mean().iloc[-1]  # MA20 pada D-1 (hari ini)
        if pd.isna(vol_ma20) or vol_ma20 == 0:
            return None

        # D-1 features
        d1_o = float(d1["open"])
        d1_h = float(d1["high"])
        d1_l = float(d1["low"])
        d1_c = float(d1["close"])
        d1_v = float(d1["volume"])
        d1_range = d1_h - d1_l

        if d1_range <= 0 or d1_o <= 0:
            return None

        d1_body    = (d1_c - d1_o) / d1_o
        d1_cpos    = (d1_c - d1_l) / d1_range
        d1_vol_r   = d1_v / vol_ma20
        d1_uwick   = (d1_h - max(d1_o, d1_c)) / d1_range
        d1_lwick   = (min(d1_o, d1_c) - d1_l) / d1_range
        d1_range_pct = d1_range / d1_c

        # D-1 change vs D-2
        d2_c = float(d2["close"])
        d1_chg = (d1_c - d2_c) / d2_c if d2_c > 0 else 0

        # D-2 change vs D-3
        if len(df) >= 4:
            d3_c = float(df.iloc[-3]["close"])
            d2_chg = (d2_c - d3_c) / d3_c if d3_c > 0 else 0
        else:
            d2_chg = 0

        # Moving averages — dihitung pada D-1 (hari ini = iloc[-1])
        ma20  = df["close"].rolling(20).mean().iloc[-1]
        ma50  = df["close"].rolling(50).mean().iloc[-1]
        ma200 = df["close"].rolling(200).mean().iloc[-1] if len(df) >= 200 else np.nan

        above_ma20  = (not pd.isna(ma20))  and d1_c > ma20
        above_ma50  = (not pd.isna(ma50))  and d1_c > ma50
        above_ma200 = (not pd.isna(ma200)) and d1_c > ma200

        # ATR14 sebagai proxy volatilitas
        tr = pd.Series(np.maximum(
            df["high"] - df["low"],
            np.maximum(
                (df["high"] - df["close"].shift(1)).abs(),
                (df["low"]  - df["close"].shift(1)).abs()
            )
        ))
        atr14   = tr.rolling(14).mean().iloc[-1]  # ATR14 pada D-1 (hari ini)
        atr_pct = float(atr14) / d1_c if (not pd.isna(atr14) and d1_c > 0) else 0

        # Value MA20 (untuk validasi likuiditas historis)
        val_ma20 = (df["close"] * df["volume"]).rolling(20).mean().iloc[-1]

        return {
            "d1_body":       round(d1_body, 4),
            "d1_cpos":       round(d1_cpos, 4),
            "d1_vol_r":      round(d1_vol_r, 2),
            "d1_chg":        round(d1_chg, 4),
            "d1_uwick":      round(d1_uwick, 4),
            "d1_lwick":      round(d1_lwick, 4),
            "d1_range_pct":  round(d1_range_pct, 4),
            "d2_chg":        round(d2_chg, 4),
            "above_ma20":    above_ma20,
            "above_ma50":    above_ma50,
            "above_ma200":   above_ma200,
            "atr_pct":       round(float(atr_pct), 4),
            "val_ma20":      float(val_ma20) if not pd.isna(val_ma20) else 0,
            "d1_close":      d1_c,
            "d1_high":       d1_h,
            "d1_low":        d1_l,
            "d1_open":       d1_o,
        }
    except Exception as e:
        log.debug(f"    BPJS feature error {ticker}: {e}")
        return None


def _bpjs_score(feat: Dict, stock_mm: Dict) -> Tuple[int, str, List[str], List[str]]:
    """
    Hitung skor BPJS dan tentukan formula yang cocok.

    Return: (score, formula_type, signals_positive, signals_negative)
      - formula_type: "REVERSAL_BOUNCE" | "QUIET_CONTINUATION" | "NONE"
      - score: 0-100

    Berdasarkan grid search 100 kombinasi pada 789.792 hari trading.
    """
    score = 0
    sig_pos = []
    sig_neg = []

    d1_body    = feat["d1_body"]
    d1_cpos    = feat["d1_cpos"]
    d1_vol_r   = feat["d1_vol_r"]
    d1_chg     = feat["d1_chg"]
    d1_range_pct = feat["d1_range_pct"]
    above_ma20 = feat["above_ma20"]
    above_ma50 = feat["above_ma50"]
    above_ma200 = feat["above_ma200"]

    # ---- CEK FORMULA 1: Reversal Bounce ----
    # D-1 merah + vol sepi + close bawah + trend masih ok
    # Backtest: WR 38.6%, n=31.924 hari, Profit Factor 0.90x
    is_reversal = (
        d1_body  <= CONFIG["BPJS_REV_BODY_MAX"] and
        d1_vol_r <= CONFIG["BPJS_REV_VOL_MAX"]  and
        d1_cpos  <= CONFIG["BPJS_REV_CPOS_MAX"] and
        above_ma50  # Trend masih ok meski D-1 merah
    )
    # Guard: Reversal disabled via config (WR max 43%, N tidak cukup stabil)
    if not CONFIG.get("BPJS_REVERSAL_ENABLED", False):
        is_reversal = False

    # ---- CEK FORMULA 2: Quiet Continuation ----
    # D-1 hijau solid/tipis + vol mati + close tengah + uptrend
    # Backtest: WR 49.8%, n=1.371 hari, Profit Factor 1.04x
    is_quiet = (
        CONFIG["BPJS_QC_BODY_MIN"] <= d1_body <= CONFIG["BPJS_QC_BODY_MAX"] and
        d1_vol_r <= CONFIG["BPJS_QC_VOL_MAX"]  and
        CONFIG["BPJS_QC_CPOS_MIN"] <= d1_cpos <= CONFIG["BPJS_QC_CPOS_MAX"] and
        above_ma20  # Harus di atas MA20 untuk uptrend
    )

    if not (is_reversal or is_quiet):
        return 0, "NONE", sig_pos, sig_neg

    # Prioritaskan Quiet Continuation (WR lebih tinggi)
    if is_quiet:
        formula = "QUIET_CONTINUATION"
        score += 40  # Base score lebih tinggi karena WR lebih baik
        sig_pos.append(
            f"Formula: Quiet Continuation — D-1 hijau tipis ({d1_body*100:+.1f}%), "
            f"volume mati ({d1_vol_r:.2f}x MA20), close tengah ({d1_cpos*100:.0f}%). "
            f"WR historis: 49.8% (n=1.371)"
        )
    else:
        formula = "REVERSAL_BOUNCE"
        score += 30  # Base score lebih rendah, butuh konfirmasi lebih kuat
        sig_pos.append(
            f"Formula: Reversal Bounce — D-1 merah ({d1_body*100:+.1f}%), "
            f"volume sepi ({d1_vol_r:.2f}x MA20), close bawah ({d1_cpos*100:.0f}%). "
            f"WR historis: 38.6% (n=31.924)"
        )

    # ---- SCORING BONUS DARI FITUR TEKNIKAL ----

    # Bonus MA positioning
    if above_ma20:
        score += CONFIG["BPJS_BONUS_ABOVE_MA20"]
        sig_pos.append("Di atas MA20 — support jangka pendek")
    else:
        sig_neg.append("Di bawah MA20 — tekanan jangka pendek")

    if above_ma50:
        score += CONFIG["BPJS_BONUS_ABOVE_MA50"]
        sig_pos.append("Di atas MA50 — tren menengah masih ok")

    if above_ma200:
        score += CONFIG["BPJS_BONUS_ABOVE_MA200"]
        sig_pos.append("Di atas MA200 — tren utama masih bullish")

    # Bonus range kecil (volatilitas rendah = noise rendah di pagi hari)
    if d1_range_pct <= 0.03:
        score += CONFIG["BPJS_BONUS_LOW_RANGE"]
        sig_pos.append(f"Range D-1 kecil ({d1_range_pct*100:.1f}%) — noise rendah besok")

    # Penalty: upper wick besar (ada tekanan jual D-1)
    if feat["d1_uwick"] >= 0.40:
        score -= 10
        sig_neg.append(f"Upper wick besar ({feat['d1_uwick']*100:.0f}%) — tekanan jual D-1")

    # ---- SCORING BONUS DARI DATA UNIVERSE REAL-TIME ----
    net_foreign   = stock_mm.get("net_foreign_today", 0) or 0
    from_screener = stock_mm.get("from_screener", False)

    if net_foreign > 0:
        score += CONFIG["BPJS_BONUS_NET_FOREIGN"]
        sig_pos.append(f"Asing beli bersih hari ini (Rp {net_foreign/1e9:.1f}B)")
    elif net_foreign < -1_000_000_000:
        score -= 5
        sig_neg.append("Asing jual bersih besar — hati-hati gap down")

    if from_screener:
        score += CONFIG["BPJS_BONUS_VOL_SCREENER"]
        sig_pos.append("Masuk screener volume Stockbit — ada perhatian pasar")

    return max(0, score), formula, sig_pos, sig_neg


def run_bpjs_pipeline(universe: List[Dict], mode: str) -> List[Dict]:
    """
    Pipeline BPJS (Beli Pagi Jual Sore) — v1.0

    Menerima universe yang sudah dikumpulkan pipeline intraday.
    Memanfaatkan get_daily_data() yang sudah di-cache — 0 download tambahan.

    PENTING: Output dari pipeline ini adalah WATCHLIST, bukan sinyal eksekusi.
    Setiap kandidat dilengkapi dengan `morning_confirmation_criteria` yang HARUS
    dicek secara real-time pagi hari sebelum membeli.

    Alur:
      1. Filter: value_today >= BPJS_MIN_VALUE_TODAY
      2. Filter: price >= BPJS_MIN_PRICE
      3. Filter: change_pct >= BPJS_MIN_CHG_PCT
      4. Hitung fitur D-1 (dari cache OHLCV)
      5. Cek formula Reversal Bounce / Quiet Continuation
      6. Scoring + bonus dari data universe
      7. Sort by score, ambil top BPJS_MAX_OUTPUT
    """
    log.info("\n" + "=" * 70)
    log.info("🌅 BPJS PIPELINE — Beli Pagi Jual Sore v1.0 (WATCHLIST)")
    log.info(f"   Universe input: {len(universe)} saham dari pipeline intraday")
    log.info("   ⚠️  Output = WATCHLIST, bukan sinyal eksekusi langsung")
    log.info("=" * 70)

    min_value = CONFIG["BPJS_MIN_VALUE_TODAY"]
    min_price = CONFIG["BPJS_MIN_PRICE"]
    min_chg   = CONFIG["BPJS_MIN_CHG_PCT"]

    candidates = []
    filtered_value = 0
    filtered_price = 0
    filtered_chg   = 0
    processed      = 0
    no_data        = 0

    for stock_mm in universe:
        ticker    = stock_mm.get("ticker", "")
        val_today = stock_mm.get("value_today", 0) or 0
        price     = stock_mm.get("price", 0) or 0
        chg_pct   = stock_mm.get("change_pct", 0) or 0

        # ---- Filter 1: Likuiditas ----
        if val_today < min_value:
            filtered_value += 1
            continue

        # ---- Filter 2: Harga minimum ----
        if price < min_price:
            filtered_price += 1
            continue

        # ---- Filter 3: Batas crash ----
        if chg_pct < min_chg:
            filtered_chg += 1
            continue

        processed += 1

        # ---- Ambil fitur D-1 (dari cache) ----
        feat = _bpjs_compute_d1_features(ticker)
        if feat is None:
            no_data += 1
            log.debug(f"    BPJS {ticker}: No D-1 data")
            continue

        # ---- Score ----
        score, formula, sig_pos, sig_neg = _bpjs_score(feat, stock_mm)
        if formula == "NONE":
            log.debug(
                f"    BPJS {ticker}: Tidak cocok formula manapun "
                f"(body={feat['d1_body']*100:+.1f}%, vol={feat['d1_vol_r']:.2f}x, "
                f"cpos={feat['d1_cpos']*100:.0f}%)"
            )
            continue

        log.info(
            f"    ✅ BPJS {ticker}: {formula} | Score {score} | "
            f"Body {feat['d1_body']*100:+.1f}% | Vol {feat['d1_vol_r']:.2f}x | "
            f"CPos {feat['d1_cpos']*100:.0f}%"
        )

        # ---- Hitung estimasi entry dan stop loss ----
        d1_close = feat["d1_close"]
        d1_low   = feat["d1_low"]
        tick     = get_tick_size(d1_close)
        atr_rp   = feat["atr_pct"] * d1_close

        # Entry: beli di harga open pagi besok (kita tidak tahu pastinya)
        # Estimasi entry range berdasarkan ATR
        entry_low  = round_bei(max(d1_close - atr_rp * 0.5, d1_low * 0.99))
        entry_high = round_bei(d1_close * 1.02)  # max 2% di atas PDC

        # Stop loss: jika turun lebih dari 1 ATR dari entry
        stop_loss = round_bei(entry_low - atr_rp)

        # Target: berdasarkan backtest (median intraday gain ~2-3% untuk formula yang lolos)
        target_modest   = round_bei(d1_close * 1.02)  # +2%
        target_moderate = round_bei(d1_close * 1.04)  # +4%

        # Morning confirmation criteria — INI YANG MEMBEDAKAN BPJS dari BSJP/ARA
        if formula == "REVERSAL_BOUNCE":
            morning_criteria = [
                "⚠️ WAJIB: Hanya beli jika terjadi Gap Down > 3% di pagi hari (09:00-09:05 WIB)",
                "✅ Skenario Gap Down >3% memiliki Profit Factor 6.78x & Win Rate 44.5%!",
                "✅ Volume 15 menit pertama ≥ 1.5x rata-rata (menandakan buying interest)",
                "❌ JANGAN BELI jika harga flat, gap up, atau gap down < 1% (Expected Value negatif)",
            ]
            timing_guide = (
                "Pantau 09:00-09:05. Jika saham gap down > 3% lalu diborong (volume naik, "
                "harga rebound dari low), BELI. Target jual: sesi sore 14:30-15:30 WIB."
            )
        else:  # QUIET_CONTINUATION
            morning_criteria = [
                "⚠️ WAJIB: Utamakan jika terjadi Gap Down > 3% saat open untuk risk/reward terbaik",
                "✅ Skenario Gap Down >3% memiliki Profit Factor 6.78x & Win Rate 44.5%",
                "✅ Volume 15 menit pertama normal/stabil di atas rata-rata",
                "❌ JANGAN BELI jika Gap Up > 3% (euforia rawan bull trap, PF hanya 0.3x)",
                "❌ JANGAN BELI jika langsung longsor drastis tanpa rebound dari low",
            ]
            timing_guide = (
                "Pantau 09:00-09:10. Beli jika terjadi gap down > 3% lalu ada rebound volume stabil. "
                "Target jual: sesi sore 14:30-15:30 WIB."
            )

        candidates.append({
            "ticker":         ticker,
            "company":        stock_mm.get("name", ticker),
            "type":           "WATCHLIST_BPJS",
            "formula":        formula,
            "score":          score,

            # Data hari ini (D-0 = hari saat screener dijalankan)
            "market_data": {
                "close":         d1_close,
                "change_pct":    round(chg_pct, 2),
                "value_today":   val_today,
                "net_foreign":   stock_mm.get("net_foreign_today", 0),
            },

            # Fitur teknikal D-1
            "d1_features": {
                "body_pct":      round(feat["d1_body"] * 100, 2),
                "close_pos_pct": round(feat["d1_cpos"] * 100, 1),
                "vol_ratio":     feat["d1_vol_r"],
                "change_pct":    round(feat["d1_chg"] * 100, 2),
                "range_pct":     round(feat["d1_range_pct"] * 100, 2),
                "upper_wick":    round(feat["d1_uwick"] * 100, 1),
                "above_ma20":    feat["above_ma20"],
                "above_ma50":    feat["above_ma50"],
                "above_ma200":   feat["above_ma200"],
                "atr_pct":       round(feat["atr_pct"] * 100, 2),
            },

            # Rencana trading
            "trading_plan": {
                "strategy":      "Beli di pagi hari (09:00-09:30), jual sore (14:30-15:30)",
                "entry_range":   f"{entry_low:,} – {entry_high:,}",
                "entry_note":    "JANGAN beli langsung. Konfirmasi dulu kriteria pagi hari.",
                "stop_loss":     stop_loss,
                "stop_note":     f"Cut loss jika turun di bawah {stop_loss:,} (1 ATR dari entry)",
                "target_modest":   target_modest,
                "target_moderate": target_moderate,
                "exit_strategy":   "Jual di sesi sore (14:30-15:30 WIB) tanpa menunggu close",
            },

            # KUNCI: Morning confirmation criteria
            "morning_confirmation_criteria": morning_criteria,
            "timing_guide":    timing_guide,

            # Konteks
            "universe_context": {
                "in_mover_types": stock_mm.get("in_mover_types", []),
                "from_gainer":    stock_mm.get("from_gainer", False),
                "from_screener":  stock_mm.get("from_screener", False),
            },

            # Reasoning transparan
            "signals_positive": sig_pos,
            "signals_negative": sig_neg,

            # Backtest stats
            "backtest_stats": {
                "formula":          formula,
                "win_rate":         "49.8%" if formula == "QUIET_CONTINUATION" else "38.6%",
                "sample_size":      "n=1.371" if formula == "QUIET_CONTINUATION" else "n=31.924",
                "profit_factor":    "1.04x" if formula == "QUIET_CONTINUATION" else "0.90x",
                "total_backtest":   "789.792 hari trading IHSG (956 saham)",
                "gap_down_sweet_spot": "Gap Down >2%: WR 30.2%, upside +3.75%, DD -0.88%",
                "gap_up_danger":    "Gap Up >5%: WR 11.8%, DD -6.67% — HINDARI",
            },

            # Disclaimer tegas
            "disclaimer": (
                f"⚠️ INI WATCHLIST, BUKAN SINYAL BELI. "
                f"Formula {formula} dari backtest forward-looking 789.792 hari trading IHSG. "
                f"WAJIB konfirmasi real-time pagi hari (lihat morning_confirmation_criteria). "
                f"Tanpa konfirmasi, JANGAN eksekusi."
            ),
        })

    # Sort by score (tertinggi dulu), ambil top N
    candidates.sort(key=lambda x: x["score"], reverse=True)
    top = candidates[:CONFIG["BPJS_MAX_OUTPUT"]]

    # Tambahkan rank
    for rank, c in enumerate(top, 1):
        c["rank"] = rank

    log.info(f"\n📊 BPJS Summary:")
    log.info(f"   Input universe           : {len(universe)} saham")
    log.info(f"   Filter value <10B        : {filtered_value}")
    log.info(f"   Filter price <100        : {filtered_price}")
    log.info(f"   Filter crash             : {filtered_chg}")
    log.info(f"   Diproses (D-1 features)  : {processed}")
    log.info(f"   No data / sedikit        : {no_data}")
    log.info(f"   Lolos formula BPJS       : {len(candidates)}")
    log.info(f"   Final watchlist output   : {len(top)} saham")
    log.info("=" * 70)

    return top


# =============================================================================
# PIPELINE SWING TRADING — v1.0 (VCP + Stage 2 Uptrend)
# =============================================================================
# Berdasarkan backtest forward-looking 467.390 hari trading (956 saham IHSG).
# Golden Setup: Sedikit di atas MA20 + Ultra Tight Squeeze + Volume Kering
# PF=1.60x | EV=+2.14% per trade | WR=48.8% | DD=-5.5%
# KILL SWITCH: Pipeline mati total jika IHSG < MA200.
# =============================================================================

def _swing_compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Compute RSI dari pd.Series close."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / (avg_loss + 1e-10)
    return 100 - (100 / (1 + rs))


def _swing_compute_features(ticker: str) -> Optional[Dict]:
    """
    Hitung fitur Swing Trading dari cache OHLCV harian.
    Return None jika data tidak cukup (butuh minimal 200 hari untuk MA200).
    """
    df = get_daily_data(ticker)
    if df is None or len(df) < 210:
        return None

    df.columns = [c.lower() if isinstance(c, str) else str(c) for c in df.columns]
    try:
        if "volume" not in df.columns:
            return None

        c = df["close"].astype(float)
        h = df["high"].astype(float)
        l = df["low"].astype(float)
        v = df["volume"].astype(float)

        ma20  = c.rolling(20).mean()
        ma50  = c.rolling(50).mean()
        ma200 = c.rolling(200).mean()

        curr_c    = float(c.iloc[-1])
        curr_ma20 = float(ma20.iloc[-1])
        curr_ma50 = float(ma50.iloc[-1])
        curr_ma200= float(ma200.iloc[-1])

        if any(pd.isna(x) for x in [curr_ma20, curr_ma50, curr_ma200]):
            return None
        if curr_ma50 == 0 or curr_ma200 == 0:
            return None

        is_stage2 = (curr_c > curr_ma50) and (curr_ma50 > curr_ma200)
        ma50_prev = float(ma50.iloc[-11]) if len(ma50) > 10 else curr_ma50
        ma50_slope = (curr_ma50 - ma50_prev) / (ma50_prev + 1e-10)
        dist_ma20 = (curr_c - curr_ma20) / curr_ma20

        h5  = h.rolling(5).max().iloc[-1]
        l5  = l.rolling(5).min().iloc[-1]
        h20 = h.rolling(20).max().iloc[-1]
        l20 = l.rolling(20).min().iloc[-1]
        range_5  = (h5 - l5) / curr_c
        range_20 = (h20 - l20) / curr_c
        squeeze = range_5 / range_20 if range_20 > 0 else 1.0

        v5  = float(v.rolling(5).mean().iloc[-1])
        v20 = float(v.rolling(20).mean().iloc[-1])
        vol_ratio = v5 / v20 if v20 > 0 else 1.0

        rsi = float(_swing_compute_rsi(c, 14).iloc[-1])

        tr = pd.concat([
            h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()
        ], axis=1).max(axis=1)
        atr14 = float(tr.rolling(14).mean().iloc[-1])
        atr_pct = atr14 / curr_c if curr_c > 0 else 0

        val_ma20 = float((c * v).rolling(20).mean().iloc[-1])

        if len(h) >= 252:
            high_52w = float(h.iloc[-252:].max())
        else:
            high_52w = float(h.max())
        dist_52w = (curr_c - high_52w) / high_52w if high_52w > 0 else -1

        # ADX via pandas_ta
        adx_df  = ta.adx(h, l, c, length=14)
        adx_val = float(adx_df["ADX_14"].iloc[-1]) if (
            adx_df is not None and not adx_df.empty and
            not pd.isna(adx_df["ADX_14"].iloc[-1])
        ) else 0.0

        return {
            "is_stage2": is_stage2, "ma50_slope": round(ma50_slope, 4),
            "dist_ma20": round(dist_ma20, 4), "squeeze": round(squeeze, 4),
            "vol_ratio": round(vol_ratio, 4), "rsi": round(rsi, 1),
            "atr_pct": round(atr_pct, 4), "val_ma20": val_ma20,
            "dist_52w": round(dist_52w, 4), "close": curr_c,
            "ma20": round(curr_ma20, 2), "ma50": round(curr_ma50, 2),
            "ma200": round(curr_ma200, 2), "above_ma200": curr_c > curr_ma200,
            "adx": round(adx_val, 1),   # [BARU]
        }
    except Exception as e:
        log.debug(f"    SWING feature error {ticker}: {e}")
        return None


def _swing_score(feat: Dict, stock_mm: Dict) -> Tuple[int, List[str], List[str]]:
    """Hitung skor Swing Trading berdasarkan Golden Setup VCP."""
    score = 0
    sig_pos = []
    sig_neg = []

    if not feat["is_stage2"]:
        return 0, [], ["Bukan Stage 2 Uptrend"]
    if feat["ma50_slope"] < CONFIG["SWING_MA50_SLOPE_MIN"]:
        return 0, [], [f"MA50 slope terlalu flat ({feat['ma50_slope']*100:.1f}%)"]

    # ADX Gate (wajib — ditambahkan v2.0)
    if feat.get("adx", 0) < CONFIG.get("SWING_ADX_MIN", 25):
        return 0, [], [
            f"ADX terlalu lemah ({feat.get('adx', 0):.0f} < "
            f"{CONFIG.get('SWING_ADX_MIN', 25)}) — squeeze belum mature"
        ]

    score += 25
    sig_pos.append(
        f"Stage 2 Uptrend: Harga > MA50 > MA200, "
        f"MA50 slope +{feat['ma50_slope']*100:.1f}%/10d"
    )

    d = feat["dist_ma20"]
    d_min = CONFIG["SWING_DIST_MA20_MIN"]
    d_max = CONFIG["SWING_DIST_MA20_MAX"]
    if d_min <= d <= d_max:
        score += 20
        sig_pos.append(f"Posisi ideal: +{d*100:.1f}% di atas MA20")
    elif d < d_min:
        score += 10
        sig_neg.append(f"Terlalu dekat MA20 (+{d*100:.1f}%)")
    else:
        sig_neg.append(f"Overextended (+{d*100:.1f}% di atas MA20)")

    sqz = feat["squeeze"]
    if sqz <= CONFIG["SWING_SQUEEZE_MAX"]:
        score += 20
        sig_pos.append(f"Ultra Tight Squeeze: {sqz*100:.0f}%")
    elif sqz <= 0.60:
        score += 10
        sig_pos.append(f"Squeeze moderat ({sqz*100:.0f}%)")
    else:
        sig_neg.append(f"Belum squeeze ({sqz*100:.0f}%)")

    vr = feat["vol_ratio"]
    if vr <= CONFIG["SWING_VOL_5D_MAX"]:
        score += 15
        sig_pos.append(f"Volume kering: {vr*100:.0f}% dari MA20")
    elif vr <= 0.90:
        score += 5
    else:
        sig_neg.append(f"Volume belum kering ({vr*100:.0f}%)")

    rsi = feat["rsi"]
    if CONFIG["SWING_RSI_MIN"] <= rsi <= CONFIG["SWING_RSI_MAX"]:
        if 55 <= rsi <= 65:
            score += CONFIG["SWING_BONUS_RSI_SWEET"]
            sig_pos.append(f"RSI sweet spot ({rsi:.0f}) — momentum awal uptrend, WR 52.0%")
        else:
            sig_pos.append(f"RSI netral ({rsi:.0f})")
    elif rsi < CONFIG["SWING_RSI_MIN"]:
        score -= 10
        sig_neg.append(f"RSI terlalu rendah ({rsi:.0f})")
    else:
        sig_neg.append(f"RSI overbought ({rsi:.0f})")

    if feat["above_ma200"]:
        score += CONFIG["SWING_BONUS_ABOVE_MA200"]
        sig_pos.append("Di atas MA200 — tren primer bullish")
    if feat["atr_pct"] <= 0.03:
        score += CONFIG["SWING_BONUS_TIGHT_ATR"]
        sig_pos.append(f"ATR rendah ({feat['atr_pct']*100:.1f}%)")
    if feat["dist_52w"] >= -0.10:
        score += CONFIG["SWING_BONUS_NEAR_52W_HIGH"]
        sig_pos.append(f"Dekat 52w high ({feat['dist_52w']*100:+.1f}%)")

    nf = stock_mm.get("net_foreign_today", 0) or 0
    if nf > 0:
        score += CONFIG["SWING_BONUS_NET_FOREIGN"]
        sig_pos.append(f"Asing beli bersih (Rp {nf/1e9:.1f}B)")

    return max(0, score), sig_pos, sig_neg


def _trend_score(feat: Dict, stock_mm: Dict) -> Tuple[int, List[str], List[str]]:
    """
    Hitung skor Trend Following v1.0.
    Return: (score, signals_positive, signals_negative)

    Backtest: 956 saham, WR (Max High>=10% dalam 20 hari) = 54.7%
    Formula: ADX_D>=30 + RSI[50-75] + MACD>0 + Mom5>0 + WkADX>=25 + WkOBV>MA
    """
    score = 0
    sig_pos: List[str] = []
    sig_neg: List[str] = []

    # Gate 1: Stage 2 Uptrend
    if not feat.get("is_stage2", False):
        return 0, [], ["Bukan Stage 2 Uptrend"]

    # Gate 2: ADX kuat
    adx = feat.get("adx", 0.0)
    adx_min = CONFIG.get("TREND_ADX_MIN", 30)
    if adx < adx_min:
        return 0, [], [f"ADX terlalu lemah ({adx:.0f} < {adx_min})"]

    # Gate 3: RSI range sehat
    rsi = feat.get("rsi", 0.0)
    rsi_min = CONFIG.get("TREND_RSI_MIN", 50)
    rsi_max = CONFIG.get("TREND_RSI_MAX", 75)
    if not (rsi_min <= rsi <= rsi_max):
        return 0, [], [f"RSI di luar range ({rsi:.0f}, butuh {rsi_min}–{rsi_max})"]

    # Gate 4: MACD Histogram positif
    if feat.get("macd_hist", 0.0) <= 0:
        return 0, [], ["MACD Histogram negatif — momentum menurun"]

    # Semua gate lolos
    score += 40
    sig_pos.append(
        f"Stage 2 + ADX kuat ({adx:.0f}) + RSI sehat ({rsi:.0f}) + MACD positif "
        f"— WR historis 54.7% (hit +10% dalam 20 hari)"
    )

    # Bonus: Momentum 5 hari
    if feat.get("mom5", 0.0) > 0:
        score += CONFIG.get("TREND_BONUS_MOM5", 5)
        sig_pos.append(f"Momentum 5 hari positif (+{feat.get('mom5', 0)*100:.1f}%)")

    # Bonus: Di atas MA200
    if feat.get("above_ma200", False):
        score += CONFIG.get("TREND_BONUS_ABOVE_MA200", 5)
        sig_pos.append("Di atas MA200 — tren primer bullish")
    else:
        sig_neg.append("Di bawah MA200")

    # Bonus: Weekly ADX
    wk_adx = feat.get("wk_adx", 0.0)
    if wk_adx >= CONFIG.get("TREND_WK_ADX_MIN", 25):
        score += 10
        sig_pos.append(f"Weekly ADX kuat ({wk_adx:.0f})")
    else:
        sig_neg.append(f"Weekly ADX lemah ({wk_adx:.0f})")

    # Bonus: Weekly OBV
    if feat.get("wk_obv_above_ma", False):
        score += CONFIG.get("TREND_BONUS_WK_OBV", 10)
        sig_pos.append("Weekly OBV di atas MA20 — akumulasi mingguan positif")
    else:
        sig_neg.append("Weekly OBV di bawah MA20")

    # Bonus: Net foreign
    nf = stock_mm.get("net_foreign_today", 0) or 0
    if nf > 0:
        score += CONFIG.get("TREND_BONUS_NET_FOREIGN", 5)
        sig_pos.append(f"Asing beli bersih (Rp {nf/1e9:.1f}B)")

    return max(0, score), sig_pos, sig_neg


def run_trend_pipeline(
    universe: List[Dict], mode: str, market_ctx: Dict
) -> List[Dict]:
    """
    Pipeline Trend Following v1.0 — ADX Kuat + MACD + Weekly Confirmation.
    Kill switch: IHSG < MA200. Output: key "trend_following" di JSON.
    WR: 54.7% (Max High>=10% dalam 20 hari)
    """
    log.info("\n" + "=" * 70)
    log.info("📈 TREND FOLLOWING PIPELINE v1.0")
    log.info(f"   Universe input: {len(universe)} saham")
    log.info("=" * 70)

    if not CONFIG.get("TREND_ENABLED", True):
        return []

    # Kill switch: IHSG < MA200
    try:
        ihsg_df = get_daily_data("^JKSE")
        if ihsg_df is not None and len(ihsg_df) >= 200:
            ihsg_c = ihsg_df["close"].astype(float)
            if float(ihsg_c.iloc[-1]) < float(ihsg_c.rolling(200).mean().iloc[-1]):
                log.warning("   ⛔ KILL SWITCH: IHSG < MA200. Trend Following mati.")
                return []
    except Exception as e:
        log.warning(f"   IHSG check error: {e}")

    min_val = CONFIG.get("TREND_MIN_VALUE_20D", 1_000_000_000)
    candidates = []

    for stock in universe:
        ticker = stock.get("ticker", "")
        if not ticker:
            continue

        # Daily data (sudah di cache via get_daily_data)
        df = get_daily_data(ticker)
        if df is None or len(df) < 210:
            continue
        df.columns = [c.lower() if isinstance(c, str) else str(c) for c in df.columns]

        try:
            c = df["close"].astype(float)
            h = df["high"].astype(float)
            l = df["low"].astype(float)
            v = df["volume"].astype(float)

            val_ma20 = float((c * v).rolling(20).mean().iloc[-1])
            if pd.isna(val_ma20) or val_ma20 < min_val:
                continue

            ma50  = float(c.rolling(50).mean().iloc[-1])
            ma200 = float(c.rolling(200).mean().iloc[-1])
            curr  = float(c.iloc[-1])
            is_stage2 = curr > ma50 and ma50 > ma200

            # ADX via pandas_ta
            adx_df  = ta.adx(h, l, c, length=14)
            adx_val = float(adx_df["ADX_14"].iloc[-1]) if (
                adx_df is not None and not adx_df.empty and
                not pd.isna(adx_df["ADX_14"].iloc[-1])
            ) else 0.0

            # RSI (gunakan fungsi yang sudah ada di screener)
            rsi_val = float(_swing_compute_rsi(c, 14).iloc[-1])

            # MACD Histogram
            ema12 = c.ewm(span=12, adjust=False).mean()
            ema26 = c.ewm(span=26, adjust=False).mean()
            macd_line = ema12 - ema26
            signal    = macd_line.ewm(span=9, adjust=False).mean()
            macd_h    = float((macd_line - signal).iloc[-1])

            # Momentum 5 hari
            mom5 = float(c.iloc[-1] / c.iloc[-6] - 1) if len(c) > 5 else 0.0

            # Weekly data
            wk_adx_val = 0.0
            wk_obv_above_ma = False
            df_wk = get_weekly_data(ticker)
            if df_wk is not None and len(df_wk) >= 30:
                wk_c = df_wk["close"].astype(float)
                wk_v = df_wk["volume"].astype(float)
                wk_h = df_wk["high"].astype(float)
                wk_l = df_wk["low"].astype(float)
                wk_adx_df  = ta.adx(wk_h, wk_l, wk_c, length=14)
                wk_adx_val = float(wk_adx_df["ADX_14"].iloc[-1]) if (
                    wk_adx_df is not None and not wk_adx_df.empty and
                    not pd.isna(wk_adx_df["ADX_14"].iloc[-1])
                ) else 0.0
                wk_obv = (np.sign(wk_c.diff().fillna(0)) * wk_v).cumsum()
                wk_obv_ma = wk_obv.rolling(20).mean()
                wk_obv_above_ma = bool(float(wk_obv.iloc[-1]) > float(wk_obv_ma.iloc[-1]))

            feat = {
                "is_stage2":       is_stage2,
                "adx":             adx_val,
                "rsi":             rsi_val,
                "macd_hist":       macd_h,
                "mom5":            mom5,
                "above_ma200":     curr > ma200,
                "wk_adx":          wk_adx_val,
                "wk_obv_above_ma": wk_obv_above_ma,
            }

            score, sig_pos, sig_neg = _trend_score(feat, stock)
            if score <= 0:
                continue

            candidates.append({
                "ticker":           ticker,
                "company":          stock.get("name", ticker),
                "score":            score,
                "close":            curr,
                "adx":              round(adx_val, 1),
                "rsi":              round(rsi_val, 1),
                "macd_hist":        round(macd_h, 4),
                "wk_adx":           round(wk_adx_val, 1),
                "wk_obv_above_ma":  wk_obv_above_ma,
                "target_pct":       CONFIG.get("TREND_TARGET_PCT", 0.10),
                "stop_loss_pct":    CONFIG.get("TREND_STOP_LOSS_PCT", 0.07),
                "max_hold_days":    CONFIG.get("TREND_MAX_HOLD_DAYS", 20),
                "signals_positive": sig_pos,
                "signals_negative": sig_neg,
                "pipeline":         "trend_following",
            })
        except Exception as e:
            log.debug(f"   TREND {ticker}: {e}")
            continue

    candidates.sort(key=lambda x: x["score"], reverse=True)
    hasil = candidates[:CONFIG.get("TREND_MAX_OUTPUT", 5)]
    log.info(f"   Kandidat lolos: {len(candidates)} | Output: {len(hasil)}")
    log.info("=" * 70)
    return hasil


def _position_score(feat: Dict, stock_mm: Dict) -> Tuple[int, List[str], List[str]]:
    """
    Hitung skor Position Trading v1.0.
    Return: (score, signals_positive, signals_negative)

    Backtest: 956 saham, WR (Max High>=20% dalam 60 hari) = 38.7%
    Formula: Near52Hi>=85% + WkRSI[55-80] + WkADX>=20 + WkOBV>MA + MoRSI>=50
    """
    score = 0
    sig_pos: List[str] = []
    sig_neg: List[str] = []

    # Gate 1: Near 52-Week High
    near52 = feat.get("near_52w_pct", 0.0)
    thresh = CONFIG.get("POSITION_NEAR_52W_PCT", 0.85)
    if near52 < thresh:
        return 0, [], [
            f"Terlalu jauh dari 52W High ({near52*100:.0f}% dari high, butuh >={thresh*100:.0f}%)"
        ]

    # Gate 2: Weekly RSI
    wk_rsi = feat.get("wk_rsi", 0.0)
    wk_rsi_min = CONFIG.get("POSITION_WK_RSI_MIN", 55)
    wk_rsi_max = CONFIG.get("POSITION_WK_RSI_MAX", 80)
    if not (wk_rsi_min <= wk_rsi <= wk_rsi_max):
        return 0, [], [f"Weekly RSI di luar range ({wk_rsi:.0f}, butuh {wk_rsi_min}–{wk_rsi_max})"]

    # Gate 3: Weekly ADX
    wk_adx = feat.get("wk_adx", 0.0)
    wk_adx_min = CONFIG.get("POSITION_WK_ADX_MIN", 20)
    if wk_adx < wk_adx_min:
        return 0, [], [f"Weekly ADX lemah ({wk_adx:.0f} < {wk_adx_min})"]

    # Gate 4: Monthly RSI
    mo_rsi = feat.get("mo_rsi", 50.0)
    mo_rsi_min = CONFIG.get("POSITION_MO_RSI_MIN", 50)
    if mo_rsi < mo_rsi_min:
        return 0, [], [f"Monthly RSI terlalu rendah ({mo_rsi:.0f}) — makro bearish"]

    score += 40
    sig_pos.append(
        f"Near 52W High ({near52*100:.0f}%) + Wk RSI ({wk_rsi:.0f}) "
        f"+ Wk ADX ({wk_adx:.0f}) — WR 38.7% (hit +20%, 60 hari)"
    )

    if near52 >= 0.95:
        score += CONFIG.get("POSITION_BONUS_NEAR_ATH", 10)
        sig_pos.append(f"Sangat dekat ATH ({near52*100:.0f}%) — breakout potential")

    if mo_rsi >= 60:
        score += CONFIG.get("POSITION_BONUS_MO_RSI", 5)
        sig_pos.append(f"Monthly RSI kuat ({mo_rsi:.0f})")

    if feat.get("wk_obv_above_ma", False):
        score += CONFIG.get("POSITION_BONUS_WK_OBV", 10)
        sig_pos.append("Weekly OBV di atas MA20 — akumulasi jangka panjang")
    else:
        sig_neg.append("Weekly OBV di bawah MA20")

    if feat.get("above_ma200", False):
        sig_pos.append("Di atas MA200 Daily — tren primer bullish")
    else:
        sig_neg.append("Di bawah MA200 Daily")

    nf = stock_mm.get("net_foreign_today", 0) or 0
    if nf > 0:
        score += CONFIG.get("POSITION_BONUS_NET_FOREIGN", 5)
        sig_pos.append(f"Asing beli bersih (Rp {nf/1e9:.1f}B)")

    return max(0, score), sig_pos, sig_neg


def run_position_pipeline(
    universe: List[Dict], mode: str, market_ctx: Dict
) -> List[Dict]:
    """
    Pipeline Position Trading v1.0 — Near 52W High + Weekly/Monthly Confirm.
    Tidak ada kill switch IHSG. Output: key "position_trading" di JSON.
    WR: 38.7% (Max High>=20% dalam 60 hari)
    """
    log.info("\n" + "=" * 70)
    log.info("📊 POSITION TRADING PIPELINE v1.0")
    log.info(f"   Universe input: {len(universe)} saham")
    log.info("=" * 70)

    if not CONFIG.get("POSITION_ENABLED", True):
        return []

    min_val = CONFIG.get("POSITION_MIN_VALUE_20D", 2_000_000_000)
    candidates = []

    for stock in universe:
        ticker = stock.get("ticker", "")
        if not ticker:
            continue

        df = get_daily_data(ticker)
        if df is None or len(df) < 210:
            continue
        df.columns = [c.lower() if isinstance(c, str) else str(c) for c in df.columns]

        try:
            c = df["close"].astype(float)
            h = df["high"].astype(float)
            v = df["volume"].astype(float)

            val_ma20 = float((c * v).rolling(20).mean().iloc[-1])
            if pd.isna(val_ma20) or val_ma20 < min_val:
                continue

            curr  = float(c.iloc[-1])
            ma200 = float(c.rolling(200).mean().iloc[-1])
            hi52w = float(h.iloc[-252:].max()) if len(h) >= 252 else float(h.max())
            near52_pct = curr / hi52w if hi52w > 0 else 0.0

            # Weekly
            wk_rsi_val = 0.0
            wk_adx_val = 0.0
            wk_obv_above_ma = False
            df_wk = get_weekly_data(ticker)
            if df_wk is not None and len(df_wk) >= 30:
                wk_c = df_wk["close"].astype(float)
                wk_v = df_wk["volume"].astype(float)
                wk_h = df_wk["high"].astype(float)
                wk_l = df_wk["low"].astype(float)
                wk_rsi_val = float(_swing_compute_rsi(wk_c, 14).iloc[-1])
                wk_adx_df  = ta.adx(wk_h, wk_l, wk_c, length=14)
                wk_adx_val = float(wk_adx_df["ADX_14"].iloc[-1]) if (
                    wk_adx_df is not None and not wk_adx_df.empty and
                    not pd.isna(wk_adx_df["ADX_14"].iloc[-1])
                ) else 0.0
                wk_obv = (np.sign(wk_c.diff().fillna(0)) * wk_v).cumsum()
                wk_obv_ma = wk_obv.rolling(20).mean()
                wk_obv_above_ma = bool(float(wk_obv.iloc[-1]) > float(wk_obv_ma.iloc[-1]))

            # Monthly
            mo_rsi_val = 50.0
            df_mo = get_monthly_data(ticker)
            if df_mo is not None and len(df_mo) >= 6:
                mo_c = df_mo["close"].astype(float)
                mo_rsi_val = float(_swing_compute_rsi(mo_c, 14).iloc[-1])

            feat = {
                "near_52w_pct":    near52_pct,
                "above_ma200":     curr > ma200,
                "wk_rsi":          wk_rsi_val,
                "wk_adx":          wk_adx_val,
                "wk_obv_above_ma": wk_obv_above_ma,
                "mo_rsi":          mo_rsi_val,
            }

            score, sig_pos, sig_neg = _position_score(feat, stock)
            if score <= 0:
                continue

            candidates.append({
                "ticker":           ticker,
                "company":          stock.get("name", ticker),
                "score":            score,
                "close":            curr,
                "near_52w_pct":     round(near52_pct * 100, 1),
                "wk_rsi":           round(wk_rsi_val, 1),
                "wk_adx":           round(wk_adx_val, 1),
                "mo_rsi":           round(mo_rsi_val, 1),
                "wk_obv_above_ma":  wk_obv_above_ma,
                "target_pct":       CONFIG.get("POSITION_TARGET_PCT", 0.20),
                "stop_loss_pct":    CONFIG.get("POSITION_STOP_LOSS_PCT", 0.10),
                "max_hold_days":    CONFIG.get("POSITION_MAX_HOLD_DAYS", 60),
                "signals_positive": sig_pos,
                "signals_negative": sig_neg,
                "pipeline":         "position_trading",
            })
        except Exception as e:
            log.debug(f"   POSITION {ticker}: {e}")
            continue

    candidates.sort(key=lambda x: x["score"], reverse=True)
    hasil = candidates[:CONFIG.get("POSITION_MAX_OUTPUT", 5)]
    log.info(f"   Kandidat lolos: {len(candidates)} | Output: {len(hasil)}")
    log.info("=" * 70)
    return hasil


def run_swing_pipeline(
    universe: List[Dict], mode: str, market_ctx: Dict
) -> List[Dict]:
    """Pipeline Swing Trading v1.0 — VCP + Stage 2. Kill switch jika IHSG < MA200."""
    log.info("\n" + "=" * 70)
    log.info("📈 SWING PIPELINE — VCP Stage 2 Uptrend v1.0")
    log.info(f"   Universe input: {len(universe)} saham")
    log.info("=" * 70)

    # KILL SWITCH: Cek IHSG > MA200
    ihsg_above_ma200 = True
    try:
        ihsg_df = get_daily_data("^JKSE")
        if ihsg_df is not None and len(ihsg_df) >= 200:
            ihsg_cols = [c.lower() if isinstance(c, str) else str(c)
                         for c in ihsg_df.columns]
            ihsg_df.columns = ihsg_cols
            ihsg_c = ihsg_df["close"].astype(float)
            ihsg_ma200 = float(ihsg_c.rolling(200).mean().iloc[-1])
            ihsg_last  = float(ihsg_c.iloc[-1])
            ihsg_above_ma200 = ihsg_last > ihsg_ma200
            status = "DI ATAS ✅" if ihsg_above_ma200 else "DI BAWAH ❌"
            log.info(f"   IHSG: {ihsg_last:,.0f} | MA200: {ihsg_ma200:,.0f} | {status}")
    except Exception as e:
        log.warning(f"   IHSG data error: {e} — skip kill switch")

    if not ihsg_above_ma200:
        log.warning("   ⛔ KILL SWITCH: IHSG < MA200. Pipeline SWING mati.")
        log.info("=" * 70)
        return []

    target_pct = CONFIG["SWING_TARGET_PCT"]
    sl_pct = CONFIG["SWING_STOP_LOSS_PCT"]
    max_hold = CONFIG["SWING_MAX_HOLD_DAYS"]

    candidates = []
    filtered_val = 0
    filtered_price = 0
    processed = 0
    no_data = 0
    no_stage2 = 0

    for stock_mm in universe:
        ticker = stock_mm.get("ticker", "")
        price  = stock_mm.get("price", 0) or 0

        if price < CONFIG["SWING_MIN_PRICE"]:
            filtered_price += 1
            continue

        processed += 1
        feat = _swing_compute_features(ticker)
        if feat is None:
            no_data += 1
            continue
        if feat["val_ma20"] < CONFIG["SWING_MIN_VALUE_20D"]:
            filtered_val += 1
            continue

        score, sig_pos, sig_neg = _swing_score(feat, stock_mm)
        if score < 40:
            if not feat["is_stage2"]:
                no_stage2 += 1
            continue

        log.info(
            f"    ✅ SWING {ticker}: Score {score} | "
            f"Dist +{feat['dist_ma20']*100:.1f}% | "
            f"Sqz {feat['squeeze']*100:.0f}% | "
            f"Vol {feat['vol_ratio']*100:.0f}% | RSI {feat['rsi']:.0f}"
        )

        entry = round_bei(feat["close"])
        target = round_bei(entry * (1 + target_pct))
        stop_loss = round_bei(entry * (1 - sl_pct))

        candidates.append({
            "ticker": ticker, "company": stock_mm.get("name", ticker),
            "type": "WATCHLIST_SWING", "score": score,
            "market_data": {
                "close": feat["close"],
                "value_today": stock_mm.get("value_today", 0),
                "net_foreign": stock_mm.get("net_foreign_today", 0),
            },
            "technical": {
                "stage2": True, "ma50_slope": f"+{feat['ma50_slope']*100:.1f}%/10d",
                "dist_ma20": f"+{feat['dist_ma20']*100:.1f}%",
                "squeeze": f"{feat['squeeze']*100:.0f}%",
                "vol_ratio": f"{feat['vol_ratio']*100:.0f}%",
                "rsi": feat["rsi"], "atr_pct": f"{feat['atr_pct']*100:.1f}%",
                "dist_52w_high": f"{feat['dist_52w']*100:+.1f}%",
                "ma20": feat["ma20"], "ma50": feat["ma50"], "ma200": feat["ma200"],
            },
            "trading_plan": {
                "strategy": f"Beli sekarang, hold max {max_hold} hari",
                "entry": entry, "target": target,
                "target_pct": f"+{target_pct*100:.0f}%",
                "stop_loss": stop_loss,
                "stop_loss_pct": f"-{sl_pct*100:.0f}%",
                "max_hold_days": max_hold,
                "exit_rule": (
                    f"Jual jika sentuh {target:,} (+{target_pct*100:.0f}%), "
                    f"ATAU cut loss di {stop_loss:,} (-{sl_pct*100:.0f}%), "
                    f"ATAU jual di hari ke-{max_hold}."
                ),
                "trailing_stop": "Aktifkan trailing stop setelah profit +5%",
            },
            "signals_positive": sig_pos, "signals_negative": sig_neg,
            "backtest_stats": {
                "golden_setup_pf": "1.60x", "golden_setup_ev": "+2.14%",
                "golden_setup_wr": "48.8%", "golden_setup_dd": "-5.5%",
                "total_backtest": "467.390 hari (956 saham IHSG)",
                "bear_market_warning": "GAGAL di 2019, 2024, 2026",
            },
            "disclaimer": (
                f"⚠️ SWING WATCHLIST. PF=1.60x, EV=+2.14%. "
                f"SL -{sl_pct*100:.0f}% KERAS. "
                f"GAGAL di bear market — kill switch IHSG<MA200 aktif."
            ),
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    top = candidates[:CONFIG["SWING_MAX_OUTPUT"]]
    for rank, c in enumerate(top, 1):
        c["rank"] = rank

    log.info(f"\n📊 SWING Summary:")
    log.info(f"   Input universe         : {len(universe)} saham")
    log.info(f"   Filter price <100      : {filtered_price}")
    log.info(f"   Diproses               : {processed}")
    log.info(f"   No data                : {no_data}")
    log.info(f"   Filter val <5B         : {filtered_val}")
    log.info(f"   Bukan Stage 2          : {no_stage2}")
    log.info(f"   Lolos Golden Setup     : {len(candidates)}")
    log.info(f"   Final output           : {len(top)} saham")
    log.info("=" * 70)

    return top


def save_combined_output_v3(
    intraday_results: List[Dict],
    ara_results: List[Dict],
    bsjp_results: List[Dict],
    bpjs_results: List[Dict],
    swing_results: List[Dict],
    mode: str,
    market_ctx: Dict,
    intraday_summary: Dict,
    session_label: str = "MARKET_DAY",
    trend_results: Optional[List[Dict]] = None,
    position_results: Optional[List[Dict]] = None,
):
    """Simpan output gabungan ke combined_screening.json (v7 — 7 pipeline)."""
    today     = datetime.now().strftime("%Y-%m-%d")
    today_str = datetime.now().strftime("%Y%m%d")

    t_res = trend_results or []
    p_res = position_results or []

    any_signal = (
        intraday_results or ara_results or bsjp_results or
        bpjs_results or swing_results or t_res or p_res
    )

    output = {
        "logika_lama_intraday":     intraday_results,
        "logika_baru_calon_ara":    ara_results,
        "bsjp_beli_sore_jual_pagi": bsjp_results,
        "bpjs_beli_pagi_jual_sore": bpjs_results,
        "swing_trading":            swing_results,
        "trend_following":          t_res,
        "position_trading":         p_res,

        "meta": {
            "status":           "success" if any_signal else "no_signal",
            "generated_at":     datetime.now().strftime("%Y-%m-%d %H:%M:%S WIB"),
            "date":             today,
            "mode":             mode,
            "session_label":    session_label,
            "pipeline_version": "v7.0",
            "session_warning": (
                "⚠️ Screening akhir pekan — referensi persiapan saja, bukan sinyal eksekusi."
                if "PRE_MARKET_WEEKEND" in session_label else None
            ),
            "mode_warning": (
                None if mode == "FULL_STOCKBIT"
                else "⚠️ TOKEN TIDAK TERSEDIA — ARA pipeline tidak berjalan"
            ),
            "intraday_count": len(intraday_results),
            "ara_count":      len(ara_results),
            "bsjp_count":     len(bsjp_results),
            "bpjs_count":     len(bpjs_results),
            "swing_count":    len(swing_results),
            "trend_count":    len(t_res),
            "position_count": len(p_res),
            "ara_disclaimer": (
                "Pipeline ARA v2: deteksi Tipe 1 (Continuation) & Tipe 2 (Silent Accumulation). "
                "Tipe 3 (Out of Nowhere, ~48% dari ARA nyata) tidak terdeteksi dari OHLCV. "
                "Precision: 8-15%. Backtest wajib sebelum live trading."
            ),
            "bsjp_disclaimer": (
                "Pipeline BSJP v1: hold overnight, jual pagi hari. "
                "Win rate historis: Tier S ~70%, Tier A ~64% (spike >2%). "
                "Backtest 252.480 hari perdagangan IHSG. Selalu pasang stop loss."
            ),
            "bpjs_disclaimer": (
                "Pipeline BPJS v1: WATCHLIST, bukan sinyal langsung. "
                "Win rate OHLCV D-1 saja: 38-50% (belum cukup). "
                "WAJIB dikonfirmasi real-time pagi hari. "
                "Backtest forward-looking 789.792 hari trading IHSG."
            ),
            "swing_disclaimer": (
                "Pipeline SWING v1: VCP setup pada Stage 2 Uptrend. "
                "PF=1.60x, EV=+2.14% per trade (n=2.077). Hold 10-15 hari. "
                "KILL SWITCH aktif jika IHSG < MA200 (bear market). "
                "Backtest 467.390 hari trading IHSG. SL -6% keras."
            ),
            "trend_disclaimer": (
                "Pipeline TREND FOLLOWING v1: ADX kuat + RSI + MACD + Weekly Confirmation. "
                "WR: 54.7% (Max High>=10% dalam 20 hari). "
                "KILL SWITCH aktif jika IHSG < MA200."
            ),
            "position_disclaimer": (
                "Pipeline POSITION TRADING v1: Near 52W High + Weekly/Monthly Confirm. "
                "WR: 38.7% (Max High>=20% dalam 60 hari)."
            ),
            "scoring_breakdown": {
                "daily_max": 70, "minute_max": 20, "stockbit_bonus": 10, "total_max": 100,
            },
        },

        "market_context":    market_ctx,
        "screening_summary": intraday_summary,

        "config_ara": {
            "vol_ma20_tier1":       CONFIG.get("ARA_VOL_MA20_TIER1", 3.0),
            "vol_ma20_tier2":       CONFIG.get("ARA_VOL_MA20_TIER2", 6.0),
            "score_min":            CONFIG.get("ARA_SCORE_MIN", 55),
            "score_strong":         CONFIG.get("ARA_SCORE_STRONG", 70),
            "max_output":           CONFIG.get("ARA_MAX_OUTPUT", 5),
        },
        "config_bsjp": {
            "min_value_today_rp":  CONFIG.get("BSJP_MIN_VALUE_TODAY", 10_000_000_000),
            "tier_s_vol":          CONFIG.get("BSJP_TIER_S_VOL", 5.0),
            "tier_a_vol":          CONFIG.get("BSJP_TIER_A_VOL", 4.0),
            "max_output":          CONFIG.get("BSJP_MAX_OUTPUT", 5),
            "backtest_sample_size": 252480,
        },
        "config_bpjs": {
            "min_value_today_rp":   CONFIG.get("BPJS_MIN_VALUE_TODAY", 10_000_000_000),
            "max_output":           CONFIG.get("BPJS_MAX_OUTPUT", 5),
            "backtest_sample_size": 789792,
        },
        "config_swing": {
            "min_value_20d_rp":     CONFIG.get("SWING_MIN_VALUE_20D", 5_000_000_000),
            "dist_ma20_range":      f"{CONFIG.get('SWING_DIST_MA20_MIN', 0.02)}-{CONFIG.get('SWING_DIST_MA20_MAX', 0.10)}",
            "squeeze_max":          CONFIG.get("SWING_SQUEEZE_MAX", 0.35),
            "vol_5d_max":           CONFIG.get("SWING_VOL_5D_MAX", 0.60),
            "rsi_range":            f"{CONFIG.get('SWING_RSI_MIN', 45)}-{CONFIG.get('SWING_RSI_MAX', 65)}",
            "target_pct":           CONFIG.get("SWING_TARGET_PCT", 0.10),
            "stop_loss_pct":        CONFIG.get("SWING_STOP_LOSS_PCT", 0.06),
            "max_hold_days":        CONFIG.get("SWING_MAX_HOLD_DAYS", 20),
            "max_output":           CONFIG.get("SWING_MAX_OUTPUT", 5),
            "backtest_sample_size": 467390,
            "golden_setup_pf":      "1.60x",
            "golden_setup_ev":      "+2.14% per trade",
        },
        "config_trend": {
            "min_value_20d_rp":     CONFIG.get("TREND_MIN_VALUE_20D", 1_000_000_000),
            "adx_min":              CONFIG.get("TREND_ADX_MIN", 30),
            "rsi_range":            f"{CONFIG.get('TREND_RSI_MIN', 50)}-{CONFIG.get('TREND_RSI_MAX', 75)}",
            "wk_adx_min":           CONFIG.get("TREND_WK_ADX_MIN", 25),
            "target_pct":           CONFIG.get("TREND_TARGET_PCT", 0.10),
            "stop_loss_pct":        CONFIG.get("TREND_STOP_LOSS_PCT", 0.07),
            "max_hold_days":        CONFIG.get("TREND_MAX_HOLD_DAYS", 20),
        },
        "config_position": {
            "min_value_20d_rp":     CONFIG.get("POSITION_MIN_VALUE_20D", 2_000_000_000),
            "near_52w_pct":         CONFIG.get("POSITION_NEAR_52W_PCT", 0.85),
            "wk_rsi_range":         f"{CONFIG.get('POSITION_WK_RSI_MIN', 55)}-{CONFIG.get('POSITION_WK_RSI_MAX', 80)}",
            "wk_adx_min":           CONFIG.get("POSITION_WK_ADX_MIN", 20),
            "mo_rsi_min":           CONFIG.get("POSITION_MO_RSI_MIN", 50),
            "target_pct":           CONFIG.get("POSITION_TARGET_PCT", 0.20),
            "stop_loss_pct":        CONFIG.get("POSITION_STOP_LOSS_PCT", 0.10),
            "max_hold_days":        CONFIG.get("POSITION_MAX_HOLD_DAYS", 60),
        },
    }

    combined_path = CONFIG.get("ARA_OUTPUT_FILE", "combined_screening.json")
    with open(combined_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)
    log.info(f"✅ Tersimpan: {combined_path}")

    dated_path = f"combined_screening_{today_str}.json"
    with open(dated_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)
    log.info(f"✅ Tersimpan: {dated_path}")


# =============================================================================
# ENTRY POINT
# =============================================================================
# Cara menjalankan:
#   python screener.py               # Full pipeline (intraday + ARA v2 + BSJP)
#   python screener.py --ara-only    # ARA v2 pipeline saja (debug/dev)
#   python screener.py --no-ara      # Intraday saja, skip ARA
#
# Membaca combined_screening.json:
#   .logika_lama_intraday[]         → sinyal intraday (max 5)
#   .logika_baru_calon_ara[]        → kandidat ARA dengan:
#     .entry_range                   → "1000 – 1050" (entry D-0 besok)
#     .estimated_target_pct          → "+8% hingga +15% dari harga beli"
#     .reason_beginner_friendly      → penjelasan tanpa jargon
#     .risk_warning                  → peringatan spesifik
#     .m1_last15_green               → bar hijau terakhir sesi D-1 (dari 14)
#     .signals_positive/negative     → reasoning transparan
# =============================================================================

