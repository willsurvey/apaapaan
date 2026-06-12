// pages/index.js
import { useState, useEffect, useMemo, useCallback, useContext } from 'react'
import Head from 'next/head'
import styles from '../styles/Home.module.css'
import { ThemeContext } from './_app'

const formatRp = (val) => {
  if (val == null || isNaN(val)) return '-'
  const n = Number(val)
  if (n >= 1_000_000_000_000) return `Rp${(n / 1_000_000_000_000).toFixed(1)}T`
  if (n >= 1_000_000_000) return `Rp${(n / 1_000_000_000).toFixed(1)}M`
  if (n >= 1_000_000) return `Rp${(n / 1_000_000).toFixed(0)}Jt`
  return `Rp${n.toLocaleString('id-ID')}`
}

const formatNum = (val, dec = 0) => {
  if (val == null || isNaN(val)) return '-'
  return Number(val).toLocaleString('id-ID', { minimumFractionDigits: dec, maximumFractionDigits: dec })
}

const fmt2 = (val) => formatNum(val, 2)

const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v))

function ChangeBadge({ val, suffix = '%' }) {
  const n = Number(val)
  if (isNaN(n)) return <span className={styles.neutral}>-</span>
  const cls = n > 0 ? styles.up : n < 0 ? styles.down : styles.neutral
  const prefix = n > 0 ? '+' : ''
  return <span className={cls}>{prefix}{fmt2(n)}{suffix}</span>
}

function TierBadge({ tier }) {
  const map = { HIGH: styles.tierHigh, MODERATE: styles.tierMod, STRONG: styles.tierStrong, LOW: styles.tierLow }
  return <span className={`${styles.tierBadge} ${map[tier] || styles.tierMod}`}>{tier}</span>
}

function BrokerBadge({ signal }) {
  const map = {
    'Big Acc': styles.brokerBigAcc,
    'Acc': styles.brokerAcc,
    'Small Acc': styles.brokerSmallAcc,
    'Normal Acc': styles.brokerNormalAcc,
    'Neutral': styles.brokerNeutral,
    'Normal Dist': styles.brokerDist,
    'Dist': styles.brokerDist,
    'Big Dist': styles.brokerBigDist,
  }
  return <span className={`${styles.brokerBadge} ${map[signal] || styles.brokerNeutral}`}>{signal || '-'}</span>
}

function ScoreRing({ score }) {
  const s = clamp(Number(score) || 0, 0, 100)
  const color = s >= 70 ? 'var(--c-teal)' : s >= 50 ? 'var(--c-amber)' : 'var(--c-coral)'
  return (
    <div className={styles.scoreRing} style={{ '--ring-color': color, '--ring-pct': `${s}%` }}>
      <span className={styles.scoreNum}>{s}</span>
      <span className={styles.scoreLbl}>skor</span>
    </div>
  )
}

function EntryRow({ label, price, pct, note, color = 'var(--c-teal)' }) {
  return (
    <div className={styles.entryRow}>
      <div className={styles.entryMeta}>
        <span className={styles.entryLbl}>{label}</span>
        {pct != null && <span className={styles.entryPct}>{pct}%</span>}
      </div>
      <span className={styles.entryPrice} style={{ color }}>{price ? `Rp${Number(price).toLocaleString('id-ID')}` : '-'}</span>
      {note && <span className={styles.entryNote}>{note}</span>}
    </div>
  )
}

function IntradayCard({ stock, rank }) {
  const ep = stock.entry_plan || {}
  const acc = stock.accumulation || {}
  const mkt = stock.market_data || {}
  const smc = stock.smc || {}
  const trend = stock.trend || {}
  const score = stock.scoring?.confidence_score ?? 0
  const tier = stock.scoring?.tier ?? 'LOW'
  const bd = acc.score_breakdown || {}

  const dirColor = { PULLBACK: 'var(--c-amber)', MOMENTUM: 'var(--c-teal)', GAP_UP: 'var(--c-coral)' }[ep.entry_direction] || 'var(--c-amber)'

  return (
    <div className={styles.card}>
      <div className={styles.cardHead}>
        <div className={styles.cardHeadLeft}>
          <span className={styles.rankBadge}>#{rank}</span>
          <div>
            <span className={styles.tickerText}>{stock.ticker}</span>
            <span className={styles.companyText}>{stock.company}</span>
          </div>
          <TierBadge tier={tier} />
        </div>
        <ScoreRing score={score} />
      </div>

      <div className={styles.cardBody}>
        {stock.universe_sources?.length > 0 && (
          <div className={styles.sourceTags}>
            {stock.universe_sources.map((s, i) => <span key={i} className={styles.sourceTag}>{s}</span>)}
          </div>
        )}

        <div className={styles.statsRow}>
          <div className={styles.stat}><span className={styles.statLbl}>Close</span><span className={styles.statVal}>Rp{formatNum(mkt.close)}</span></div>
          <div className={styles.stat}><span className={styles.statLbl}>Perubahan</span><ChangeBadge val={mkt.change_pct} /></div>
          <div className={styles.stat}><span className={styles.statLbl}>Nilai</span><span className={styles.statVal}>{formatRp(mkt.value_today)}</span></div>
          <div className={styles.stat}><span className={styles.statLbl}>Frekuensi</span><span className={styles.statVal}>{formatNum(mkt.frequency_today)}x</span></div>
          <div className={styles.stat}><span className={styles.statLbl}>Net Foreign</span><ChangeBadge val={acc.net_foreign_today != null ? (acc.net_foreign_today / 1e9).toFixed(2) : null} suffix='B' /></div>
          <div className={styles.stat}><span className={styles.statLbl}>For 3D</span><span className={styles.statVal}>{acc.net_foreign_3d > 0 ? `${acc.net_foreign_3d}d ✅` : '-'}</span></div>
        </div>

        <div className={styles.twoCol}>
          <div className={styles.colBlock}>
            <span className={styles.blockTitle}>Broker & Akumulasi</span>
            <BrokerBadge signal={acc.broker_signal} />
            <div className={styles.miniRow}><span>Skor Acc</span><strong>{acc.acc_score ?? '-'}</strong></div>
            {bd.total != null && (
              <div className={styles.breakdownGrid}>
                {[['Broker', bd.broker], ['For 1D', bd.foreign_1d], ['For 3D', bd.foreign_3d], ['Candle', bd.candle], ['Vol', bd.volume]].map(([lbl, v]) =>
                  v != null && (
                    <div key={lbl} className={styles.breakdownItem}>
                      <span>{lbl}</span>
                      <strong style={{ color: v >= 0 ? 'var(--c-teal)' : 'var(--c-coral)' }}>{v > 0 ? `+${v}` : v}</strong>
                    </div>
                  )
                )}
              </div>
            )}
            <div className={styles.miniRow}><span>MA50</span><strong>Rp{formatNum(trend.ma50)}</strong></div>
            <div className={styles.miniRow}><span>Gap MA50</span><ChangeBadge val={trend.gap_from_ma50_pct} /></div>
            <div className={styles.miniRow}>
              <span>MA50 Slope</span>
              <strong style={{ color: trend.ma50_slope === 'POSITIVE' ? 'var(--c-teal)' : trend.ma50_slope === 'NEGATIVE' ? 'var(--c-coral)' : '' }}>{trend.ma50_slope || '-'}</strong>
            </div>
          </div>
          <div className={styles.colBlock}>
            <span className={styles.blockTitle}>SMC Structure</span>
            <div className={styles.smcGrid}>
              <div className={styles.smcItem}><span className={styles.smcLbl}>Internal</span><span className={styles.smcVal}>{smc.internal_structure || 'NONE'}</span></div>
              <div className={styles.smcItem}>
                <span className={styles.smcLbl}>Swing Bias</span>
                <span className={`${styles.smcVal} ${smc.swing_trend_bias === 'BULLISH' ? styles.up : smc.swing_trend_bias === 'BEARISH' ? styles.down : ''}`}>{smc.swing_trend_bias || 'NEUTRAL'}</span>
              </div>
              {smc.ob_zone && <div className={styles.smcItem}><span className={styles.smcLbl}>OB Zone</span><span className={styles.smcVal}>{smc.ob_zone}</span></div>}
              {smc.fvg_zone && <div className={styles.smcItem}><span className={styles.smcLbl}>FVG Zone</span><span className={styles.smcVal}>{smc.fvg_zone}</span></div>}
              {smc.strong_low != null && <div className={styles.smcItem}><span className={styles.smcLbl}>Strong Low</span><span className={styles.smcVal}>Rp{formatNum(smc.strong_low)}</span></div>}
              {smc.weak_high != null && <div className={styles.smcItem}><span className={styles.smcLbl}>Weak High</span><span className={styles.smcVal}>Rp{formatNum(smc.weak_high)}</span></div>}
            </div>
          </div>
        </div>

        <div className={styles.entrySection}>
          <div className={styles.entryHeader}>
            <span className={styles.blockTitle}>Entry Plan</span>
            <span className={styles.directionTag} style={{ color: dirColor }}>{ep.entry_direction_label || ep.entry_direction || 'PULLBACK'}</span>
          </div>
          {ep.entry_direction_reason && <div className={styles.entryReason}>{ep.entry_direction_reason}</div>}
          {ep.entry_zone && <div className={styles.miniRow}><span>Entry Zone</span><strong>Rp{ep.entry_zone}</strong></div>}
          <div className={styles.entryList}>
            <EntryRow label="Entry 1" price={ep.entry_1} pct={ep.entry_1_pct} note={ep.entry_1_note} />
            <EntryRow label="Entry 2" price={ep.entry_2} pct={ep.entry_2_pct} note={ep.entry_2_note} />
            <EntryRow label="Entry 3" price={ep.entry_3} pct={ep.entry_3_pct} note={ep.entry_3_note} />
            <div className={styles.avgEntryRow}><span>Avg Entry</span><strong>Rp{formatNum(ep.average_entry)}</strong></div>
          </div>
        </div>

        <div className={styles.slTpRow}>
          <div className={styles.slBox}>
            <span className={styles.slLbl}>Stop Loss</span>
            <span className={styles.slVal}>Rp{formatNum(ep.sl)}</span>
            <span className={styles.slPct}>-{fmt2(ep.sl_pct_risk)}%</span>
            {ep.sl_note && <span className={styles.tpNote}>{ep.sl_note}</span>}
          </div>
          <div className={styles.tpBox}>
            <span className={styles.tpLbl}>TP 1</span>
            <span className={styles.tpVal}>Rp{formatNum(ep.tp1)}</span>
            {ep.tp1_note && <span className={styles.tpNote}>{ep.tp1_note}</span>}
          </div>
          <div className={styles.tpBox}>
            <span className={styles.tpLbl}>TP 2</span>
            <span className={styles.tpVal}>Rp{formatNum(ep.tp2)}</span>
            {ep.tp2_note && <span className={styles.tpNote}>{ep.tp2_note}</span>}
          </div>
          <div className={styles.tpBox}>
            <span className={styles.tpLbl}>TP 3</span>
            <span className={styles.tpVal}>Rp{formatNum(ep.tp3)}</span>
            {ep.tp3_note && <span className={styles.tpNote}>{ep.tp3_note}</span>}
          </div>
          <div className={styles.rrBox}>
            <span className={styles.rrLbl}>R/R</span>
            <span className={styles.rrVal}>{ep.rr_ratio || '-'}</span>
            {ep.risk_pct != null && <span className={styles.tpNote}>Max {ep.risk_pct}% porto</span>}
          </div>
        </div>

        {stock.signals?.length > 0 && (
          <div className={styles.signalBox}>
            {stock.signals.map((s, i) => <div key={i} className={styles.signalItem}>{s}</div>)}
          </div>
        )}
        {stock.warnings?.length > 0 && (
          <div className={styles.warningBox}>
            {stock.warnings.map((w, i) => <div key={i} className={styles.warningItem}>{w}</div>)}
          </div>
        )}
      </div>

      <div className={styles.cardFoot}>
        <span>{stock.mode}</span>
        <span>{stock.updated_at}</span>
      </div>
    </div>
  )
}

function AraCard({ stock }) {
  const s = stock.score ?? 0
  const color = s >= 80 ? 'var(--c-teal)' : s >= 60 ? 'var(--c-amber)' : 'var(--c-coral)'

  return (
    <div className={`${styles.card} ${styles.araCard}`}>
      <div className={styles.cardHead}>
        <div className={styles.cardHeadLeft}>
          <div>
            <span className={styles.tickerText}>{stock.ticker}</span>
            <span className={styles.companyText}>{stock.company}</span>
          </div>
          <TierBadge tier={stock.score_tier} />
        </div>
        <ScoreRing score={s} />
      </div>

      <div className={styles.cardBody}>
        {stock.universe_sources?.length > 0 && (
          <div className={styles.sourceTags}>
            {stock.universe_sources.map((src, i) => <span key={i} className={styles.sourceTag}>{src}</span>)}
          </div>
        )}

        <div className={styles.araPattern}>
          <span className={styles.patternBadge}>{stock.pattern_type}</span>
          <BrokerBadge signal={stock.broker_signal} />
          {stock.confluence_count != null && <span className={styles.confluenceBadge}>{stock.confluence_count} sinyal</span>}
        </div>

        {stock.reason_beginner_friendly && (
          <div className={styles.reasonBox}>{stock.reason_beginner_friendly}</div>
        )}

        <div className={styles.statsRow}>
          <div className={styles.stat}><span className={styles.statLbl}>Close D-1</span><span className={styles.statVal}>Rp{formatNum(stock.d1_close)}</span></div>
          <div className={styles.stat}><span className={styles.statLbl}>Change D-1</span><ChangeBadge val={stock.d1_change_pct} /></div>
          <div className={styles.stat}><span className={styles.statLbl}>Vol/MA20</span><span className={styles.statVal}>{fmt2(stock.d1_vol_ratio_ma20)}x</span></div>
          <div className={styles.stat}><span className={styles.statLbl}>Vol/MA5</span><span className={styles.statVal}>{fmt2(stock.d1_vol_ratio_ma5)}x</span></div>
        </div>

        <div className={styles.araMetrics}>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Open D-1</span><span className={styles.metricVal}>Rp{formatNum(stock.d1_open)}</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>High D-1</span><span className={styles.metricVal}>Rp{formatNum(stock.d1_high)}</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Low D-1</span><span className={styles.metricVal}>Rp{formatNum(stock.d1_low)}</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Nilai D-1</span><span className={styles.metricVal}>{formatRp(stock.d1_value_rp)}</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Upper Wick</span><span className={styles.metricVal}>{fmt2(stock.d1_upper_wick)}</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Body Pct</span><span className={styles.metricVal}>{fmt2(stock.d1_body_pct)}</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Close Pos</span><span className={styles.metricVal}>{fmt2(stock.d1_close_pos)}</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Range Exp</span><span className={styles.metricVal}>{fmt2(stock.d1_range_expansion)}x</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>MA20</span><span className={styles.metricVal}>Rp{formatNum(stock.ma20)}</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>MA50</span><span className={styles.metricVal}>Rp{formatNum(stock.ma50)}</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>5D Trend</span><span className={styles.metricVal} style={{ color: (stock.trend_5d_pct ?? 0) >= 0 ? 'var(--c-teal)' : 'var(--c-coral)' }}>{stock.trend_5d_pct != null ? `+${fmt2(stock.trend_5d_pct)}%` : '-'}</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Di atas MA</span><span className={styles.metricVal} style={{ color: 'var(--c-teal)' }}>{[stock.above_ma20 && 'MA20', stock.above_ma50 && 'MA50'].filter(Boolean).join(' ') || '-'}</span></div>
        </div>

        {(stock.d2_change_pct != null || stock.d2_vol_ratio_ma20 != null) && (
          <div className={styles.d2Row}>
            <span className={styles.blockTitle}>D-2</span>
            <ChangeBadge val={stock.d2_change_pct} />
            {stock.d2_body_pct != null && <span>Body {fmt2(stock.d2_body_pct)}</span>}
            {stock.d2_vol_ratio_ma20 != null && <span>Vol {fmt2(stock.d2_vol_ratio_ma20)}x MA20</span>}
          </div>
        )}

        {stock.m1_data_available && (
          <div className={styles.m1Row}>
            <span className={styles.blockTitle}>Intraday M1</span>
            <div className={styles.araMetrics} style={{ marginTop: '0.25rem' }}>
              <div className={styles.metricItem}><span className={styles.metricLbl}>Bar Hijau</span><span className={styles.metricVal}>{stock.m1_last15_green}/14</span></div>
              <div className={styles.metricItem}><span className={styles.metricLbl}>vs VWAP</span><span className={styles.metricVal} style={{ color: (stock.m1_close_vs_vwap ?? 0) >= 0 ? 'var(--c-teal)' : 'var(--c-coral)' }}>{stock.m1_close_vs_vwap != null ? `${stock.m1_close_vs_vwap > 0 ? '+' : ''}${fmt2(stock.m1_close_vs_vwap)}%` : '-'}</span></div>
              <div className={styles.metricItem}><span className={styles.metricLbl}>Close Pos</span><span className={styles.metricVal}>{fmt2(stock.m1_close_pos_intra)}</span></div>
              <div className={styles.metricItem}><span className={styles.metricLbl}>High Timing</span><span className={styles.metricVal}>{stock.m1_high_timing != null ? `${Math.round(stock.m1_high_timing * 100)}%` : '-'}</span></div>
            </div>
          </div>
        )}

        <div className={styles.araEntrySection}>
          <span className={styles.blockTitle}>Entry Plan ARA</span>
          <div className={styles.miniRow}><span>Range Entry</span><strong>Rp{stock.entry_range}</strong></div>
          <div className={styles.miniRow}><span>Assumed Entry</span><strong>Rp{formatNum(stock.assumed_entry)}</strong></div>
          <div className={styles.miniRow}><span>Target Est.</span><strong>{stock.estimated_target_pct}</strong></div>
          {stock.entry_note && <div className={styles.entryNoteBox}>{stock.entry_note}</div>}
        </div>

        <div className={styles.targetGrid}>
          <div className={styles.targetItem}><span className={styles.targetLbl}>Konservatif</span><span className={styles.targetVal}>Rp{formatNum(stock.target_conservative)}</span><span className={styles.targetPct}>+{stock.target_conservative_pct}%</span></div>
          <div className={styles.targetItem}><span className={styles.targetLbl}>Moderat</span><span className={styles.targetVal}>Rp{formatNum(stock.target_base)}</span><span className={styles.targetPct}>+{stock.target_base_pct}%</span></div>
          <div className={styles.targetItem}><span className={styles.targetLbl}>Full ARA</span><span className={styles.targetVal}>Rp{formatNum(stock.target_optimistic)}</span><span className={styles.targetPct}>+{stock.target_optimistic_pct}%</span></div>
        </div>
        {stock.target_note && <div className={styles.targetNote}>{stock.target_note}</div>}

        {stock.signals_positive?.length > 0 && (
          <div className={styles.signalBox}>
            {stock.signals_positive.map((s, i) => <div key={i} className={styles.signalItem}>+ {s}</div>)}
          </div>
        )}
        {stock.signals_negative?.length > 0 && (
          <div className={styles.warningBox}>
            {stock.signals_negative.map((s, i) => <div key={i} className={styles.warningItem}>- {s}</div>)}
          </div>
        )}
        {stock.risk_warning && (
          <div className={styles.warningBox}>
            <span className={styles.warningItem}>{stock.risk_warning}</span>
          </div>
        )}
        <div className={`${styles.warningBox} ${styles.araWarning}`}>
          <span className={styles.warningItem}>{stock.warning}</span>
        </div>
      </div>

      <div className={styles.cardFoot}>
        <span>{stock.up_streak_days > 0 ? `${stock.up_streak_days}d streak` : 'no streak'}</span>
        <span>{stock.generated_at}</span>
      </div>
    </div>
  )
}

function BsjpCard({ stock }) {
  const mkt = stock.market_data || {}
  const ep = stock.entry_plan || {}
  const feat = stock.bsjp_features || {}
  const s = stock.score ?? 0
  const tierColor = stock.tier === 'S' ? 'var(--c-teal)' : stock.tier === 'A' ? 'var(--c-blue)' : 'var(--c-amber)'

  return (
    <div className={`${styles.card} ${styles.bsjpCard}`}>
      <div className={styles.cardHead}>
        <div className={styles.cardHeadLeft}>
          <span className={styles.rankBadge}>#{stock.rank}</span>
          <div>
            <span className={styles.tickerText}>{stock.ticker}</span>
            <span className={styles.companyText}>{stock.company || 'BSJP Candidate'}</span>
          </div>
          <span className={styles.tierBadge} style={{background: tierColor, color: '#fff', borderColor: tierColor}}>Tier {stock.tier}</span>
        </div>
        <ScoreRing score={s} />
      </div>
      <div className={styles.cardBody}>
        {stock.universe_context?.in_mover_types?.length > 0 && (
          <div className={styles.sourceTags}>
            {stock.universe_context.in_mover_types.map((src, i) => <span key={i} className={styles.sourceTag}>{src}</span>)}
          </div>
        )}
        <div className={styles.statsRow}>
          <div className={styles.stat}><span className={styles.statLbl}>Close</span><span className={styles.statVal}>Rp{formatNum(mkt.close)}</span></div>
          <div className={styles.stat}><span className={styles.statLbl}>Change</span><ChangeBadge val={mkt.change_pct} /></div>
          <div className={styles.stat}><span className={styles.statLbl}>Nilai</span><span className={styles.statVal}>{formatRp(mkt.value_today)}</span></div>
          <div className={styles.stat}><span className={styles.statLbl}>Net Foreign</span><ChangeBadge val={mkt.net_foreign != null ? (mkt.net_foreign / 1e9).toFixed(2) : null} suffix='B' /></div>
        </div>
        <div className={styles.araMetrics}>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Vol/MA20</span><span className={styles.metricVal}>{fmt2(feat.vol_ratio20)}x</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Body Pct</span><span className={styles.metricVal}>{fmt2(feat.body_pct)}%</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Close Pos</span><span className={styles.metricVal}>{fmt2(feat.close_pos_pct)}%</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>ATR</span><span className={styles.metricVal}>{fmt2(feat.atr_pct)}%</span></div>
          {feat.above_ma20 != null && <div className={styles.metricItem}><span className={styles.metricLbl}>Di atas MA</span><span className={styles.metricVal} style={{color:'var(--c-teal)'}}>{[feat.above_ma20 && 'MA20', feat.above_ma50 && 'MA50', feat.above_ma200 && 'MA200'].filter(Boolean).join(' ') || '-'}</span></div>}
        </div>
        <div className={styles.entrySection}>
          <span className={styles.blockTitle}>Rencana BSJP — Beli Sore, Jual Pagi</span>
          <div className={styles.miniRow}><span>Strategi</span><strong>{ep.strategy}</strong></div>
          <div className={styles.miniRow}><span>Entry Range</span><strong>{ep.entry_range}</strong></div>
          {ep.entry_note && <div className={styles.entryNoteBox}>{ep.entry_note}</div>}
          <div className={styles.entryList} style={{marginTop: '12px'}}>
            <EntryRow label="Stop Loss" price={ep.stop_loss} color="var(--c-coral)" note={ep.stop_note} />
          </div>
          <div className={styles.miniRow}><span>Target Pagi</span><strong>{ep.target_pct}</strong></div>
          {ep.exit_strategy && <div className={styles.entryNoteBox}>{ep.exit_strategy}</div>}
        </div>
        <div className={styles.miniRow}><span>Win Probability</span><strong>{stock.win_probability}</strong></div>
        {stock.signals_positive?.length > 0 && (
          <div className={styles.signalBox}>
            {stock.signals_positive.map((r, i) => <div key={i} className={styles.signalItem}>+ {r}</div>)}
          </div>
        )}
        {stock.signals_negative?.length > 0 && (
          <div className={styles.warningBox}>
            {stock.signals_negative.map((r, i) => <div key={i} className={styles.warningItem}>- {r}</div>)}
          </div>
        )}
        {stock.disclaimer && <div className={`${styles.warningBox} ${styles.araWarning}`}><span className={styles.warningItem}>{stock.disclaimer}</span></div>}
      </div>
    </div>
  )
}

function BpjsCard({ stock }) {
  const mkt = stock.market_data || {}
  const tp = stock.trading_plan || {}
  const d1 = stock.d1_features || {}
  const s = stock.score ?? 0

  return (
    <div className={`${styles.card} ${styles.bpjsCard}`}>
      <div className={styles.cardHead}>
        <div className={styles.cardHeadLeft}>
          <span className={styles.rankBadge}>#{stock.rank}</span>
          <div>
            <span className={styles.tickerText}>{stock.ticker}</span>
            <span className={styles.companyText}>{stock.company || 'BPJS Candidate'}</span>
          </div>
          <span className={styles.tierBadge} style={{background: 'var(--c-teal)', color: '#fff', borderColor: 'var(--c-teal)'}}>{stock.formula || 'BPJS'}</span>
        </div>
        <ScoreRing score={s} />
      </div>
      <div className={styles.cardBody}>
        {stock.universe_context?.in_mover_types?.length > 0 && (
          <div className={styles.sourceTags}>
            {stock.universe_context.in_mover_types.map((src, i) => <span key={i} className={styles.sourceTag}>{src}</span>)}
          </div>
        )}
        <div className={styles.statsRow}>
          <div className={styles.stat}><span className={styles.statLbl}>Close</span><span className={styles.statVal}>Rp{formatNum(mkt.close)}</span></div>
          <div className={styles.stat}><span className={styles.statLbl}>Change</span><ChangeBadge val={mkt.change_pct} /></div>
          <div className={styles.stat}><span className={styles.statLbl}>Nilai</span><span className={styles.statVal}>{formatRp(mkt.value_today)}</span></div>
          <div className={styles.stat}><span className={styles.statLbl}>Net Foreign</span><ChangeBadge val={mkt.net_foreign != null ? (mkt.net_foreign / 1e9).toFixed(2) : null} suffix='B' /></div>
        </div>
        <div className={styles.araMetrics}>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Body D-1</span><span className={styles.metricVal}>{fmt2(d1.body_pct)}%</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Close Pos</span><span className={styles.metricVal}>{fmt2(d1.close_pos_pct)}%</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Vol Ratio</span><span className={styles.metricVal}>{fmt2(d1.vol_ratio)}x</span></div>
          <div className={styles.metricItem}><span className={styles.metricLbl}>Change D-1</span><span className={styles.metricVal}><ChangeBadge val={d1.change_pct} /></span></div>
          {d1.above_ma20 != null && <div className={styles.metricItem}><span className={styles.metricLbl}>Di atas MA</span><span className={styles.metricVal} style={{color:'var(--c-teal)'}}>{[d1.above_ma20 && 'MA20', d1.above_ma50 && 'MA50', d1.above_ma200 && 'MA200'].filter(Boolean).join(' ') || '-'}</span></div>}
        </div>
        <div className={styles.entrySection}>
          <span className={styles.blockTitle}>Rencana BPJS — Beli Pagi, Jual Sore</span>
          <div className={styles.miniRow}><span>Strategi</span><strong>{tp.strategy}</strong></div>
          <div className={styles.miniRow}><span>Entry Range</span><strong>{tp.entry_range}</strong></div>
          {tp.entry_note && <div className={styles.entryNoteBox}>{tp.entry_note}</div>}
          <div className={styles.entryList} style={{marginTop: '12px'}}>
            <EntryRow label="Target Konservatif" price={tp.target_modest} color="var(--c-teal)" />
            <EntryRow label="Target Moderat" price={tp.target_moderate} color="var(--c-blue)" />
            <EntryRow label="Stop Loss" price={tp.stop_loss} color="var(--c-coral)" note={tp.stop_note} />
          </div>
          {tp.exit_strategy && <div className={styles.entryNoteBox}>{tp.exit_strategy}</div>}
        </div>
        {stock.morning_confirmation_criteria?.length > 0 && (
          <div className={styles.signalBox}>
            <span className={styles.blockTitle} style={{marginBottom:'4px'}}>Konfirmasi Pagi</span>
            {stock.morning_confirmation_criteria.map((c, i) => <div key={i} className={styles.signalItem}>{c}</div>)}
          </div>
        )}
        {stock.timing_guide && <div className={styles.entryNoteBox}>{stock.timing_guide}</div>}
        {stock.signals_positive?.length > 0 && (
          <div className={styles.signalBox}>
            {stock.signals_positive.map((r, i) => <div key={i} className={styles.signalItem}>+ {r}</div>)}
          </div>
        )}
        {stock.signals_negative?.length > 0 && (
          <div className={styles.warningBox}>
            {stock.signals_negative.map((r, i) => <div key={i} className={styles.warningItem}>- {r}</div>)}
          </div>
        )}
        {stock.disclaimer && <div className={`${styles.warningBox} ${styles.araWarning}`}><span className={styles.warningItem}>{stock.disclaimer}</span></div>}
      </div>
    </div>
  )
}

function SwingCard({ stock }) {
  const ep = stock.entry_plan || {}
  return (
    <div className={`${styles.card} ${styles.swingCard}`}>
      <div className={styles.cardHead}>
        <div className={styles.cardHeadLeft}>
          <div>
            <span className={styles.tickerText}>{stock.ticker}</span>
            <span className={styles.companyText}>{stock.company || 'SWING Candidate'}</span>
          </div>
          <span className={styles.swingStage}>STAGE 2</span>
        </div>
      </div>
      <div className={styles.cardBody}>
        <div className={styles.statsRow}>
          <div className={styles.stat}><span className={styles.statLbl}>Close</span><span className={styles.statVal}>Rp{formatNum(stock.close)}</span></div>
          <div className={styles.stat}><span className={styles.statLbl}>Trend 20D</span><ChangeBadge val={stock.trend_20d_pct} /></div>
          <div className={styles.stat}><span className={styles.statLbl}>VCP</span><span className={styles.statVal}>{stock.vcp_detected ? 'Ya ✅' : 'Tidak'}</span></div>
        </div>
        
        {stock.vcp_detected && (
          <div className={styles.miniRow} style={{padding: '8px', background: 'rgba(128,128,128,0.05)', borderRadius: '6px', marginBottom: '8px'}}>
            <span>Contraction</span>
            <strong>{formatNum(stock.vcp_contraction_pct, 2)}%</strong>
          </div>
        )}

        <div className={styles.entrySection}>
          <span className={styles.blockTitle}>Swing Plan (1-2 Minggu)</span>
          <div className={styles.entryList}>
             <EntryRow label="Entry Range" price={ep.entry_1} note={`Max Rp${ep.entry_2}`} />
             <EntryRow label="Target (+10%)" price={ep.tp1} pct={ep.tp1_pct} color="var(--c-blue)" />
             <EntryRow label="Stop Loss" price={ep.sl} pct={ep.sl_pct_risk} color="var(--c-coral)" />
          </div>
        </div>
        <div className={styles.signalBox}>
          {stock.reasons?.map((r, i) => <div key={i} className={styles.signalItem}>+ {r}</div>)}
        </div>
      </div>
    </div>
  )
}

function FilterBar({ search, onSearch, sortKey, onSort, filterTier, onFilterTier }) {
  return (
    <div className={styles.filterBar}>
      <input
        type="search"
        placeholder="Cari ticker / nama..."
        value={search}
        onChange={e => onSearch(e.target.value)}
        className={styles.searchInput}
      />
      <select value={sortKey} onChange={e => onSort(e.target.value)} className={styles.selectInput}>
        <option value="rank">Urut: Rank</option>
        <option value="score_desc">Urut: Skor ↓</option>
        <option value="change_desc">Urut: Change ↓</option>
        <option value="value_desc">Urut: Nilai ↓</option>
        <option value="freq_desc">Urut: Frekuensi ↓</option>
        <option value="rr_desc">Urut: R/R ↓</option>
      </select>
      <select value={filterTier} onChange={e => onFilterTier(e.target.value)} className={styles.selectInput}>
        <option value="">Semua Tier</option>
        <option value="HIGH">HIGH</option>
        <option value="MODERATE">MODERATE</option>
        <option value="LOW">LOW</option>
      </select>
    </div>
  )
}

function AraFilterBar({ search, onSearch, filterPattern, onFilterPattern }) {
  return (
    <div className={styles.filterBar}>
      <input
        type="search"
        placeholder="Cari ticker / nama..."
        value={search}
        onChange={e => onSearch(e.target.value)}
        className={styles.searchInput}
      />
      <select value={filterPattern} onChange={e => onFilterPattern(e.target.value)} className={styles.selectInput}>
        <option value="">Semua Pola</option>
        <option value="CONTINUATION">CONTINUATION</option>
        <option value="SILENT_ACCUMULATION">SILENT_ACCUMULATION</option>
        <option value="VOLUME_SPIKE">VOLUME_SPIKE</option>
      </select>
    </div>
  )
}

function SummaryFunnel({ summary }) {
  if (!summary) return null
  const steps = [
    ['Universe', summary.universe],
    ['Likuiditas', summary.after_liquidity],
    ['Akumulasi', summary.after_accumulation],
    ['Trend', summary.after_trend],
    ['SMC', summary.after_smc],
    ['Entry', summary.after_entry],
    ['Final', summary.final],
  ].filter(([, v]) => v != null)

  return (
    <div className={styles.funnel}>
      {steps.map(([label, val], i) => (
        <div key={label} className={styles.funnelStep}>
          <span className={styles.funnelVal}>{formatNum(val)}</span>
          <span className={styles.funnelLbl}>{label}</span>
          {i < steps.length - 1 && <span className={styles.funnelArrow}>›</span>}
        </div>
      ))}
    </div>
  )
}

function TabBar({ active, onChange, counts }) {
  return (
    <div className={styles.tabBar}>
      <button className={`${styles.tab} ${active === 'intraday' ? styles.tabActive : ''}`} onClick={() => onChange('intraday')}>
        Intraday <span className={styles.tabCount}>{counts.intraday || 0}</span>
      </button>
      <button className={`${styles.tab} ${active === 'ara' ? styles.tabActive : ''}`} onClick={() => onChange('ara')}>
        Calon ARA <span className={styles.tabCount}>{counts.ara || 0}</span>
      </button>
      <button className={`${styles.tab} ${active === 'bsjp' ? styles.tabActive : ''}`} onClick={() => onChange('bsjp')}>
        BSJP <span className={styles.tabCount}>{counts.bsjp || 0}</span>
      </button>
      <button className={`${styles.tab} ${active === 'bpjs' ? styles.tabActive : ''}`} onClick={() => onChange('bpjs')}>
        BPJS <span className={styles.tabCount}>{counts.bpjs || 0}</span>
      </button>
      <button className={`${styles.tab} ${active === 'swing' ? styles.tabActive : ''}`} onClick={() => onChange('swing')}>
        Swing <span className={styles.tabCount}>{counts.swing || 0}</span>
      </button>
    </div>
  )
}

function IhsgBanner({ ctx }) {
  if (!ctx) return null
  return (
    <div className={styles.ihsgBanner}>
      <div className={styles.ihsgItem}>
        <span className={styles.ihsgLbl}>IHSG</span>
        <span className={styles.ihsgVal}>{formatNum(ctx.ihsg_close)}</span>
      </div>
      <div className={styles.ihsgItem}>
        <span className={styles.ihsgLbl}>Perubahan</span>
        <ChangeBadge val={ctx.ihsg_change_pct} />
      </div>
      <div className={styles.ihsgItem}>
        <span className={styles.ihsgLbl}>Trend</span>
        <span className={`${styles.ihsgVal} ${ctx.ihsg_trend === 'BULLISH' ? styles.up : ctx.ihsg_trend === 'BEARISH' ? styles.down : ''}`}>
          {ctx.ihsg_trend}
        </span>
      </div>
      <div className={styles.ihsgItem}>
        <span className={styles.ihsgLbl}>Above MA50</span>
        <span className={ctx.ihsg_above_ma50 ? styles.up : styles.down}>
          {ctx.ihsg_above_ma50 ? 'Ya' : 'Tidak'}
        </span>
      </div>
    </div>
  )
}

export default function Home({ initialData, loadError }) {
  const { theme, toggleTheme } = useContext(ThemeContext)
  const [data, setData] = useState(initialData)
  const [error, setError] = useState(loadError || null)
  const [loading, setLoading] = useState(false)
  const [lastRefresh, setLastRefresh] = useState(null)
  const [activeTab, setActiveTab] = useState('intraday')

  const [intradaySearch, setIntradaySearch] = useState('')
  const [intradaySortKey, setIntradaySortKey] = useState('rank')
  const [intradayFilterTier, setIntradayFilterTier] = useState('')

  const [araSearch, setAraSearch] = useState('')
  const [araFilterPattern, setAraFilterPattern] = useState('')

  const fetchData = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetch('/api/combined')
      if (!res.ok) {
        const e = await res.json().catch(() => ({}))
        throw new Error(e.message || `HTTP ${res.status}`)
      }
      const json = await res.json()
      setData(json)
      setLastRefresh(new Date().toLocaleTimeString('id-ID'))
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    if (!initialData) fetchData()
    const iv = setInterval(fetchData, 15 * 60 * 1000)
    return () => clearInterval(iv)
  }, [fetchData, initialData])

  const intradayStocks = useMemo(() => {
    const raw = data?.logika_lama_intraday ?? []
    let list = [...raw]
    if (intradaySearch) {
      const q = intradaySearch.toLowerCase()
      list = list.filter(s => s.ticker?.toLowerCase().includes(q) || s.company?.toLowerCase().includes(q))
    }
    if (intradayFilterTier) {
      list = list.filter(s => s.scoring?.tier === intradayFilterTier)
    }
    switch (intradaySortKey) {
      case 'score_desc': list.sort((a, b) => (b.scoring?.confidence_score ?? 0) - (a.scoring?.confidence_score ?? 0)); break
      case 'change_desc': list.sort((a, b) => (b.market_data?.change_pct ?? 0) - (a.market_data?.change_pct ?? 0)); break
      case 'value_desc': list.sort((a, b) => (b.market_data?.value_today ?? 0) - (a.market_data?.value_today ?? 0)); break
      case 'freq_desc': list.sort((a, b) => (b.market_data?.frequency_today ?? 0) - (a.market_data?.frequency_today ?? 0)); break
      case 'rr_desc': {
        const rrNum = s => parseFloat((s.entry_plan?.rr_ratio ?? '0').replace('1:', '')) || 0
        list.sort((a, b) => rrNum(b) - rrNum(a))
        break
      }
      default: list.sort((a, b) => (a.rank ?? 99) - (b.rank ?? 99))
    }
    return list
  }, [data, intradaySearch, intradaySortKey, intradayFilterTier])

  const araStocks = useMemo(() => {
    const raw = data?.logika_baru_calon_ara ?? []
    let list = [...raw]
    if (araSearch) {
      const q = araSearch.toLowerCase()
      list = list.filter(s => s.ticker?.toLowerCase().includes(q) || s.company?.toLowerCase().includes(q))
    }
    if (araFilterPattern) {
      list = list.filter(s => s.pattern_type === araFilterPattern)
    }
    return list
  }, [data, araSearch, araFilterPattern])

  const bsjpStocks = data?.bsjp_beli_sore_jual_pagi ?? []
  const bpjsStocks = data?.bpjs_beli_pagi_jual_sore ?? []
  const swingStocks = data?.swing_trading ?? []

  const meta = data?.meta || {}
  const ctx = data?.market_context || null
  const summary = data?.screening_summary || null

  return (
    <>
      <Head>
        <title>Screener IDX — {meta.date || 'Dashboard'}</title>
        <meta name="description" content="Stock screener Indonesia: intraday SMC + calon ARA detector" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.ico" />
      </Head>

      <div className={styles.shell}>
        <header className={styles.header}>
          <div className={styles.headerBrand}>
            <div className={styles.logoBox}>IDX</div>
            <div>
              <h1 className={styles.siteTitle}>Screener IDX</h1>
              <p className={styles.siteSub}>Multi-Pipeline Trading Intelligence</p>
            </div>
          </div>
          <div className={styles.headerActions}>
            {lastRefresh && <span className={styles.refreshTime}>{lastRefresh}</span>}
            <button className={styles.themeToggle} onClick={toggleTheme} title="Toggle Theme">
              {theme === 'dark' ? '☀️' : '🌙'}
            </button>
            <button className={styles.btn} onClick={fetchData} disabled={loading}>
              {loading ? '⏳' : '↺'} Refresh
            </button>
          </div>
        </header>

        {ctx && <IhsgBanner ctx={ctx} />}

        {ctx?.warning && (
          <div className={styles.alertBanner}>{ctx.warning}</div>
        )}
        {meta.mode_warning && (
          <div className={styles.alertBanner}>{meta.mode_warning}</div>
        )}

        <div className={styles.metaRow}>
          <span className={styles.metaChip}>{meta.mode || '-'}</span>
          <span className={styles.metaChip}>{meta.session_label || '-'}</span>
          <span className={styles.metaChip}>{meta.generated_at || '-'}</span>
          <span className={`${styles.metaChip} ${meta.status === 'success' ? styles.metaSuccess : styles.metaWarn}`}>
            {meta.status || 'unknown'}
          </span>
        </div>

        <SummaryFunnel summary={summary} />

        {loading && !data && (
          <div className={styles.stateBox}>
            <div className={styles.spinner} />
            <p>Memuat data screening...</p>
          </div>
        )}

        {error && !loading && (
          <div className={styles.errorBox}>
            <p className={styles.errorTitle}>Gagal memuat data</p>
            <p className={styles.errorMsg}>{error}</p>
            <button className={styles.btn} onClick={fetchData}>Coba Lagi</button>
          </div>
        )}

        {!loading && !error && data && (
          <>
            <TabBar
              active={activeTab}
              onChange={setActiveTab}
              counts={{ 
                intraday: data.logika_lama_intraday?.length ?? 0, 
                ara: data.logika_baru_calon_ara?.length ?? 0,
                bsjp: data.bsjp_beli_sore_jual_pagi?.length ?? 0,
                bpjs: data.bpjs_beli_pagi_jual_sore?.length ?? 0,
                swing: data.swing_trading?.length ?? 0
              }}
            />

            {activeTab === 'intraday' && (
              <>
                <FilterBar
                  search={intradaySearch}
                  onSearch={setIntradaySearch}
                  sortKey={intradaySortKey}
                  onSort={setIntradaySortKey}
                  filterTier={intradayFilterTier}
                  onFilterTier={setIntradayFilterTier}
                />
                {intradayStocks.length === 0 ? (
                  <div className={styles.stateBox}>
                    <p>Tidak ada saham yang memenuhi filter.</p>
                  </div>
                ) : (
                  <div className={styles.cardGrid}>
                    {intradayStocks.map((s, i) => (
                      <IntradayCard key={s.ticker} stock={s} rank={s.rank ?? (i + 1)} />
                    ))}
                  </div>
                )}
              </>
            )}

            {activeTab === 'ara' && (
              <>
                <div className={styles.araDisclaimer}>
                  {data.meta?.ara_disclaimer}
                </div>
                <AraFilterBar
                  search={araSearch}
                  onSearch={setAraSearch}
                  filterPattern={araFilterPattern}
                  onFilterPattern={setAraFilterPattern}
                />
                {araStocks.length === 0 ? (
                  <div className={styles.stateBox}>
                    <p>Tidak ada kandidat ARA yang memenuhi filter.</p>
                  </div>
                ) : (
                  <div className={styles.cardGrid}>
                    {araStocks.map((s) => (
                      <AraCard key={s.ticker} stock={s} />
                    ))}
                  </div>
                )}
              </>
            )}
            {activeTab === 'bsjp' && (
              <div className={styles.cardGrid}>
                {bsjpStocks.length > 0 ? bsjpStocks.map((s) => <BsjpCard key={s.ticker} stock={s} />) : <div className={styles.stateBox}><p>Tidak ada kandidat BSJP saat ini.</p></div>}
              </div>
            )}

            {activeTab === 'bpjs' && (
              <div className={styles.cardGrid}>
                {bpjsStocks.length > 0 ? bpjsStocks.map((s) => <BpjsCard key={s.ticker} stock={s} />) : <div className={styles.stateBox}><p>Tidak ada kandidat BPJS saat ini.</p></div>}
              </div>
            )}

            {activeTab === 'swing' && (
              <div className={styles.cardGrid}>
                {swingStocks.length > 0 ? swingStocks.map((s) => <SwingCard key={s.ticker} stock={s} />) : <div className={styles.stateBox}><p>Tidak ada setup Swing Stage 2 saat ini.</p></div>}
              </div>
            )}
          </>
        )}

        <footer className={styles.footer}>
          <p>
            <strong>Disclaimer:</strong> Bukan rekomendasi investasi. Lakukan riset mandiri sebelum trading.
            Risiko ditanggung masing-masing. Data: Stockbit API &amp; Yahoo Finance.
          </p>
        </footer>
      </div>
    </>
  )
}

export async function getStaticProps() {
  try {
    const fs = await import('fs')
    const path = await import('path')
    const filePath = path.join(process.cwd(), 'combined_screening.json')
    if (!fs.existsSync(filePath)) {
      return { props: { initialData: null, loadError: 'combined_screening.json belum tersedia.' }, revalidate: 60 }
    }
    const raw = fs.readFileSync(filePath, 'utf-8')
    const json = JSON.parse(raw)
    return {
      props: { initialData: json, loadError: null },
      revalidate: 300,
    }
  } catch (e) {
    return { props: { initialData: null, loadError: String(e.message) }, revalidate: 60 }
  }
}
