"""
Report Generator — exports backtest results as HTML/JSON reports.

Generates:
  - Interactive Plotly equity curve chart
  - HTML report with metrics table
  - JSON dump for programmatic use
"""

from __future__ import annotations

import html
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from ..utils.logger import get_logger
from ..utils.tz_utils import fmt_ts

if TYPE_CHECKING:
    from .backtest_engine import BacktestResult

logger = get_logger("report_generator")


class ReportGenerator:

    def __init__(self, output_dir: str = "backtest_results") -> None:
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, result: "BacktestResult") -> dict[str, str]:
        """Generate all report formats. Returns dict of {format: filepath}."""
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        slug = f"{result.strategy_name}_{result.symbol}_{result.timeframe}_{ts}"
        paths: dict[str, str] = {}

        paths["json"] = self._write_json(result, slug)
        paths["html"] = self._write_html(result, slug)

        try:
            paths["chart"] = self._write_chart(result, slug)
        except ImportError:
            logger.warning("plotly not installed — skipping chart generation")

        logger.info("Reports saved: %s", list(paths.values()))
        return paths

    def generate_multi(self, results: list["BacktestResult"]) -> str:
        """Generate a comparison HTML report for multiple backtest results."""
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        slug = f"comparison_{ts}"
        path = str(self._output_dir / f"{slug}.html")
        html = self._build_comparison_html(results)
        Path(path).write_text(html, encoding="utf-8")
        logger.info("Comparison report saved: %s", path)
        return path

    # ── Writers ───────────────────────────────────────────────────────────────

    def _write_json(self, result: "BacktestResult", slug: str) -> str:
        path = self._output_dir / f"{slug}.json"
        data = {
            "strategy": result.strategy_name,
            "symbol": result.symbol,
            "timeframe": result.timeframe,
            "initial_capital": result.initial_capital,
            "final_equity": result.final_equity,
            "min_balance": result.min_balance,
            "max_balance": result.max_balance,
            "balance_summary_usd": {
                "initial": result.initial_capital,
                "final": result.final_equity,
                "min_total": result.min_balance,
                "max_total": result.max_balance,
            },
            "total_return_pct": result.total_return_pct,
            "max_drawdown_pct": result.max_drawdown_pct,
            "max_drawdown_daily_pct": result.max_drawdown_daily_pct,
            "sharpe_ratio": result.sharpe_ratio,
            "profit_factor": result.profit_factor,
            "winrate": result.winrate,
            "total_trades": result.total_trades,
            "winning_trades": result.winning_trades,
            "losing_trades": result.losing_trades,
            "avg_win_pips": result.avg_win_pips,
            "avg_loss_pips": result.avg_loss_pips,
            "trades": result.trades,
        }
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        return str(path)

    def _write_html(self, result: "BacktestResult", slug: str) -> str:
        path = self._output_dir / f"{slug}.html"
        html = self._build_html(result)
        path.write_text(html, encoding="utf-8")
        return str(path)

    def _write_chart(self, result: "BacktestResult", slug: str) -> str:
        import plotly.graph_objects as go

        path = self._output_dir / f"{slug}_chart.html"
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            y=result.equity_curve,
            mode="lines",
            name="Equity Curve",
            line={"color": "#00d4aa", "width": 2},
        ))
        fig.add_hline(
            y=result.min_balance, line_dash="dot", line_color="#ff8c00",
            annotation_text="Min", annotation_position="bottom left",
        )
        fig.add_hline(
            y=result.max_balance, line_dash="dot", line_color="#6abf69",
            annotation_text="Max", annotation_position="top left",
        )

        win_bars = [t["bar_index"] for t in result.trades if t["result"] == "TP"]
        loss_bars = [t["bar_index"] for t in result.trades if t["result"] == "SL"]
        win_equity = [result.equity_curve[min(b + 1, len(result.equity_curve) - 1)] for b in win_bars]
        loss_equity = [result.equity_curve[min(b + 1, len(result.equity_curve) - 1)] for b in loss_bars]

        fig.add_trace(go.Scatter(
            x=win_bars, y=win_equity, mode="markers",
            name="Win", marker={"color": "#00d4aa", "symbol": "triangle-up", "size": 8},
        ))
        fig.add_trace(go.Scatter(
            x=loss_bars, y=loss_equity, mode="markers",
            name="Loss", marker={"color": "#ff4b4b", "symbol": "triangle-down", "size": 8},
        ))

        fig.update_layout(
            title=f"{result.strategy_name} | {result.symbol} {result.timeframe} — Equity Curve",
            xaxis_title="Bar Index",
            yaxis_title="Equity (USD)",
            template="plotly_dark",
            hovermode="x unified",
        )
        fig.write_html(str(path))
        return str(path)

    @staticmethod
    def _fmt_price(price: float, symbol: str) -> str:
        """Format price with appropriate decimal places."""
        sym = symbol.upper()
        if sym in ("XAUUSD", "XAGUSD"):
            return f"{price:.2f}"
        if "JPY" in sym:
            return f"{price:.3f}"
        return f"{price:.5f}"

    def _build_html(self, r: "BacktestResult") -> str:
        color = "#00d4aa" if r.total_return_pct >= 0 else "#ff4b4b"

        active_trades = [t for t in r.trades if t.get("result") != "EXPIRED"]
        expired_count = len(r.trades) - len(active_trades)

        total_profit = sum(t.get("pnl_usd", 0.0) for t in active_trades if t.get("pnl_usd", 0) > 0)
        total_loss   = sum(t.get("pnl_usd", 0.0) for t in active_trades if t.get("pnl_usd", 0) < 0)

        def _row(t: dict) -> str:
            result     = t.get("result", "")
            is_expired = result == "EXPIRED"
            res_color  = (
                "#00d4aa" if result == "TP" else
                "#66cc88" if result == "PARTIAL" else  # PARTIAL = partial close (light green)
                "#ffb347" if result == "BE" else        # BE = break-even (orange)
                "#ff4b4b" if result == "SL" else
                "#555555" if is_expired else "#aaaaaa"
            )
            row_style  = " style='opacity:0.45'" if is_expired else ""
            pnl        = t.get("pnl_usd", 0.0)
            pnl_color  = "#00d4aa" if pnl >= 0 else "#ff4b4b"
            pnl_str    = (f"{'+' if pnl >= 0 else ''}${pnl:.2f}"
                          if not is_expired else "—")
            bal_after  = t.get("balance_after", 0.0)
            bal_str    = f"${bal_after:,.2f}" if not is_expired else "—"
            fp         = lambda p: self._fmt_price(p, r.symbol) if p else "—"
            vol        = t.get("volume", 0.0)
            vol_str    = f"{vol:.2f}" if not is_expired else "—"
            sl_str     = fp(t["sl"])  if not is_expired else "—"
            tp_str     = fp(t["tp"])  if not is_expired else "—"
            ex_ts    = t.get("exit_timestamp")
            exit_str = fmt_ts(ex_ts) if ex_ts is not None else "—"
            otype      = t.get("order_type", "")
            otype_badge = (
                "<span style='font-size:0.75em;color:#8ab4f8;margin-left:3px'>[L]</span>"
                if otype == "LIMIT" else ""
            )
            raw_notes = (t.get("notes") or "").strip()
            if raw_notes:
                n_esc = html.escape(raw_notes)
                n_show = n_esc[:800] + ("…" if len(raw_notes) > 800 else "")
                title_safe = html.escape(
                    raw_notes.replace("\n", " ").replace("\r", " ")[:1200],
                    quote=True,
                )
                notes_cell = (
                    f"<td style='max-width:280px;font-size:0.78em;color:#9fb8d0;"
                    f"white-space:pre-wrap;word-break:break-word' title=\"{title_safe}\">"
                    f"{n_show}</td>"
                )
            else:
                notes_cell = "<td style='color:#555'>—</td>"
            return (
                f"<tr{row_style}>"
                f"<td>{fmt_ts(t['timestamp'])}</td>"
                f"<td><b>{t['action']}</b>{otype_badge}</td>"
                f"<td>{fp(t['entry'])}</td>"
                f"<td style='color:#ff8c00'>{sl_str}</td>"
                f"<td style='color:#00d4aa'>{tp_str}</td>"
                f"<td style='color:#8ab4f8'>{vol_str}</td>"
                f"<td style='color:{res_color}'><b>{result}</b></td>"
                f"<td style='color:#a8c7ff;font-size:0.92em'>{exit_str}</td>"
                f"<td style='color:{pnl_color}'><b>{pnl_str}</b></td>"
                f"<td style='color:#cccccc'>{bal_str}</td>"
                f"{notes_cell}"
                f"</tr>"
            )

        rows = "".join(_row(t) for t in r.trades[:500])
        expired_note = (
            f"<p style='color:#888;font-size:0.82em'>"
            f"⏳ {expired_count} limit orders expired unfilled (shown greyed out)</p>"
        ) if expired_count > 0 else ""

        return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Backtest — {r.strategy_name} {r.symbol} {r.timeframe}</title>
<style>
  body {{ font-family: 'Segoe UI', sans-serif; background: #0f1117; color: #e0e0e0; margin: 20px; }}
  h1 {{ color: #00d4aa; margin-bottom: 4px; }}
  h2 {{ color: #aaa; font-weight: normal; margin-top: 0; }}
  .metrics {{ display: flex; flex-wrap: wrap; gap: 16px; margin: 20px 0; }}
  .card {{ background: #1a1d2e; border-radius: 8px; padding: 16px 24px; min-width: 140px; }}
  .card .val {{ font-size: 1.6em; font-weight: bold; color: {color}; }}
  .card .lbl {{ font-size: 0.78em; color: #888; margin-top: 4px; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 0.85em; margin-top: 8px; }}
  th {{ background: #1a1d2e; padding: 8px 12px; text-align: left; color: #aaa; font-size: 0.8em; letter-spacing: 0.5px; }}
  td {{ padding: 6px 12px; border-bottom: 1px solid #1e2135; font-family: monospace; }}
  tr:hover td {{ background: #1e2135; }}
  .legend {{ font-size: 0.75em; color: #666; margin: 8px 0 16px; }}
  .balance-summary {{ font-size: 0.95em; color: #c8c8c8; margin: 12px 0 8px; padding: 10px 14px; background: #1a1d2e; border-radius: 8px; border-left: 3px solid #00d4aa; }}
</style>
</head>
<body>
<h1>Backtest Report</h1>
<h2>{r.strategy_name} &nbsp;|&nbsp; {r.symbol} / {r.timeframe}</h2>
<p class="balance-summary">
  <b>Total balance (USD)</b> — min:
  <span style="color:#ff8c00;font-weight:bold">${r.min_balance:,.2f}</span>
  &nbsp;|&nbsp; max:
  <span style="color:#6abf69;font-weight:bold">${r.max_balance:,.2f}</span>
  &nbsp;|&nbsp; final:
  <span style="color:#a8c7ff;font-weight:bold">${r.final_equity:,.2f}</span>
  &nbsp;|&nbsp; initial:
  <span style="color:#888">${r.initial_capital:,.2f}</span>
</p>

<div class="metrics">
  <div class="card"><div class="val">{r.total_return_pct:+.2f}%</div><div class="lbl">Total Return</div></div>
  <div class="card"><div class="val">{r.winrate:.1f}%</div><div class="lbl">Winrate ({r.winning_trades}W / {r.losing_trades}L)</div></div>
  <div class="card"><div class="val">{r.profit_factor:.2f}</div><div class="lbl">Profit Factor</div></div>
  <div class="card"><div class="val">{r.max_drawdown_pct:.2f}%</div><div class="lbl">Max drawdown (full period)</div></div>
  <div class="card"><div class="val" style="color:#ffb347">{r.max_drawdown_daily_pct:.2f}%</div><div class="lbl">Max drawdown (worst calendar day)</div></div>
  <div class="card"><div class="val">{r.sharpe_ratio:.3f}</div><div class="lbl">Sharpe Ratio</div></div>
  <div class="card"><div class="val">{r.avg_win_pips:.1f}</div><div class="lbl">Avg Win (pips)</div></div>
  <div class="card"><div class="val">{r.avg_loss_pips:.1f}</div><div class="lbl">Avg Loss (pips)</div></div>
  <div class="card"><div class="val">{r.total_trades}</div><div class="lbl">Total Trades</div></div>
  <div class="card"><div class="val">${r.initial_capital:,.0f} → ${r.final_equity:,.2f}</div><div class="lbl">Initial → Final equity</div></div>
  <div class="card"><div class="val" style="color:#ff8c00">${r.min_balance:,.2f}</div><div class="lbl">Min total balance (USD)</div></div>
  <div class="card"><div class="val" style="color:#00d4aa">${r.max_balance:,.2f}</div><div class="lbl">Max total balance (USD)</div></div>
  <div class="card"><div class="val" style="color:#00d4aa">+${total_profit:,.2f}</div><div class="lbl">Gross Profit</div></div>
  <div class="card"><div class="val" style="color:#ff4b4b">-${abs(total_loss):,.2f}</div><div class="lbl">Gross Loss</div></div>
</div>

<h3>Trade Log <span style="font-size:0.75em; color:#888; font-weight:normal">🕐 UTC+7 (Giờ Việt Nam)</span></h3>
<p class="legend">
  <span style="color:#ff4b4b">■</span> SL &nbsp;|&nbsp;
  <span style="color:#00d4aa">■</span> TP &nbsp;|&nbsp;
  <span style="color:#66cc88">■</span> PARTIAL = chốt lời từng phần (SL dời về gần BE) &nbsp;|&nbsp;
  <span style="color:#ffb347">■</span> BE = break-even (SL dời về entry) &nbsp;|&nbsp;
  <span style="color:#8ab4f8">■</span> Volume (lot) &nbsp;|&nbsp;
  <span style="color:#8ab4f8">[L]</span> = Limit order entry &nbsp;|&nbsp;
  P&amp;L and Balance in USD &nbsp;|&nbsp;
  <b>Exit</b> = thời điểm nến chạm TP/SL (hoặc hết hạn lệnh) &nbsp;|&nbsp;
  <b>Notes</b> = cùng nội dung <code>signal.notes</code> như Telegram &nbsp;|&nbsp;
  Data source: UTC+0 → displayed as UTC+7
</p>
{expired_note}
<table>
<tr>
  <th>Entry time (UTC+7)</th><th>Dir</th><th>Entry</th>
  <th>SL</th><th>TP</th><th>Vol(lot)</th>
  <th>Result</th><th>Exit TP/SL (UTC+7)</th><th>P&amp;L (USD)</th><th>Balance</th>
  <th>Notes (strategy)</th>
</tr>
{rows}
</table>
</body>
</html>"""

    def _build_comparison_html(self, results: list["BacktestResult"]) -> str:
        rows = "".join(
            f"<tr>"
            f"<td>{r.strategy_name}</td><td>{r.symbol}/{r.timeframe}</td>"
            f"<td style='color:{'#00d4aa' if r.total_return_pct>=0 else '#ff4b4b'}'>{r.total_return_pct:+.2f}%</td>"
            f"<td>${r.min_balance:,.2f}</td>"
            f"<td>${r.max_balance:,.2f}</td>"
            f"<td>{r.max_drawdown_pct:.2f}% / {r.max_drawdown_daily_pct:.2f}% d</td>"
            f"<td>{r.sharpe_ratio:.3f}</td>"
            f"<td>{r.profit_factor:.2f}</td>"
            f"<td>{r.winrate:.1f}%</td>"
            f"<td>{r.total_trades}</td>"
            f"</tr>"
            for r in sorted(results, key=lambda x: x.total_return_pct, reverse=True)
        )
        return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Strategy Comparison</title>
<style>
  body {{ font-family: 'Segoe UI', sans-serif; background: #0f1117; color: #e0e0e0; margin: 20px; }}
  h1 {{ color: #00d4aa; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th {{ background: #1a1d2e; padding: 10px 14px; }}
  td {{ padding: 8px 14px; border-bottom: 1px solid #2a2d3e; }}
</style>
</head>
<body>
<h1>Strategy Comparison Report</h1>
<table>
<tr><th>Strategy</th><th>Symbol/TF</th><th>Return</th><th>Min total $</th><th>Max total $</th><th>Max DD % / 1-day %</th><th>Sharpe</th><th>Profit Factor</th><th>Winrate</th><th>Trades</th></tr>
{rows}
</table>
</body>
</html>"""
