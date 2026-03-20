"""
Report Generator — exports backtest results as HTML + JSON + Plotly chart.

HTML trade log columns
-----------------------
  Entry Time (UTC+7) | Dir | Type | Entry | SL | TP | Vol | Result |
  Exit Time (UTC+7) | Exit Price | Pips | P&L (USD) | Balance
"""

from __future__ import annotations

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
        """Generate JSON, HTML, and optional Plotly chart. Returns paths dict."""
        ts   = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        slug = f"{result.strategy_name}_{result.symbol}_{result.timeframe}_{ts}"
        paths: dict[str, str] = {}

        paths["json"] = self._write_json(result, slug)
        paths["html"] = self._write_html(result, slug)
        try:
            paths["chart"] = self._write_chart(result, slug)
        except ImportError:
            logger.warning("plotly not installed — skipping chart")

        logger.info("Reports saved: %s", list(paths.values()))
        return paths

    def generate_multi(self, results: list["BacktestResult"]) -> str:
        ts   = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = str(self._output_dir / f"comparison_{ts}.html")
        Path(path).write_text(self._build_comparison_html(results), encoding="utf-8")
        return path

    # ── Writers ───────────────────────────────────────────────────────────────

    def _write_json(self, r: "BacktestResult", slug: str) -> str:
        path = self._output_dir / f"{slug}.json"
        data = {
            "strategy":       r.strategy_name,
            "symbol":         r.symbol,
            "timeframe":      r.timeframe,
            "initial_capital": r.initial_capital,
            "final_equity":   r.final_equity,
            "total_return_pct": r.total_return_pct,
            "max_drawdown_pct": r.max_drawdown_pct,
            "sharpe_ratio":   r.sharpe_ratio,
            "profit_factor":  r.profit_factor,
            "winrate":        r.winrate,
            "total_trades":   r.total_trades,
            "winning_trades": r.winning_trades,
            "losing_trades":  r.losing_trades,
            "avg_win_pips":   r.avg_win_pips,
            "avg_loss_pips":  r.avg_loss_pips,
            "gross_profit":   r.gross_profit,
            "gross_loss":     r.gross_loss,
            "trades":         r.trades,
        }
        path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
        return str(path)

    def _write_html(self, r: "BacktestResult", slug: str) -> str:
        path = self._output_dir / f"{slug}.html"
        path.write_text(self._build_html(r), encoding="utf-8")
        return str(path)

    def _write_chart(self, r: "BacktestResult", slug: str) -> str:
        import plotly.graph_objects as go
        path = self._output_dir / f"{slug}_chart.html"
        fig  = go.Figure()

        fig.add_trace(go.Scatter(
            y=r.equity_curve, mode="lines", name="Equity",
            line={"color": "#00d4aa", "width": 2},
        ))

        for label, color, sym_marker in (
            ("TP", "#00d4aa", "triangle-up"),
            ("SL", "#ff4b4b", "triangle-down"),
        ):
            idxs = [t["bar_index"] for t in r.trades if t["result"] == label]
            vals = [r.equity_curve[min(b + 1, len(r.equity_curve) - 1)] for b in idxs]
            tips = [
                f"{fmt_ts(r.trades[i]['timestamp'])}"
                f"<br>{r.trades[i]['action']} @ {r.trades[i]['entry']}"
                f"<br>P&L: {'+' if r.trades[i].get('pnl_usd',0)>=0 else ''}"
                f"${r.trades[i].get('pnl_usd',0):.2f}"
                f"<br>Bal: ${r.trades[i].get('balance_after',0):,.2f}"
                for i, t in enumerate(r.trades) if t["result"] == label
            ]
            fig.add_trace(go.Scatter(
                x=idxs, y=vals, mode="markers", name=label,
                marker={"color": color, "symbol": sym_marker, "size": 9},
                text=tips, hovertemplate="%{text}<extra></extra>",
            ))

        fig.update_layout(
            title=f"{r.strategy_name} | {r.symbol} {r.timeframe} — Equity Curve",
            xaxis_title="Bar Index",
            yaxis_title="Equity (USD)",
            template="plotly_dark",
            hovermode="x unified",
        )
        fig.write_html(str(path))
        return str(path)

    # ── HTML builders ─────────────────────────────────────────────────────────

    @staticmethod
    def _fmt_price(price: float, symbol: str) -> str:
        return f"{price:.2f}" if symbol.upper() in ("XAUUSD", "XAGUSD") else f"{price:.5f}"

    def _build_html(self, r: "BacktestResult") -> str:
        color    = "#00d4aa" if r.total_return_pct >= 0 else "#ff4b4b"
        is_gold  = r.symbol.upper() in ("XAUUSD", "XAGUSD")
        fp       = lambda p: self._fmt_price(p, r.symbol)

        wins   = [t for t in r.trades if t.get("result") == "TP"]
        losses = [t for t in r.trades if t.get("result") == "SL"]

        def _row(t: dict) -> str:
            res      = t.get("result", "?")
            res_col  = "#00d4aa" if res == "TP" else ("#ff4b4b" if res == "SL" else "#aaa")
            pnl      = t.get("pnl_usd", 0.0)
            pnl_col  = "#00d4aa" if pnl >= 0 else "#ff4b4b"
            pnl_str  = f"{'+'if pnl>=0 else ''}${pnl:.2f}"
            pips     = t.get("pips", 0.0)
            pip_col  = "#00d4aa" if pips >= 0 else "#ff4b4b"
            pip_str  = f"{'+'if pips>=0 else ''}{pips:.1f}"
            bal      = t.get("balance_after", 0.0)
            otype    = t.get("order_type", "MKT")
            otype_cl = "#8ab4f8" if otype == "LIMIT" else "#aaa"
            vol      = t.get("volume", 0.0)
            exit_ts  = t.get("exit_timestamp", "")
            return (
                f"<tr>"
                f"<td>{fmt_ts(t['timestamp'])}</td>"
                f"<td><b style='color:{'#00d4aa' if t['action']=='BUY' else '#ff4b4b'}'>{t['action']}</b></td>"
                f"<td><small style='color:{otype_cl}'>{otype}</small></td>"
                f"<td>{fp(t['entry'])}</td>"
                f"<td style='color:#ff8c00'>{fp(t['sl'])}</td>"
                f"<td style='color:#00d4aa'>{fp(t['tp'])}</td>"
                f"<td style='color:#8ab4f8'>{vol:.2f}</td>"
                f"<td style='color:{res_col}'><b>{res}</b></td>"
                f"<td>{fmt_ts(exit_ts)}</td>"
                f"<td>{fp(t.get('exit_price', 0))}</td>"
                f"<td style='color:{pip_col}'>{pip_str}</td>"
                f"<td style='color:{pnl_col}'><b>{pnl_str}</b></td>"
                f"<td style='color:#cccccc'>${bal:,.2f}</td>"
                f"</tr>"
            )

        rows = "".join(_row(t) for t in r.trades[:500])
        now_vn = fmt_ts(datetime.now(tz=timezone.utc), "%Y-%m-%d %H:%M")

        return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Backtest — {r.strategy_name} {r.symbol}</title>
<style>
  body   {{ font-family: 'Segoe UI', sans-serif; background:#0f1117; color:#e0e0e0; margin:20px; }}
  h1     {{ color:#00d4aa; margin-bottom:4px; }}
  h2     {{ color:#aaa; margin-top:2px; font-weight:normal; font-size:1em; }}
  .meta  {{ color:#666; font-size:0.8em; margin-bottom:16px; }}
  .cards {{ display:flex; flex-wrap:wrap; gap:12px; margin-bottom:24px; }}
  .card  {{ background:#1a1d2e; border-radius:8px; padding:14px 20px; min-width:130px; }}
  .card .val {{ font-size:1.5em; font-weight:bold; }}
  .card .lbl {{ font-size:0.75em; color:#888; margin-top:2px; }}
  table  {{ border-collapse:collapse; width:100%; font-size:0.82em; }}
  th     {{ background:#1a1d2e; padding:8px 10px; text-align:left; white-space:nowrap; position:sticky; top:0; }}
  td     {{ padding:5px 10px; border-bottom:1px solid #2a2d3e; white-space:nowrap; }}
  tr:hover {{ background:#1e2135; }}
  .green {{ color:#00d4aa; }}
  .red   {{ color:#ff4b4b; }}
</style>
</head>
<body>
<h1>📊 Backtest Report</h1>
<h2>{r.strategy_name} &nbsp;|&nbsp; {r.symbol} / {r.timeframe}</h2>
<p class="meta">Generated {now_vn} UTC+7 &nbsp;·&nbsp;
  {r.total_trades} trades &nbsp;·&nbsp;
  Data timestamps: UTC+0 → displayed as UTC+7</p>

<div class="cards">
  <div class="card">
    <div class="val" style="color:{color}">{r.total_return_pct:+.2f}%</div>
    <div class="lbl">Total Return</div>
  </div>
  <div class="card">
    <div class="val">${r.final_equity:,.2f}</div>
    <div class="lbl">Final Equity (from ${r.initial_capital:,.2f})</div>
  </div>
  <div class="card">
    <div class="val" style="color:#ff8c00">{r.max_drawdown_pct:.2f}%</div>
    <div class="lbl">Max Drawdown</div>
  </div>
  <div class="card">
    <div class="val">{r.sharpe_ratio:.3f}</div>
    <div class="lbl">Sharpe Ratio</div>
  </div>
  <div class="card">
    <div class="val">{r.profit_factor:.2f}</div>
    <div class="lbl">Profit Factor</div>
  </div>
  <div class="card">
    <div class="val">{r.winrate:.1f}%</div>
    <div class="lbl">Winrate ({r.winning_trades}W / {r.losing_trades}L)</div>
  </div>
  <div class="card">
    <div class="val" style="color:#00d4aa">+${r.gross_profit:,.2f}</div>
    <div class="lbl">Gross Profit</div>
  </div>
  <div class="card">
    <div class="val" style="color:#ff4b4b">-${r.gross_loss:,.2f}</div>
    <div class="lbl">Gross Loss</div>
  </div>
  <div class="card">
    <div class="val">{r.avg_win_pips:.0f}p / {r.avg_loss_pips:.0f}p</div>
    <div class="lbl">Avg Win / Loss Pips</div>
  </div>
</div>

<h3>Trade Log</h3>
<table>
<tr>
  <th>Entry (UTC+7)</th>
  <th>Dir</th>
  <th>Type</th>
  <th>Entry</th>
  <th>SL</th>
  <th>TP</th>
  <th>Vol(L)</th>
  <th>Result</th>
  <th>Exit (UTC+7)</th>
  <th>Exit Price</th>
  <th>Pips</th>
  <th>P&amp;L (USD)</th>
  <th>Balance</th>
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
            f"<td>{r.max_drawdown_pct:.2f}%</td>"
            f"<td>{r.sharpe_ratio:.3f}</td>"
            f"<td>{r.profit_factor:.2f}</td>"
            f"<td>{r.winrate:.1f}%</td>"
            f"<td>{r.total_trades}</td>"
            f"<td style='color:#00d4aa'>+${r.gross_profit:,.2f}</td>"
            f"<td style='color:#ff4b4b'>-${r.gross_loss:,.2f}</td>"
            f"</tr>"
            for r in sorted(results, key=lambda x: x.total_return_pct, reverse=True)
        )
        return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Strategy Comparison</title>
<style>
  body  {{ font-family:'Segoe UI',sans-serif; background:#0f1117; color:#e0e0e0; margin:20px; }}
  h1    {{ color:#00d4aa; }}
  table {{ border-collapse:collapse; width:100%; }}
  th    {{ background:#1a1d2e; padding:10px 14px; }}
  td    {{ padding:8px 14px; border-bottom:1px solid #2a2d3e; }}
</style>
</head>
<body>
<h1>Strategy Comparison</h1>
<table>
<tr>
  <th>Strategy</th><th>Symbol/TF</th><th>Return</th>
  <th>Max DD</th><th>Sharpe</th><th>PF</th>
  <th>Winrate</th><th>Trades</th>
  <th>Gross+</th><th>Gross-</th>
</tr>
{rows}
</table>
</body>
</html>"""
