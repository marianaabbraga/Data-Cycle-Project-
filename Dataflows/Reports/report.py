"""
report.py
=========
Stock Market Portfolio Report — with role-based access control.

Reads data from the Silver parquet layer and generates a self-contained
HTML report. What the user sees depends on their role:

  basic    (free)  — Portfolio summary, closing prices, normalised
                     performance, volume, volatility, price correlation
  advanced (paid)  — All of the above + indicator correlation heatmap +
                     per-ticker panels (SMA/Bollinger/RSI/MACD)

Users are managed by access_control.py (users.json alongside this file).
Default accounts created on first run:

  Username      Password        Role
  ----------    ----------      --------
  analyst       analyst123      advanced
  admin         admin123        advanced
  basic_user    basic123        basic
  viewer        viewer123       basic

Usage
-----
    # Run interactively (will prompt for username + password)
    python report.py

    # Pass credentials directly (useful for scripting)
    python report.py --user analyst --password analyst123

    # Custom Silver path and output location
    python report.py --user analyst --password analyst123 \
        --silver ../../DataLake/Silver \
        --output ../../Reports/portfolio.html
"""

import argparse
import getpass
import os
import sys
from glob import glob
from datetime import datetime
from io import BytesIO
import base64

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.dates as mdates
import seaborn as sns
from matplotlib.ticker import FuncFormatter

# access_control.py must be in the same folder as this script
sys.path.insert(0, os.path.dirname(__file__))
from access_control import UserManager, User

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_SILVER = os.path.join(os.path.dirname(__file__), "..", "..", "DataLake", "Silver")
DEFAULT_OUTPUT = os.path.join(os.path.dirname(__file__), "stock_report.html")

TICKER_COLORS = {
    "AAPL": "#0071e3",
    "MSFT": "#00a4ef",
    "NVDA": "#76b900",
    "SPY":  "#ff6900",
    "QQQ":  "#c50f1f",
}
FALLBACK_COLORS = plt.cm.tab10.colors

STYLE = {
    "bg":      "#0d1117",
    "card":    "#161b22",
    "border":  "#30363d",
    "text":    "#e6edf3",
    "subtext": "#8b949e",
    "accent":  "#58a6ff",
    "green":   "#3fb950",
    "red":     "#f85149",
}


# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════════════════════

def load_silver(silver_dir: str, table: str) -> pd.DataFrame:
    pattern = os.path.join(silver_dir, table, "**", "*.parquet")
    files   = glob(pattern, recursive=True)
    if not files:
        print(f"  [WARN] No files found for '{table}' in {silver_dir}")
        return pd.DataFrame()
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    print(f"  Loaded '{table}': {len(df):,} rows from {len(files)} file(s)")
    return df


def prepare_prices(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["ticker", "date"]).reset_index(drop=True)


def prepare_technical(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values(["ticker", "date"]).reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
# FIGURE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def fig_to_b64(fig) -> str:
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor=STYLE["bg"])
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()


def ticker_color(ticker: str, idx: int) -> str:
    return TICKER_COLORS.get(ticker, FALLBACK_COLORS[idx % len(FALLBACK_COLORS)])


def style_ax(ax, title="", ylabel=""):
    ax.set_facecolor(STYLE["card"])
    ax.tick_params(colors=STYLE["subtext"], labelsize=8)
    for spine in ax.spines.values():
        spine.set_edgecolor(STYLE["border"])
    if title:
        ax.set_title(title, color=STYLE["text"], fontsize=10, pad=8, fontweight="bold")
    if ylabel:
        ax.set_ylabel(ylabel, color=STYLE["subtext"], fontsize=8)
    ax.grid(True, color=STYLE["border"], linewidth=0.5, alpha=0.7)


def format_large(x, _):
    if abs(x) >= 1e9: return f"{x/1e9:.1f}B"
    if abs(x) >= 1e6: return f"{x/1e6:.0f}M"
    if abs(x) >= 1e3: return f"{x/1e3:.0f}K"
    return f"{x:.0f}"


# ══════════════════════════════════════════════════════════════════════════════
# CHARTS — available to ALL users
# ══════════════════════════════════════════════════════════════════════════════

def plot_price_timeseries(prices: pd.DataFrame) -> str:
    tickers = sorted(prices["ticker"].unique())
    fig, ax = plt.subplots(figsize=(12, 4.5))
    fig.patch.set_facecolor(STYLE["bg"])
    style_ax(ax, title="Closing Price — All Tickers", ylabel="Price (USD)")
    for i, t in enumerate(tickers):
        sub = prices[prices["ticker"] == t]
        ax.plot(sub["date"], sub["close"], label=t,
                color=ticker_color(t, i), linewidth=1.4, alpha=0.9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.legend(facecolor=STYLE["card"], edgecolor=STYLE["border"],
              labelcolor=STYLE["text"], fontsize=8)
    fig.tight_layout()
    return fig_to_b64(fig)


def plot_normalized_performance(prices: pd.DataFrame) -> str:
    tickers = sorted(prices["ticker"].unique())
    fig, ax = plt.subplots(figsize=(12, 4))
    fig.patch.set_facecolor(STYLE["bg"])
    style_ax(ax, title="Normalised Performance (Base = 100)", ylabel="Index")
    for i, t in enumerate(tickers):
        sub = prices[prices["ticker"] == t].set_index("date").sort_index()
        if sub.empty:
            continue
        indexed = sub["close"] / sub["close"].iloc[0] * 100
        ax.plot(indexed.index, indexed, label=t,
                color=ticker_color(t, i), linewidth=1.4)
    ax.axhline(100, color=STYLE["border"], linestyle="--", linewidth=0.7)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.legend(facecolor=STYLE["card"], edgecolor=STYLE["border"],
              labelcolor=STYLE["text"], fontsize=8)
    fig.tight_layout()
    return fig_to_b64(fig)


def plot_volume(prices: pd.DataFrame) -> str:
    df = prices.copy()
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
    monthly = df.groupby(["month", "ticker"])["volume"].mean().reset_index()
    pivot   = monthly.pivot(index="month", columns="ticker", values="volume").fillna(0)
    tickers = pivot.columns.tolist()
    fig, ax = plt.subplots(figsize=(12, 3.5))
    fig.patch.set_facecolor(STYLE["bg"])
    style_ax(ax, title="Monthly Average Volume", ylabel="Avg Volume")
    ax.yaxis.set_major_formatter(FuncFormatter(format_large))
    bottom = np.zeros(len(pivot))
    for i, t in enumerate(tickers):
        ax.bar(pivot.index, pivot[t], bottom=bottom,
               label=t, color=ticker_color(t, i), alpha=0.85, width=20)
        bottom += pivot[t].values
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.legend(facecolor=STYLE["card"], edgecolor=STYLE["border"],
              labelcolor=STYLE["text"], fontsize=8)
    fig.tight_layout()
    return fig_to_b64(fig)


def plot_volatility(prices: pd.DataFrame) -> str:
    tickers = sorted(prices["ticker"].unique())
    fig, ax = plt.subplots(figsize=(12, 4))
    fig.patch.set_facecolor(STYLE["bg"])
    style_ax(ax, title="Rolling 30-Day Annualised Volatility", ylabel="Volatility")
    for i, t in enumerate(tickers):
        sub = prices[prices["ticker"] == t].set_index("date").sort_index()
        vol = sub["close"].pct_change().rolling(30).std() * np.sqrt(252)
        ax.plot(vol.index, vol, label=t,
                color=ticker_color(t, i), linewidth=1.2, alpha=0.85)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:.0%}"))
    ax.legend(facecolor=STYLE["card"], edgecolor=STYLE["border"],
              labelcolor=STYLE["text"], fontsize=8)
    fig.tight_layout()
    return fig_to_b64(fig)


def plot_price_correlation(prices: pd.DataFrame) -> str:
    pivot   = prices.pivot_table(index="date", columns="ticker", values="close")
    returns = pivot.pct_change().dropna()
    corr    = returns.corr()
    fig, ax = plt.subplots(figsize=(6, 5))
    fig.patch.set_facecolor(STYLE["bg"])
    ax.set_facecolor(STYLE["card"])
    mask = np.zeros_like(corr, dtype=bool)
    np.fill_diagonal(mask, True)
    sns.heatmap(corr, ax=ax, mask=mask,
                cmap=sns.diverging_palette(10, 130, as_cmap=True),
                center=0, vmin=-1, vmax=1,
                annot=True, fmt=".2f", annot_kws={"size": 9},
                linewidths=0.5, linecolor=STYLE["border"],
                cbar_kws={"shrink": 0.8})
    ax.set_title("Price Returns Correlation", color=STYLE["text"],
                 fontsize=11, fontweight="bold", pad=10)
    ax.tick_params(colors=STYLE["text"], labelsize=9)
    fig.tight_layout()
    return fig_to_b64(fig)


def build_summary(prices: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for ticker, sub in prices.groupby("ticker"):
        sub = sub.sort_values("date")
        ret = sub["close"].pct_change().dropna()
        rows.append({
            "Ticker":       ticker,
            "From":         sub["date"].min().strftime("%Y-%m-%d"),
            "To":           sub["date"].max().strftime("%Y-%m-%d"),
            "Days":         len(sub),
            "Start ($)":    f"{sub['close'].iloc[0]:.2f}",
            "End ($)":      f"{sub['close'].iloc[-1]:.2f}",
            "Total Return": f"{(sub['close'].iloc[-1]/sub['close'].iloc[0]-1)*100:.1f}%",
            "Ann. Vol.":    f"{ret.std() * np.sqrt(252) * 100:.1f}%",
            "Max Drawdown": f"{((sub['close']/sub['close'].cummax())-1).min()*100:.1f}%",
            "Avg Volume":   f"{sub['volume'].mean():,.0f}",
        })
    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
# CHARTS — advanced users only
# ══════════════════════════════════════════════════════════════════════════════

def plot_indicator_correlation(tech: pd.DataFrame) -> str:
    if tech.empty:
        return ""
    cols = [c for c in ["sma_20", "sma_50", "rsi", "macd", "atr",
                         "bb_upper", "bb_lower"] if c in tech.columns]
    if len(cols) < 2:
        return ""
    corr = tech[cols].dropna().corr()
    fig, ax = plt.subplots(figsize=(7, 6))
    fig.patch.set_facecolor(STYLE["bg"])
    ax.set_facecolor(STYLE["card"])
    sns.heatmap(corr, ax=ax,
                cmap=sns.diverging_palette(10, 130, as_cmap=True),
                center=0, vmin=-1, vmax=1,
                annot=True, fmt=".2f", annot_kws={"size": 8},
                linewidths=0.5, linecolor=STYLE["border"],
                cbar_kws={"shrink": 0.8})
    ax.set_title("Technical Indicators Correlation", color=STYLE["text"],
                 fontsize=11, fontweight="bold", pad=10)
    ax.tick_params(colors=STYLE["text"], labelsize=8)
    fig.tight_layout()
    return fig_to_b64(fig)


def plot_ticker_indicators(prices: pd.DataFrame, tech: pd.DataFrame) -> list:
    """Per-ticker three-panel: Price+Bollinger+SMA / RSI / MACD."""
    images  = []
    tickers = sorted(prices["ticker"].unique())

    for ticker in tickers:
        p     = prices[prices["ticker"] == ticker].set_index("date")
        t     = tech[tech["ticker"] == ticker].set_index("date") if not tech.empty else pd.DataFrame()
        color = ticker_color(ticker, tickers.index(ticker))

        fig = plt.figure(figsize=(12, 8))
        fig.patch.set_facecolor(STYLE["bg"])
        gs  = gridspec.GridSpec(3, 1, height_ratios=[3, 1.2, 1.2])

        # ── Price + SMA + Bollinger ───────────────────────────────────────────
        ax0 = fig.add_subplot(gs[0])
        style_ax(ax0, title=f"{ticker} — Price & Indicators", ylabel="Price (USD)")
        ax0.tick_params(labelbottom=False)
        ax0.plot(p.index, p["close"], color=color, linewidth=1.3, label="Close", zorder=5)

        if not t.empty:
            if "sma_20" in t.columns:
                ax0.plot(t.index, t["sma_20"], color="#ffa500", linewidth=0.9,
                         linestyle="--", label="SMA 20", alpha=0.8)
            if "sma_50" in t.columns:
                ax0.plot(t.index, t["sma_50"], color="#ff69b4", linewidth=0.9,
                         linestyle="--", label="SMA 50", alpha=0.8)
            if "bb_upper" in t.columns and "bb_lower" in t.columns:
                ax0.fill_between(t.index, t["bb_lower"], t["bb_upper"],
                                 alpha=0.12, color=color, label="Bollinger Bands")
                ax0.plot(t.index, t["bb_upper"], color=color, linewidth=0.6, alpha=0.5)
                ax0.plot(t.index, t["bb_lower"], color=color, linewidth=0.6, alpha=0.5)

        ax0.legend(facecolor=STYLE["card"], edgecolor=STYLE["border"],
                   labelcolor=STYLE["text"], fontsize=7, loc="upper left")

        # ── RSI ───────────────────────────────────────────────────────────────
        ax1 = fig.add_subplot(gs[1], sharex=ax0)
        style_ax(ax1, ylabel="RSI")
        ax1.tick_params(labelbottom=False)
        ax1.set_ylim(0, 100)

        if not t.empty and "rsi" in t.columns:
            ax1.plot(t.index, t["rsi"], color=STYLE["accent"], linewidth=1.0)
            ax1.axhline(70, color=STYLE["red"],   linestyle="--", linewidth=0.8, alpha=0.8)
            ax1.axhline(30, color=STYLE["green"], linestyle="--", linewidth=0.8, alpha=0.8)
            ax1.fill_between(t.index, t["rsi"], 70,
                             where=(t["rsi"] >= 70), alpha=0.2, color=STYLE["red"])
            ax1.fill_between(t.index, t["rsi"], 30,
                             where=(t["rsi"] <= 30), alpha=0.2, color=STYLE["green"])
            ax1.text(t.index[-1], 72, "Overbought", color=STYLE["red"],
                     fontsize=7, ha="right")
            ax1.text(t.index[-1], 28, "Oversold", color=STYLE["green"],
                     fontsize=7, ha="right", va="top")

        # ── MACD ──────────────────────────────────────────────────────────────
        ax2 = fig.add_subplot(gs[2], sharex=ax0)
        style_ax(ax2, ylabel="MACD")
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        ax2.xaxis.set_major_locator(mdates.YearLocator())

        if not t.empty and "macd" in t.columns:
            clrs = [STYLE["green"] if v >= 0 else STYLE["red"] for v in t["macd"].values]
            ax2.bar(t.index, t["macd"], color=clrs, width=3, alpha=0.8)
            ax2.axhline(0, color=STYLE["subtext"], linewidth=0.7)

        fig.subplots_adjust(left=0.07, right=0.97, top=0.95, bottom=0.06, hspace=0.08)
        images.append((ticker, fig_to_b64(fig)))

    return images


# ══════════════════════════════════════════════════════════════════════════════
# HTML ASSEMBLY
# ══════════════════════════════════════════════════════════════════════════════

def _img(b64: str, alt: str = "") -> str:
    return f'<img src="data:image/png;base64,{b64}" alt="{alt}" class="chart-img">'


def _table(df: pd.DataFrame) -> str:
    headers = "".join(f"<th>{c}</th>" for c in df.columns)
    rows    = "".join(
        f"<tr>{''.join(f'<td>{v}</td>' for v in row.values)}</tr>"
        for _, row in df.iterrows()
    )
    return (f'<table class="summary-table">'
            f'<thead><tr>{headers}</tr></thead>'
            f'<tbody>{rows}</tbody></table>')


def _locked_section(title: str) -> str:
    return f"""
    <div class="section locked-section">
      <h2>{title}</h2>
      <div class="locked-box">
        <span class="lock-icon">🔒</span>
        <p>This section is available to <strong>Advanced (Paid)</strong> users only.</p>
        <p class="lock-sub">Upgrade your account to unlock technical indicator charts,
        indicator correlation heatmaps, and per-ticker SMA / RSI / MACD panels.</p>
      </div>
    </div>"""


def build_html(
    user: User,
    summary_df: pd.DataFrame,
    img_price: str,
    img_norm: str,
    img_volume: str,
    img_volat: str,
    img_corr_price: str,
    img_corr_ind: str,
    ticker_imgs: list,
    generated_at: str,
) -> str:

    is_advanced = user.can("technical_indicators")
    badge_bg    = "#1f6feb" if is_advanced else "#30363d"

    # ── advanced sections ─────────────────────────────────────────────────────
    if is_advanced:
        corr_ind_block = f"""
        <div class="section">
          <h2>Technical Indicators Correlation</h2>
          <p class="section-desc">
            Pearson correlation between all computed indicators.
            SMA20 and SMA50 correlate strongly (both track price trend);
            RSI and MACD independence confirms they measure different market dynamics.
          </p>
          <div class="chart-center">{_img(img_corr_ind, "Indicator Correlation")}</div>
        </div>""" if img_corr_ind else ""

        ticker_blocks = "".join(f"""
        <div class="section">
          <h2>{ticker} — Price &amp; SMA / Bollinger Bands / RSI / MACD</h2>
          {_img(b64, ticker)}
        </div>""" for ticker, b64 in ticker_imgs)

        advanced_content = corr_ind_block + ticker_blocks
        nav_extra = '<a href="#indicators">Indicators</a>'
        indicators_anchor = '<div id="indicators"></div>'
    else:
        advanced_content  = _locked_section("Technical Indicators")
        nav_extra         = ""
        indicators_anchor = ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Stock Market Portfolio Report</title>
<style>
  :root {{
    --bg:      {STYLE["bg"]};
    --card:    {STYLE["card"]};
    --border:  {STYLE["border"]};
    --text:    {STYLE["text"]};
    --subtext: {STYLE["subtext"]};
    --accent:  {STYLE["accent"]};
    --green:   {STYLE["green"]};
    --red:     {STYLE["red"]};
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text);
          font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
          padding-bottom: 60px; }}

  header {{ background: var(--card); border-bottom: 1px solid var(--border); padding: 24px 40px; }}
  header h1 {{ font-size: 1.5rem; font-weight: 700; }}
  header .meta {{ color: var(--subtext); font-size: 0.82rem; margin-top: 6px; }}
  .role-badge {{
    display: inline-block;
    background: {badge_bg}; color: #fff;
    font-size: 0.72rem; font-weight: 700;
    padding: 2px 10px; border-radius: 20px; margin-left: 10px;
    vertical-align: middle;
  }}

  nav {{ background: var(--card); border-bottom: 1px solid var(--border);
         padding: 0 40px; display: flex; gap: 4px; overflow-x: auto; }}
  nav a {{ color: var(--subtext); text-decoration: none; font-size: 0.82rem;
           padding: 12px 14px; border-bottom: 2px solid transparent; white-space: nowrap; }}
  nav a:hover {{ color: var(--accent); border-bottom-color: var(--accent); }}

  main {{ max-width: 1280px; margin: 0 auto; padding: 32px 40px; }}
  .section {{ background: var(--card); border: 1px solid var(--border);
              border-radius: 8px; padding: 24px; margin-bottom: 28px; }}
  .section h2 {{ font-size: 1.05rem; font-weight: 600; margin-bottom: 12px; }}
  .section-desc {{ color: var(--subtext); font-size: 0.83rem; margin-bottom: 16px; line-height: 1.55; }}
  .chart-img {{ width: 100%; border-radius: 4px; display: block; }}
  .chart-center {{ display: flex; justify-content: center; }}
  .chart-center .chart-img {{ width: auto; max-width: 100%; }}
  .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 28px; }}
  @media (max-width: 860px) {{ .two-col {{ grid-template-columns: 1fr; }} }}

  .summary-table {{ width: 100%; border-collapse: collapse; font-size: 0.82rem; }}
  .summary-table th {{ background: var(--bg); color: var(--accent); font-weight: 600;
                       text-align: left; padding: 8px 12px;
                       border-bottom: 2px solid var(--border); }}
  .summary-table td {{ padding: 8px 12px; border-bottom: 1px solid var(--border); }}
  .summary-table tr:hover td {{ background: rgba(88,166,255,0.04); }}

  .locked-section {{ border-style: dashed; }}
  .locked-box {{ text-align: center; padding: 36px 16px; }}
  .lock-icon {{ font-size: 2.4rem; display: block; margin-bottom: 12px; }}
  .locked-box p {{ color: var(--subtext); font-size: 0.9rem; line-height: 1.6; }}
  .locked-box strong {{ color: var(--accent); }}
  .lock-sub {{ margin-top: 8px; font-size: 0.82rem; }}

  footer {{ text-align: center; color: var(--subtext); font-size: 0.78rem; margin-top: 48px; }}
</style>
</head>
<body>

<header>
  <h1>📈 Stock Market Portfolio Report
    <span class="role-badge">{user.label()}</span>
  </h1>
  <div class="meta">
    User: <strong>{user.full_name}</strong>
    &nbsp;·&nbsp; Generated: {generated_at}
    &nbsp;·&nbsp; Source: Silver Layer (Parquet)
  </div>
</header>

<nav>
  <a href="#summary">Summary</a>
  <a href="#prices">Prices</a>
  <a href="#performance">Performance</a>
  <a href="#volume">Volume</a>
  <a href="#volatility">Volatility</a>
  <a href="#correlations">Correlations</a>
  {nav_extra}
</nav>

<main>

  <div class="section" id="summary">
    <h2>Portfolio Summary</h2>
    <p class="section-desc">Key statistics for each ticker computed from the full Silver price history.</p>
    {_table(summary_df)}
  </div>

  <div class="section" id="prices">
    <h2>Closing Price History</h2>
    <p class="section-desc">Daily closing prices for all tickers.</p>
    {_img(img_price, "Closing Prices")}
  </div>

  <div class="section" id="performance">
    <h2>Normalised Performance</h2>
    <p class="section-desc">
      All tickers rebased to 100 at their first available date, making growth
      rates directly comparable regardless of absolute price levels.
    </p>
    {_img(img_norm, "Normalised Performance")}
  </div>

  <div class="section" id="volume">
    <h2>Monthly Average Volume</h2>
    <p class="section-desc">Average daily trading volume per month, stacked by ticker.</p>
    {_img(img_volume, "Volume")}
  </div>

  <div class="section" id="volatility">
    <h2>Rolling 30-Day Volatility</h2>
    <p class="section-desc">
      Annualised volatility (30-day rolling std of daily returns × √252).
      Higher values indicate periods of greater market uncertainty.
    </p>
    {_img(img_volat, "Volatility")}
  </div>

  <div class="section" id="correlations">
    <h2>Price Returns Correlation</h2>
    <p class="section-desc">
      Pearson correlation of daily percentage returns between all tickers.
      Values near 1 mean the stocks move together; values near 0 indicate independence.
    </p>
    {_img(img_corr_price, "Price Correlation")}
  </div>

  {indicators_anchor}
  {advanced_content}

</main>

<footer>
  Stock Market Portfolio Report &nbsp;·&nbsp;
  Data sourced from Silver Layer (Yahoo Finance via yfinance)
</footer>
</body>
</html>"""


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Stock Market Portfolio Report")
    parser.add_argument("--user",     default=None, help="Username (prompted if omitted)")
    parser.add_argument("--password", default=None, help="Password (prompted if omitted)")
    parser.add_argument("--silver",   default=DEFAULT_SILVER, help="Path to DataLake/Silver")
    parser.add_argument("--output",   default=DEFAULT_OUTPUT, help="Output HTML path")
    args = parser.parse_args()

    print("=" * 60)
    print("Stock Market Portfolio Report")
    print("=" * 60)

    # ── 1. Authenticate ───────────────────────────────────────────────────────
    mgr      = UserManager()
    username = args.user     or input("\nUsername: ").strip()
    password = args.password or getpass.getpass("Password: ")

    try:
        user = mgr.authenticate(username, password)
        print(f"\n[Auth] Welcome, {user.full_name}  ({user.label()})")
    except PermissionError as e:
        print(f"\n[Auth] {e}")
        sys.exit(1)

    # ── 2. Load data ──────────────────────────────────────────────────────────
    print("\nLoading Silver data...")
    prices_raw = load_silver(args.silver, "price_history")
    tech_raw   = load_silver(args.silver, "technical_indicators")

    if prices_raw.empty:
        print("\n❌  No price data found. Run the Silver pipeline first.")
        sys.exit(1)

    prices = prepare_prices(prices_raw)
    tech   = prepare_technical(tech_raw) if not tech_raw.empty else pd.DataFrame()
    print(f"  Tickers: {sorted(prices['ticker'].unique())}")

    # ── 3. Charts — all users ─────────────────────────────────────────────────
    print("\nGenerating charts...")
    plt.rcParams.update({
        "font.family":      "DejaVu Sans",
        "font.size":        9,
        "figure.facecolor": STYLE["bg"],
        "axes.facecolor":   STYLE["card"],
        "text.color":       STYLE["text"],
    })

    img_price      = plot_price_timeseries(prices);      print("  ✓ Closing price")
    img_norm       = plot_normalized_performance(prices); print("  ✓ Normalised performance")
    img_volume     = plot_volume(prices);                 print("  ✓ Volume")
    img_volat      = plot_volatility(prices);             print("  ✓ Volatility")
    img_corr_price = plot_price_correlation(prices);      print("  ✓ Price correlation heatmap")
    summary_df     = build_summary(prices)

    # ── 4. Charts — advanced users only ──────────────────────────────────────
    img_corr_ind = ""
    ticker_imgs  = []

    if user.can("technical_indicators"):
        img_corr_ind = plot_indicator_correlation(tech)
        print("  ✓ Indicator correlation heatmap")
        ticker_imgs = plot_ticker_indicators(prices, tech)
        print(f"  ✓ Per-ticker indicator panels ({len(ticker_imgs)} tickers)")
    else:
        print("  ⊘ Indicator charts skipped  (Basic role — locked in report)")

    # ── 5. Assemble & save ────────────────────────────────────────────────────
    print("\nAssembling report...")
    html = build_html(
        user           = user,
        summary_df     = summary_df,
        img_price      = img_price,
        img_norm       = img_norm,
        img_volume     = img_volume,
        img_volat      = img_volat,
        img_corr_price = img_corr_price,
        img_corr_ind   = img_corr_ind,
        ticker_imgs    = ticker_imgs,
        generated_at   = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✅  Report saved → {os.path.abspath(args.output)}")
    print("   Open in any browser to view.")


if __name__ == "__main__":
    main()