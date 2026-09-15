# S&P 500 DCA Calculator (SPY)

## Project Background

Dollar Cost Averaging (DCA) is an investment strategy where an investor divides up the total amount to be invested across periodic purchases of a target asset, to reduce the impact of volatility on the overall purchase. It is also how most people already invest in the stock market: a 401(k) takes a fixed contribution from every paycheck, often topped up by an employer match, and buys the same fund on the same schedule whatever the price.

This project replays that 401(k)-style plan against real historical prices for **SPY** — the SPDR S&P 500 ETF Trust, which has tracked the S&P 500 since January 1993 — so you can see what a given contribution schedule would have produced.

## Live Tool

**[S&P 500 DCA Calculator](https://project401k.netlify.app/)** — a browser-based simulator. Set a start date, how often you contribute, how much, and an optional employer match, and the portfolio value, return figures, growth chart and full purchase history update live as you go.

> **Dividends are not included.** The prices are SPY's actual traded closes — adjusted for splits, not for dividends — and the simulation never reinvests the roughly 1–2% a year SPY pays out. Every figure therefore understates what a 401(k) fund that reinvests its dividends would have earned over the same period, and the gap widens the longer the run.

## Data Source & Workflow

Price data is pre-baked static JSON committed to this repo — the browser never calls a market-data API.

| | |
|---|---|
| Source | [Yahoo Finance `SPY`](https://finance.yahoo.com/quote/SPY/) via `yfinance` |
| Coverage | 1993-01-29 (SPY's first trading day) → today (8,400+ daily closes) |
| Prices | Actual closing prices in USD — split-adjusted, **not** dividend-adjusted |
| Schedule | Weekdays, 21:00 UTC (after the US market close) |
| Credentials | None — no API keys, no repo secrets |
| Script | `scripts/fetch_spy_prices.py` |
| Output | `data/spy-prices.json` |

The scheduled Action (`.github/workflows/update-spy-data.yml`) runs the script, commits `data/spy-prices.json` if anything changed, and Netlify auto-deploys that commit. It can also be started by hand from the Actions tab. This means:

- **No client-side API calls** — price data is pre-baked and served from the CDN
- **No credentials anywhere** — `yfinance` needs no key, so there is nothing to expire
- **No CORS or rate-limit issues** — the browser only fetches one same-origin static file
- **One upstream dependency** — Yahoo Finance, reached through the unofficial `yfinance` library. If it breaks or stalls, the workflow commits nothing, the site keeps serving the last good file, and the header chip turns amber once the newest close is more than three days old. The chip allows five days, so weekends and a single market holiday don't trip it.

`netlify.toml` is load-bearing twice:

- `publish = "."` serves the repo root, which is what makes the page's **absolute** `/data/spy-prices.json` request resolve.
- The `must-revalidate` header on that path is what stops the CDN serving yesterday's prices after a data deploy.

## Project Structure

```
├── index.html                      # the calculator — markup, inline CSS and JS, no build step
├── netlify.toml                    # publish root + no-cache header for the data file
├── data/
│   └── spy-prices.json             # auto-generated, weekdays
├── scripts/
│   └── fetch_spy_prices.py         # yfinance SPY → daily closes
└── .github/
    └── workflows/
        └── update-spy-data.yml     # weekday cron
```

## Running it locally

The page fetches `/data/spy-prices.json` by **absolute path**, and browsers block
`fetch()` on `file://` outright — so opening `index.html` by double-clicking it
will always end in the "price data could not be loaded" error. Serve the directory instead:

```bash
python3 -m http.server 8000
```

Then open <http://localhost:8000>. Any static server works; it just has to serve
the repo root so `/data/…` resolves. No build step, no package install, no API keys
— the price file is committed, so there is nothing to fetch first. (The chart itself
loads Chart.js from jsDelivr; without it, the figures and purchase history still render.)

Rebuilding the price data is only needed for fresher numbers than the committed file,
and needs no credentials:

```bash
pip install yfinance && python3 scripts/fetch_spy_prices.py
```

That rewrites `data/spy-prices.json` in place — exactly what the scheduled Action does.

### Data file format

`data/spy-prices.json` (~275 KB) is a standalone, reusable artifact:

```jsonc
{
  "generated": "2026-09-15T05:27:33Z",   // UTC build time
  "source":    "Yahoo Finance / yfinance — SPY daily closes (not dividend-adjusted)",
  "count":     8463,
  "prices":    [ { "ts": 728265600, "price": 43.94 } ]   // ts = UNIX SECONDS (UTC midnight of the trading date), ascending
}
```

`price` is SPY's closing price in USD, rounded to the cent. Days the market was closed
have no entry; a purchase that lands on one fills at the most recent earlier close.

## DCA Simulation

The calculator supports:
- **Start date**: any date from 29 January 1993, SPY's first trading day (defaults to one year ago).
  Quick presets (1Y · 3Y · 5Y · 10Y · Max) sit above the date field, and a timeline slider scrubs
  the start date through history
- **Frequency**: weekly, bi-weekly or monthly — bi-weekly by default, matching most US payroll cycles
- **Your contribution**: any USD amount per purchase (defaults to $100)
- **Employer match**: an additional USD amount per purchase, the way a 401(k) match accrues —
  reported separately from your own contributions
- **Shareable plans**: once you change anything, the address bar links to exactly the plan on screen
  (`?start=2000-01-03&freq=14&amt=100&match=50`), and *Copy link* puts it on the clipboard

For each purchase date, the simulator binary-searches for the most recent SPY close **at or before** that date — never a later one, so the model can't look ahead — buys (contribution + match) ÷ price in fractional shares, and calculates:
- Total invested, split into your contributions and the employer match
- Total SPY shares accumulated (fractional, to 6 decimal places)
- Average cost per share
- Current portfolio value, at the latest close
- Total profit/loss
- Return on investment (%) — cumulative over the whole period
- Annualised return (XIRR) — money-weighted, shown once a run spans at least a year

Results update live — there is no Run button. The page leads with the portfolio value and a plain-English
summary of the plan, then six KPI tiles, a chart of portfolio value against total invested, an
**averaging effect** card, and a collapsible purchase history with sortable columns and CSV export.

### What the model assumes

These are stated on the page too, under *How this works*, but they matter to anyone reading the numbers:

- **Dividends are excluded.** SPY's closes are not dividend-adjusted, and the simulation never reinvests the ~1–2% a year SPY distributes. Over a multi-decade plan that compounds into a large gap, so read every figure as *price return only* — a floor for what a dividend-reinvesting 401(k) fund would have shown, not an estimate of it.
- **"Monthly" means every 30 days**, not the same calendar date — roughly 12.2 purchases a year, with the date drifting earlier over time. Weekly is 7 days, bi-weekly 14.
- **No fees, spreads, or taxes.** Any commission, plan fee or bid–ask spread would reduce every figure. SPY's own 0.09% expense ratio is the exception — it is already reflected in the price. Nothing is ever sold, so profit is unrealised.
- **Daily closes only** — each purchase fills at SPY's actual closing price on or before its date; intraday highs and lows are ignored, and fractional shares are assumed.
- **Purchases run through today**, so an earlier start date also means more total dollars invested. Compare runs on ROI rather than absolute profit.
- **Portfolio Value** prices the whole stack at the most recent daily close. In the history table, *Value on Date* prices it as of that row's date instead — which is why the last row and the summary differ.
- **Annualised return is XIRR**, solved by Newton's method with a bisection fallback. It is money-weighted, so it accounts for *when* each contribution went in. The naive `(value / invested)^(1/years)` shortcut — which this tool's first version used — credits every dollar with the full elapsed period, and materially understates a contribution stream whose latest dollars have barely had time to compound — on a bi-weekly plan since January 2000 it reports about 5.8%/yr against a true 9.8%/yr (as of September 2026).

## Key Features

- **Live results** — every change re-runs the simulation; drag the start-date timeline and watch the numbers move
- **401(k)-style employer match** — reported separately from your own contributions
- **Presets and a timeline** — 1Y · 3Y · 5Y · 10Y · Max, or scrub the start date anywhere back to 1993
- **Shareable links** — the URL encodes the whole plan
- **Chart readout** — value, invested and ROI at whatever date the pointer is on, on a linear or log scale, with gain/loss shading between the lines
- **Averaging effect** — average cost per share against the average price on your purchase dates
- **Sortable history table** — every column sorts, by click or keyboard; the order survives live re-runs; exports to CSV
- **Always-current data** via an automated GitHub Actions pipeline, with localStorage caching keyed per day
- **Staleness warning** — the header chip turns amber if the newest close is more than five days old, so weekends and a market holiday stay green
- **Responsive, dark-only design** — works on desktop and mobile
- **No external dependencies at runtime** beyond Chart.js (pinned to 4.5.1, with an SRI hash)

---

**Disclaimer:** This tool is for educational and informational purposes only. Past performance does not guarantee future results, and the figures here exclude dividends, fees and taxes. Equity markets can fall as well as rise — always conduct your own research before making any investment decisions.
