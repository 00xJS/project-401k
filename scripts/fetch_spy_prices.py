#!/usr/bin/env python3
"""
fetch_spy_prices.py
───────────────────
Builds data/spy-prices.json from Yahoo Finance via yfinance.

Data source: SPY (SPDR S&P 500 ETF Trust) daily closing prices, back to its first
trading day (1993-01-29). These are the prices SPY actually closed at — adjusted
for splits, NOT for dividends — so the calculator's figures exclude reinvested
dividends (roughly 1–2% a year for SPY).

Run each weekday via GitHub Actions (21:00 UTC, after the US market close).
Requires: pip install yfinance

Output format:
  {
    "generated": "2026-09-15T21:00:00Z",
    "source": "Yahoo Finance / yfinance — SPY daily closes (not dividend-adjusted)",
    "count": 8460,
    "prices": [{"ts": 728265600, "price": 43.94}, ...]
  }

  ts    — Unix timestamp (UTC midnight of the trading date)
  price — SPY closing price in USD, rounded to 2 decimal places
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "spy-prices.json"
TICKER      = "SPY"
START_DATE  = "1993-01-01"   # SPY began trading on 1993-01-29


def fetch_spy_prices() -> list:
    """Download SPY's full daily close history via yfinance.

    Returns a list of {"ts": int, "price": float} dicts sorted ascending by ts.
    """
    try:
        import yfinance as yf
    except ImportError:
        print("ERROR: yfinance not installed. Run: pip install yfinance")
        sys.exit(1)

    print(f"Downloading {TICKER} daily prices from Yahoo Finance …")

    # auto_adjust=False keeps "Close" as the traded closing price. With
    # auto_adjust=True, yfinance would fold dividends into every past price.
    hist = yf.Ticker(TICKER).history(start=START_DATE, auto_adjust=False)

    if hist.empty:
        raise ValueError(f"yfinance returned an empty DataFrame for {TICKER}")

    prices = {}
    for date, row in hist.iterrows():
        close = row["Close"]
        # Skip NaN or non-positive values
        if close is None or close != close or close <= 0:
            continue
        # Normalise to UTC midnight of the trading date — yfinance's timezone
        # handling varies by version, so strip it and re-add UTC.
        dt = date.to_pydatetime().replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )
        prices[int(dt.timestamp())] = round(float(close), 2)   # last row per day wins

    result = [{"ts": ts, "price": price} for ts, price in sorted(prices.items())]
    print(f"  {len(result)} daily price points fetched")
    return result


def main() -> None:
    prices = fetch_spy_prices()

    if not prices:
        print("ERROR: No price data returned. Aborting.")
        sys.exit(1)

    first_date = datetime.fromtimestamp(prices[0]["ts"],  tz=timezone.utc).date()
    last_date  = datetime.fromtimestamp(prices[-1]["ts"], tz=timezone.utc).date()
    print(f"Date range: {first_date} → {last_date}")

    output = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source":    "Yahoo Finance / yfinance — SPY daily closes (not dividend-adjusted)",
        "count":     len(prices),
        "prices":    prices,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(output, separators=(",", ":")), encoding="utf-8")

    print(f"Written {len(prices)} records to {OUTPUT_PATH}")
    print(f"Latest SPY close: ${prices[-1]['price']:,.2f} on {last_date}")


if __name__ == "__main__":
    main()
