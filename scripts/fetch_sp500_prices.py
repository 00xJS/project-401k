#!/usr/bin/env python3
"""
fetch_sp500_prices.py
─────────────────────
Builds data/sp500-prices.json from Yahoo Finance via yfinance.

Data source: ^GSPC (S&P 500 index) daily closing prices.
yfinance can retrieve ^GSPC back to approximately 1927-12-30.

Note: The historical data (up to ~2018) closely matches what is available in the
Kaggle dataset "camnugent/sandp500" (https://www.kaggle.com/datasets/camnugent/sandp500).
yfinance is used here for automation, completeness, and to include recent data
beyond the Kaggle snapshot — no API key or account required.

Run daily via GitHub Actions (weekdays at 21:00 UTC, after US market close).
Requires: pip install yfinance

Output format (mirrors btc-prices.json schema):
  {
    "generated": "2026-04-05T21:00:00Z",
    "source": "Yahoo Finance / yfinance — ^GSPC daily closes",
    "count": 24650,
    "prices": [{"ts": 1234567890, "price": 104.87}, ...]
  }

  ts    — Unix timestamp (UTC midnight of the trading date)
  price — S&P 500 closing index level rounded to 2 decimal places
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "sp500-prices.json"
TICKER      = "^GSPC"
START_DATE  = "1927-01-01"   # yfinance returns ^GSPC data from ~1927-12-30


def fetch_sp500_prices() -> list[dict]:
    """Download full ^GSPC daily close history via yfinance.

    Returns a list of {"ts": int, "price": float} dicts sorted ascending by ts.
    """
    try:
        import yfinance as yf
    except ImportError:
        print("ERROR: yfinance not installed. Run: pip install yfinance")
        sys.exit(1)

    print(f"Downloading {TICKER} daily prices from Yahoo Finance …")

    ticker = yf.Ticker(TICKER)
    # start=START_DATE fetches all available history (back to ~1927 for ^GSPC)
    hist = ticker.history(start=START_DATE, auto_adjust=True)

    if hist.empty:
        raise ValueError(f"yfinance returned an empty DataFrame for {TICKER}")

    prices = []
    for date, row in hist.iterrows():
        close = row["Close"]
        # Skip NaN or non-positive values
        if close is None or close != close:
            continue
        if close <= 0:
            continue
        # Normalize to UTC midnight — ^GSPC dates are trading day dates,
        # timezone info varies by yfinance version so we strip and re-add UTC.
        dt = date.to_pydatetime().replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )
        prices.append({"ts": int(dt.timestamp()), "price": round(float(close), 2)})

    # Sort ascending (yfinance is already sorted, but be defensive)
    prices.sort(key=lambda x: x["ts"])

    # Deduplicate: if two rows share the same UTC midnight timestamp, keep last
    seen: dict[int, float] = {}
    for p in prices:
        seen[p["ts"]] = p["price"]
    prices = [{"ts": ts, "price": price} for ts, price in sorted(seen.items())]

    print(f"  {len(prices)} daily price points fetched")
    return prices


def main() -> None:
    prices = fetch_sp500_prices()

    if not prices:
        print("ERROR: No price data returned. Aborting.")
        sys.exit(1)

    first_date = datetime.fromtimestamp(prices[0]["ts"],  tz=timezone.utc).date()
    last_date  = datetime.fromtimestamp(prices[-1]["ts"], tz=timezone.utc).date()
    print(f"Date range: {first_date} → {last_date}")

    output = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source":    "Yahoo Finance / yfinance — ^GSPC daily closes",
        "count":     len(prices),
        "prices":    prices,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(output, separators=(",", ":")),
        encoding="utf-8",
    )

    print(f"Written {len(prices)} records to {OUTPUT_PATH}")
    print(f"Latest S&P 500 close: ${prices[-1]['price']:,.2f} on {last_date}")


if __name__ == "__main__":
    main()
