#!/usr/bin/env python
import os
import sys
import time
import yfinance as yf
import mplfinance as mpf

def fetch_stock_data(ticker, period="1y"):
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            # Add .T suffix for Tokyo Stock Exchange
            yahoo_ticker = f"{ticker}.T"
            stock = yf.Ticker(yahoo_ticker)
            hist = stock.history(period=period)
            if hist.empty:
                raise ValueError(f"No data fetched for {ticker}. Possibly delisted or wrong ticker.")

            print(f"got data of {ticker}", file=sys.stderr)
            return hist
        except Exception as e:
            print(f"Attempt {attempt}: Failed to fetch data for {ticker} - {e}", file=sys.stderr)
            if attempt < max_attempts:
                print("Retrying...", file=sys.stderr)
                time.sleep(2)  # Reduced wait time
            else:
                print(f"Failed to fetch stock data for {ticker} after {max_attempts} attempts. Giving up.", file=sys.stderr)
                return None


def main():
    if len(sys.argv) < 2:
        print("Usage: get-stock-data.py <ticker> [period]")
        sys.exit(1)
    elif len(sys.argv) < 3:
        period = "1y"
    else:
        # Handle period correctly (e.g., "5" -> "5y")
        period_arg = sys.argv[2]
        if period_arg.isdigit():
            period = f"{period_arg}y"
        else:
            period = period_arg

    ticker = sys.argv[1]
    data = fetch_stock_data(ticker, period)

    if data is not None:
        data.to_csv(sys.stdout)
    else:
        print(f"No data available for {ticker}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
