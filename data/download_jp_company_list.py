#!/usr/bin/env python
import sys
import pandas as pd

jpx_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

def download_company_list(url):
    df = None
    try:
        # Try to read Excel file
        df = pd.read_excel(url, engine='xlrd')
    except Exception as e:
        print(f"Read Excel Error: {e}", file=sys.stderr) 
        return False

    try:
        # Save as CSV
        csv_file = "data_j.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        return True
    except Exception as e:
        print(f"Save File Error: {e}", file=sys.stderr) 
        return False


download_company_list(jpx_url)