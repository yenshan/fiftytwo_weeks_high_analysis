#!/usr/bin/env python
import sys
import pandas as pd


def extract_stock_codes(filepath):
    try:
        df = pd.read_csv(filepath)
        
        # Extract codes and clean them
        codes = df['コード'].dropna().astype(str).tolist()
        
        # Clean codes (remove non-numeric characters, ensure 4 digits)
        clean_codes = []
        for code in codes:
            # Remove any non-digit characters
            clean_code = ''.join(filter(str.isdigit, str(code)))
            if len(clean_code) == 4:  # Japanese stock codes are 4 digits
                clean_codes.append(clean_code)
        
        return clean_codes
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr) 
        return []
    
res = extract_stock_codes("data_j.csv") 
print(res)