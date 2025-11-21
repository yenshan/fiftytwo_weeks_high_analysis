#!/usr/bin/env python
"""
Get all stock data from JPX listed companies
1. Download company list from JPX and save as data_j.csv
2. Fetch stock data for all companies using get-stock-data.py
"""
import os
import sys
import pandas as pd
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_company_list():
    """
    Download company list from JPX and save as data_j.csv
    """
    jpx_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    
    try:
        logger.info("Downloading company list from JPX...")
        
        # Try to read Excel file with different engines
        df = None
        for engine in ['openpyxl', 'xlrd', None]:
            try:
                df = pd.read_excel(jpx_url, engine=engine)
                logger.info(f"Successfully read Excel file with engine: {engine}")
                break
            except Exception as e:
                logger.warning(f"Failed with engine {engine}: {e}")
                continue
        
        if df is None:
            logger.error("Failed to read Excel file with any engine")
            return False
        
        # Save as CSV
        csv_file = "data_j.csv"
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
        
        logger.info(f"Company list saved to {csv_file}")
        logger.info(f"Total companies: {len(df)}")
        
        # Show column names for debugging
        logger.info(f"Columns: {df.columns.tolist()}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to download company list: {e}")
        return False

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

def fetch_single_stock(stock_code, period="5y"):
    """
    Fetch stock data for a single stock using get-stock-data.py
    """
    try:
        # Ensure jp_all directory exists
        os.makedirs("jp_all", exist_ok=True)
        
        # Path to get-stock-data.py script (in same directory)
        script_path = "get-stock-data.py"
        
        # Output file path
        output_file = f"jp_all/{stock_code}.stock.csv"
        
        # Skip if file already exists
        if os.path.exists(output_file):
            logger.info(f"Skipping {stock_code} - file already exists")
            return True
        
        # Run get-stock-data.py
        cmd = [sys.executable, script_path, stock_code, period]
        
        with open(output_file, 'w') as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)
        
        if result.returncode == 0:
            # Check if file has data
            if os.path.exists(output_file) and os.path.getsize(output_file) > 100:
                logger.info(f"✓ {stock_code}: Data downloaded successfully")
                return True
            else:
                logger.warning(f"⚠ {stock_code}: File created but no data")
                return False
        else:
            logger.error(f"✗ {stock_code}: {result.stderr.strip()}")
            # Remove empty file
            if os.path.exists(output_file):
                os.remove(output_file)
            return False
            
    except Exception as e:
        logger.error(f"✗ {stock_code}: Exception - {e}")
        return False

def fetch_all_stocks(stock_codes, period="5y", max_workers=5):
    """
    Fetch stock data for all stocks with parallel processing
    """
    logger.info(f"Fetching stock data for {len(stock_codes)} companies...")
    logger.info(f"Period: {period}, Workers: {max_workers}")
    
    successful = 0
    failed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_code = {
            executor.submit(fetch_single_stock, code, period): code 
            for code in stock_codes
        }
        
        # Process completed tasks
        for future in as_completed(future_to_code):
            code = future_to_code[future]
            try:
                success = future.result()
                if success:
                    successful += 1
                else:
                    failed += 1
                    
                # Progress update every 50 stocks
                if (successful + failed) % 50 == 0:
                    logger.info(f"Progress: {successful + failed}/{len(stock_codes)} processed")
                    
            except Exception as e:
                logger.error(f"Exception for {code}: {e}")
                failed += 1
    
    logger.info(f"Stock data fetch completed:")
    logger.info(f"  Successful: {successful}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"  Total: {successful + failed}")
    
    return successful

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Download all stock data from JPX listed companies')
    parser.add_argument('--period', default='5y', help='Data period (default: 5y)')
    parser.add_argument('--workers', type=int, default=5, help='Number of parallel workers')
    parser.add_argument('--skip-download', action='store_true', help='Skip downloading company list')
    parser.add_argument('--codes-only', action='store_true', help='Only extract and show stock codes')
    
    args = parser.parse_args()
    
    start_time = time.time()
    
    # Step 1: Download company list
    if not args.skip_download:
        if not download_company_list():
            logger.error("Failed to download company list")
            sys.exit(1)
    
    # Step 2: Extract stock codes
    stock_codes = extract_stock_codes("data_j.csv")
    if not stock_codes:
        logger.error("No stock codes found")
        sys.exit(1)
    
    if args.codes_only:
        logger.info(f"Found {len(stock_codes)} stock codes:")
        for code in stock_codes[:10]:  # Show first 10
            print(f"  {code}")
        if len(stock_codes) > 10:
            print(f"  ... and {len(stock_codes) - 10} more")
        sys.exit(0)
    
    # Step 3: Fetch all stock data
    successful = fetch_all_stocks(stock_codes, args.period, args.workers)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Total execution time: {elapsed_time:.1f} seconds")
    
    if successful > 0:
        logger.info("Stock data download completed successfully")
        sys.exit(0)
    else:
        logger.error("No stock data was downloaded")
        sys.exit(1)

if __name__ == "__main__":
    main()