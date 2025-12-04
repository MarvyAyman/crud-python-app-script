"""
Polymarket Logger - Converts FastAPI records to Google Sheets logs
Reads market configurations and generates realistic price simulations
Runs continuously and processes all markets in parallel
"""

import json
import random
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict
import gspread
from google.oauth2.service_account import Credentials

# Configuration
DB_FILE = Path("db.json")
SPREADSHEET_EMAIL = "your-spreadsheet@gmail.com"  # Replace with actual email
SHEET_NAME = "logs"

# Google Sheets API setup
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


class PolymarketLogger:
    def __init__(self, credentials_file: str = "credentials.json"):
        """Initialize with Google service account credentials"""
        self.creds = Credentials.from_service_account_file(
            credentials_file, 
            scopes=SCOPES
        )
        self.gc = gspread.authorize(self.creds)
        self.sheet = None
        
    def open_spreadsheet(self, email: str, sheet_name: str = SHEET_NAME):
        """Open spreadsheet by email and get the logs sheet"""
        try:
            spreadsheet = self.gc.open_by_key(email) if '@' not in email else self.gc.open(email)
            
            # Try to get existing sheet or create new one
            try:
                self.sheet = spreadsheet.worksheet(sheet_name)
                print(f"Found existing sheet '{sheet_name}', will continue from last row")
            except gspread.WorksheetNotFound:
                self.sheet = spreadsheet.add_worksheet(
                    title=sheet_name, 
                    rows=1000, 
                    cols=11
                )
                print(f"Created new sheet '{sheet_name}'")
            
            self._format_header()
            return True
        except Exception as e:
            print(f"Error opening spreadsheet: {e}")
            return False
    
    def _format_header(self):
        """Format the header row"""
        header = [
            "log_id",
            "market_id",
            "market_label",
            "yes_price",
            "no_price",
            "sum_price",
            "difference_from_1",
            "below_threshold1",
            "below_threshold2",
            "below_threshold3",
            "timestamp_UTC"
        ]
        
        # Check if header exists
        if self.sheet.row_count > 0:
            first_row = self.sheet.row_values(1)
            if first_row and first_row[0] == "log_id":
                return
        
        # Set header
        self.sheet.update('A1:K1', [header])
        
        # Format header
        self.sheet.format('A1:K1', {
            "backgroundColor": {"red": 0, "green": 0.48, "blue": 1},
            "textFormat": {
                "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                "bold": True
            },
            "horizontalAlignment": "CENTER"
        })
    
    def load_markets(self) -> List[Dict]:
        """Load market configurations from db.json"""
        if not DB_FILE.exists():
            return []
        
        with open(DB_FILE, 'r') as f:
            return json.load(f)
    
    def generate_realistic_prices(self) -> Dict[str, float]:
        """Generate realistic Polymarket YES/NO prices"""
        # YES ranges between 0.05 and 0.95
        yes_price = random.uniform(0.05, 0.95)
        
        # Add small noise to simulate market inefficiency
        noise = random.uniform(-0.03, 0.03)
        
        # NO = (1 - YES) + noise
        no_price = 1 - yes_price + noise
        
        # Clamp to valid range
        no_price = max(0.01, min(0.99, no_price))
        
        return {
            'yes': round(yes_price, 2),
            'no': round(no_price, 2)
        }
    
    def get_utc_timestamp(self) -> str:
        """Get current UTC timestamp as formatted string"""
        now = datetime.utcnow()
        return now.strftime("%Y-%m-%d %H:%M:%S UTC")
    
    def apply_flag_color(self, row_index: int, col_index: int, value: str):
        """Apply color coding to flag cells"""
        cell = f"{chr(64 + col_index)}{row_index}"
        
        if value == "YES":
            color = {"red": 0.97, "green": 0.84, "blue": 0.85}  # Light red
        else:
            color = {"red": 0.83, "green": 0.93, "blue": 0.85}  # Light green
        
        self.sheet.format(cell, {"backgroundColor": color})
    
    def simulate_logs(self, num_logs: int = 60):
        """Generate dummy logs based on market configurations - processes all markets in parallel"""
        markets = self.load_markets()
        
        if not markets:
            print("No markets found in db.json. Using dummy data.")
            markets = [
                {
                    "marketId": "MKT001",
                    "marketLabel": "Will BTC close above $100k?",
                    "threshold1": 1.0,
                    "threshold2": 0.95,
                    "threshold3": 0.90
                }
            ]
        
        print(f"Processing {len(markets)} markets in parallel...")
        
        # Get current last row
        existing_rows = len(self.sheet.get_all_values())
        log_id = existing_rows - 1 if existing_rows > 1 else 0
        
        rows_to_add = []
        
        # Process all markets simultaneously - each market gets equal representation
        logs_per_market = num_logs // len(markets)
        remainder = num_logs % len(markets)
        
        for market_idx, market in enumerate(markets):
            # Distribute remainder logs across first few markets
            market_logs = logs_per_market + (1 if market_idx < remainder else 0)
            
            for i in range(market_logs):
                # Generate prices
                prices = self.generate_realistic_prices()
                yes_price = prices['yes']
                no_price = prices['no']
                
                sum_price = round(yes_price + no_price, 2)
                difference = round(1 - sum_price, 3)
                
                # Check thresholds
                threshold1 = market.get('threshold1', 1.0)
                threshold2 = market.get('threshold2', 0.95)
                threshold3 = market.get('threshold3', 0.90)
                
                below_t1 = "YES" if sum_price < threshold1 else "NO"
                below_t2 = "YES" if sum_price < threshold2 else "NO"
                below_t3 = "YES" if sum_price < threshold3 else "NO"
                
                log_id += 1
                
                row = [
                    log_id,
                    market['marketId'],
                    market['marketLabel'],
                    yes_price,
                    no_price,
                    sum_price,
                    difference,
                    below_t1,
                    below_t2,
                    below_t3,
                    self.get_utc_timestamp()
                ]
                
                rows_to_add.append(row)
        
        # Shuffle to interleave markets (simulate parallel processing)
        random.shuffle(rows_to_add)
        
        # Batch append all rows
        if rows_to_add:
            print(f"Appending {len(rows_to_add)} logs to sheet...")
            self.sheet.append_rows(rows_to_add)
            
            # Safety delay after batch operation
            time.sleep(1)
            
            # Apply color coding to flags
            start_row = existing_rows + 1
            for idx, row in enumerate(rows_to_add):
                row_num = start_row + idx
                self.apply_flag_color(row_num, 8, row[7])   # threshold1
                self.apply_flag_color(row_num, 9, row[8])   # threshold2
                self.apply_flag_color(row_num, 10, row[9])  # threshold3
                
                # Safety delay to avoid quota limits (every 10 rows)
                if (idx + 1) % 10 == 0:
                    time.sleep(1)
                    print(f"Processed {idx + 1}/{len(rows_to_add)} rows...")
        
        print(f"✓ Added {num_logs} logs to spreadsheet")
        return len(rows_to_add)


def main():
    """Main execution function - runs continuously"""
    print("=" * 60)
    print("Polymarket Continuous Logger Started")
    print("=" * 60)
    
    # Initialize logger
    logger = PolymarketLogger(credentials_file="credentials.json")
    
    # Open spreadsheet (use email or spreadsheet ID)
    if not logger.open_spreadsheet(SPREADSHEET_EMAIL):
        print("Failed to open spreadsheet")
        return
    
    print("\nStarting continuous logging...")
    print("Press Ctrl+C to stop\n")
    
    cycle = 0
    
    try:
        while True:
            cycle += 1
            print(f"\n{'='*60}")
            print(f"Cycle #{cycle} - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print(f"{'='*60}")
            
            # Generate logs for all markets
            logs_added = logger.simulate_logs(num_logs=60)
            
            print(f"\n✓ Cycle #{cycle} complete - {logs_added} logs added")
            print(f"Waiting 5 seconds before next cycle...")
            
            # Wait between cycles (adjust as needed)
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n\n" + "="*60)
        print("Logging stopped by user")
        print(f"Total cycles completed: {cycle}")
        print("="*60)
    except Exception as e:
        print(f"\n\nError occurred: {e}")
        print(f"Cycles completed before error: {cycle}")


if __name__ == "__main__":
    main()