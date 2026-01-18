import requests
import csv
import json
import time
import os

BITQUERY_URL = "https://streaming.bitquery.io/graphql"
BATCH_SIZE = 10000 
RECORDS_PER_FILE = 20000
CONFIG_FILE = "config.json"

def load_api_key():
    """Load API key from config file if it exists"""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                return config.get('api_key')
        except:
            return None
    return None

def save_api_key(api_key):
    """Save API key to config file"""
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump({'api_key': api_key}, f)
    except:
        pass

def get_total_buys(token_address, api_key):
    """Get the total number of buy transactions for a token"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    query = """
    query GetTotalBuys {
      Solana(dataset: realtime) {
        DEXTradeByTokens(
          where: {
            Trade: {
              Currency: {
                MintAddress: {is: "%s"}
              }
              Side: {
                Type: {is: buy}
              }
            }
            Transaction: {Result: {Success: true}}
          }
          limit: {count: 1}
        ) {
          Block {
            Time
          }
        }
      }
    }
    """ % token_address
    
    try:
        print("Checking token buy transactions...")
        response = requests.post(BITQUERY_URL, json={"query": query}, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if 'errors' in data:
            print("Error from Bitquery:")
            print(json.dumps(data['errors'], indent=2))
            return None
        
        return "Unknown total (will count during fetch)"
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return None

def fetch_batch(token_address, api_key, before_timestamp=None):
    """Fetch in batch of 10,000 buy transactions"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    time_filter = f'Time: {{before: "{before_timestamp}"}}' if before_timestamp else ''
    
    query = """
    query GetBatch {
      Solana(dataset: realtime) {
        DEXTradeByTokens(
          where: {
            Trade: {
              Currency: {
                MintAddress: {is: "%s"}
              }
              Side: {
                Type: {is: buy}
              }
            }
            Transaction: {Result: {Success: true}}
            Block: {
              %s
            }
          }
          orderBy: {descendingByField: "Block_Time"}
          limit: {count: %d}
        ) {
          Block {
            Time
          }
          Transaction {
            Signature
            Signer
          }
          Trade {
            Account {
              Address
            }
            Amount
            Price
            PriceInUSD
            Side {
              Amount
              AmountInUSD
              Currency {
                Symbol
                MintAddress
              }
            }
          }
        }
      }
    }
    """ % (token_address, time_filter, BATCH_SIZE)
    
    try:
        response = requests.post(BITQUERY_URL, json={"query": query}, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if 'errors' in data:
            print("Error from Bitquery:")
            print(json.dumps(data['errors'], indent=2))
            return None
        
        trades = data['data']['Solana']['DEXTradeByTokens']
        return trades
    
    except Exception as e:
        print(f"Error fetching batch: {str(e)}")
        return None

def save_to_csv(trades, filename):
    """Saving trades to a CSV file"""
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Timestamp', 'Buyer_Wallet', 'Token_Amount', 'Amount_Paid_SOL', 'Amount_Paid_USD', 'Transaction_Signature', 'Signer'])
        
        for trade in trades:
            
            amount_paid = trade['Trade']['Side'].get('Amount', 0) if trade['Trade']['Side'].get('Amount') else 0
            amount_paid_usd = trade['Trade']['Side'].get('AmountInUSD', 0) if trade['Trade']['Side'].get('AmountInUSD') else 0
            
            writer.writerow([
                trade['Block']['Time'],
                trade['Trade']['Account']['Address'],
                trade['Trade']['Amount'],
                amount_paid,
                amount_paid_usd,
                trade['Transaction']['Signature'],
                trade['Transaction']['Signer']
            ])

def combine_csv_files(input_files, output_file):
    """Combining multiple CSV files into one"""
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Timestamp', 'Buyer_Wallet', 'Token_Amount', 'Amount_Paid_SOL', 'Amount_Paid_USD', 'Transaction_Signature', 'Signer'])
        
        for filename in input_files:
            try:
                with open(filename, 'r', encoding='utf-8') as infile:
                    reader = csv.reader(infile)
                    next(reader)  
                    for row in reader:
                        writer.writerow(row)
            except FileNotFoundError:
                print(f"File not found: {filename}")



def main():
    print("=" * 70)
    print("solana token buy transactions exporter - by willzy")
    print("=" * 70)
    print()
    
   
    saved_key = load_api_key()
    
    if saved_key:
        print("Found saved API key")
        use_saved = input("Use saved API key? (yes/no): ").strip().lower()
        if use_saved in ['yes', 'y']:
            api_key = saved_key
            print("Using saved API key.")
        else:
            api_key = input("\nEnter new Bitquery API key: ").strip()
            if api_key:
                save_api_key(api_key)
    else:
        print("STEP 1: Bitquery API Key")
        api_key = input("\nEnter your Bitquery API key: ").strip()
        if api_key:
            save_api_key(api_key)
            print("API key saved for future use")
    
    if not api_key:
        print("API key cannot be empty!")
        return
    
    print()
    
    print("STEP 2: Token Contract Address")
    print("-" * 70)
    print("Enter the Solana token contract address")
    token_address = input("\nEnter token address: ").strip()
    
    if not token_address:
        print("Token address cannot be empty!")
        return
    
    print()
    print("=" * 70)
    
    total = get_total_buys(token_address, api_key)
    
    if total is None:
        print("Failed to connect to Bitquery. Please check your API key and token address.")
        return
    
    print(f"Token found!")
    print(f"Total buy transactions: {total}")
    print()
    
    print("=" * 70)
    print("this script will fetch ALL buy transactions for this token and save them to CSV files.")
    print(f"Files will be saved as: token_buys_file1.csv, token_buys_file2.csv, etc.")
    print(f"{RECORDS_PER_FILE:,} records per file")
    print("=" * 70)
    
    start = input("\nStart fetching? (yes/no): ").strip().lower()
    
    if start not in ['yes', 'y']:
        print("Cancelled by user.")
        return
    
    print()
    print("=" * 70)
    print("starting to fetch...")
    print("=" * 70)
    print()
    
    all_trades = []
    batch_num = 1
    last_timestamp = None
    batch_files = []
    file_num = 1
    total_fetched = 0
    
    while True:
        print(f"Fetching batch #{batch_num}...", end=" ")
        
        trades = fetch_batch(token_address, api_key, last_timestamp)
        
        if not trades:
            print("Failed to fetch / no more data.")
            break
        
        if len(trades) == 0:
            print("No more trades!")
            break
        
        print(f"{len(trades):,} trades")
        all_trades.extend(trades)
        total_fetched += len(trades)
        
        if len(all_trades) >= RECORDS_PER_FILE:
            filename = f'token_buys_file{file_num}.csv'
            trades_to_save = all_trades[:RECORDS_PER_FILE]
            save_to_csv(trades_to_save, filename)
            batch_files.append(filename)
            print(f"   Saved {len(trades_to_save):,} trades to: {filename}")
            print(f"   Total fetched so far: {total_fetched:,}")
            
            all_trades = all_trades[RECORDS_PER_FILE:]
            file_num += 1
        
        last_timestamp = trades[-1]['Block']['Time']
        
        if len(trades) < BATCH_SIZE:
            print("   Reached end of data")
            break
        
        batch_num += 1
        time.sleep(1)  
      
    if len(all_trades) > 0:
        filename = f'token_buys_file{file_num}.csv'
        save_to_csv(all_trades, filename)
        batch_files.append(filename)
        print(f"Saved remaining {len(all_trades):,} trades to: {filename}")
    
    print()
    
    if len(batch_files) > 0:
        print("=" * 70)
        print("Combining all files into one file...")
        print("=" * 70)
        
        combined_filename = 'token_buys_ALL_COMBINED.csv'
        combine_csv_files(batch_files, combined_filename)
        
        total_records = 0
        with open(combined_filename, 'r', encoding='utf-8') as f:
            total_records = sum(1 for line in f) - 1
        
        print(f"Master file created: {combined_filename}")
        print()
    
    print("=" * 70)
    print("EXPORT COMPLETE!")
    print("=" * 70)
    print(f"Total buy transactions fetched: {total_records:,}")
    print(f"Total API calls made: {batch_num - 1}")
    print(f"\nFiles created:")
    
    for i, filename in enumerate(batch_files, 1):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                count = sum(1 for line in f) - 1
            print(f"   {i}. {filename} ({count:,} records)")
        except:
            print(f"   {i}. {filename}")
    
    print(f"   {len(batch_files) + 1}. {combined_filename} ({total_records:,} records)  MASTER FILE")
    print()
    print("=" * 70)
 


if __name__ == "__main__":
    main()
