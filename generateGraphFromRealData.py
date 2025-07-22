import argparse
import math
import threading
import time
import os
import csv

from generator.generateNodes import generateNodes
from generator.generateEdges import generateEdges
from generator.generateTransactions import generateTransactions
from generator.generatePatterns import generatePatterns
from generator.utils import log

### Script arguments setup ###
parser = argparse.ArgumentParser("Generate Graph From Real Data")
parser.add_argument("--dataset", help="path to the dataset folder containing real data", type=str, action="store", default='./dataset')
parser.add_argument("--data", help="path to the data folder", type=str, action="store", default='./data')
parser.add_argument(
    "--steps",
    help="Steps to do. possible values (comma - separated): prepare, edges, transactions, patterns",
    type=str,
    action="store",
    default="prepare,edges,transactions,patterns"
)
parser.add_argument(
    "--batch-size",
    help="Size of batch window to write",
    type=int,
    action="store",
    default=10000
)

args = parser.parse_args()
### ### ###

### Variables definition ###
dataDir = args.data + '/' + time.strftime("%H.%M.%S_%d-%m-%Y")
os.makedirs(dataDir, exist_ok=True)

files = {
    "client": dataDir + '/nodes.clients.csv',
    "company": dataDir + '/nodes.companies.csv',
    "atm": dataDir + '/nodes.atms.csv',

    "clients-clients-edges": dataDir + '/edges.client-client.csv',
    "clients-companies-edges": dataDir + '/edges.client-company.csv',
    "clients-atms-edges": dataDir + '/edges.client-atm.csv',
    "companies-clients-edges": dataDir + '/edges.company-client.csv',

    "clients-sourcing-transactions": dataDir + '/nodes.transactions.client-sourcing.csv',
    "companies-sourcing-transactions": dataDir + '/nodes.transactions.company-sourcing.csv',

    "flow-pattern-transactions": dataDir + '/nodes.transactions.patterns.flow.csv',
    "circular-pattern-transactions": dataDir + '/nodes.transactions.patterns.circular.csv',
    "time-pattern-transactions": dataDir + '/nodes.transactions.patterns.time.csv'
}

steps = set(map(lambda x: x, args.steps.split(',')))
batchSize = getattr(args, 'batch_size')

log('Steps to execute: ' + str(steps))

def prepare_real_data():
    """Prepare real transaction data for processing"""
    log('Preparing real transaction data...')
    
    # Check if transaction file exists
    transactions_file = os.path.join(args.dataset, 'Txn_data.csv')
    if not os.path.exists(transactions_file):
        log(f"Error: Transaction file not found at {transactions_file}")
        return False
    
    # Extract unique accounts from transactions
    accounts = set()
    
    with open(transactions_file, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        header = next(reader)  # Skip header
        
        # Verify file format
        if len(header) < 5:
            log(f"Error: Transaction file does not have enough columns. Expected: id, Date/Time, From_Account_Id, To_Account_Id, Amount")
            return False
        
        for row in reader:
            if len(row) < 5:
                continue  # Skip incomplete rows
            
            # Format: id, Date/Time, From_Account_Id, To_Account_Id, Amount
            from_account = row[2]
            to_account = row[3]
            
            accounts.add(from_account)
            accounts.add(to_account)
    
    log(f"Found {len(accounts)} unique accounts")
    
    # Generate client data
    with open(files["client"], 'w') as f:
        f.write("id|first_name|last_name|age|email|occupation|political_views|nationality|university|academic_degree|address|postal_code|country|city\n")
        for account in accounts:
            # Generate placeholder data for each account
            f.write(f"{account}|User|{account}|30|user{account}@example.com|Unknown|Unknown|Unknown|Unknown||Unknown|Unknown|Unknown|Unknown\n")
    
    log(f"Generated client data for {len(accounts)} accounts")
    
    # Create empty company and ATM files for compatibility
    with open(files["company"], 'w') as f:
        f.write("id|type|name|country\n")
    
    with open(files["atm"], 'w') as f:
        f.write("id|latitude|longitude\n")
    
    return True

def generate_edges_from_real_data():
    """Generate edges from real transaction data"""
    log('Generating edges from real transaction data...')
    
    transactions_file = os.path.join(args.dataset, 'Txn_data.csv')
    
    # Initialize edge dictionaries
    client_client_edges = {}
    
    with open(transactions_file, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)  # Skip header
        
        for row in reader:
            if len(row) < 5:
                continue  # Skip incomplete rows
            
            # Format: id, Date/Time, From_Account_Id, To_Account_Id, Amount
            from_account = row[2]
            to_account = row[3]
            
            # Add to client-client edges
            if from_account not in client_client_edges:
                client_client_edges[from_account] = {}
            
            if to_account not in client_client_edges[from_account]:
                client_client_edges[from_account][to_account] = 0
            
            client_client_edges[from_account][to_account] += 1
    
    # Write edges to file
    with open(files["clients-clients-edges"], 'w') as f:
        for source_id, targets in client_client_edges.items():
            f.write(f"{source_id}|\"{targets}\"\n")
    
    log(f"Generated edges between accounts")
    
    # Create empty edge files for other types for compatibility
    with open(files["clients-companies-edges"], 'w') as f:
        pass
    
    with open(files["clients-atms-edges"], 'w') as f:
        pass
    
    with open(files["companies-clients-edges"], 'w') as f:
        pass

def generate_transactions_from_real_data():
    """Generate transaction files from real transaction data"""
    log('Generating transaction files from real data...')
    
    transactions_file = os.path.join(args.dataset, 'Txn_data.csv')
    
    with open(files["clients-sourcing-transactions"], 'w') as outfile:
        outfile.write("id|source|target|date|time|amount|currency\n")
        
        with open(transactions_file, 'r') as infile:
            reader = csv.reader(infile, delimiter=',')
            next(reader)  # Skip header
            
            for row in reader:
                if len(row) < 5:
                    continue  # Skip incomplete rows
                
                # Format: id, Date/Time, From_Account_Id, To_Account_Id, Amount
                trans_id = row[0]
                datetime_str = row[1]
                from_account = row[2]
                to_account = row[3]
                amount = row[4]
                
                # Parse date and time
                date_str = "2023-01-01"
                time_str = "00:00:00"
                
                if " " in datetime_str:
                    parts = datetime_str.split(" ")
                    date_str = parts[0]
                    if len(parts) > 1:
                        time_str = parts[1]
                
                # Write transaction in the format expected by the system
                outfile.write(f"{trans_id}|{from_account}|{to_account}|{date_str}|{time_str}|{amount}|USD\n")
    
    log("Generated transaction files from real data")
    
    # Create empty company transactions file for compatibility
    with open(files["companies-sourcing-transactions"], 'w') as f:
        f.write("id|source|target|date|time|amount|currency\n")

# Execute steps based on user selection
if 'prepare' in steps:
    log()
    log('------------##############------------')
    log('Preparing real data')
    if not prepare_real_data():
        log("Failed to prepare real data. Exiting.")
        exit(1)

if 'edges' in steps:
    log()
    log('------------##############------------')
    log('Generating edges from real data')
    generate_edges_from_real_data()

if 'transactions' in steps:
    log()
    log('------------##############------------')
    log('Generating transactions from real data')
    generate_transactions_from_real_data()

if 'patterns' in steps:
    log()
    log('------------##############------------')
    log('Generating patterns')
    generatePatterns(files, {"client": 100, "company": 0, "atm": 0}, batchSize)

log()
log('Processing complete! Output files are in: ' + dataDir)