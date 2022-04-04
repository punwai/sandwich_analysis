# 🥪 sandwich_analysis 🥪

## Intro
This repo analyzes the number of sandwich attacks that are present in UniswapV3 pools.

### Extracting Data
Download data from Ethereum ETL, and save the CSV files to the output directory. Save the names of each of your files (blocks, transactions, logs) in the constant of analysis.py
To convert CSV into parquet, add the following flags when executing the program:
-lb = Load Blocks
-ltt = Load Token Transfers
-lt = Load Transactions
-lr = Load Receipts
-llogs = Load Logs

### Running Program
To start the program, run the following script:
python3 analysis.py N_BLOCKS

Where N_BLOCKS is the number of latest blocks that you will be analysing.

