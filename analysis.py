from itertools import groupby
from multiprocessing import pool
from tokenize import Token
from unittest import result
from numpy import result_type, signedinteger
import pandas as pd
import os
import sys, getopt

pd.options.mode.chained_assignment = None

# Change pool address to analyze a different pool
POOL_ADDRESS = '0x11b815efb8f581194ae79006d24e0d814b7697f6'

# Used to calculate the analytics for multiple pools. Eg. percentage of transactions that got sandwiched. etc.
POOLS_BIPS_30 = ['0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8', '0x4e68ccd3e89f51c3074ca5072bbac773960dfa36']
POOLS_BIPS_5 = ['0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640', '0x11b815efb8f581194ae79006d24e0d814b7697f6']

BLOCK_HEAD = 13959999
SWAP_TOPIC = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
CSV_PATH = "output"
PARQUET_PATH = "pqoutput"
LOGS_PATH = "logs_13950000_13959999"
BLOCKS_PATH = "blocks_13950000_13959999"
TRANSACTIONS_PATH = "transactions_13950000_13959999"
TOKEN_TRANSFER_PATH = "token_transfers_13950000_13959999"
RECEIPTS_PATH = "receipts_13950000_13959999"

SWAP_ABI = [
    {
        'field': 'sender',
        'type': 'hex',
        'index': True,
    },
    {
        'field': 'recipient',
        'type': 'hex',
        'index': True,
    },
    {
        'field': 'amount0',
        'type': 'int',
        'index': False,
    },
    {
        'field': 'amount1',
        'type': 'int',
        'index': False,
    },
    {
        'field': 'sqrtPriceX96',
        'type': 'uint',
        'index': False,
    },
    {
        'field': 'liquidity',
        'type': 'uint',
        'index': False,
    },
    {
        'field': 'tick',
        'type': 'int',
        'index': False,
    },
]

class Transactions:
    @staticmethod
    def convert_csv_to_parquet(file_name):
        df = pd.read_csv(os.path.join(CSV_PATH, file_name + '.csv'))
        df.sort_values(by=['block_number', 'transaction_index'])
        df.to_parquet(os.path.join(PARQUET_PATH, file_name + '.gzip'), compression='gzip', engine='pyarrow')

    @staticmethod
    def load_from_parquet(file_name, min_block):
        return pd.read_parquet(os.path.join(PARQUET_PATH, file_name + '.gzip'), filters=[('block_number','>', min_block)])

class Blocks:
    @staticmethod
    def convert_csv_to_parquet(file_name):
        df = pd.read_csv(os.path.join(CSV_PATH, file_name + '.csv'))
        df.sort_values(by=['number'])
        df.to_parquet(os.path.join(PARQUET_PATH, file_name + '.gzip'), compression='gzip', engine='pyarrow')

    @staticmethod
    def load_from_parquet(file_name, min_block):
        return pd.read_parquet(os.path.join(PARQUET_PATH, file_name + '.gzip'), filters=[('number', '>', min_block)])

class TokenTransfers:
    @staticmethod
    def convert_csv_to_parquet(file_name):
        df = pd.read_csv(os.path.join(CSV_PATH, file_name + '.csv'))
        df.sort_values(by=['block_number'])
        df.to_parquet(os.path.join(PARQUET_PATH, file_name + '.gzip'), compression='gzip', engine='pyarrow')

    @staticmethod
    def load_from_parquet(file_name, min_block):
        return pd.read_parquet(os.path.join(PARQUET_PATH, file_name + '.gzip'), filters=[('block_number', '>', min_block)])

class Logs:
    @staticmethod
    def convert_csv_to_parquet(file_name):
        df = pd.read_csv(os.path.join(CSV_PATH, file_name + '.csv'))
        df.sort_values(by=['block_number'])
        df.to_parquet(os.path.join(PARQUET_PATH, file_name + '.gzip'), compression='gzip', engine='pyarrow')

    @staticmethod
    def load_from_parquet(file_name, min_block):
        return pd.read_parquet(os.path.join(PARQUET_PATH, file_name + '.gzip'), filters=[('block_number', '>', min_block)])

class Receipts:
    @staticmethod
    def convert_csv_to_parquet(file_name):
        df = pd.read_csv(os.path.join(CSV_PATH, file_name + '.csv'))
        df.sort_values(by=['block_number'])
        df.to_parquet(os.path.join(PARQUET_PATH, file_name + '.gzip'), compression='gzip', engine='pyarrow')

    @staticmethod
    def load_from_parquet(file_name, min_block):
        return pd.read_parquet(os.path.join(PARQUET_PATH, file_name + '.gzip'), filters=[('block_number', '>', min_block)])

class Helpers:
    # Helper function to extract the data by masking the bytes into 256bit chunks
    @staticmethod
    def bit_mask(data, masks, signed, field_names):
        data = int(data, 16)
        results = []
        masks.reverse()
        signed.reverse()
        for i in range(len(masks)):
            l = masks[i]
            mask = (1 << l) - 1
            if signed[i]:
                bytes = int.to_bytes(data & mask, length=32, signed=False, byteorder='big')
                results.append(int.from_bytes(bytes, signed=True, byteorder='big'))
            else:
                results.append(data & mask)
            data >>= l
        results.reverse()
        signed.reverse()
        masks.reverse()
        return pd.Series(results, index=field_names)

class LogWrapper:

    @staticmethod
    def extract_multiple_logs(df, addresses, topic, abi):
        indexes = LogWrapper.extract_indexes(abi)
        # Filter for address
        address_logs = df.loc[df['address'].isin(list(addresses))].sort_values(by = ['address','block_number'])
        splitted_topics = address_logs['topics'].str.strip('"').str.split(',', expand=True)
        no_topics = len(splitted_topics)
        topics_arr = ['topic1', 'topic2', 'topic3', 'topic4']
        address_logs[topics_arr[:no_topics]] = address_logs['topics'].str.strip('"').str.split(',', expand=True)
        # Filter by topic
        topic_logs = address_logs.loc[address_logs['topic1'] == topic]

        NUM_TOPICS = 4
        # Rename Topics
        for i in range(len(indexes)):
            topic_logs = topic_logs.rename(columns={'topic'+str(i + 2): indexes[i]})
        for i in range((NUM_TOPICS - 1) - len(indexes)):
            topic_logs.drop(columns={'topic' + str(NUM_TOPICS - i)})

        # Drop original topics column
        topic_logs = topic_logs.drop(columns={'topics'})
        # Extract data
        final_logs = LogWrapper.extract_data(topic_logs, abi)
        return final_logs

    @staticmethod
    def extract_indexes(abi):
        indexes = []
        for field in abi:
            if field['index']:
                indexes.append(field['field'])
        return indexes

    # df: logs dataframe of logs to extract data from
    # topic: The key of the topic to extract
    # abi: The abi that defines the data structure of topics and data
    # address: The address of the contract to extract the logs for
    @staticmethod
    def extract_logs(df, address, topic, abi):
        indexes = LogWrapper.extract_indexes(abi)
        # Filter for address
        address_logs = df.loc[df['address'] == address]
        splitted_topics = address_logs['topics'].str.strip('"').str.split(',', expand=True)
        no_topics = len(splitted_topics.columns)
        topics_arr = ['topic1', 'topic2', 'topic3', 'topic4']
        address_logs[topics_arr[:no_topics]] = address_logs['topics'].str.strip('"').str.split(',', expand=True)
        # Filter for topic
        topic_logs = address_logs.loc[address_logs['topic1'] == topic]

        NUM_TOPICS = 4
        # Rename Topics
        for i in range(len(indexes)):
            topic_logs = topic_logs.rename(columns={'topic'+str(i + 2): indexes[i]})
        for i in range((NUM_TOPICS - 1) - len(indexes)):
            topic_logs.drop(columns={'topic' + str(NUM_TOPICS - i)})

        # Drop original topics column
        topic_logs = topic_logs.drop(columns={'topics'})
        # Extract data
        final_logs = LogWrapper.extract_data(topic_logs, abi)
        return final_logs

    # extracts data from the data field in a df
    # abi: specifies what is being stored in the data field.
    @staticmethod
    def extract_data(df, abi):
        field_names = []
        signed = []
        lengths = []
        for field in abi:
            if field['index']:
                continue
            else:
                lengths.append(256)
                field_names.append(field['field'])
                if field['type'] == 'int':
                    signed.append(True)
                else:
                    signed.append(False)
        new_data = df.apply(lambda row: Helpers.bit_mask(row['data'], lengths, signed, field_names), axis=1)
        df = pd.concat([df, new_data], axis=1)
        df = df.drop(columns={'data'})
        return df

if __name__ == '__main__':
    argumentList = sys.argv[1:]
    # Options
    options = "lb:lt:ltt:llogs:lr"
        
    try:
        # Parsing argument
        # checking each argument
        for currentArgument in argumentList:
            if currentArgument in ("-lb"):
                Blocks.convert_csv_to_parquet(BLOCKS_PATH)
            elif currentArgument in ("-lt"):
                Transactions.convert_csv_to_parquet(TRANSACTIONS_PATH)
            elif currentArgument in ("-ltt"):
                TokenTransfers.convert_csv_to_parquet(TOKEN_TRANSFER_PATH)
            elif currentArgument in ("-llogs"):
                Logs.convert_csv_to_parquet(LOGS_PATH)
            elif currentArgument in ("-lr"):
                Receipts.convert_csv_to_parquet(RECEIPTS_PATH)

    except getopt.error as err:
        # output error, and return with an error code
        print (str(err))

    # Last N blocks
    N = sys.argv[1]
    earliest_block = BLOCK_HEAD - int(N)
    
    #
    #
    # 1. LOAD DATA
    #
    #

    # We load up all the data. Currently, only logs are used.
    # ts: pd.DataFrame = Transactions.load_from_parquet(TRANSACTIONS_PATH, earliest_block)
    blocks = Blocks.load_from_parquet(BLOCKS_PATH, earliest_block)
    # tt = TokenTransfers.load_from_parquet(TOKEN_TRANSFER_PATH, earliest_block)
    logs = Logs.load_from_parquet(LOGS_PATH, earliest_block)
    receipts = Receipts.load_from_parquet(RECEIPTS_PATH, earliest_block)

    #
    #
    # 2. COUNT SANDWICH ATTACKS OF A SINGULAR POOL
    #
    #

    v3_single_pool_logs = LogWrapper.extract_logs(logs, POOL_ADDRESS, SWAP_TOPIC, SWAP_ABI)
    v3_single_pool_logs.to_csv('log_extract.csv')

    def check_sandwich(v3_logs, output_file):
        # In a sandwich attack, the attacker will sandwich T1 and T3 between a transaction of a viction T2.

        # 1_shifted will be T_2
        # We shift by one, so that we can analyze the 'victim' transaction. 
        logs_one_shifted = v3_logs[['amount0', 'amount1', 'block_number', 'recipient', 'sender']].shift(1, axis = 0).rename(columns={'amount0': 'amount0_1_shifted', 'amount1': 'amount1_1_shifted', 'block_number': 'block_number_1', 'recipient': 'recipient_1', 'sender': 'sender_1'})

        # 1_shifted will be T_1
        # We shift by two, so that we can check if the transaction amounts are similar in the transaction after.
        logs_two_shifted = v3_logs[['amount0', 'amount1', 'block_number', 'recipient', 'sender', 'address']].shift(2, axis = 0).rename(columns={'amount0': 'amount0_2_shifted', 'amount1': 'amount1_2_shifted', 'block_number': 'block_number_2', 'recipient': 'recipient_2', 'sender': 'sender_2', 'address': 'address_2'})
        
        v3_merged = pd.concat([v3_logs, logs_one_shifted, logs_two_shifted], axis=1)

        # Filter out trades that are in the same direction. Only when T3 and T1 are opposite signs
        v3_merged_filter = v3_merged.loc[((v3_merged['amount0'] > 0) & (v3_merged['amount0_2_shifted'] < 0 )) | (( v3_merged['amount0'] < 0 )& (v3_merged['amount0_2_shifted'] > 0))]
        # Only take trades when T2 and T3 are opposite signs.
        v3_merged_filter = v3_merged_filter.loc[((v3_merged_filter['amount0'] > 0) & (v3_merged_filter['amount0_1_shifted'] < 0 )) | (( v3_merged_filter['amount0'] < 0 ) & (v3_merged_filter['amount0_1_shifted'] > 0))]
        # Only take trades when they are in the same block
        v3_merged_filter = v3_merged_filter.loc[(v3_merged_filter['block_number'] == v3_merged_filter['block_number_1']) & (v3_merged_filter['block_number'] == v3_merged_filter['block_number_2'])]
        # Only take trades when they are in the same addresses
        v3_merged_filter = v3_merged_filter.loc[v3_merged_filter['address'] == v3_merged_filter['address_2']]

        # Filter out trades so that T1 and T3 have similar magnitudes.
        # T1 + T3 = difference
        # (T1 - T3)/ 2 = average
        # 2*(T1 + T3)/(T1-T3) = 'variance'
        # If 'variance' < threshold, then we consider that trade a sandwich attack.
        SIMILARITY_THRESHOLD = 0.02
        v3_check_sandwich = v3_merged_filter.loc[abs(((v3_merged_filter['amount0'] + v3_merged_filter['amount0_2_shifted']) * 2) / (v3_merged_filter['amount0'] - v3_merged_filter['amount0_2_shifted'])) < SIMILARITY_THRESHOLD]

        v3_check_sandwich[['amount0', 'amount0_1_shifted', 'amount0_2_shifted', 'sender', 'recipient', 'sender_2', 'recipient_2']].to_csv(output_file + '.csv')

        # Alternate method of detecting sandwich attacks: terrible way, because of the amount of router contracts out there.
        v3_check_sandwich_recipient = v3_merged_filter.loc[(v3_merged_filter['recipient'] == v3_merged_filter['recipient_2']) | (v3_merged_filter['recipient_2'] == v3_merged_filter['sender']) | (v3_merged_filter['recipient'] == v3_merged_filter['sender_2']) | (v3_merged_filter['sender'] == v3_merged_filter['sender_2'])]

        return (v3_check_sandwich, v3_check_sandwich_recipient)
    
    (single_sandwich, single_alternate_sandwich) = check_sandwich(v3_single_pool_logs, 'sandwich')

    single_alternate_sandwich[['amount0', 'amount0_1_shifted', 'amount0_2_shifted', 'sender', 'recipient']].to_csv('alternate_sandwich' + '.csv')

    print('#')
    print('# SINGULAR POOL SANDWICH ATTACKS DATA')
    print('#')
    print("Number of attacks in the past N blocks, if we just check recipient and sender: ", len(single_alternate_sandwich))
    print("Number of attacks in the past N blocks: ", len(single_sandwich))
    print("Percentage of swaps that got sandwiched: ", str(round(len(single_sandwich) / len(v3_single_pool_logs) * 100, 2)) + '%')

    #
    #
    # 3. COUNT SANDWICH ATTACKS OF A LIST OF POOLS
    #
    #

    print('#')
    print('# MULTIPLE POOL SANDWICH ATTACKS DATA, 30 BIPS fee pools (ETH/USDC and ETH/USDT)')
    print('#')

    logs_30_bips = LogWrapper.extract_multiple_logs(logs, POOLS_BIPS_30, SWAP_TOPIC, SWAP_ABI)
    (sandwich_30_bips, sandwich_30_bips_alternate) = check_sandwich(logs_30_bips, 'sandwich_30_bips')

    print("Number of attacks in the past N blocks, same recipient method: ", len(sandwich_30_bips_alternate))
    print("Number of attacks in the past N blocks, similar amounts method: ", len(sandwich_30_bips))
    print("Percentage of swaps that got sandwiched, similar amounts method: ", str(round(len(sandwich_30_bips) / len(logs_30_bips) * 100, 2)) + '%')


    print('#')
    print('# MULTIPLE POOL SANDWICH ATTACKS DATA, 5 BIPS fee pools (ETH/USDC and ETH/USDT)')
    print('#')

    logs_5_bips = LogWrapper.extract_multiple_logs(logs, POOLS_BIPS_5, SWAP_TOPIC, SWAP_ABI)
    (sandwich_5_bips, sandwich_5_bips_alternate) = check_sandwich(logs_5_bips, 'sandwich_5_bips')

    sandwich_5_bips_alternate[['amount0', 'amount0_1_shifted', 'amount0_2_shifted', 'sender', 'recipient']].to_csv('alternate_sandwich_bips5' + '.csv')

    print("Number of attacks in the past N blocks, same recipient method: ", len(sandwich_5_bips_alternate))
    print("Number of attacks in the past N blocks, similar amounts method: ", len(sandwich_5_bips))
    print("Percentage of swaps that got sandwiched, similar amounts method: ", str(round(len(sandwich_5_bips) / len(logs_5_bips) * 100, 2)) + '%')

    #
    #
    # 4. CALCULATE AVERAGE GAS
    #
    #

    # Average Gas Price = SUM(Gas Used * Effective Gas Price)/SUM(GAS Used)
    receipts = receipts[['gas_used', 'effective_gas_price']]
    weighted_mean_gas = sum(receipts['effective_gas_price'] * receipts['gas_used']) / (receipts['gas_used'].sum())
    median_gas = receipts[['effective_gas_price']].median(axis = 1).iloc[0]
    print('#')
    print('# BLOCKS MEAN DATA')
    print('#')
    print("The mean gas used in the past N blocks is: ", weighted_mean_gas)
    print("The median gas used in the past N blocks is: ", median_gas)