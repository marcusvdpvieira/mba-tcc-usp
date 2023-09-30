# Runtime : Python 3.11
# Entry point : send_games_transactions

from google.cloud import bigquery
import numpy as np
import random as rd
import datetime as dt
import time
import uuid

game_ids = ['1001', '1001', '1002', '1003', '1003', '1003', '1004', '1005']
user_ids = range(1000, 9999)
transaction_type = ['premiumcoin', 'normalcoin', 'normal_coin', 'comestic_item'] # Insertion of uncleaned data


# Cloud Function handler
def send_games_transactions(event, context):
    try:
        # Create a BigQuery client
        client = bigquery.Client()

        # Get the BigQuery dataset and table references
        dataset_ref = client.dataset('games_bronze')
        table_ref = dataset_ref.table('transactions_raw') 

        # Create a BigQuery table row
        for i in range(rd.randint(1,3)):

            transactions_values = [np.random.uniform(1,10), np.random.uniform(1,10), np.random.uniform(1,30), np.random.uniform(1,100)]
            
            row = {
            'transactionId': str('transaction_' + str(dt.date.today().day) + str(dt.datetime.today().hour) + uuid.uuid4().hex),
            'gameId': rd.choice(game_ids),
            'userId': rd.choice(user_ids),
            'eventTimestamp': str(dt.datetime.today() - dt.timedelta(minutes=rd.randint(1,10), seconds=rd.randint(1,59))),
            'transactionType': rd.choice(transaction_type),
            'value': round(rd.choice(transactions_values), 2) 
            }

            # Insert the row into the BigQuery table
            client.insert_rows_json(table_ref, [row])

            time.sleep(rd.choice(range(1,10)))

        return 'Event sent to BigQuery table successfully', 200
    except Exception as e:
        print('Failed to send event to BigQuery table:', e)
        return 'Failed to send event to BigQuery table', 500
