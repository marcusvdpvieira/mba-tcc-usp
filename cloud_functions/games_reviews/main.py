from google.cloud import bigquery
import pandas as pd
import datetime as dt
import requests
import json

# Closest game name match with Steam games
## Legend's Ascent ('1001') LIKE Ascend: Reborn ('978520')
## Cosmic Crusade ('1002') LIKE Cosmic Star Heroine ('256460')
## Neon Nexus ('1003') LIKE Neon White ('1533420')
## Shadow's Veil ('1004') LIKE Shadows: Awakening ('585450')
## Ethernal Heart ('1005') LIKE Ocean's Heart ('1393750')

games_ids = ['978520', '256460', '1533420', '585450', '1393750']

def send_games_reviews(event, context):

  reviews_df = pd.DataFrame([])

  try:
    # Create a BigQuery client
    client = bigquery.Client()

    # Get the BigQuery dataset and table references
    dataset_ref = client.dataset('customer_support_bronze')
    table_ref = dataset_ref.table('reviews_raw') 

    for i in games_ids:

      # Get Steam game reviews
      r = requests.get(r"https://store.steampowered.com/appreviews/{}?json=1&language=all".format(i))

      reviews_json = r.json()

      row = reviews_json['query_summary']

      row['gameId'] = i
      row['fetchedAt'] = str(dt.datetime.today())

      # Insert the row into the BigQuery table
      client.insert_rows_json(table_ref, [row])

    return 'BigQuery table overwritten successfully', 200
  except Exception as e:
    print('Failed to overwrite BigQuery table:', e)
    return 'Failed to overwrite BigQuery table', 500	
