# Runtime : Python 3.11
# Entry point : send_games_sales

from google.cloud import bigquery
import datetime as dt
import requests
import random as rd
import uuid

# Closest game name match with Steam games
## Legend's Ascent ('1001') LIKE Legend of Keepers: Career of a Dungeon Manager ('978520')
## Cosmic Crusade ('1002') LIKE Cosmic Star Heroine ('256460')
## Neon Nexus ('1003') LIKE Neon White ('1533420')
## Shadow's Veil ('1004') LIKE Shadows: Awakening ('585450')
## Ethernal Heart ('1005') LIKE Ocean's Heart ('1393750')

games_ids = ['978520', '978520', '256460', '1533420', '1533420', '1533420', '585450', '1393750']

sales_chances = [rd.randint(1,2), rd.randint(2,3), rd.randint(2,3), rd.randint(2,4)]

def send_games_sales(event, context):

  try:
    # Create a BigQuery client
    client = bigquery.Client()

    # Get the BigQuery dataset and table references
    dataset_ref = client.dataset('sales_bronze')
    table_ref = dataset_ref.table('sales_raw') 

    for i in range(rd.choice(sales_chances)):

      game_id_choice = rd.choice(games_ids)

      r = requests.get(r"http://store.steampowered.com/api/appdetails?appids={}".format(game_id_choice))

      sales_json = r.json()

      row = sales_json[game_id_choice]['data']

      row['saleId'] = str('sale_' + str(dt.date.today().day) + str(dt.datetime.today().hour) + uuid.uuid4().hex)
      row['purchasedAt'] = str(dt.datetime.today() - dt.timedelta(minutes=rd.randint(1,9), seconds=rd.randint(1,59)))

      new_row = {
          "name": row['name'],
          "steam_appid": row['steam_appid'],
          "purchasedAt": row['purchasedAt'],
          "currency": row['price_overview']['currency'],
          "initial": row['price_overview']['initial'],
          "final": row['price_overview']['final'],
          "discount_percent": row['price_overview']['discount_percent'],
          "initial_formatted": row['price_overview']['initial_formatted'],
          "final_formatted": row['price_overview']['final_formatted'],
          "saleId": row['saleId']
      }

      # Insert the row into the BigQuery table
      client.insert_rows_json(table_ref, [new_row])
    
    return 'Event sent to BigQuery table successfully', 200
  except Exception as e:
    print('Failed to send event to BigQuery table:', e)
    return 'Failed to send event to BigQuery table', 500
