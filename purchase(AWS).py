import pandas as pd
import json
import random
import time
import hashlib
import boto3

# Define constants and static data
stream_name = 'stream_ecommerce'
region = 'us-west-2'
KinesisClient = boto3.client('kinesis', region_name=region, aws_access_key_id='', aws_secret_access_key='')

cities = ['Los Ángeles', 'Concepción', 'Santiago']
payment_online = ['Credit_card', 'Debit_card']
payment_store = ['Cash', 'Credit_card', 'Debit_card']
source = ['Online', 'On-site']
status_purchase = ['COMPLETED', 'FAILED_CHECKOUT', 'FAILED_API_RESPONSE', 'INSUFFICIENT_FUNDS', 'COMPLETED',
                   'COMPLETED', 'COMPLETED', 'COMPLETED', 'COMPLETED', 'COMPLETED', 'FAILED_API_RESPONSE',
                   'INSUFFICIENT_FUNDS', 'USER_ERROR', 'FRAUD', 'COMPLETED', 'COMPLETED', 'COMPLETED']

la_city = [(-37.46827078074566, -72.35255427015483)]
conce_city = [(-36.82574208790205, -73.0484756767045)]
stgo_city = [(-33.41811053129037, -70.6069033367)]

# Functions to get information
def get_pay_method(source, status_purchase, payment_online, payment_store):
    if source == 'On-site':
        payment = random.choice(payment_store)
        status = 'COMPLETED'
        order_type = 'STORE'
    elif source == 'Online':
        payment = random.choice(payment_online)
        status = random.choice(status_purchase)
        order_type = 'ONLINE'
    return payment, status, order_type

def get_coords(city):
    city_coords = {'Los Ángeles': random.choice(la_city),
                   'Concepción': random.choice(conce_city),
                   'Santiago': random.choice(stgo_city)}
    return city_coords.get(city, (0, 0))

# Read the product file
df = pd.read_excel('products.xlsx')

# Function to generate a random purchase
def generate_random_purchase(x, df, source, status_purchase, payment_online, payment_store, cities):
    date = pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')
    product = df['PRODUCT_NAME'][random.randint(0, 14)]
    price = df[df['PRODUCT_NAME'] == product]['PRICING'].values[0]
    brand = df[df['PRODUCT_NAME'] == product]['BRAND'].values[0]
    category = df[df['PRODUCT_NAME'] == product]['CATEGORY'].values[0]
    source_choice = random.choice(source)
    pay = get_pay_method(source_choice, status_purchase, payment_online, payment_store)
    city = random.choice(cities)

    purchase = {
        'purchase_id': str(hashlib.sha256(f"{x} {product} {price} {date} {source_choice} {pay[1]}".encode('utf-8')).hexdigest()[:10]),
        'product_name': product,
        'price': str(price),
        'Payment_Method': pay[0],
        'Status': pay[1],
        'Order_Type': pay[2],
        'City': city,
        'Latitude': str(get_coords(city)[0]),
        'Longitude': str(get_coords(city)[1]),
        'Source': source_choice,
        'Brand': brand,
        'Category': category,
        'Created_at': date
    }

    return purchase

x = 0
data_purchase = []

# Infinite loop for continuously generating and sending random purchases
while True:
    purchase = generate_random_purchase(x, df, source, status_purchase, payment_online, payment_store, cities)
    record_value =  json.dumps(purchase).encode('utf-8')
    records = KinesisClient.put_record(StreamName=stream_name, Data=record_value, PartitionKey='purchase_id')
    print("Total data ingested:" + str(x) + ", ReqID:" + records['ResponseMetadata']['RequestId'] +
          ", HTTPStatusCode:" + str(records['ResponseMetadata']['HTTPStatusCode']))

    x += 1
    
    time.sleep(random.choice([1, 1.5]))
