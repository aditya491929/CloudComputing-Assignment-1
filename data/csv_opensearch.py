import json
import boto3
import datetime
from opensearchpy import OpenSearch, RequestsHttpConnection
from io import BytesIO
import csv

host = 'search-restaurants-gsh6qt7r4ya3v26vjh57ykm4gi.us-east-1.es.amazonaws.com'

with open('credentials.json', 'r') as json_file:
    data = json.load(json_file)

es = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = (data['OPENSEARCH_USERNAME'], data['OPENSEARCH_PASSWORD']),
    port = 443,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)
print(es)

cuisines_to_include = ['italian', 'indian', 'chinese']
max_count = 50
restaurants_inserted = {"italian": 0, "indian": 0, "chinese": 0}

with open('Yelp_Restaurants.csv', newline='') as f:
    reader = csv.reader(f)
    next(reader)
    for restaurant in reader:
        cuisine = restaurant[2]

        if cuisine in cuisines_to_include and restaurants_inserted[cuisine] < max_count:
            index_data = {
                'Restuarant_ID': restaurant[0],
                'Cuisine': restaurant[2]
            }
            print ('dataObject', index_data)

            es.index(index="restaurants", body=index_data)
            restaurants_inserted[cuisine] += 1

        if all(count >= max_count for count in restaurants_inserted.values()):
            break