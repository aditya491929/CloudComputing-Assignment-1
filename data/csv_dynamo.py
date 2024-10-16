import time
import boto3
import pandas as pd
from decimal import Decimal

import json

# Open and read the JSON file
with open('credentials.json', 'r') as json_file:
    data = json.load(json_file)

# upload to DynamoDB
AWS_DB_REGION = 'us-east-1'
AWS_TABLE_NAME = 'yelp-restaurants'
AWS_PRIMARY_KEY = 'Restaurant_ID'
CSV_FILE = 'Yelp_Restaurants.csv'
CSV_HEAD = [AWS_PRIMARY_KEY, 'Name', 'Cuisine', 'Rating', 'TotalReviews',
            'Address', 'ZipCode', 'Latitude', 'Longitude', 'IsClosed',
            'TimeStamp']


YELP_REQ_CUISINES = ['italian', 'chinese', 'cuban', 'french',
                     'korean', 'thai', 'japanese', 'lebanese',
                     'vietnamese', 'indian']

def uploadToDynamoDB():
    print ('=========== start uploading data to DynamoDB ===========')
    # init csv file and AWS
    yelp_csv = pd.read_csv(CSV_FILE)
    dynamodb = boto3.client(
        'dynamodb', 
        region_name=AWS_DB_REGION, 
        aws_access_key_id=data['AWS_DYNAMO_ACCESS_KEY_ID'],
        aws_secret_access_key=data['AWS_DYNAMO_SECRET_ACCESS_KEY']
    )

    # init counting var
    cuisine_last = str(yelp_csv[CSV_HEAD[2]][0])
    cuisine_type = 1
    cuisine_count = 0
    total_count = 0
    start_time = time.time()
    point_time = start_time

    # upload
    for i in range(len(yelp_csv)):
        cuisine_curr = str(yelp_csv[CSV_HEAD[2]][i])
        cuisine_count += 1
        total_count += 1
        item = {
            CSV_HEAD[0]: {'S': str(yelp_csv[CSV_HEAD[0]][i])},
            CSV_HEAD[1]: {'S': str(yelp_csv[CSV_HEAD[1]][i])},
            CSV_HEAD[2]: {'S': str(yelp_csv[CSV_HEAD[2]][i])},
            CSV_HEAD[3]: {'S': str(Decimal(yelp_csv[CSV_HEAD[3]][i].astype(Decimal)))},
            CSV_HEAD[4]: {'S': str(Decimal(yelp_csv[CSV_HEAD[4]][i].astype(Decimal)))},
            CSV_HEAD[5]: {'S': str(yelp_csv[CSV_HEAD[5]][i])},
            CSV_HEAD[6]: {'S': str(yelp_csv[CSV_HEAD[6]][i])},
            CSV_HEAD[7]: {'S': str(yelp_csv[CSV_HEAD[7]][i])},
            CSV_HEAD[8]: {'S': str(yelp_csv[CSV_HEAD[8]][i])},
            CSV_HEAD[9]: {'S': str(yelp_csv[CSV_HEAD[9]][i].astype(Decimal))},
            CSV_HEAD[10]: {'S': str(yelp_csv[CSV_HEAD[10]][i])}
        }

        dynamodb.put_item(
            TableName=AWS_TABLE_NAME, 
            Item=item, 
            ReturnValues='NONE'
        )

        # finish uploading a cuisine type
        if cuisine_curr != cuisine_last:
            now = time.time()
            print ('(%d/%d) cuisine: "%s" uploaded, time spent: %ds, total time: %ds, current item: %d, total item: %d' % (
                cuisine_type, len(YELP_REQ_CUISINES), cuisine_last,
                int(now - point_time), int(now - start_time),
                cuisine_count, total_count))
            point_time = now
            cuisine_last = cuisine_curr
            cuisine_count = 0
            cuisine_type += 1

    # finish uploading last cuisine type
    print ('(%d/%d) cuisine: "%s" uploaded, time spent: %ds, total time: %ds, current item: %d, total item: %d' % (
        cuisine_type, len(YELP_REQ_CUISINES), cuisine_curr,
        int(now - point_time), int(now - start_time),
        cuisine_count, total_count))

    print ('=========== uploading data to DynamoDB done ===========')


uploadToDynamoDB()