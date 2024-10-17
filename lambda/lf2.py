import boto3
import json
import os
import urllib3
from base64 import b64encode
import random

# Set environment variables for credentials and domain endpoint
OPENSEARCH_ENDPOINT = os.environ['ENDPOINT']
OPENSEARCH_USERNAME = os.environ['USERNAME']
OPENSEARCH_PASSWORD = os.environ['PASSWORD']
SENDER_EMAIL_ADDRESS = os.environ['SENDER_EMAIL']
QUEUE_URL = os.environ['QUEUE_URL']

def lambda_handler(event, context):
    print(f'LF2: Start')
    
    print('LF2: Polling SQS')
    sqs = boto3.client('sqs', region_name="us-east-1")
    
    response = sqs.receive_message(QueueUrl=QUEUE_URL, MessageAttributeNames=['All'])
    
    messages = response.get('Messages', [])
    
    for message in messages:
        message_body = message['Body']
        message_data = message['MessageAttributes']
        print(f'LF2: Processing Message: {message_data}')
        
        receipt_handle = message['ReceiptHandle']
        
        Cuisine = message_data['Cuisine']['StringValue']
        DiningDate = message_data['DiningDate']['StringValue']
        CountPeople = message_data['CountPeople']['StringValue']
        DiningTime = message_data['DiningTime']['StringValue']
        Location = message_data['Location']['StringValue']
        EmailAddr = message_data['EmailAddr']['StringValue']
    
        restaurant_id_List = get_restaurants_from_opensearch(Cuisine, 'restaurants')
        print(f'Handler1: {restaurant_id_List}')
        
        restaurant_details = get_restaurant_details_from_dynamo(restaurant_id_List, EmailAddr)
        print(f'Handler2: {restaurant_details}')
        
        send_email_using_ses(restaurant_details, EmailAddr, Cuisine, Location)
        print(f'Handler3: Done')
        
        sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=receipt_handle
        )
        print(f'LF2: Deleted SQS Message')
        
        return {
            'statusCode': 200,
            'body': json.dumps("LF2 running succesfully")
        }
    
def poll_sqs():
    print('LF2: Polling SQS')
    sqs = boto3.client('sqs', region_name="us-east-1")
    response = sqs.receive_message(QueueUrl=QUEUE_URL)
    messages = response.get('Messages', [])
    for message in messages:
        message_body = message['Body']
        receipt_handle = message['ReceiptHandle']
        print(f'LF2: Processing Message: {message_body}')

def get_restaurants_from_opensearch(cuisine, index):
    index_name = index  
    search_url = f"{OPENSEARCH_ENDPOINT}/{index_name}/_search"

    query = {
        "size": 25,
        "query": {
            "multi_match": {
                "query": cuisine,
                "fields": ["Cuisine"]
            }
        }
    }

    credentials = f"{OPENSEARCH_USERNAME}:{OPENSEARCH_PASSWORD}"
    encoded_credentials = b64encode(credentials.encode('utf-8')).decode('utf-8')
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {encoded_credentials}"
    }

    http = urllib3.PoolManager()
    
    response = http.request(
        'GET',
        search_url,
        body=json.dumps(query),
        headers=headers
    )
    
    search_results = json.loads(response.data.decode('utf-8'))
    matches = search_results['hits']['hits']
    sampled_matches = random.sample(matches, k=5)
    restaurantsIdList = [restaurant['_source']['Restuarant_ID'] for restaurant in sampled_matches]
    return restaurantsIdList
    

        
def get_restaurant_details_from_dynamo(restaurantIdList, email_addr):
    dynamo = boto3.client('dynamodb')
    final_restaurant_List = []
    for count, restaurantId in enumerate(restaurantIdList):
        response = dynamo.get_item(
            TableName="yelp-restaurants",
            Key={
                "Restaurant_ID": {
                    "S": restaurantId
                }
            }
        )
        final_restaurant_List.append(response['Item'])
        response['Item']['Email'] = {
            'S' : f'{email_addr}{count}'
        }
        print('Adding Data to search history')
        dynamo.put_item(TableName="search-history", Item=response['Item'])
    return final_restaurant_List
    
def send_email_using_ses(restaurant_detail_set, email_addr, cuisine, location):
    ses = boto3.client('ses', region_name='us-east-1')
    
    response = ses.list_identities(IdentityType='EmailAddress')
    email_identities = response['Identities']
    
    if email_addr not in email_identities:
        verifyEmailResponse = ses.verify_email_identity(EmailAddress=email_addr)
        return
    
    message = "Hi, Here is the list of top 5 {} restaurants at {} I found that mights suit you: \n\n".format(cuisine, location)
    message_restaurant = ""
    count = 1
    
    for restaurant in restaurant_detail_set:
        restaurantName = restaurant['Name']['S']
        restaurantAddress = restaurant['Address']['S']
        restaurantZip = restaurant['ZipCode']['S']
        reviewCount = restaurant['TotalReviews']['S']
        ratings = restaurant['Rating']['S']
        message_restaurant += str(count)+". {} located at {}, {} with {} star rating and {} reviews. \n\n".format(restaurantName, restaurantAddress, restaurantZip, round(float(ratings)), reviewCount)
        count += 1
    
    mailResponse = ses.send_email(
        Source=SENDER_EMAIL_ADDRESS,
        Destination={'ToAddresses': [email_addr]},
        Message={
            'Subject': {
                'Data': "Dining Conceirge Chatbot has a message for you!",
                'Charset': 'UTF-8'
            },
            'Body': {
                'Text': {
                    'Data': message+message_restaurant,
                    'Charset': 'UTF-8'
                },
                'Html': {
                    'Data': message+message_restaurant,
                    'Charset': 'UTF-8'
                }
            }
        }
    )