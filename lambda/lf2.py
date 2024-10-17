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

def lambda_handler(event, context):
    print(event)
    Cuisine = event['Records'][0]['messageAttributes']['Cuisine']['stringValue']
    DiningDate = event['Records'][0]['messageAttributes']['Date']['stringValue']
    CountPeople = event['Records'][0]['messageAttributes']['Nos']['stringValue']
    DiningTime = event['Records'][0]['messageAttributes']['Time']['stringValue']
    Location = event['Records'][0]['messageAttributes']['Location']['stringValue']
    EmailAddr = event['Records'][0]['messageAttributes']['Email']['stringValue']
    
    restaurant_id_List = get_restaurants_from_opensearch(Cuisine, 'restaurants')
    print(f'Handler1: {restaurant_id_List}')
    
    restaurant_details = get_restaurant_details_from_dynamo(restaurant_id_List)
    print(f'Handler2: {restaurant_details}')
    
    send_email_using_ses(restaurant_details, EmailAddr, Cuisine, Location)
    print(f'Handler3: Done')
    return {
        'statusCode': 200,
        'body': json.dumps("LF2 running succesfully")
    }

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
    

        
def get_restaurant_details_from_dynamo(restaurantIdList):
    dynamo = boto3.client('dynamodb')
    final_restaurant_List = []
    for restaurantId in restaurantIdList:
        response = dynamo.get_item(
            TableName="yelp-restaurants",
            Key={
                "Restaurant_ID": {
                    "S": restaurantId
                }
            }
        )
        final_restaurant_List.append(response['Item'])
    return final_restaurant_List
    
def send_email_using_ses(restaurant_detail_set, email_addr, cuisine, location):
    ses = boto3.client('ses', region_name='us-east-1')
    
    response = ses.list_identities(IdentityType='EmailAddress')
    email_identities = response['Identities']
    
    if email_addr not in email_identities:
        verifyEmailResponse = ses.verify_email_identity(EmailAddress=email_addr)
        return
    
    message = "Hi, Here is the list of top 5 {} restaurants at {} I found that mights suit you: ".format(cuisine, location)
    message_restaurant = ""
    count = 1
    
    for restaurant in restaurant_detail_set:
        restaurantName = restaurant['Name']['S']
        restaurantAddress = restaurant['Address']['S']
        restaurantZip = restaurant['ZipCode']['S']
        reviewCount = restaurant['TotalReviews']['S']
        ratings = restaurant['Rating']['S']
        message_restaurant += str(count)+". {} located at {}, {} with Ratings of {} and {} reviews. \n\n".format(restaurantName, restaurantAddress, restaurantZip, ratings, reviewCount)
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