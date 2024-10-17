import json
import datetime
import boto3
import os
import random

def lambda_handler(event, context):
    print(f'LF0: Start, event{event}')
    
    dynamo = boto3.client('dynamodb')
    
    lex = boto3.client('lexv2-runtime', region_name='us-east-1')
    lex_resp = lex.recognize_text(
        botId= os.environ['BOT_ID'],
        botAliasId= os.environ['BOT_ALIAS'],
        localeId='en_US',
        sessionId='user_lambda_lf0',
        text=event['messages'][0]['unstructured']['text']
    )
    
    messages = []
    
    if '@gmail.com' in event['messages'][0]['unstructured']['text']:
        response = dynamo.get_item(
            TableName="search-history",
            Key={
                "Email": {
                    "S": f"{event['messages'][0]['unstructured']['text']}{random.randint(-1, 5)}"
                }
            }
        )
        print('Item' in response)
        if 'Item' in response:
            history_reply = f"Here is a result from one of your recent search: \n 1.{response['Item']['Name']['S']}, located at {response['Item']['Address']['S']} with {round(float(response['Item']['Rating']['S']))} star rating."
            messages.append({
                "type": "unstructured",
                "unstructured":{
                    "id": context.aws_request_id,
                    "text": history_reply,
                    "timestamp": datetime.datetime.utcnow().isoformat()
                }
            })
            
    for message in lex_resp['messages']:
        messages.append({
            "type": "unstructured",
            "unstructured":{
                "id": context.aws_request_id,
                "text": message['content'],
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
        })
    
    response = {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'messages': messages
    }
    
    return response