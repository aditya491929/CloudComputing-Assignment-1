import json
import datetime
import boto3

def lambda_handler(event, context):
    print(event['messages'][0]['unstructured']['text'])
    lex = boto3.client('lexv2-runtime', region_name='us-east-1')
    lex_resp = lex.recognize_text(
        botId='6DHKZBKUND',
        botAliasId='GQ66W8VOMK',
        localeId='en_US',
        sessionId='user_lambda_lf0',
        text=event['messages'][0]['unstructured']['text']
    )
    messages = []
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

