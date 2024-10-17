import boto3
import datetime
import dateutil.parser
import json
import math
from zoneinfo import ZoneInfo

def lambda_handler(event, context):
    return search_intent(event)


def search_intent(event_info):
    intent_name = event_info['sessionState']['intent']['name']
    print(f'LF1: Start, intent name: {intent_name}')
    if intent_name == 'GreetingIntent':
        return greeting_intent(event_info)
    elif intent_name == 'DiningSuggestionIntent':
        return dining_suggestions_intent(event_info)
    elif intent_name == 'ThankYouIntent':
        return thank_you_intent(event_info)

    raise Exception('Intent with name ' + intent_name + ' not supported')


def greeting_intent(event_info):
    return {
        'sessionState': {
            'dialogAction': {
                "type": "ElicitIntent",
                'message': {
                    'contentType': 'PlainText',
                    'content': 'Hi there, how can I help?'
                }
            }
        }
    }


def thank_you_intent(event_info):
    return {
        'sessionState': {
            'dialogAction': {
                "type": "ElicitIntent",
                'message': {
                    'contentType': 'PlainText',
                    'content': 'You are welcome!'
                }
            }
        }
    }


def dining_suggestions_intent(event_info):
    slots = get_slots(event_info)
    print(f'LF1: DiningSuggestionIntent, event info: {event_info}')
    location = slots.get("Location", {}).get('value', {}).get('interpretedValue', None) if slots['Location'] != None else slots['Location']
    cuisine = slots.get("Cuisine", {}).get('value', {}).get('interpretedValue') if slots['Cuisine'] != None else slots['Cuisine']
    count_people = slots.get("Nos", {}).get('value', {}).get('interpretedValue') if slots['Nos'] != None else slots['Nos']
    date = slots.get("Date", {}).get('value', {}).get('interpretedValue') if slots['Date'] != None else slots['Date']
    time = slots.get("Time", {}).get('value', {}).get('interpretedValue') if slots['Time'] != None else slots['Time']
    email = slots.get("Email", {}).get('value', {}).get('interpretedValue') if slots['Email'] != None else slots['Email']

    source = event_info['invocationSource']
    print(f'LF1: Source: {source}')
    if source == 'DialogCodeHook':
        validation_result = validate_slots(location, cuisine, count_people, date, time)
        print(f'LF1: Validation Result: {validation_result}')
        if not validation_result['isValid']:
            print(f'LF1: Eliciting Slot')
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(event_info['sessionState']['sessionAttributes'],
                               event_info['sessionState']['intent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        output_session_attributes = event_info['sessionState'].get('sessionAttributes', {})
        if None not in [location, cuisine, count_people, date, time, email]:
            print(f'LF1: Sending details to SQS')
            send_data_to_sqs(cuisine, location, email, date, time, count_people, event_info)
        print(f'LF1: Delegating control')
        return delegate_return(output_session_attributes, slots)

def send_data_to_sqs(cuisine, location, email, date, time, count_people, event_info):
    # Send data to SQS
    sqs = boto3.client('sqs')
    sqs_url = 'https://sqs.us-east-1.amazonaws.com/390844783720/Q1'
    attributes = {
        'Cuisine': {
            'DataType': 'String',
            'StringValue': cuisine
        },
        'DiningDate': {
            'DataType': 'String',
            'StringValue': date
        },
        'CountPeople': {
            'DataType': 'Number',
            'StringValue': count_people
        },
        'DiningTime': {
            'DataType': 'String',
            'StringValue': time
        },
        'Location': {
            'DataType': 'String',
            'StringValue': location
        },
        'EmailAddr': {
            'DataType': 'String',
            'StringValue': email
        }
    }
    sqs.send_message(QueueUrl=sqs_url, MessageBody="message from LF1", MessageAttributes=attributes)

def get_slots(event_info):
    return event_info['sessionState']['intent']['slots']


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'intent': {
                'name': 'DiningSuggestionIntent',
            },
            'dialogAction': {
                'type': 'Close',
                'fulfillmentState': fulfillment_state,
                'message': message
            }
        }
    }

    return response


def validation_res(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitSlot',
                'slotToElicit': slot_to_elicit,
            },
            'intent': {
                'name': intent_name,
                'slots': slots,
                'state': 'InProgress'
            }
        },
        'messages': [
                message
        ]
    }


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def delegate_return(session_attributes, slots):
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'intent': {
                'name': 'DiningSuggestionIntent',
                'slots': slots,
                'state': 'InProgress'
            },
            'dialogAction': {
                'type': 'Delegate',
                'slots': slots
            }
        }
    }


def validate_slots(location, cuisine, count_people, date, time):
    cuisines = ['italian', 'chinese', 'korean', 'thai', 'vietnamese', 'indian', 'japanese', 'lebanese', 'cuban', 'french']
    if cuisine is not None and cuisine.lower() not in cuisines:
        return validation_res(False, 'Cuisine', '{} cuisine not found. Please try one of the following cuisines {}!'.format(cuisine, ', '.join(cuisines)))
        
    locations = ['manhattan', 'new york', 'nyc', 'new york city', 'ny']
    if location is not None and location.lower() not in locations:
        return validation_res(False, 'Location', 'We do not have suggestions for "{}", try a different city'.format(location))

    if count_people is not None:
        count_people = int(count_people)
        if count_people > 20 or count_people < 0:
            return validation_res(False, 'Nos', 'Maximum 20 people allowed. Please try again.')

    if date is not None:
        if datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return validation_res(False, 'Date', 'Sorry, the date entered is incorrect. Please enter a future date.')

    if time is not None:
        if len(time) != 5:
            return validation_res(False, 'Time', None)

        hour, minute = time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        
        current_time = datetime.datetime.now(ZoneInfo("America/New_York")).strftime("%H:%M")
        current_hour, current_minute = current_time.split(':')
        current_hour = parse_int(current_hour)

        if (hour < 7 or hour > 21) or (hour < current_hour+1):
            return validation_res(False, 'Time', 'We accept reservations between 7 AM and 9 PM. We don’t accept reservations if they are not made at least 1 hour in advance. Please specify a time within this range.')

    return validation_res(True, None, None)