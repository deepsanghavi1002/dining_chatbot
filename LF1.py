import json
import os
import time
import boto3
import math



import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }
    return response

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')

def build_validation_result(is_valid, violated_slot, message_content):
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

def validate_dining(location, cuisine, dining_time, num_people, phone):
    locations = ['new york', 'manhattan']
    if location is not None and location.lower() not in locations:
        return build_validation_result(False,
                                       'location',
                                       'Sorry! We do not serve recommendations for this location right now!')

    cuisines = ['indian', 'mexican','chinese', 'italian', ]
    if cuisine is not None and cuisine.lower() not in cuisines:
        return build_validation_result(False,
                                       'cuisine',
                                       'Sorry! We do not serve recommendations for this cuisine right now!')

    if num_people is not None:
        num_people = int(num_people)
        if num_people > 20 or num_people <= 0:
            return build_validation_result(False,
                                      'numberofpeople',
                                      'Number of people can only be between 0 and 20')

    if dining_time:
        hour, minute = dining_time.split(':')
        hour = parse_int(hour)
        minute = parse_int(minute)
        if math.isnan(hour) or math.isnan(minute):
            # Not a valid time; use a prompt defined on the build-time model.
            return build_validation_result(False, 'dining_time', 'Not a valid time')

    return build_validation_result(True, None, None)

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }
def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

def Dining_Suggestions(intent_request):

    try:

        location = get_slots(intent_request)['location']
        cuisine = get_slots(intent_request)['cuisine']
        #dining_date =  try_ex(lambda: intent_request['currentIntent']['slots']['DiningDate'])
        dining_time = get_slots(intent_request)['diningtime']
        num_people = get_slots(intent_request)['numberofpeople']
        phone = get_slots(intent_request)['phonenumber']
        session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}

        requestData = {
                    "location": location,
                    "cuisine":cuisine,
                    "dining_time":dining_time,
                    "num_people":str(num_people),
                    "phone": phone
                }

        session_attributes['requestData'] = json.dumps(requestData)

    except:
        return
    #
    if intent_request['invocationSource'] == 'DialogCodeHook':
        # Validate any slots which have been specified.  If any are invalid, re-elicit for their value
        slots = get_slots(intent_request)
        validation_result = validate_dining(location, cuisine, dining_time, num_people, phone)
        #
        if not validation_result['isValid']:
            print("elicit slots")
            slots = intent_request['currentIntent']['slots']
            slots[validation_result['violatedSlot']] = None

            return elicit_slot(
                session_attributes,
                intent_request['currentIntent']['name'],
                slots,
                validation_result['violatedSlot'],
                validation_result['message']
            )
        return delegate(session_attributes, intent_request['currentIntent']['slots'])


    # push info to SQS: location, cuisine, dining_date, dining_time, num_people
    # Create SQS client
    sqs = boto3.client('sqs')
    # Get URL for SQS queue
    #response = sqs.get_queue_url(QueueName='DiningRequest')
    #queue_url = response['QueueUrl']
    queue_url = "https://sqs.us-east-1.amazonaws.com/772764957281/sqs"
    print(queue_url)
    print(location, cuisine, dining_time, num_people, phone)
    # Send message to SQS queue
    # supported 'DataType': string, number, binary
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageAttributes={
            'location': {
                'DataType': 'String',
                'StringValue': location
            },
            'cuisine': {
                'DataType': 'String',
                'StringValue': cuisine
            },
            #'dining_date': {
             #   'DataType': 'String',
              #  'StringValue': dining_date
            #},
            'dining_time': {
               'DataType': 'String',
               'StringValue': dining_time
            },
            'num_people': {
                'DataType': 'Number',
                'StringValue': num_people
            },
            'phone': {
                'DataType': 'Number',
                'StringValue': str(phone)
            }
        },

        MessageBody=(
            'Information about user inputs of Dining Chatbot.'
        )
    )
    print("SQS messageID:"+str(response['MessageId']))

    res_msg = "Thanks for your information. Our recommendation will be sent to your phone shortly!"
    return close(session_attributes,
                'Fulfilled',
                {'contentType': 'PlainText',
                'content': res_msg})

def Greeting(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Hi there, how can I help you?'
        }
    )

def Thank_You(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'You are welcome! Thanks for using the service. See you next time!'
        }
    )

def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """
    #logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))
    intent_name = intent_request['currentIntent']['name']
    #intent_name = "DiningSuggestionsIntent"

    # Dispatch to your bot's intent handlers
    if intent_name == 'GreetingIntent':
        return Greeting(intent_request)
    elif intent_name == "DiningSuggestionsIntent":
        return Dining_Suggestions(intent_request)
    elif intent_name == "ThankYouIntent":
        return Thank_You(intent_request)
    raise Exception('Intent with name ' + intent_name + ' not supported')

def lambda_handler(event, context):
    # TODO implement
    #return {
    #   'statusCode': 200,
    #  'body': json.dumps('Hello from Lambda!')
    #}
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    #logger.debug('event.chatbot.name={}'.format(event['chatbot']['name']))
    return dispatch(event)
