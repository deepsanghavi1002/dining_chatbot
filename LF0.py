import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    print('event start')
    print(event)
    print('event end')
    #
    client = boto3.client('lex-runtime')
    response = client.post_text(
        botName='chatbot',
        botAlias='chatbot_lex',
        userId='10',
        sessionAttributes={
            },
        requestAttributes={
        },
        inputText = event['messages'][0]['unstructured']['text']
    )
    print("Message from bot:" +response["message"])
    return {
        'statusCode': 200,
        'body': json.dumps(response["message"])
    }
