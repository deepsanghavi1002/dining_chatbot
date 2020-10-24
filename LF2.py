import json
import boto3
from botocore.vendored import requests
from boto3.dynamodb.conditions import Key, Attr
from elasticsearch import Elasticsearch, RequestsHttpConnection
from boto3.dynamodb.conditions import Key, Attr
import requests
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import logging
import random

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)



def lambda_handler(event, context):

    # TODO implement
    # 1. pulls a message from the SQS queue
    # Create SQS client
    sqs = boto3.client('sqs')

    queue_url = "https://sqs.us-east-1.amazonaws.com/772764957281/sqs" #response['QueueUrl']
    message = None

    logger.debug('event:', event )

    message = event['Records'][0]#['Messages'][0]
    print("message ====",message)
    receipt_handle = message['receiptHandle']
    # Delete received message from queue
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )

    location = message['messageAttributes']['location']
    cuisine = message['messageAttributes']['cuisine']
    #dining_date =  message['messageAttributes']['dining_date']['StringValue']
    dining_time = message['messageAttributes']['dining_time']
    num_people = message['messageAttributes']['num_people']
    phone =  message['messageAttributes']['phone']
    print('location===', location)

    credentials = boto3.Session().get_credentials()
    region = "us-east-1"
    service = "es"
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        service,
        session_token=credentials.token,
    )
    host = "search-restaurants-dll3qvepilpnq4smjnd3usrjy4.us-east-1.es.amazonaws.com"

    es = Elasticsearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
    )
    dynamodb = boto3.resource('dynamodb',region_name= "us-east-1")
    table = dynamodb.Table('yelp-restaurant')
    #count = es.count(index="restaurants", doc_type="restaurants", body ={"query": {"match": {"Cuisine":str(cuisine['stringValue']) }}})

    #print("Count" , count)
    print("cuisine to be searched = ",cuisine['stringValue'])
    data = es.search(index="restaurants", doc_type="restaurants", body ={"query": {"match": {"Cuisine":str(cuisine['stringValue'])}}})

    count = int(data['hits']['total']['value'])
    randNos = random.sample(range(1, count), 5)
    print(count,randNos,"=====")

    temp = []
    sendMessage = "Hello! For {}, we recommend :".format(location['stringValue'])
    for count,index in enumerate(randNos):

        Business_ID = (data['hits']['hits'][index]['_source']['Business_ID'])
        # # search DynamoDB using Business_ID
        response = table.query(KeyConditionExpression=Key('Business_ID').eq(Business_ID))

        name = response['Items'][0]['Name']
        address = response['Items'][0]['Address']
        num_reviews = response['Items'][0]['Num_of_Reviews']
        rating = response['Items'][0]['Rating']
        msg = str(count+1)+"-"+str(name)+" located at "+str(address)+" with "+str(num_reviews)+" reviews and average rating of "+str(rating)  + "stars\n"
        sendMessage+=msg




    print('final message: ',sendMessage)


    sns = boto3.client('sns')

    # Publish a simple message to the specified SNS topic
    response = sns.publish(
         TopicArn='arn:aws:sns:us-east-1:772764957281:sns_dining',
         Message=sendMessage,
     )

    print('message sent')
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda LF2!')
    }
