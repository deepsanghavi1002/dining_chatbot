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

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# LF2 as a queue worker
# Whenever it is invoked by the CloudWatch event trigger that runs every minute:
# 1. pulls a message from the SQS queue,
# 2. gets a random restaurant recommendation for the cuisine collected through conversation from ElasticSearch and DynamoDB,
# 3. formats them and
# 4. sends them over text message to the phone number included in the SQS message, using SNS

# choose the elasticsearch index: "restaurants" is the easier version, "predictions" is with the ML service
search_index = "restaurants"
#search_index = "predictions"


def lambda_handler(event, context):
    # TODO implement
    print("Testing CloudWatch: Call LF2 every minute.")
    # 1. pulls a message from the SQS queue
    # Create SQS client
    sqs = boto3.client('sqs')
    # # Get URL for SQS queue
    #response = sqs.get_queue_url(QueueName='DiningRequest')
    queue_url = "https://sqs.us-east-1.amazonaws.com/772764957281/sqs" #response['QueueUrl']
    message = None
    #Receive a message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
          'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=30,
        WaitTimeSeconds=0
    )
    logger.debug('event:', event )
    print(event)
    message = event['Records'][0]#['Messages'][0]
    receipt_handle = message['receiptHandle']
    # Delete received message from queue
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )
    # #print('Received and deleted message: %s' % message)
    # # 2. gets a random restaurant recommendation for the cuisine collected through conversation from ElasticSearch
    print(message['Body'])
    # # all information stored in sqs queue
    print(message)
    location = message['messageAttributes']['location']
    cuisine = message['messageAttributes']['cuisine']
    dining_date =  message['messageAttributes']['dining_date']['StringValue']
    dining_time = message['messageAttributes']['dining_time']
    num_people = message['messageAttributes']['num_people']
    phone =  message['messageAttributes']['phone']
    # #print(location, cuisine, dining_date, dining_time, num_people, phone)
    # # use http request to search ElasticSearch index: restaurant
    sendMessage = None
    # search_index = "restaurants"
    # #if search_index == "restaurants":
    print('inside')
    # pick a restaurant randomly, get the business_ID (always pick first one here, need further work)

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
    #count = es.count(index="restaurants", doc_type="restaurants", body={ "query": {"match_all" : { }}})

    #print("Count" , count)
    #print("cuisine to be searched = ",cuisine)
    data = es.search(index="restaurants", doc_type="restaurants", body ={"query": {"match": {"Cuisine":"Chinese"}}, 'size': 10})

    Business_ID = (data['hits']['hits'][0]['_source']['Business_ID'])

    #res = es.search(index="restaurants", body=)
    #print("index from elasticsearch", res)
    #Business_ID = "01m-oqxaCifh0s0eU03bBg" #data['hits']['hits'][0]['_source']['Business_ID']




    # # 3. get more information from DynamoDB
    # # search DynamoDB using Business_ID
    dynamodb = boto3.resource('dynamodb',region_name= "us-east-1")
    table = dynamodb.Table('yelp-restaurant')
    response = table.query(KeyConditionExpression=Key('Business_ID').eq(Business_ID))
    print("dynamo============",response)
    # # 4. format the message
    name = response['Items'][0]['Name']
    address = response['Items'][0]['Address']
    num_reviews = response['Items'][0]['Num_of_Reviews']
    rating = response['Items'][0]['Rating']
    print(name,address,num_reviews,rating)
    sendMessage = "Hello! For {}, we recommend the {} {} restaurant on {}. The place has {} of reviews and an average score of {} on Yelp. Enjoy!".format(location['stringValue'], name, cuisine['stringValue'], address, num_reviews, rating)
    #print('final message: ',sendMessage)


    else:
    # pick a restaurant randomly, get the business_ID (always pick first one here, need further work)

    Business_ID = data['hits']['hits'][0]['_source']['Business_ID']
    print(Business_ID)
    # 3. get more information from DynamoDB
    # search DynamoDB using Business_ID
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurant')
    response = table.query(
       KeyConditionExpression=Key('Business_ID').eq(Business_ID)
    )
    # 4. format the message
    name = response['Items'][0]['Name']
    address = response['Items'][0]['Address']
    num_reviews = response['Items'][0]['Num_of_Reviews']
    rating = response['Items'][0]['Rating']
    sendMessage = "Hello! For {}, we recommend the {} {} restaurant on {}. The place has {} of reviews and an average score of {} on Yelp. Enjoy!".format(location, name, cuisine, address, num_reviews, rating)
    print(sendMessage)
    # 5. send the message using SNS
    # Create SQS client

    sns = boto3.client('sns')

    # Publish a simple message to the specified SNS topic
    # response = sns.publish(
    #     TopicArn='arn:aws:sns:us-east-1:772764957281:sns_dining',
    #     Message=sendMessage,
    # )

    # print('message sent')
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda LF2!')
    }
