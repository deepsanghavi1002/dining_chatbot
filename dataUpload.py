import json
import boto3
import datetime
from botocore.vendored import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection


def lambda_handler(event, context):
    restaurantData = []
    cuisines = ['indian', 'italian', 'chinese', 'thai']
    locations = ["Manhattan"]
    if event['key'] == 'yelp':
        restaurantIterations = 20
        for cuisine in cuisines:
            for i in range(restaurantIterations):
                for location in locations:
                    requestData = {
                        "term": cuisine + " restaurants",
                        "location": location,
                        "limit": 50,
                        "offset": 50 * i
                    }
                    yelp_rest_endpoint = "https://api.yelp.com/v3/businesses/search"

                    querystring = requestData

                    payload = ""
                    headers = {
                        "Authorization": "Bearer QwDojskWQoyI4f9eXpRsMQmDSygM986lZQjll18_G78bvW8rHsVdSwEEjxujxrVfLY0MZaCRIfH6RGO9_6JJw7A94bfb-c98-1ilzOndM4XyVzRAPsjvbIl_ityDX3Yx",
                        'cache-control': "no-cache"
                    }

                    response = requests.request("GET", yelp_rest_endpoint, data=payload, headers=headers,
                                                params=querystring)
                    message = json.loads(response.text)
                    result = message['businesses']
                    restaurantData = restaurantData + result

        # Add data to DynamodDB
        dynamoInsert(restaurantData)

        # Add index data to the ElasticSearch
        addElasticIndex(restaurantData)

    return {
        'statusCode': 200,
        'body': json.dumps('success')
    }


def dynamoInsert(restaurants):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('yelp-restaurants')

    for restaurant in restaurants:

        tableEntry = {
            'id': restaurant['id'],
            'alias': restaurant['alias'],
            'name': restaurant['name'],
            'is_closed': restaurant['is_closed'],
            'categories': restaurant['categories'],
            'rating': int(restaurant['rating']),
            'review_count': int(restaurant['review_count']),
            'address': restaurant['location']['display_address']
        }

        if (restaurant['coordinates'] and restaurant['coordinates']['latitude'] and restaurant['coordinates']['longitude']):
            tableEntry['latitude'] = str(restaurant['coordinates']['latitude'])
            tableEntry['longitude'] = str(restaurant['coordinates']['longitude'])

        if (restaurant['location']['zip_code']):
            tableEntry['zip_code'] = restaurant['location']['zip_code']

        # Add necessary attributes to the yelp-restaurants table
        table.put_item(
            Item={
                'insertedAtTimestamp': str(datetime.datetime.now()),
                'id': tableEntry['id'],
                'name': tableEntry['name'],
                'address': tableEntry['address'],
                'latitude': tableEntry.get('latitude', None),
                'longitude': tableEntry.get('longitude', None),
                'review_count': tableEntry['review_count'],
                'rating': tableEntry['rating'],
                'zip_code': tableEntry.get('zip_code', None),
                'categories': tableEntry['categories']
            }
        )


def addElasticIndex(restaurants):
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
    host = 'search-restaurants-dll3qvepilpnq4smjnd3usrjy4.us-east-1.es.amazonaws.com'
    es = Elasticsearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    for restaurant in restaurants:
        index_data = {
            'id': restaurant['id'],
            'categories': restaurant['categories']
        }
        print('dataObject', index_data)

        es.index(index="restaurants", doc_type="restaurants", id=restaurant['id'], body=index_data, refresh=True)
