import json
import logging
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal


logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    table = boto3.resource('dynamodb').Table('polyver_database')
    logger.info(event)
    
    lat = event["lat"]
    long = event["long"]
    pkVal = event["rover"]
    
    
    newPos = [  long , lat  ]
    newPosjson = json.loads(json.dumps(newPos), parse_float=Decimal)
    
    table.update_item(
        Key={
            'pk': pkVal,
            'sk': 'info'
        },
        UpdateExpression="set pos = :o",
        ExpressionAttributeValues={
            ':o': newPosjson,
        },
        ReturnValues="UPDATED_NEW"
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('sucessfully updated gps coords')
    }