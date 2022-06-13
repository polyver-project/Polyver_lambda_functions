import json
import logging
import boto3
from boto3.dynamodb.conditions import Key


logger = logging.getLogger()
logger.setLevel(logging.INFO)




def update_table(table, event):
    pkVal = event['dynamodb']['Keys']['pk']['S']
    skVal = event['dynamodb']['Keys']['sk']['S']
    eventName = event['eventName']
    
    
    
    response = table.get_item(
        Key={
            'pk': pkVal,
            'sk': 'info'
        }
    )

    queueSize = response['Item']['queuesize']
    
    if(eventName == "INSERT"):
        queueSize+=1
    elif(eventName == "REMOVE"):
        queueSize-=1

    if(queueSize <= 1):
        newTime = 600
    elif(queueSize <= 5):
        newTime = 300
    elif(queueSize <= 30):
        newTime = 60
    else:
        newTime = 30

    

    table.update_item(
        Key={
            'pk': pkVal,
            'sk': 'info'
        },
        UpdateExpression="set timeslot = :o, queuesize = :q",
        ExpressionAttributeValues={
            ':o': newTime,
            ':q': queueSize
        },
        ReturnValues="UPDATED_NEW"
    )
    
    if(eventName == "INSERT"):
        table.update_item(
            Key={
                'pk': pkVal,
                'sk': skVal
            },
            UpdateExpression="set qpos = :o",
            ExpressionAttributeValues={
                ':o': queueSize,
            },
            ReturnValues="UPDATED_NEW"
        )
    elif(eventName == "REMOVE"):
        response = table.query(
            KeyConditionExpression=Key('pk').eq(pkVal) & Key('sk').gt(skVal)
        )
        logger.info(response)
        
        for item in response['Items']:
            table.update_item(
                Key={
                    'pk': pkVal,
                    'sk': item['sk']
                },
                UpdateExpression="set qpos = qpos - :o",
                ExpressionAttributeValues={
                    ':o': 1,
                },
                ReturnValues="UPDATED_NEW"
            )


def lambda_handler(event, context):
    table = boto3.resource('dynamodb').Table('polyver_database')
    logger.info(event)
    events =  event['Records']
    
    for curEvent in events:
        update_table(table, curEvent)

    return {
        'statusCode': 200,
        'body': json.dumps('sucessfully updated timeslot')
    }
