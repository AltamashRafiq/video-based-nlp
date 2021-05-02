import boto3
from decimal import Decimal

def sentiments_to_dynamo(key, mixed, negative, neutral, positive, sentiment, table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.put_item(
       Item={
            'partitionKey': key,
            'Mixed': Decimal(str(mixed)),
            'Negative': Decimal(str(negative)),
            'Neutral': Decimal(str(neutral)),
            'Positive': Decimal(str(positive)),
            'created': Decimal(str(0)), 
            'Sentiment': sentiment
        }
    )
    return response


if __name__ == '__main__':
    resp = sentiments_to_dynamo("0000", Decimal('0.1'), Decimal('0.3'), Decimal('0.3'), Decimal('0.5'), "POSITIVE", 'youtube-sentiments')
    print("Put movie succeeded:")
    print(resp)