import boto3

dynamodb = boto3.resource('dynamodb', region_name="localhost", endpoint_url="http://localhost:8000",
                          aws_access_key_id="access_key_id", aws_secret_access_key="secret_access_key")

table = dynamodb.create_table(
    TableName='harmondex-metadata',
    KeySchema=[
        {
            'AttributeName': 'PK',
            'KeyType': 'HASH'
        },
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'PK',
            'AttributeType': 'S'
        },
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 1,
        'WriteCapacityUnits': 1
    }

)
print(table)
