import boto3

_dynamodb = boto3.resource('dynamodb', region_name="localhost", endpoint_url="http://localhost:8000",
                           aws_access_key_id="access_key_id", aws_secret_access_key="secret_access_key")

_table = _dynamodb.Table('harmondex-metadata')


def get(filename):
    res = _table.get_item(Key={'PK': filename})
    return res["Item"] if "Item" in res else None


def put(filename, metadata):
    _table.put_item(Item={'PK': filename, **metadata})
