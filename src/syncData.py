import boto3
import json

def assume_role(region):
    """Assume the Cross-Account Role in PSDS to get temporary credentials."""
    sts_client = boto3.client("sts")

    response = sts_client.assume_role(
        RoleArn="arn:aws:iam::637423318347:role/PLS-KB-CrossAccountRole", # Role in PSDS Account
        RoleSessionName="SyncPlsData"
    )

    credentials = response["Credentials"]

    return boto3.client(
        "dynamodb",
        region_name=region,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"]
    )

# Get the DynamoDB clients for both regions
dynamodb_psds_us = assume_role("us-east-1")
dynamodb_psds_eu = assume_role("eu-west-1")

# Table names
ASIN_OPTIONS_TABLE = "asin-options-data"
PRODUCT_SUPPORT_TABLE = "product-support-asins"

def process_asin_options_data(dynamodb_client, product_key, new_workflow_id, ajuda_guid):
    response = dynamodb_client.get_item(
        TableName=ASIN_OPTIONS_TABLE,
        Key={'productKey': {'S': product_key}}
    )

    if 'Item' in response:
        existing_item = response['Item']

        if 'contentSupportAttributes' in existing_item and 'M' in existing_item['contentSupportAttributes']:
            existing_workflow_id = existing_item['contentSupportAttributes']['M'].get('workflowId', {}).get('S')

            if existing_workflow_id != new_workflow_id:
                existing_item['contentSupportAttributes']['M']['workflowId'] = {'S': new_workflow_id}
                dynamodb_client.put_item(
                    TableName=ASIN_OPTIONS_TABLE,
                    Item=existing_item
                )
                print(f"Updated workflowId for {product_key}")
            else:
                print(f"WorkflowId for {product_key} is already up to date")
    else:
        new_item = {
            'productKey': {'S': product_key},
            'contentSupportAttributes': {
                'M': {
                    'ajudaGUID': {'S': ajuda_guid},
                    'workflowId': {'S': new_workflow_id}
                }
            }
        }
        dynamodb_client.put_item(
            TableName=ASIN_OPTIONS_TABLE,
            Item=new_item
        )
        print(f"Inserted new item for {product_key}")

def process_product_support_asins(dynamodb_client, product_key):
    response = dynamodb_client.get_item(
        TableName=PRODUCT_SUPPORT_TABLE,
        Key={'productKey': {'S': product_key}}
    )

    if 'Item' in response:
        existing_item = response['Item']
        content_metadata = existing_item.get('contentSupportMetadata', {}).get('M', {})

        # Extract asinMetadata and productAttributesMetadata correctly
        asin_metadata = content_metadata.get('asinMetadata', {}).get('M', None)
        product_attr_metadata = content_metadata.get('productAttributesMetadata', {}).get('M', None)

        is_available = None
        if asin_metadata:
            is_available = asin_metadata.get('isAvailable', {}).get('BOOL', None)

        #  If productAttributesMetadata exists but asinMetadata is missing, create it
        if product_attr_metadata and asin_metadata is None:
            print(f" Creating asinMetadata for {product_key}")

            # Ensure contentSupportMetadata exists
            if 'contentSupportMetadata' not in existing_item:
                existing_item['contentSupportMetadata'] = {'M': {}}

            existing_item['contentSupportMetadata']['M']['asinMetadata'] = {
                'M': {'isAvailable': {'BOOL': True}}
            }
            dynamodb_client.put_item(
                TableName=PRODUCT_SUPPORT_TABLE,
                Item=existing_item
            )
            print(f"Created asinMetadata for {product_key}")

        #  If asinMetadata exists but isAvailable is False, update it
        elif is_available is False:
            print(f" Updating asinMetadata.isAvailable to True for {product_key}")
            existing_item['contentSupportMetadata']['M']['asinMetadata']['M']['isAvailable'] = {'BOOL': True}
            dynamodb_client.put_item(
                TableName=PRODUCT_SUPPORT_TABLE,
                Item=existing_item
            )
            print(f" Updated asinMetadata.isAvailable to True for {product_key}")

        else:
            print(f"No update required in product-support-asins for {product_key}")
    #productKey is not exists so creating new entry
    else:
        print(f" Creating new entry in product-support-asins for {product_key}")
        new_item = {
            'productKey': {'S': product_key},
            'contentSupportMetadata': {
                'M': {
                    'asinMetadata': {'M': {'isAvailable': {'BOOL': True}}}
                }
            }
        }
        dynamodb_client.put_item(
            TableName=PRODUCT_SUPPORT_TABLE,
            Item=new_item
        )
        print(f" Inserted new item with asinMetadata.isAvailable=True for {product_key}")


def lambda_handler(event, context):
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            new_image = record['dynamodb']['NewImage']

            asin = new_image['asin']['S']
            marketplace_id = new_image['marketplaceid']['S']
            product_key = f"{asin}#{marketplace_id}"
            workflow_id = new_image['workflowId']['S']
            ajuda_guid = new_image['ajudaGUID']['S']

            na_marketplaces = ["ATVPDKIKX0DER", "A2EUQ1WTGCTBG2", "A1AM78C64UM0Y8"]
            region = "us-east-1" if marketplace_id in na_marketplaces else "eu-west-1"
            dynamodb_client = dynamodb_psds_us if region == "us-east-1" else dynamodb_psds_eu

            process_asin_options_data(dynamodb_client, product_key, workflow_id, ajuda_guid)
            process_product_support_asins(dynamodb_client, product_key)

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda execution completed!')
    }
