import json
import boto3
from botocore.client import Config

KMS_KEY_ALIAS = 'smylee_com'
QUEUE_NAME = 'smylee_com_email'


def lambda_handler(event, context):

    try:

        message_file = '/tmp/message.txt'

        # Get the event info
        s3_key = event['Records'][0]['s3']['object']['key']
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_key_new = s3_key.replace('raw', 'encrypt')

        # download raw message
        s3_resource = boto3.resource('s3')
        s3_resource.Object(s3_bucket, s3_key).download_file(message_file)

        # kms resolver
        kms_client = boto3.client('kms')
        response = kms_client.describe_key(
            KeyId='alias/{0}'.format(KMS_KEY_ALIAS)
        )
        kms_arn = response['KeyMetadata']['Arn']
        if kms_arn is None:
            raise Exception('No KMS ARN found for the KMS name provided.')

        # get message contents
        with open(message_file, 'r') as f:
            email_payload = f.read()

        # put message back up encrypted
        s3_client = boto3.client('s3', config=Config(signature_version='s3v4'))
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key_new,
            Body=email_payload,
            SSEKMSKeyId=kms_arn,
            ServerSideEncryption='aws:kms'
        )

        # delete raw message
        s3_client.delete_object(
                Bucket=s3_bucket,
                Key=s3_key
            )

        # Create the message
        message = {
            "bucket": s3_bucket,
            "key": s3_key_new
        }
        message_output = json.dumps(message)

        # get the queue URL
        sqs_client = boto3.client('sqs')
        response = sqs_client.get_queue_url(
            QueueName=QUEUE_NAME
        )

        # Send message to SQS
        sqs_client.send_message(
            QueueUrl=response['QueueUrl'],
            MessageBody=message_output
        )
        return_message = "message sent to queue"
    except Exception as e:
        return_message = "there was an error: {0}".format(e)
        raise Exception(return_message)

    return return_message
