import json
import boto3
import logging
import re
import base64
from botocore.client import Config

# Set logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Set Static Variables
MY_VERIFIED_EMAIL = ''
EMAIL_BUCKET = 'smylee.com.emails'
QUEUE_NAME_PREFIX = 'smylee_com_email'
MESSAGE_FILE = '/tmp/message.txt'


def lambda_handler(event, context):

    # clients and resources
    sqs_client = boto3.client('sqs')
    ses_client = boto3.client('ses')
    s3_client = boto3.client('s3', config=Config(signature_version='s3v4'))

    # get the queue info
    list_of_queues = sqs_client.list_queues(
        QueueNamePrefix=QUEUE_NAME_PREFIX
    )
    queue_url = list_of_queues['QueueUrls'][0]

    # get messages for the queue url
    messages = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        VisibilityTimeout=30
    )

    # if there were no messages returned stop run
    while 'Messages' in messages:

        # loop through all the messages received
        for message in messages['Messages']:

            # get info about message
            receipt_handle = message['ReceiptHandle']
            message_body_raw = message['Body']
            message_body = json.loads(message_body_raw)
            bucket = message_body['bucket']
            message_key = message_body['key']

            logging.info("processing: {0}".format(message_key))

            # get email file from s3
            s3_client.download_file(bucket, message_key, MESSAGE_FILE)

            # decrypt ses: cannot do it in python
            # http://docs.aws.amazon.com/kms/latest/developerguide/services-ses.html#services-ses-decrypt
            with open(MESSAGE_FILE, 'r') as f:
                email_payload = f.read()

            # set re to use multiline
            re.multiline = True

            # get the from email address
            compile_from = re.compile('((\nFrom:).+\n)')
            original_from_full_raw = compile_from.search(email_payload)

            original_from_full = original_from_full_raw.group(1)

            # change original "From:" to "Reply-To:"
            original_from_full = re.sub('From:', 'Reply-To:', original_from_full)

            compile_from_raw = re.compile('<.+>')
            from_email_bkup = compile_from_raw.search(original_from_full)

            # remove <> from email
            raw_email = from_email_bkup.group(0)[1:-1]

            # replace all instances of the orginial "from" with static email addy
            email_payload = re.sub('({0})'.format(raw_email), MY_VERIFIED_EMAIL, email_payload)

            # replace "From:" with static email and add new "Reply-To:"
            email_payload = re.sub('((\nFrom:).+\n)', '\nFrom: Patty Ramert <{0}>{1}'.format(MY_VERIFIED_EMAIL, original_from_full), email_payload)

            # send email messages through ses
            response = ses_client.send_raw_email(
                Source=MY_VERIFIED_EMAIL,
                Destinations=[
                    MY_VERIFIED_EMAIL
                ],
                RawMessage={
                    'Data': email_payload
                }
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:

                # delete message; email processed ok.
                response = sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )
                logger.info(response['ResponseMetadata'])

        # get messages for the queue url
        messages = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            VisibilityTimeout=30
        )

    logger.info('No messages in queue')
    return 'No messages in queue'

