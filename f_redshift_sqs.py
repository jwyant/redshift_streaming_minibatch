import json
import boto3
from datetime import datetime

sqs = boto3.resource('sqs')

def lambda_handler(event, context):
    current_timestamp = datetime.utcnow().isoformat().replace(':','')
    t1 = event['arguments']
    queue = sqs.get_queue_by_name(QueueName=t1[0][0])

    s3_objects = []
    while True:
        messages = queue.receive_messages(MessageAttributeNames=['All'], MaxNumberOfMessages=10, VisibilityTimeout=60)
        if len(messages) == 0:
            break
        for message in messages:
            message_body = json.loads(message.body)
            try:
                records = message_body['Records']
            except KeyError as e:
                print('KeyError', e)
                pass
            for rec in records:
                s3_objects.append('s3://{0}/{1}'.format(rec['s3']['bucket']['name'], rec['s3']['object']['key']))
            message.delete()
    if len(s3_objects) > 0:
        entries_dict = dict(
                entries = [dict(url=x, mandatory=True) for x in s3_objects]
            )

        s3 = boto3.client('s3')
        s3.put_object(
            Body=json.dumps(entries_dict),
            Bucket='jwyant-bigdatablog',
            Key='stg_bikedata_manifests/{}.json'.format(current_timestamp)
        )
        ret = dict()
        ret['success'] = True
        ret['results'] = ['s3://jwyant-bigdatablog/stg_bikedata_manifests/{}.json'.format(current_timestamp)]
    else:
        ret = dict()
        ret['success'] = False
        ret['error_msg'] = "No Items in SQS Queue"
    ret_json = json.dumps(ret)
    return ret_json