#!/usr/bin/env python3
import boto3
import snappy
import json
import sys
import pprint
import base64
import random
import time
from itertools import chain, islice
from multiprocessing.pool import ThreadPool, Pool
from contextlib import closing

def get_matching_s3_prefixes(bucket, prefix='', delimiter='/'):
    """
    Thanks https://alexwlchan.net/2019/07/listing-s3-keys/ !
    Generate the common prefixes in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param delimiter: use this delimter for "folders" (optional).
    """
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket, 'Delimiter': delimiter}

    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:
        resp = s3.list_objects_v2(**kwargs)
        for common_prefix in resp['CommonPrefixes']:
            prefix = common_prefix['Prefix']
            yield prefix
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Thanks https://alexwlchan.net/2019/07/listing-s3-keys/ !
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield key

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

def stream_s3_snappy_object_line_by_line_generator(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj['Body']
    with closing(body):
        decompressor = snappy.hadoop_snappy.StreamDecompressor()
        last_line = ''
        try:
            while True:
                chunk = last_line+decompressor.decompress(next(body)).decode("utf-8")
                chunk_by_line = chunk.split('\n')
                last_line = chunk_by_line.pop()
                for line in chunk_by_line:
                    yield line
        except StopIteration:
            yield last_line
            return

def stream_data_from_key(bucket, current_key, scale_index):
    if current_key.endswith('.json.snappy'):
        print('Worker {0} - Sending data from: s3://{1}/{2}'.format(str(scale_index).zfill(3), bucket, current_key))

        s3 = boto3.client('s3')
        mysnappystream = stream_s3_snappy_object_line_by_line_generator(bucket=bucket, key=current_key)
        i = 0
        records = []
        while True:
            try:
                rec = next(mysnappystream)
            except StopIteration:
                break
            try:
                rec_dict = json.loads(rec)
                # bike id has a max of 5 digits in the data I sampled, so I'm just adding a 6+ digit to it.
                rec_dict['bikeid'] = int(rec_dict['bikeid'])+100000*int(scale_index)
            except json.decoder.JSONDecodeError:
                continue
            try:
                record = dict(
                    Data = json.dumps(rec_dict),
                    PartitionKey = str(rec_dict['bikeid'])
                )
            except KeyError:
                continue
            records.append(record)
            i += 1
            if i%100 == 0:
                kinesis_client = boto3.client('kinesis', region_name='us-east-1')
                response = kinesis_client.put_records(Records=records, StreamName='bikedata')
                records = []
        return True
    else:
        return False

#def stream_data(bucket, prefix, scale_index):
def stream_data(bucket_prefix_scale_tuple):
    bucket = bucket_prefix_scale_tuple[0]
    prefix = bucket_prefix_scale_tuple[1]
    scale_index = bucket_prefix_scale_tuple[2]
    random_wait = random.uniform(0,5)
    print('Worker {0} - Created, waiting {1} secs'.format(str(scale_index).zfill(3), random_wait))
    time.sleep(random_wait)
    bikedates = get_matching_s3_prefixes(bucket=bucket, prefix=prefix)
    for bikedate in bikedates:
        for current_key in get_matching_s3_keys(bucket, bikedate):
            stream_data_from_key(bucket, current_key, scale_index)

def main(bucket, prefix, scale):
    print('in main')
    pool = ThreadPool(scale)
    #pool = Pool(scale)
    print(scale, 'thread pool made')
    pool.map(stream_data, [(bucket, prefix, x) for x in range(0, scale+1)])
    print('pool map')
    pool.close()
    print('pool\'s closed')

if __name__ == "__main__":
    try:
        scale = int(sys.argv[1])
    except IndexError:
        scale = 1
    main('jwyant-bigdatablog', 'bikedata_json/', scale)