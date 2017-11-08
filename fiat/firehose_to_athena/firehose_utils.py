#! /usr/bin/env python
"""Firehose utils to create a stream."""
from glob import glob
from os import environ as env

import boto3
from botocore.errorfactory import ClientError

SCHEMA_PATH = env["SCHEMA_PATH"]

region_name = boto3.Session().region_name
firehose = boto3.client('firehose', region_name=region_name or 'us-east-1')


def get_schemas(path):
    """Grab all schemas from git subtree nes_schemas."""
    return glob(path)


def get_schema_from_path(path):
    """Match table from path."""
    return path.split('/')[-2]


def create_stream(event=None, context=None):
    """Create a simple stream."""
    responses = []
    for path in get_schemas(SCHEMA_PATH):
        stream = get_schema_from_path(path)
        print(stream)
        try:
            response = firehose.create_delivery_stream(
                DeliveryStreamName=stream,
                DeliveryStreamType='DirectPut',
                S3DestinationConfiguration=dict(
                    RoleARN=env['FIREHOSE_DELIVERY_ROLE_ARN'],
                    BucketARN=env['FIREHOSE_BUCKET_ARN'],
                    Prefix=f'{stream}/',
                    BufferingHints=dict(
                        SizeInMBs=1,
                        IntervalInSeconds=60,
                    ),
                    CompressionFormat='Snappy',
                    EncryptionConfiguration=dict(
                        NoEncryptionConfig='NoEncryption'
                    ),
                    CloudWatchLoggingOptions=dict(
                        Enabled=False
                    )
                )
            )
        except ClientError:
            response = f'{stream} in use'
        responses.append(response)
    return responses


if __name__ == '__main__':
    print(create_stream())
