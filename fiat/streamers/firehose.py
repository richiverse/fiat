#! /usr/bin/env python
"""Firehose writer."""
# from base64 import b64encode
from json import dumps

import boto3
from boto3.compat import six


def _init(**kwargs):
    """Initialize region for firehose client."""
    return boto3.client(
        'firehose',
        region_name=boto3.Session().region_name or 'us-east-1',
        **kwargs
    )


def write(client, stream_name: str, data: list) -> dict:
    """Write the data to the same stream."""
    if not isinstance(data, (list, six.binary_type)):
        raise(TypeError)

    """
    utf_encoded = [
        dumps(item).encode('utf-8') #str(item).encode('utf-8')
        for item in data
        if not isinstance(item, six.binary_type)
    ]

    b64_encoded = [
        b64encode(item).decode('utf-8')
        for item in utf_encoded
    ]
    """

    params = dict(
        DeliveryStreamName=stream_name,
        Records=[dict(Data=dumps(final) + '\n') for final in data]
    )

    response = client.put_record_batch(**params)
    return response
