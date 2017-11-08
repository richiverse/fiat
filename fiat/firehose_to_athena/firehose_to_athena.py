#! /usr/bin/env python
"""This function was literally translated from a node js implementation.

This takes a log from one s3 bucket and transfers it to another bucket in the
format that Athena needs.
"""
import urllib
from os import environ

import boto3

region_name = boto3.Session().region_name
s3 = boto3.client('s3', region_name=region_name)

ATHENA_BUCKET = environ["ATHENA_BUCKET"]


def main(event=None, context=None):
    """Add dt= to whatever bucket you have set in your ATHENA_BUCKET env."""
    for record in event['Records']:

        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        ukey = urllib.parse.unquote(key)
        meta = ukey.split('/')

        part = dict(
            subfolder=meta[0],
            year=meta[1],
            month=meta[2],
            day=meta[3],
            hour=meta[4],
            name=meta[5],
        )

        target = (
            f'{part["subfolder"]}/dt={part["year"]}-'
            f'{part["month"]}-{part["day"]}/{part["name"]}'
        )
        params = dict(
            Bucket=f'{ATHENA_BUCKET}',
            CopySource=f'{bucket}/{ukey}',
            Key=target,
        )
        s3.copy_object(**params)
        s3.delete_object(
            Bucket=f'{bucket}',
            Key=f'{ukey}'
        )
