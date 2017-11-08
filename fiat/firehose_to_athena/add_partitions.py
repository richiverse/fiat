#! /usr/bin/env python
"""Zappa schedules this module to run once a day to create new partitions."""
from datetime import datetime
from os import environ as env, listdir

import boto3

region_name = boto3.Session().region_name
athena = boto3.client('athena', region_name=region_name or 'us-east-1')

ATHENA_BUCKET = env["ATHENA_BUCKET"]
SCHEMA_PATH = env["SCHEMA_PATH"]
TODAY = datetime.strftime(datetime.now(), '%Y-%m-%d')


def get_schemas(path):
    """Grab all schemas from git subtree nes_schemas."""
    return listdir(path)


def main(event=None, context=None):
    """Run start_query_execution boto call for Athena."""
    for table in get_schemas(SCHEMA_PATH):
        query = f'''
            ALTER TABLE {table}
            ADD PARTITION (dt='{TODAY}');
        '''
        print(query)
        athena.start_query_execution(
            QueryString=query,
            ResultConfiguration=dict(
                OutputLocation=f's3://{ATHENA_BUCKET}/devnull',
                EncryptionConfiguration=dict(
                    EncryptionOption='SSE_S3'
                )
            )
        )


if __name__ == '__main__':
    main()
