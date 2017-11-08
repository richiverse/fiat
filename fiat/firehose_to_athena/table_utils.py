"""This is a WIP project to create tables from different schemas."""
from glob import glob
from json import load
from os import environ as env

import boto3

region_name = boto3.Session().region_name
athena = boto3.client('athena', region_name=region_name or 'us-east-1')

ATHENA_BUCKET = env["ATHENA_BUCKET"]
SCHEMA_PATH = env["SCHEMA_PATH"]


def get_schema_paths(ifile):
    """Match path for SCHEMA_PATH.

    ../../nes_schemas/schemas/*/athena.json.
    """
    print(ifile)
    return glob(ifile)


def get_schema_from_path(path):
    """Match table from path."""
    return path.split('/')[-2]


def get_columns_from_path(path):
    """Get columns from path."""
    schema = load(open(path))
    columns = schema["columns"]
    print(columns)
    return ',\n'.join([''.join([f'{k}  {v[k]}' for k in v]) for v in columns])


def create_table(event=None, context=None):
    """Create a table in Athena for a given stream."""
    for path in get_schema_paths(SCHEMA_PATH):
        table = get_schema_from_path(path)
        columns = get_columns_from_path(path)
        query = (
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {table} (\n"
            f"{columns}\n"
            ") PARTITIONED BY (dt string)\n"
            "ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'\n"
            f"LOCATION 's3://{ATHENA_BUCKET}/{table}/';"
        )

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
    create_table()
