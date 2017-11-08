#! /usr/bin/env python
"""Validated Log class that validates your log prior to streaming."""
from functools import lru_cache
from importlib import import_module
from json import load

from jsonschema import validate


@lru_cache(maxsize=64)
def get_schema(schema):
    """Cache schema for reuse."""
    lowercase_schema = schema.lower()
    return load(
        open(
            f'nes_schemas/schemas/{lowercase_schema}/json_schema.json'
        )
    )


@lru_cache(maxsize=64)
def get_streamer(streamer):
    """Cache streamer for reuse."""
    return import_module(f'src.streamers.{streamer}')


class ValidatedLog():
    """Validate a log prior to sending it out.

    A validated log is composed of 2 components:
        * json-schema schema
        * streamer (Log facility)

    """

    def __init__(self, stream, **params):
        """Initialize schema name must equal stream name."""
        if not hasattr(self, 'params'):
            self.params = {}

        self.params.update(params)

        self._validate(stream)

        self.params = {
            str(k): str(v)
            for k, v in self.params.items()
        }
        self.write_log(stream, self.params)

    def _validate(self, schema):
        """Validate schema object must match that of the stream name."""
        schema = get_schema(schema)
        validate(self.params, schema)

    @classmethod
    def configure(
        cls,
        streamer,
        options=None,
        **params,
    ):
        """Configure your class first."""
        cls.params = dict(**params) or {}
        options = options or {}
        cls.streamer = streamer
        cls.stream_client = get_streamer(streamer)._init(**options)
        return cls

    @classmethod
    def log(cls, stream, **data):
        """Log data to stream."""
        return cls(stream, **data)

    @classmethod
    def write_log(cls, stream, data):
        """Initialize streamer and write to log."""
        streamer = get_streamer(cls.streamer)
        if not isinstance(data, list):
            data = [data]
        streamer.write(cls.stream_client, stream.title(), data)
        return data


if __name__ == '__main__':
    # from logger import ValidatedLog

    # example task
    def _add(x, y): return x + y

    # global or session defined config
    config = dict(
        app='audit',
        email='richardfernandeznyc@gmail.com',
        tags=dict(
            environment='non-prod',
            team='NTA',
        )
    )

    # logger can be initialized elsewhere
    logger = ValidatedLog.configure('firehose', **config)

    # runtime execution happens here
    audit = logger.log(
        'Audit',
        task='_add',
        task_args=dict(x=1, y=2),
        ticket_id='nta-1000',
    )

    result = _add(1, 2)
    # audit.log('audit_output', output=result)
