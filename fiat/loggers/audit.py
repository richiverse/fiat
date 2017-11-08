#! /usr/bin/env python
"""Helper class for generic auditing functionality."""
from datetime import datetime
from uuid import uuid4

from validated_log import ValidatedLog


class Audit(ValidatedLog):
    """Audit class adds ticket_id and task_args to validated log."""

    def __init__(self, stream, **params):
        """Magic happens on instantiation."""
        if not hasattr(self, 'params'):
            self.params = {}
        self.uuid = str(uuid4())
        # Auto added
        self.params.update(uuid=self.uuid)
        self.params.update(timestamp=str(datetime.now()))

        # Default optional fields as they are required in Athena
        self.params['ticket_id'] = self.params.get('ticket_id')
        self.params['task_args'] = self.params.get('task_args')
        super().__init__(stream, **params)

    @classmethod
    def log(cls, **data):
        """Write a log to audit stream."""
        return cls('audit', **data)

    def log_output(self, output):
        """Write to audit_output stream."""
        self.uuid
        data = dict(
            uuid=self.uuid,
            timestamp=str(datetime.now()),
            output=output,
        )
        super().write_log('audit_output', data)
        del self.uuid


if __name__ == '__main__':
    # from loggers.audit import Audit

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
    logger = Audit.configure('firehose', **config)

    # runtime execution happens here
    audit = logger.log(
        task='_add',
        task_args=dict(x=1, y=2),
        ticket_id='nta-1000',
    )

    result = _add(1, 2)
    audit.log_output(output=result)
