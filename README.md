# Fiat
**Fi**rehose to **At**hena

## Features
* Stream arbitrary data using json-schema validation to AWS Athena (s3 backed warehouse)


## Walkthrough

#### Audit
In order to have audit data for your app, you can use the prebuilt audit class. There is no additional setup required to use this.

```python
from loggers import Audit

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
# Optionally, log your output results.
audit.log_output(output=result)
```
#### New Streams
If you need your own brand new data stream:

1. Add a schema to schemas folder in this project's root.
    * Make sure to include the following:
        * A new folder for the name of your stream
	* athena.json mapping columns and types [valid Athena schema](https://docs.aws.amazon.com/athena/latest/ug/getting-started.html)
	* json_schema.json must be a valid [json-schema](http://json-schema.org/examples.html)

2. Run the following script to create your table in Athena:

```bash
zappa invoke 'src/firehose_to_athena/table_utils.create_table'
```

3. Now we have to create the stream in AWS.
```bash
zappa invoke 'src/firehose_to_athena/firehose_utils.create_stream'
```
4. (Optional) Manually running previous step in AWS console:
    * In the AWS console, go to the [Firehose console](https://console.aws.amazon.com/firehose/home?region=us-east-1#).
    * Click on the blue button for **Create Delivery Stream**
    * Give it a Delivery Stream Name and set Source as **Direct PUT or other sources** and click the **Next** banking.
    * If you would like, you can have a lambda transform or process your data but it is not required. **select Disabled** for now and click **Next**.
    * Set Destination to **Amazon s3**. Set **S3 bucket** to *namely-spectrum* and **Prefix** to *$STREAM* and click **Next**.
    * Set **Buffer Size** to *1MB*, **Buffer Interval** to *60 seconds*, **s3 compression** to *GZIP*, **s3 encryption** to *enabled*, **error logging** to *Enabled*
    * For **IAM role**, choose *firehose_delivery_role* with the policy name *oneClick_firehose_delivery_role_xxxx* and click **Allow**
    * Click **Next**
    * Review your changes and click **Create Delivery Stream**

5. Test your implementation:
```bash
python src/loggers/audit.py
```

6. Modify this example in your code for your stream.
```python
from logger import ValidatedLog

# example task
def _add(x, y): return x + y

# global or session defined config
config = dict(
    app='audit',
    email='rich@namely.com',
    tags=dict(
            environment='non-prod',
	    team='NTA',
    )
)

# logger can be initialized elsewhere
logger = ValidatedLog.configure('firehose', **config)

# runtime execution happens here
# Notice that here we explicilty reference the stream we are 
writing to.
audit = logger.log(
    'Audit',
    task='_add',
    task_args=dict(x=1, y=2),
    ticket_id='nta-1000',
)
...
```
