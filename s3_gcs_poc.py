#----this code is compatible with airflow 2.0
import datetime, logging, json

import airflow
from airflow import models
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    PythonVirtualenvOperator,
    ShortCircuitOperator,
)
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

from google.cloud import storage

'''
workflow:
copy s3 objects to a gcs bucket using the same prefix as s3.
```

```
params:
    s3_bucket           (required)
    s3_object_prefix    (required, folder path not file path)
    gcs_bucket          (required)

    exmaple config:
        {
            "s3_bucket": "s3-bucket-poc",
            "s3_object_prefix": "raw/dataset_test/table_test/",
            "gcs_bucket": "gcs-bucket-poc"
        }
'''

default_dag_args={
    'start date': days_ago(1)
}

with models.DAG(
    's3_to_gcs_poc',
    schedule_interval=None,
    render_template_as_native_obj=True, # to return native python code data types
    default_args=default_dag_args) as dag:
    
    print_conf = BashOperator(
        task_id='print_conf',
        bash_command='echo conf: {{ dag_run.conf }}',
        retries=3
    )

    # initialize variables
    params = {}
    params['s3_bucket'] = None
    params['s3_object_prefix'] = None
    params['gcs_bucket'] = None


    def parse_params(ti, **context) :
        if context['dag_run'].conf:
            params['s3_bucket'] = context['dag_run'].conf.get('s3_bucket',None)
            params['s3_object_prefix'] = context['dag_run'].conf.get('s3_object_prefix', None)
            params['gcs_bucket'] = context['dag_run'].conf.get('gcs_bucket', None)
        else: # fail without retry
            raise AirflowFailException("config missing. please trigger the dag with proper config")
        # check for required params and fail without retry if not found
        if (params['s3_bucket'] is None
                or params['s3_object_prefix'] is None
                or params['gcs_bucket'] is None) :
            raise AirflowFailException("one or more of the following required params missing. s3_bucket, s3_object_prefix, gcs_bucket")
        
        logging.info("params: {}".format(params))
    
        
        ti.xcom_push(key='s3_bucket', value=params['s3_bucket'])
        ti.xcom_push(key='s3_object_prefix', value=params['s3_object_prefix'])
        ti.xcom_push(key='gcs_bucket', value=params['gcs_bucket'])
    

    parse_params = PythonOperator(
        task_id='parse_params',
        python_callable=parse_params,
        provide_context=True,
        retries=1
    )
    

    s3_to_gcs = S3ToGCSOperator (
        task_id='s3_to_gcs',
        bucket="{{ task_instance.xcom_pull(key='s3_bucket', task_ids='parse_params') }}",
        prefix="{{ task_instance.xcom_pull(key='s3_object_prefix', task_ids='parse_params') }}",
        aws_conn_id='aws_default',
        verify=None,
        dest_gcs_conn_id='google_cloud_default',
        dest_gcs="gs://{}/".format("{{ task_instance.xcom_pull(key='gcs_bucket', task_ids='parse_params') }}"), # write to gcs bucket with the same prefix as s3
        replace=True # overwrite same-key objects
    )


    list_gcs_files = GCSListObjectsOperator(
        task_id='list_gcs_files',
        bucket="{{ task_instance.xcom_pull(key='gcs_bucket', task_ids='parse_params') }}",
        gcp_conn_id='google_cloud_default',
        prefix="{{ task_instance.xcom_pull(key='s3_object_prefix', task_ids='parse_params') }}", # currently the dest gcs prefix matches source s3 prefix
        retries=1
    )


    print_conf >> parse_params >> s3_to_gcs >> list_gcs_files