# airflow-s3-gcs-poc
Moving/Copying files from S3 bucket to GCS bucket using airflow (airflow 2.1.4 version) (GCS path prefix same as S3 path prefix)

## Config required to kickoff this Airflow Dag

params:


    s3_bucket           (required)
    s3_object_prefix    (required, folder path not file path)
    gcs_bucket          (required)
  
  exmaple config:
    
```json
        {
            "s3_bucket": "s3-bucket-poc",
            "s3_object_prefix": "raw/dataset_test/table_test/",
            "gcs_bucket": "gcs-bucket-poc"
        }
```

## Connection id's setup (Amazon and Google)
I am using Google CLoud Composer (2.0.11) to create airflow environment using appropriate PYPI packages (botocore, boto3, etc)

Go to Airflow web interface ---> admin ---> connection ---> create new connection

#### Airflow Amazon Default Conn ID Setup
Provide Amazon account login and password

![Screenshot](/screenshots/airflow_amazon_default_conn_id.jpeg)

#### Airflow Google Default Conn ID Setup
Provide your Google Service Account info under keyfile details

Provide your Google Project Id

![Screenshot](/screenshots/google_conn_id.jpg)
