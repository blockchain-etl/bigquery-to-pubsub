# bigquery-to-pubsub

A tool for streaming time series data from a BigQuery table to Pub/Sub

1. Create a Service Account with the following roles:
    - BigQuery Admin
    - Storage Admin
    - Pub/Sub Publisher 
  
1. Create a key file for the Service Account and download it as `credentials_file.json`.

1. Create a Pub/Sub topic called `bigquery-to-pubsub-test0`.

1. Create a temporary GCS bucket and a temporary BigQuery dataset:

```bash
> bash create_temp_resources.sh
```
 
1. Run replay for Ethereum transactions:

```bash
> docker build -t bigquery-to-pubsub:latest -f Dockerfile .
> project=$(gcloud config get-value project 2> /dev/null)
> temp_resource_name=$(./get_temp_resource_name.sh)
> echo "Replaying Ethereum transactions"
> docker run \
    -v /path_to_credentials_file/:/bigquery-to-pubsub/ --env GOOGLE_APPLICATION_CREDENTIALS=/bigquery-to-pubsub/credentials_file.json \
    bigquery-to-pubsub:latest \
    --bigquery-table bigquery-public-data.crypto_ethereum.transactions \
    --timestamp-field block_timestamp \
    --start-timestamp 2019-10-23T00:00:00 \
    --end-timestamp 2019-10-23T01:00:00 \
    --batch-size-in-seconds 1800 \
    --replay-rate 0.1 \
    --pubsub-topic projects/${project}/topics/bigquery-to-pubsub-test0 \
    --temp-bigquery-dataset ${temp_resource_name} \
    --temp-bucket ${temp_resource_name}
```
