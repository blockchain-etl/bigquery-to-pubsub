# bigquery-to-pubsub

A tool for streaming time series data from a BigQuery table to Pub/Sub

## Create a Service Account with the following roles:
    - BigQuery Admin
    - Storage Admin
    - Pub/Sub Publisher 

## Set the project

```bash
gcloud config set project project-name
project=$(gcloud config get-value project 2> /dev/null)
```

## Create a key file for the Service Account and download it as `credentials.json`.

```
credentials=credentials.json
credentials_path=$(dirname $PWD/credentials.json)
```

## Create a Pub/Sub topic called `bigquery-to-pubsub-test0`.

```bash
topic=bigquery-to-pubsub-test0
```

## Create a temporary GCS bucket and a temporary BigQuery dataset:

```bash
bash create_temp_resources.sh
temp_resource_name=$(./get_temp_resource_name.sh)
```

## Build the docker image

```bash
docker build -t bigquery-to-pubsub:latest -f Dockerfile .
```
 
## Run replay for all Ethereum transactions in a specified time range at rate=0.1 (10x speed)

```bash
echo "Replaying Ethereum transactions"
docker run \
    -v $credentials_path:/bigquery-to-pubsub/ --env GOOGLE_APPLICATION_CREDENTIALS=/bigquery-to-pubsub/$credentials \
    bigquery-to-pubsub:latest \
    --timestamp-field block_timestamp \
    --start-timestamp 2019-10-23T00:00:00 \
    --end-timestamp 2019-10-23T01:00:00 \
    --batch-size-in-seconds 1800 \
    --replay-rate 0.1 \
    --pubsub-topic projects/${project}/topics/${topic} \
    --temp-bigquery-dataset ${temp_resource_name} \
    --temp-bucket ${temp_resource_name} \
    --bigquery-table bigquery-public-data.crypto_ethereum.transactions
```

## Run replay for transactions using a query in a specified time range at rate=2 (0.5x speed)

```bash
query=$(cat example_query_2.txt)
echo "Replaying Ethereum transactions"
docker run \
    -v $credentials_path:/bigquery-to-pubsub/ --env GOOGLE_APPLICATION_CREDENTIALS=/bigquery-to-pubsub/$credentials \
    bigquery-to-pubsub:latest \
    --timestamp-field block_timestamp \
    --start-timestamp 2019-10-23T00:00:00 \
    --end-timestamp 2019-10-23T01:00:00 \
    --batch-size-in-seconds 1800 \
    --replay-rate 2 \
    --pubsub-topic projects/${project}/topics/${topic} \
    --temp-bigquery-dataset ${temp_resource_name} \
    --temp-bucket ${temp_resource_name} \
    --query "$query"
```

Thanks to Merkle Science for donating some exchange wallet labels as CSV here:
https://github.com/merklescience/ethereum-exchange-addresses
