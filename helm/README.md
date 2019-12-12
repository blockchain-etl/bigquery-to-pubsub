# bigquery-to-pubsub-helm

Helm charts for deploying https://github.com/blockchain-etl/bigquery-to-pubsub

## Requirements

- gcloud
- kubectl
- helm

## Usage

1. Create and initialize GKE cluster, Pub/Sub topic and temp bucket and BigQuery dataset:

    ```bash   
    bash scripts/setup.sh
    ```

1. Copy and edit example values:

    ```bash
    cp values-sample.yaml values-dev.yaml 
    ```

    For `tempBigqueryDataset` and `tempBucket` use value printed by `bash scripts/get_temp_resource_name.sh`

1. Install the chart:

    ```bash
    helm install bigquery-to-pubsub/ --name bigquery-to-pubsub-0 --values values-dev.yaml
    ```

1. Inspect the output of `kubectl get pods`. The job is done when the status is "Completed". Use 
`kubectl logs <POD> -f` to see the progress.

1. Cleanup the resources:

    ```bash
    bash scripts/cleanup.sh
    ```
