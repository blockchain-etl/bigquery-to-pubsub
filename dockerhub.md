# Uploading to Docker Hub

```bash
> BIGQUERY_TO_PUBSUB_VERSION=0.0.1
> docker build -t bigquery-to-pubsub:${BIGQUERY_TO_PUBSUB_VERSION} -f Dockerfile .
> docker tag bigquery-to-pubsub:${BIGQUERY_TO_PUBSUB_VERSION} blockchainetl/bigquery-to-pubsub:${BIGQUERY_TO_PUBSUB_VERSION}
> docker push blockchainetl/bigquery-to-pubsub:${BIGQUERY_TO_PUBSUB_VERSION}
```