### Delete cluster

CLUSTER_NAME=bigquery-to-pubsub-0
gcloud container clusters delete ${CLUSTER_NAME} --quiet --zone us-central1-a

### Delete service account

SA_NAME=bigquery-to-pubsub-0
PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
#gcloud iam service-accounts delete --quiet "${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

rm key.json


 
