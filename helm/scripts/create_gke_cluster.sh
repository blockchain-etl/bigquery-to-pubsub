### Create a cluster

CLUSTER_NAME=bigquery-to-pubsub-0
gcloud container clusters create ${CLUSTER_NAME} \
--zone us-central1-a \
--num-nodes 1 \
--disk-size 20GB \
--machine-type n1-standard-1 \
--network default \
--subnetwork default \
--scopes bigquery,pubsub,storage-rw,logging-write,monitoring-write,service-management,service-control,trace

gcloud container clusters get-credentials ${CLUSTER_NAME} --zone us-central1-a

# Initialize Helm

dirname=$(dirname $0)

helm init   
bash "${dirname}"/patch-tiller.sh 

### Create a service account

SA_NAME=bigquery-to-pubsub-0
gcloud iam service-accounts create $SA_NAME

PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
gcloud projects add-iam-policy-binding "${PROJECT_ID}" --member "serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" --role roles/bigquery.admin
gcloud projects add-iam-policy-binding "${PROJECT_ID}" --member "serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" --role roles/bigquery.jobUser
gcloud projects add-iam-policy-binding "${PROJECT_ID}" --member "serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" --role roles/storage.admin
gcloud projects add-iam-policy-binding "${PROJECT_ID}" --member "serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" --role roles/pubsub.publisher

# Create a key

gcloud iam service-accounts keys create key.json --iam-account "${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

kubectl create secret generic bigquery-to-pubsub-app-key --from-file=key.json=key.json


 
