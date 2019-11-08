project=$(gcloud config get-value project 2> /dev/null)
if [[ -z "$project" ]]; then
    >&2 echo "No default project was found."
    >&2 echo "Please use the Cloud Shell or set your default project by typing:"
    >&2 echo "gcloud config set project YOUR-PROJECT-NAME"
    exit 1
fi

region=$(gcloud config get-value compute/region 2> /dev/null)
if [[ -z "$region" ]]; then
    region=us-east1
fi

temp_name=$(./get_temp_resource_name.sh)

# Create temp GCS bucket
gsutil mb -c standard -l ${region} gs://"${temp_name}"

# Create temp BigQuery dataset
bq --location=US mk \
--dataset \
--default_table_expiration 36000 \
"${project}":"${temp_name}"