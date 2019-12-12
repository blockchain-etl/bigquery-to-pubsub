project=$(gcloud config get-value project 2> /dev/null)
if [[ -z "$project" ]]; then
    >&2 echo "No default project was found."
    >&2 echo "Please use the Cloud Shell or set your default project by typing:"
    >&2 echo "gcloud config set project YOUR-PROJECT-NAME"
    exit 1
fi

project_with_underscores=${project//-/_}

temp_name=${project_with_underscores}_bigquery_to_pubsub_temp

echo "${temp_name}"