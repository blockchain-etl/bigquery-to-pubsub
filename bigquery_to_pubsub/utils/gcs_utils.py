from google.cloud import storage

MEGABYTE = 1024 * 1024


def download_from_gcs(bucket, object, filename):
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket)
    blob_meta = bucket.get_blob(object)

    if blob_meta.size > 10 * MEGABYTE:
        blob = bucket.blob(object, chunk_size=10 * MEGABYTE)
    else:
        blob = bucket.blob(object)

    blob.download_to_filename(filename)


def delete_in_gcs(bucket, object):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(object)
    blob.delete()
