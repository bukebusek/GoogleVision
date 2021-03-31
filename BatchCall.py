
from google.cloud import vision_v1
from google.cloud import storage

def list_blobs(bucket_name='example_bucket_momo_xukun_1'):
    #bucket_name = "gs://example_bucket_momo_xukun_1/prefix2/"
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    for blob in blobs:
        print(blob.name)

def list_blobs_with_prefix(bucket_name='example_bucket_momo_xukun_1', prefix='Feed/', delimiter=None):

    storage_client = storage.Client()
    blobs = storage_client.list_blobs(
        bucket_name, prefix=prefix, delimiter=delimiter
    )
    a = []
    url = "gs://example_bucket_momo_xukun_1/"
    for blob in blobs:
        a.append(url + blob.name)
    return a


def sample_async_batch_annotate_images(
    output_uri="gs://example_bucket_momo_xukun_1/prefix4/",
):
    """Perform async batch image annotation."""
    client = vision_v1.ImageAnnotatorClient()

    input_image_uri = list_blobs_with_prefix()

    requests = []

    for i in input_image_uri:

        source = {"image_uri": i}
        image = {"source": source}
        features = [
            {"type_": vision_v1.Feature.Type.OBJECT_LOCALIZATION},
        ]
        y = [{"image": image, "features": features}]
        requests += y


    gcs_destination = {"uri": output_uri}

    # The max number of responses to output in each JSON file
    batch_size = 100
    output_config = {"gcs_destination": gcs_destination,
                     "batch_size": batch_size}

    operation = client.async_batch_annotate_images(requests=requests, output_config=output_config)

    print("Waiting for operation to complete...")
    response = operation.result(90)

    # The output is written to GCS with the provided output_uri as prefix
    gcs_output_uri = response.output_config.gcs_destination.uri
    print("Output written to GCS with prefix: {}".format(gcs_output_uri))

if __name__ == '__main__':
    sample_async_batch_annotate_images()

