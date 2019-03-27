import requests
import boto3, botocore
import json
import os
import uuid
import time
from bs4 import BeautifulSoup
from sanic import Sanic
from sanic.response import text as sanic_text, json as sanic_json


# Specify the access keys below here
# in a ~/.aws/credentials file, then delete these lines
aws_access_key_id = ""
aws_secret_access_key = ""

# Specify the region information below here
# or in a ~/.aws/config file
region = ""

DATA_ACCESS_ROLE_ARN = "<your_role_arn_for_access_to_s3>"

# Create an S3 recource
s3_resource = boto3.resource('s3')

# Generate the bucket name to use
bucket_name = ''.join(["topic-modelling-", str(uuid.uuid4().hex[:6])])

# Create the bucket
session = boto3.session.Session()
current_region = session.region_name
## Do not specify CreateBucketConfiguration if region is us-east-1
if current_region == 'us-east-1':
    s3_resource.create_bucket(
        Bucket=bucket_name,
    )
else:
    s3_resource.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint': current_region},
    )

# Create the bucket_resource to use from the bucket created
bucket_resource = s3_resource.Bucket(name=bucket_name)

# Create a client resource
comprehend = boto3.client(service_name='comprehend')


app = Sanic()


def detect_topic_model(input_config, output_config):
    response = comprehend.start_topics_detection_job(
        InputDataConfig=input_config,
        OutputDataConfig=output_config,
        DataAccessRoleArn=DATA_ACCESS_ROLE_ARN,
    )
    job_id = response["JobId"]
    while True:
        result = comprehend.describe_topics_detection_job(JobId=job_id)
        job_status = result["TopicsDetectionJobProperties"]["JobStatus"]

        if job_status == 'COMPLETED':
            print("Job completed")
            output_s3uri = result["TopicsDetectionJobProperties"]["OutputDataConfig"]["S3Uri"]

            return {
                "status": job_status,
                "output": output_s3uri
            }
        elif job_status == 'FAILED':
            print("Job failed")
            return {"status": job_status}
        else:
            print("job_status: " + job_status)
            time.sleep(10)

def create_temp_file(url_hostname, file_content):
    file_name_ending = str(url_hostname) + ".txt"
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name_ending])
    with open(random_file_name, 'w') as f:
        f.write(file_content)

    # now, check if the created file size is up to the minimum 500 bytes
    # If it isn't, append newlines to fill it up
    file_full_path = ''.join([os.getcwd(), '/', random_file_name])
    file_size_in_bytes = os.path.getsize(file_full_path)
    if file_size_in_bytes < 500:
        char_difference = 500 - file_size_in_bytes
        str_to_append = "\n" * (char_difference + 1)
        with open(random_file_name, 'a') as f:
            f.write(str_to_append)

    return random_file_name

def download_from_s3(key, local_path):
    while True:
        try:
            bucket_resource.download_file(
                Key=key,
                Filename=local_path
            )
            break
        except botocore.exceptions.ClientError:
            time.sleep(5)
            pass


@app.route("/", methods=["POST",])
async def endpoint(request):
    # fetch the url from the request body
    if request.form.get("url") is None:
        return sanic_json({"error_message": "url not specified"})

    _URL = request.form.get("url")

    # fetch the html content of the url
    page_body = requests.get(_URL)

    if page_body.status_code != 200:
        return sanic_json({"error_message": "Error fetching URL"})

    soup = BeautifulSoup(page_body.content, 'html.parser')
    [s.extract() for s in soup(['iframe', 'script', 'img', 'style'])]
    body_part = "\n".join(soup.body.stripped_strings)

    # Write the cleaned bodypart to a txt file and save to aws S3
    # Get the S3 input uri and make a input_config object
    # Define the S3 output uri and make a output_config object
    # Detect the topic model, then output the result

    url_hostname = str(_URL).replace('/', '').replace('.', '-')
    created_file_name = create_temp_file(url_hostname, body_part)
    bucket_resource.upload_file(
        Filename=created_file_name,
        Key=created_file_name
    )

    # Generate the s3 input and output uri now
    s3_input_uri = "s3://{}/{}".format(bucket_name, created_file_name)
    s3_output_uri = "s3://{}".format(bucket_name)

    input_config = {"S3Uri": s3_input_uri}
    output_config = {"S3Uri": s3_output_uri}

    response = detect_topic_model(input_config, output_config)

    if response["status"] == 'FAILED':
        return sanic_json(json.dumps(response))

    output_s3uri = response["output"].replace('\/', '/')

    # download_key = output_s3uri[31:].replace("\/", "/")
    # download_local_path = output_s3uri[93:]

    # download_from_s3(download_key, download_local_path)

    return sanic_json({"output_uri": output_s3uri})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
