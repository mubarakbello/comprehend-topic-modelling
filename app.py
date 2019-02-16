import requests
import boto3
import json
import re
import uuid
import time
from bs4 import BeautifulSoup
from sanic import Sanic
from sanic.response import text as sanic_text, json as sanic_json


# Specify the access keys below here
# in a ~/.aws/credentials file
aws_access_key_id = ""
aws_secret_access_key = ""

# Specify the region information below here
# in a ~/.aws/config file
region = ""

DATA_ACCESS_ROLE_ARN = "" # specify this here

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
            return result
        elif job_status == 'FAILED':
            print("Job failed")
            return result
        else:
            print("job_status: " + job_status)
            time.sleep(3)

def create_temp_file(url_hostname, file_content):
    file_name_ending = str(url_hostname) + ".txt"
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name_ending])
    with open(random_file_name, 'w') as f:
        f.write(file_content)
    return random_file_name


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

    return sanic_json(response)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)


# TODO
# 1. Check out RabbitMQ for possible queuing solution
# i.e. for storing the data, before feeding it to comprehend
# 2. Store the data in a file as an alternative to above
