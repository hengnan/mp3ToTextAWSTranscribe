import boto3
import os
import time
import pandas as pd
import re
import uuid

file_list = []
for audio_file in os.listdir():
    if audio_file.split('.')[-1] in ['mp3']:
        file_list.append(audio_file)

audio_data = pd.DataFrame({'file_name': file_list})

#aws_access_key_id = "----"
#aws_secret_access_key = "----"
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region_name = 'us-east-1'
session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key,
                                region_name=region_name)

# Instantiate client
s3 = boto3.client('s3')

# List created buckets
response = s3.list_buckets()
buckets = [bucket['Name'] for bucket in response['Buckets']]

for bucket in buckets:
    print(bucket)

#bucket_name = str(uuid.uuid4()) use this line once to
#generate your uuid and replace bucket_name with that uuid
bucket_name = "14a16c4d-2040-4740-be29-06532dff6c88"
client_s3 = boto3.client('s3')
client_s3.create_bucket(Bucket=bucket_name)

for audio_file in audio_data.file_name.values:
    print(f"uploading {audio_file}")
    client_s3.upload_file(audio_file, bucket_name, audio_file)

for index, row in audio_data.iterrows():
    bucket_location = boto3.client('s3').get_bucket_location(Bucket=bucket_name)
    object_url = f"s3://{bucket_name}/{row['file_name']}"
    audio_data.at[index, 'url'] = object_url
    print(object_url)


def start_transcription(bucket, job_name, file_url, wait_process=True):
    client_transcribe = boto3.client('transcribe', region_name='us-east-1')

    client_transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': file_url},
        MediaFormat='wav',
        LanguageCode='en-US',
        OutputBucketName=bucket,
        Settings={
            'ShowSpeakerLabels': True,
            'MaxSpeakerLabels': 2,
        }
    )

    status = None
    
    if wait_process:
        while True:
            status = client_transcribe.get_transcription_job(TranscriptionJobName=job_name)

            # Check the transcription job status
            job_status = status['TranscriptionJob']['TranscriptionJobStatus']

            if job_status == 'COMPLETED':
                print("Transcription completed successfully!")
                break
            elif job_status == 'FAILED':
                print(f"Transcription job failed: {status['TranscriptionJob']['FailureReason']}")
                break
            else:
                print("Transcription not ready yet, waiting...")
                time.sleep(20)  # Wait for 20 seconds before checking again

            print("Not ready yet...")
            time.sleep(20)

    print('Transcription finished')
    return status


for index, row in audio_data.iterrows():
    # Sanitize transcription job name
    job_name = re.sub(r'[^0-9a-zA-Z._-]', '_', row[0])
    print(job_name, row['url'])

    start_transcription(bucket_name, job_name, row['url'], wait_process=False)
    audio_data.at[index, 'transcription_url'] = f"https://{bucket_name}.s3.amazonaws.com/{row['file_name']}.json"
    audio_data.at[index, 'json_transcription'] = f"{row['file_name']}.json"

# Initiate s3 resource
s3 = boto3.resource('s3')

# Select bucket
my_bucket = s3.Bucket(bucket_name)

# Download file into current directory
for s3_object in my_bucket.objects.all():
    # Need to split s3_object.key into path and file name, else it will give error file not found.
    path, filename = os.path.split(s3_object.key)

    if filename.split('.')[-1] in ['json']:
        my_bucket.download_file(s3_object.key, filename)
        print(filename)

for s3_object in my_bucket.objects.all():
    # Need to split s3_object.key into path and file name, else it will give error file not found.
    path, filename = os.path.split(s3_object.key)

    if filename.split('.')[-1] in ['wav', 'WAV']:
        print(filename)
        client_transcribe = boto3.client('transcribe', region_name='us-east-1')

        try:
            client_transcribe.delete_transcription_job(TranscriptionJobName=filename)
        except:
            print("missed", filename)

client_transcribe = boto3.client('transcribe', region_name='us-east-1')
response_transcribe = client_transcribe.list_transcription_jobs(Status='COMPLETED')
print(response_transcribe)
