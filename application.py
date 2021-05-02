import boto3
import time
import pandas as pd
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from urllib import parse
import hashlib
import time
import youtube_dl
import argparse
import os
import flask
from flask import Flask, render_template

def hash_filename(url):
    video_id = parse.urlparse(url).query
    return hashlib.sha256(video_id.encode('utf8')).hexdigest()
    
def get_filenames(directory, extension=None):
    os.chdir(directory)
    filenames = [f for f in os.listdir('.') if os.path.isfile(f)]
    if extension is not None:
        filenames = [file for file in filenames if file.endswith(f".{extension}")]
    return filenames
    
def youtube_download(url):
    def my_hook(d):
        if d['status'] == 'finished':
            print('Done downloading, now converting ...')
    ydl_opts = {
    'format': 'bestaudio',
    'postprocessors': [{
        'key': 'FFmpegExtractAudio',
        'preferredcodec': 'mp3',
        'preferredquality': '192',
    }],
    'progress_hooks': [my_hook]
    }
    
    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

def delete_downloads():
    downloaded_mp3s = [file for file in os.listdir() if file.endswith(".mp3")]
    for file in downloaded_mp3s:
    	os.remove(file)

def transcribe(key, bucket_name, audio_file_name):
    client = boto3.client('transcribe', region_name = 'us-east-1')
    job_uri = "s3://" + bucket_name + "/" + audio_file_name
    client.start_transcription_job(
        TranscriptionJobName=key,
        Media={'MediaFileUri': job_uri},
        MediaFormat = audio_file_name.split('.')[1],
        LanguageCode = 'en-US',
        Settings = {'ShowSpeakerLabels': True, 'MaxSpeakerLabels': 10})
        
    while True:
        result = client.get_transcription_job(TranscriptionJobName = key)
        if result['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        time.sleep(15)
    if result['TranscriptionJob']['TranscriptionJobStatus'] == "COMPLETED":
        data = pd.read_json(result['TranscriptionJob']['Transcript']['TranscriptFileUri'])
        
    return data['results']['transcripts'][0]['transcript']
    

def sentiments(text):
    client = boto3.client('comprehend', region_name='us-east-1')
    results = dict(client.detect_sentiment(Text=text, LanguageCode='en'))
    df = pd.DataFrame(results['SentimentScore'], index = [0])
    df['Sentiment'] = results['Sentiment']
    return df
    
def sentiments_to_dynamo(key, mixed, negative, neutral, positive, sentiment, table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.put_item(
       Item={
            'partitionKey': key,
            'Mixed': Decimal(str(mixed)),
            'Negative': Decimal(str(negative)),
            'Neutral': Decimal(str(neutral)),
            'Positive': Decimal(str(positive)),
            'Sentiment': sentiment
        }
    )
    
    return response
    
def pull_sentiments(key, table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    response = table.scan(FilterExpression = Key('partitionKey').begins_with(key))['Items']
    df = pd.DataFrame(response, index = [0])
    if df.empty:
        return None
    else:
        return df[['Neutral', 'Positive', 'Negative', 'Mixed', 'Sentiment']]
        
def upload_file(file_name, bucket, object_name=None):

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3 = boto3.client('s3')
    with open(file_name, "rb") as f:
        s3.upload_fileobj(f, bucket, object_name)
        return True
    return False

application = Flask(__name__, template_folder = 'templates')
    
@application.route('/', methods = ["GET", "POST"])
def main():
    if flask.request.method == "POST":
        url = flask.request.form['url']
        bucket_name = "youtube-sentiments-altamash2"
        key = hash_filename(url) 
        table_name = "youtube-sentiments"
        sentiment_results = pull_sentiments(key, table_name)
        if sentiment_results is None:
            youtube_download(url)
            MP3_DIRECTORY = "./"
            EXTENSION = 'mp3'
            filename = get_filenames(MP3_DIRECTORY, EXTENSION)[0]
            file_uploaded = upload_file(filename, bucket_name, key + ".mp3")  # pass filename_hash
            delete_downloads()
            if file_uploaded:
                text = transcribe(key, bucket_name, key + ".mp3")
                sentiment_results = sentiments(text)
                sentiments_to_dynamo(key, sentiment_results['Mixed'][0], sentiment_results['Negative'][0], sentiment_results['Neutral'][0], sentiment_results['Positive'][0], sentiment_results['Sentiment'][0], table_name)
                return sentiment_results
            else:
                return "Could not send file to S3!"
        else:
            return sentiment_results
    else:
        return flask.render_template("index.html", default_url=DEFAULT_URL)

if __name__ == '__main__':
    application.run(host = '0.0.0.0', port = 8080)