import os
import youtube_dl
import boto3
from urllib import parse
import hashlib
from boto3.dynamodb.conditions import Key
import pandas as pd
import time
import argparse

def hash_filename(url):
    video_id = parse.urlparse(url).query
    return hashlib.sha256(video_id.encode('utf8')).hexdigest()

def get_filenames(directory, extension=None):
    """ 
    Return all filenames in a directory with an arbitrary extension.
    
    Parameters:
    -----------
    directory: A string representing absolute or relative path.
    extension: Extension that you want to look for. E.g. 'mp3'
               Returns all files in current directory if no extension is None
               default=None
    
    Returns:
    --------
    A list with filenames that match the extension in directory.
    
    """
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

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3 = boto3.client('s3')
    with open(file_name, "rb") as f:
        s3.upload_fileobj(f, bucket, object_name)

    return True

def pull_sentiments(key):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('transcribe-audio-project-ResultsDDBtable-UYEWCZ84K563')
    response = table.scan(FilterExpression = Key('partitionKey').begins_with(key))['Items']
    df = pd.DataFrame(response, index = [0])
    if df.empty:
        return None
    else:
        return df[['Neutral', 'Positive', 'Negative', 'Mixed', 'Sentiment']]
        
def file_exists(hashed_filename):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('transcribe-audio-project-ResultsDDBtable-UYEWCZ84K563')
    
def process_sentiment(hashed_filename):
    sentiments = pull_sentiments(hashed_filename)
    while sentiments is None:
        time.sleep(15)
        sentiments = pull_sentiments(hashed_filename)
    return sentiments

def delete_downloads():
    downloaded_mp3s = [file for file in os.listdir() if file.endswith(".mp3")]
    for file in downloaded_mp3s:
    	os.remove(file)


def main():
    url = opt.url
    hashed_filename = hash_filename(url) + ".mp3"
    sentiments = pull_sentiments(hashed_filename)
    if sentiments is None:
        # download mp3 to project root directory i.e. same level as `app`.
        youtube_download(url)

        # upload all mp3 files to s3
        MP3_DIRECTORY = "./"
        EXTENSION = 'mp3'
        filename = get_filenames(MP3_DIRECTORY, EXTENSION)[0]
        BUCKET = 'bucket-for-audio-1234'
        print("\nUploading file to S3... ", end = "")
        file_uploaded = upload_file(filename, BUCKET, hashed_filename)  # pass filename_hash
        print("DONE\n")
        delete_downloads()
        if file_uploaded:
            print(f"Video uploaded successfully to s3 bucket: {BUCKET}.\n")
            print("Calculating Sentiments - This may take several minutes")
            sentiments = process_sentiment(hashed_filename)
            print("DONE\n")
            print(sentiments)
            return sentiments
        else:
            print(f"Video could not be processed!")
    else:
        print(f"Returning Pre-Generated Sentiments for {opt.url}")
        print(sentiments)
        return sentiments

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', type = str)
    opt = parser.parse_args()
    main()