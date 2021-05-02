import boto3
import time
import pandas as pd

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
      
if __name__ == '__main__':
    bucket_name = 'bucket-for-audio-1234'
    key = 'positive_speech6'
    audio_file_name = 'positive_speech.mp3'
    transcribe(key, bucket_name, audio_file_name)