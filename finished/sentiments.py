import boto3
import json
import pandas as pd

def get_transcript(fname):
    with open(fname) as f:
        text = f.read()
    return text

def sentiments(text):
    client = boto3.client('comprehend', region_name='us-east-1')
    results = dict(client.detect_sentiment(Text=text, LanguageCode='en'))
    df = pd.DataFrame(results['SentimentScore'], index = [0])
    df['Sentiment'] = results['Sentiment']
    return df

if __name__ == '__main__':
    text = get_transcript('transcript.txt')
    sentiments = sentiments(text)
    print(sentiments)