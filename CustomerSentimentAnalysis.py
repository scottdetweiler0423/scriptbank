from textblob import TextBlob as tb
import pandas as pd
import csv

output_csv = 'output.csv'
input_csv = 'input.csv'

df = pd.read_csv(input_csv, encoding = 'utf-8')

def sentiment(text):
    t_review = tb(text)
    review_sentiment = t_review.sentiment.polarity
    return review_sentiment 

df['sentimentcolumn'] = df['reviewcolumn'].apply(lambda review: sentiment(str(review)))

print(df.head())

with open(output_csv, 'a', newline='', encoding = "utf-8") as f:
    df.to_csv(f, header=True)
