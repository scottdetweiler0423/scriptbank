from textblob import TextBlob as tb
import pandas as pd
import csv

output_csv = 'NissanReviewSentiments.csv'
input_csv = 'NissanCustomerReviews.csv'

file_reader =  open(input_csv, "r", encoding = 'utf-8')
read = csv.reader(file_reader)
reviews = [x[0] for x in read if x]
reviewlist = []

for review in reviews:
    reviewlist.append(review)


review_dict = {'review': [],
               'sentiment': []
                }

for review in reviewlist:
    t_review = tb(review)
    review_sentiment = t_review.sentiment.polarity

    review_dict['review'].append(review)
    review_dict['sentiment'].append(review_sentiment)

# writing review_dict to csv
df = pd.DataFrame(review_dict)
print(df.head())
with open(output_csv, 'a', newline='', encoding = "utf-8") as f:
    df.to_csv(f, header=False)


