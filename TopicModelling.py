"""Importing and formatting Nissan Customer Reviews"""

import pandas as pd

input_csv = 'C:\\Users\\sd301759\\Documents\\Scott Local\\Nissan Project\\2018 Q2-Q3 KPI_NCAR Raw Data SQL.csv'

df = pd.read_csv(input_csv, encoding = 'utf-8')

doc_complete = df['LIKED_OR_DISLIKED_VERB'].apply(lambda review: str(review))


"""Cleaning dataset and adding stop words"""
from nltk.corpus import stopwords 
from nltk.stem.wordnet import WordNetLemmatizer
import string

stop = set(stopwords.words('english'))

new_stopwords = ['excellent', 'great', 'best', 'wonderful', 'perfect', 'awesome', 'good', 'amazing',
         'horrible', 'awful', 'terrible', 'bad', 'worst', 'dislike', 'u', 'mr']

stop.update(new_stopwords)
   
exclude = set(string.punctuation) 
lemma = WordNetLemmatizer()

def clean(doc):
    stop_free = " ".join([i for i in doc.lower().split() if i not in stop])
    punc_free = ''.join(ch for ch in stop_free if ch not in exclude)
    normalized = " ".join(lemma.lemmatize(word) for word in punc_free.split())
    return normalized

#apply clean function to lowercase, remove stopwords, and lemmatize
doc_clean = [clean(doc).split() for doc in doc_complete] 

#remove null lists
corpus = [e for e in doc_clean if e]

"""Preparing document-term matrix"""
# Importing Gensim
import gensim
from gensim import corpora

# Creating the term dictionary of our courpus, where every unique term is assigned an index.

dictionary = corpora.Dictionary(corpus)

# Converting list of documents (corpus) into Document Term Matrix using dictionary prepared above.
doc_term_matrix = [dictionary.doc2bow(doc) for doc in corpus]


"""Running the Model"""
# Creating the object for LDA model using gensim library
Lda = gensim.models.ldamodel.LdaModel

# Running and Trainign LDA model on the document term matrix.
ldamodel = Lda(doc_term_matrix, num_topics=10, id2word = dictionary, passes=5)

#ldamodel = Lda(doc_term_matrix, num_topics=10, id2word = dictionary, passes=5)

"""Creating visualization"""
import pyLDAvis
from pyLDAvis import gensim

ldamodel = Lda(doc_term_matrix, num_topics=10, id2word = dictionary, passes=5)

prepared_lda = pyLDAvis.gensim.prepare(ldamodel, doc_term_matrix, dictionary)
pyLDAvis.show(prepared_lda)






