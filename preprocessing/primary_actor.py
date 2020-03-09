import pandas as pd
import numpy as np 
import ast
import nltk
from nltk.stem.snowball import SnowballStemmer
import re
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer, TfidfTransformer
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity



file_path=('C:/Users/malav/Desktop/dat500-project/output.txt')

actor=[]
with open(file_path,'rt') as f:
    for i,line in enumerate(f):
        line=(ast.literal_eval(line))
        actor.append(line)

#Generating ranked list of primary (actor[0]) actors and avg genre score: 
actors=[]
for n,ID in enumerate(list(actor[0].values())[0]):
    for k,v in ID.items():
        if v:
            if len(v)==1:
                score=(list(v[0].values()))
                actors.append({k: score[0]})
            else:
                t=(len(v))
                tot=[]
                for i in range(0,t):
                    tot.append((list(v[i].values()))[0])
                score=(sum(tot)/t)
                actors.append({k: score})
aid=[]
g_score=[]
for i in actors:
    aid.append(list(i.keys())[0])
    g_score.append(list(i.values())[0])

#Ranked actor_ID     
data0=(pd.DataFrame([aid,g_score], index=None).T)
data0.columns=['Actor ID', 'Avg Genre Score']
data0=data0.sort_values(by=['Avg Genre Score'], ascending= False)

#Collecting titles from principal.tsv
db=pd.read_csv('C:/Users/malav/Desktop/dat500-project/principals.tsv', sep='\t')
df = pd.merge(data0, db, how='inner', left_on='Actor ID', right_on='nconst')
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

#list of titles per actor ID
df1=df.groupby(['nconst'])['tconst'].apply(list)

#Comparing plot 
file_path1=('C:/Users/malav/Desktop/dat500-project/input.txt')

with open(file_path1,'rt') as f:
    for i,line in enumerate(f):
        if line.split()[0] == 'plot:' :
            plot=' '.join(line.split()[1:])

summ=pd.read_csv('C:/Users/malav/Desktop/dat500-project/summary_box_office.tsv', sep='\t')

#Merged dataframe containing Actor ID, Title ID, summary and avg g_Score
df2=pd.merge(df, summ, how='inner', left_on='tconst', right_on='tconst')
df2=df2.drop(['nconst','boxoffice'],axis=1)

summary=list(df2['summary'])

#Setting all non-str values to 'NA'
summaries=[]
for text in summary:
    if type(text)!= str:
        summaries.append('NA')
    else:
        summaries.append(text)

# Define a function to perform both stemming and tokenization
stemmer = SnowballStemmer("english")
def tokenize_and_stem(text):
    
    # Tokenize by sentence, then by word
    tokens = [word for sentence in nltk.sent_tokenize(text) for word in nltk.word_tokenize(sentence)]
    
    # Filter out raw tokens to remove noise
    filtered_tokens = [token for token in tokens if re.search('[a-zA-Z]', token)]
    
    # Stem the filtered_tokens
    stems = [stemmer.stem(token) for token in filtered_tokens]
    return stems

# Instantiate TfidfVectorizer object with stopwords and tokenizer
# parameters for efficient processing of text
tfidf_vectorizer = TfidfVectorizer(max_df=0.8, max_features=200000,
                                 min_df=0.2, stop_words='english',
                                 use_idf=True, tokenizer=tokenize_and_stem,
                                 ngram_range=(1,3))

movies=summaries+list(plot)
# Fit and transform the tfidf_vectorizer with the "plot" of each movie
# to create a vector representation of the plot summaries
tfidf_matrix = tfidf_vectorizer.fit_transform([x for x in movies])

#print(tfidf_matrix.shape)

# Create a KMeans object with 5 clusters and save as km
km = KMeans(n_clusters=5)

# Fit the k-means object with tfidf_matrix
km.fit(tfidf_matrix)

#corresponding cluster of movies in summaries 
clusters = km.labels_.tolist()

# Calculate the similarity distance
similarity_distance = 1 - cosine_similarity(tfidf_matrix)


def find_similar():
  vector = similarity_distance[-1, :]
  most_similar = movies[np.argsort(vector)[1]]
  return most_similar

print(find_similar()) # prints "The Graduate"


