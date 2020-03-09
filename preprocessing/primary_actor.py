import pandas as pd
import numpy as np 
import ast
from collections import defaultdict
from gensim import corpora
import logging
import nltk
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from gensim import models
from gensim import similarities


logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

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


# Tokenizing and removing stopwrods & punctuations from summary
tokenizer = RegexpTokenizer(r'\w+')
stop_words = set(stopwords.words('english')) 
texts=[]
for summ in summary:
    #print(type(summ))
    if type(summ)== str:
        tok=tokenizer.tokenize(summ)
        texts.append([w for w in tok if not w.lower() in stop_words])

#Tokenizing and removing stopwrods & punctuations from plot
tokens=tokenizer.tokenize(plot)
doc = [w for w in tokens if not w.lower() in stop_words] 

#creating corpus
dictionary = corpora.Dictionary(texts)
corpus = [dictionary.doc2bow(text) for text in texts]


#similarity model (Latent Semantic Indexing)
lsi = models.LsiModel(corpus, id2word=dictionary, num_topics=2)

# convert the query (plot) to LSI space
vec_bow = dictionary.doc2bow(doc)
vec_lsi = lsi[vec_bow]  

# transform corpus to LSI space and index it
index = similarities.MatrixSimilarity(lsi[corpus])  
index.save('deerwester.index')
index = similarities.MatrixSimilarity.load('deerwester.index')

#Querying 
sims = index[vec_lsi]  # perform a similarity query against the corpus

#Print the top ten similar summaries to plot
sims = sorted(enumerate(sims), reverse=False)
for i, s in enumerate(sims):
    if i<=9:
        print(s, summary[i])

'''

stoplist = set('for a of the and to in . , ? ! : ;'.split())
texts = [
    [word for word in document.lower().split() if word not in stoplist]
    for document in summary]


# remove words that appear only once
frequency = defaultdict(int)
for text in texts:
    for token in text:
        frequency[token] += 1

texts = [
    [token for token in text if frequency[token] > 1]
    for text in texts]

dictionary = corpora.Dictionary(texts)
corpus = [dictionary.doc2bow(text) for text in texts]

print(corpus)

'''