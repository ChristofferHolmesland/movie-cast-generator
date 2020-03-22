#Generates a ranked list of actors based on weighted average of genre_score and summary_score 
#Requires output.txt, principals.tsv, input.txt, title.tsv, summary_box_office.tsv
#Outputs into primary_actor.tsv

import pandas as pd 
import numpy as np 
import ast
import os
import re
import seaborn as sns
import tensorflow as tf
import tensorflow_hub as hub

#Loading the actor_gen output 
file_path=('../output2.txt')
actor=[]
with open(file_path,'rt') as f:
    for i,line in enumerate(f):
        line=(ast.literal_eval(line))
        actor.append(line)

#Generating ranked list of primary (actor[0]) actors and avg genre score: 
actors=[]
for n,ID in enumerate(list(actor[0].values())[0]):
    if ID:
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

#print(data0)

#Collecting titles from principal.tsv
principals=pd.read_csv('../data/principals.tsv', sep='\t')
df = pd.merge(data0, principals, how='inner', left_on='Actor ID', right_on='nconst')
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

#list of titles per actor ID
df1=df.groupby(['nconst'])['tconst'].apply(list)

#Loading genres and plot from input
file_path1=('../input2.txt')
with open(file_path1,'rt') as f:
    for i,line in enumerate(f):
        if line.split()[0] == 'plot:' :
            plot=' '.join(line.split()[1:])
        if line.split()[0] == 'genre:':
            genre=[x.strip() for x in line.split(',')[1:-1]]

#Checking if the movies' genre matches our input genres
df_genre=pd.read_csv('../data/title.tsv', sep='\t')
df_genre['presence']=df_genre['genres'].apply(lambda x: 0 < sum([1 for y in x.split(',') if y in genre]))
df_genre=df_genre[df_genre['presence']==True]
#print(df_genre)

#Merging ranked actors and genre checked movies 
df3=pd.merge(df,df_genre, how= 'inner', left_on='tconst', right_on='tconst')
#print(df3)

#Comparing plot with all summaries 
#Loading all summaries 
summ=pd.read_csv('../data/summary_box_office.tsv', sep='\t')

#Merged dataframe containing Actor ID, Title ID, summary and avg g_Score
df2=pd.merge(df3, summ, how='inner', left_on='tconst', right_on='tconst')
df2=df2.drop(['nconst','boxoffice','presence','genres'],axis=1)

summary=list(df2['summary'])

#Setting all non-str values to 'NA'
summaries=[]
for text in summary:
    if type(text)!= str:
        summaries.append('NA')
    else:
        summaries.append(text)

#importing the pre-trained model 
module_url = "https://tfhub.dev/google/universal-sentence-encoder/4" 
model = hub.load(module_url)
print ("module %s loaded" % module_url)

def embed(input):
    return model(input)

s=[plot]
summaries=s+summaries

sims = embed(summaries)
sims2 = []
for i in range(len(summaries)):
    sims2.append((i, np.dot(sims[0], sims[i])))
sims2.sort(key=lambda x: x[1], reverse=True)

#computing similar summaries to plot 
summ_match=[]
summ_score=[]
sum_dic=[]
for i in range(1,len(sims2)):
   #print("{}. {}".format(sims2[i][1], summaries[sims2[i][0]]))
   dic={(sims2[i][1]) : summaries[sims2[i][0]]}
   #summ_score.append(sims2[i][1])
   #summ_match.append(summaries[sims2[i][0]])
   sum_dic.append(dic)

summ_list=[]
for i,text in enumerate(summary):
    for summ in sum_dic:
        boo=0
        for key,value in summ.items():
            if text== value:
                summ_list.append(i)
                summ_score.append(key)
                boo=1
                break
        if boo==1:
            break

#Getting the summary score
df_rank=df2[df2.index.isin(summ_list)]
df_rank['summary_score']=summ_score

#Dropping summary and titles
df_rank_final=df_rank.drop(['summary','tconst'],axis=1)
#Grouping by Actor ID
df_rank_final=df_rank_final.groupby('Actor ID').agg(lambda x: x.tolist())
#Choosing max avg_genre_score
df_rank_final['Avg Genre Score']=df_rank_final['Avg Genre Score'].apply(lambda x: max(x))
#Computing mean of summary_score
df_rank_final['summary_score']=df_rank_final['summary_score'].apply(lambda x: np.mean(x))

#Multiplying summary_score by 10
df_rank_final['summary_score']=df_rank_final['summary_score'].apply(lambda x: 10*(x))

#Average of summary_score and genre_score as score
col = df_rank_final.loc[: , "Avg Genre Score":"summary_score"]
df_rank_final['score']= col.mean(axis=1)

#Sorting by score
df_rank_final=df_rank_final.sort_values(by=['score'], ascending= False)
#print(df_rank_final)

#Writing it to csv
df_rank_final.to_csv('../data/primary_actor.tsv', sep= '\t', header=True)


