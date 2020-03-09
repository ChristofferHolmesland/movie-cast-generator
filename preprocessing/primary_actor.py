import pandas as pd
import numpy as np 
import ast
import os
import re
import seaborn as sns
import tensorflow as tf
import tensorflow_hub as hub

file_path=('C:/Users/malav/Desktop/dat500-project/output2.txt')

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
db=pd.read_csv('C:/Users/malav/Desktop/dat500-project/principals.tsv', sep='\t')
df = pd.merge(data0, db, how='inner', left_on='Actor ID', right_on='nconst')
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

#list of titles per actor ID
df1=df.groupby(['nconst'])['tconst'].apply(list)

#print(df1)

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

#print(df2)

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

#print((summ_score))
df_rank=df2[df2.index.isin(summ_list)]
df_rank['summary_score']=summ_score
print(df_rank)
