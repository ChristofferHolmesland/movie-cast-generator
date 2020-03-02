import pandas as pd
import numpy as np 
import ast

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
file_path=('C:/Users/malav/Desktop/dat500-project/input.txt')

with open(file_path,'rt') as f:
    for i,line in enumerate(f):
        if line.split()[0] == 'plot:' :
            plot=' '.join(line.split()[1:])

summ=pd.read_csv('C:/Users/malav/Desktop/dat500-project/summary_box_office.tsv', sep='\t')

#MErged dataframe containing Actor ID, Title ID, summary and avg g_Score
df2=pd.merge(df, summ, how='inner', left_on='tconst', right_on='tconst')
df2=df2.drop(['nconst','boxoffice'],axis=1)

summary=list(df2['summary'])
print(len(summary))

