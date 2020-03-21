#Evaluates the relationship between primary-secondary-tertiary actors
#Requires primary_actor.tsv, secondary_actor.tsv, tertiary_actor.tsv, principals.tsv
#Outputs into 

import pandas as pd
import numpy as np 
import sys

principals=pd.read_csv('../data/principals.tsv', sep="\t", header= 0)
primary=pd.read_csv('../data/primary_actor.tsv', sep="\t", header= 0)
secondary=pd.read_csv('../data/secondary_actor.tsv', sep="\t", header= 0)
tertiary=pd.read_csv('../data/tertiary_actor.tsv', sep="\t", header= 0)

#Grouping titles by actors
grouped = principals.groupby(['nconst'])['tconst'].apply(list)

#Merging all actors dataframes with titles list
pdf=pd.merge(primary,grouped, how= 'inner', left_on='Actor ID', right_on='nconst')
sdf=pd.merge(secondary,grouped, how= 'inner', left_on='Actor ID', right_on='nconst')
tdf=pd.merge(tertiary,grouped, how= 'inner', left_on='Actor ID', right_on='nconst')

#Computing grouped average
def grouped_average(primary,secondary,tertiary):
    lst=[]
    for i,s1 in enumerate(primary['score']):
        for j,s2 in enumerate(secondary['score']):
            for k,s3 in enumerate(tertiary['score']):
                dic={(i,j,k): ((s1+s2+s3)/3)}
                lst.append(dic)

    #Sorting it
    a=[]
    for i in lst:
        a.append({k: v for k, v in sorted(i.items(), key=lambda item: item[1])})

    score=[]
    index=[]
    top=a[:20]
    for i in a[:20]:
        for k,v in i.items():
            index.append(k)
            score.append(v)

    p=[]
    s=[]
    t=[]
    for i in index:
        p.append(i[0])
        s.append(i[1])
        t.append(i[2])

    pri=[]
    sec=[]
    ter=[]
    for i in p:
        pri.append(primary['Actor ID'].iloc[i])
        
    for i in s:
        sec.append(secondary['Actor ID'].iloc[i])
        
    for i in t:
        ter.append(tertiary['Actor ID'].iloc[i])
    

    df_final=pd.DataFrame({'Primary Actor': pri,'Secondary Actor': sec,'Third Actor': ter})
    df_final.to_csv('../data/final.tsv', sep= '\t', header=True)
    return (df_final)
    

#Co_actors of primary actors column added to pdf
co_actors1=[]
for item in pdf['tconst'].to_list():
    coactors=[]
    for title in item:
        match=(principals.loc[(principals['tconst'] == title)])
        coactors.append(match['nconst'])
    co_actors1.append(coactors)
pdf['co_actors']=co_actors1


#Co_actors of secondary actors column added to sdf
co_actors2=[]
for item in sdf['tconst'].to_list():
    coactors=[]
    for title in item:
        match=(principals.loc[(principals['tconst'] == title)])
        coactors.append(match['nconst'])
    co_actors2.append(coactors)    
sdf['co_actors']=co_actors2


#Co_actors of tertiary actors column added to tdf
co_actors3=[]
for item in tdf['tconst'].to_list():
    coactors=[]
    for title in item:
        match=(principals.loc[(principals['tconst'] == title)])
        coactors.append(match['nconst'])
    co_actors3.append(coactors)    
tdf['co_actors']=co_actors3


p_actors=primary['Actor ID']
s_actors=secondary['Actor ID']
t_actors=tertiary['Actor ID']


#Checking if pdf contains s_actors or t_actors
s1=[]
t1=[]
for index,i in enumerate(co_actors1):
    for j in s_actors:
        for k in i:
            if j in k:
                s1.append(j)
    for j in t_actors:
        for k in i:
            if j in k:
                t1.append(j)

#Checking if match exists and then applying grouped average
if s1:
    for actor in s1:
        sec=secondary.loc[(secondary['Actor ID']==actor)]
if t1:
    for actor in t1:
        ter=tertiary.loc[(tertiary['Actor ID']==actor)]
if len(s1)==0:
    result=grouped_average(primary,secondary,tertiary)   
elif len(t1)==0:
    result=grouped_average(primary,secondary,tertiary)
else:
    result=grouped_average(primary,sec,ter)


#Checking if sdf contains p_actors or t_actors
p2=[]
t2=[]
for index,i in enumerate(co_actors2):
    for j in p_actors:
        for k in i:
            if j in k:
                p2.append(j)
    for j in t_actors:
        for k in i:
            if j in k:
                t2.append(j)


#Checking if match exists and then applying grouped average
if p2:
    for actor in p2:
        pri=primary.loc[(primary['Actor ID']==actor)]
if t2:
    for actor in t2:
        ter=tertiary.loc[(tertiary['Actor ID']==actor)]
if len(p2)==0:
    result=grouped_average(primary,secondary,tertiary)   
elif len(t2)==0:
    result=grouped_average(primary,secondary,tertiary)
else:
    result=grouped_average(pri,secondary,ter)


#Checking if tdf contains p_actors or s_actors
p3=[]
s3=[]
for index,i in enumerate(co_actors3):
    for j in p_actors:
        for k in i:
            if j in k:
                p3.append(j)
    for j in s_actors:
        for k in i:
            if j in k:
                s3.append(j)
                
#Checking if match exists and then applying grouped average
if p3:
    for actor in p3:
        pri=primary.loc[(primary['Actor ID']==actor)]
if s3:
    for actor in s3:
        sec=secondary.loc[(secondary['Actor ID']==actor)]
if len(p3)==0:
    result=grouped_average(primary,secondary,tertiary)   
elif len(s3)==0:
    result=grouped_average(primary,secondary,tertiary)
else:
    result=grouped_average(pri,sec,tertiary)



