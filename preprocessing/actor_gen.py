import pandas as pd
import numpy as np 
import ast

file_path=('C:/Users/malav/Desktop/dat500-project/input.txt')

with open(file_path,'rt') as f:
    for i,line in enumerate(f):
        if line.split()[0] == 'genre:':
            genre=(line.split(',')[1:-1])
        if line.split()[0] == 'actors:':
            actors=(line.split(';')[1:-1])
        if line.split()[0] == 'plot:' :
            plot=' '.join(line.split()[1:])

genre_data=pd.read_csv('C:/Users/malav/Dropbox/dat500-project/genre_scores.tsv', sep="\t", header= 0, index_col=0)
actor_data=pd.read_csv('C:/Users/malav/Dropbox/dat500-project/name.tsv', sep="\t", header= 0, index_col=0)
summary_data=pd.read_csv('C:/Users/malav/Dropbox/dat500-project/summary_box_office.tsv', sep="\t", header= 0, index_col=0)

#Converting genre_score column to dictionaries 
genre_data['genre_score'].apply(ast.literal_eval)

#Including AGE value in name.tsv dataframe
if actor_data['deathYear'].dtype != 'int' :
    actor_data['age']=(2020-(actor_data['birthYear']))
else:
    actor_data['age']='NA'

#generating input actor dataframe
gender=[]
ag=[]
ages=[]
for l in range(0,len(actors)):
    for i,a in enumerate(actors[l]):
        if a == ',':
            gender.append(actors[l][1:i])
            ag.append((actors[l][i+2:-1]))   
    #checking for age range 
for i in ag:
    if '-' in i:
        low=(i.split('-'))[0]
        up=(i.split('-'))[1]
        ages.append({'low':int(low),'high':int(up)})
    elif '-' not in i:
        ages.append(int(i))
#print(ages)
actor=pd.DataFrame({'gender': gender, 'age': ages}) 
actor['gender'].loc[actor['gender'] == 'Male'] = 0.0
actor['gender'].loc[actor['gender'] == 'Female'] = 1.0

#Comparing input actor dataframe and names.tsv dataframe 
match=[]
fin=[]
for i in range(0,len(actor.values)):
    if type(actor.values[i][1])== dict:
        for j in range(list(actor.values[i][1].values())[0],list(actor.values[i][1].values())[1]+1):
            match=(actor_data.loc[(actor_data['gender'] == actor.values[i][0]) & (actor_data["age"]==j)])
    else:
        match=(actor_data.loc[(actor_data['gender'] == actor.values[i][0]) & (actor_data["age"]==actor.values[i][1])])
    actor_id=match.index
    lst=[]
    for ID in actor_id:
        lt=[]
        f=(genre_data.loc[(genre_data.index == (ID))])
        f['genre_score']=f['genre_score'].apply(ast.literal_eval)
        for g in genre:
            for key, value in f['genre_score'][0].items():
                if g.strip() == key:
                    kv={key:value}
                    #lst.append ({ID: kv})
                    lt.append(kv)
        lst.append({ID:lt})
    dic={i:lst}
    fin.append(dic)

#Writing into output file
outF = open("C:/Users/malav/Desktop/dat500-project/output.txt", "w")
for line in fin:
  outF.write(str(line))
  outF.write("\n")
outF.close()
