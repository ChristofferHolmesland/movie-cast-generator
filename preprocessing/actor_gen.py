import pandas as pd
import numpy as np 
import ast
import sys

file_path=('../input2.txt')

with open(file_path,'rt') as f:
    for i,line in enumerate(f):
        if line.split()[0] == 'genre:':
            genre=(line.split(',')[1:-1])
        if line.split()[0] == 'actors:':
            actors=(line.split(';')[1:-1])
        if line.split()[0] == 'plot:' :
            plot=' '.join(line.split()[1:])

genre_data=pd.read_csv('../mrjob_data/genre_scores.tsv', sep="\t", header= 0, index_col=0)
actor_data=pd.read_csv('../data/name.tsv', sep="\t", header= 0, index_col=0)
summary_data=pd.read_csv('../data/summary_box_office.tsv', sep="\t", header= 0, index_col=0)

#Converting genre_score column to dictionaries 
genre_data['genre_score'].apply(ast.literal_eval)

#print(type(genre_data['genre_score']))

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

    lst=[None] * len(actor_id)
    genre = [g.strip() for g in genre] # This should only be done once after reading input
    for j, ID in enumerate(actor_id):
        # Get the genre dictionary
        f = genre_data.loc[(genre_data.index == (ID))]["genre_score"]
        # If the actor has genres we are looking for 
        if len(f) == 0:
           continue
        # Convert to python dictionary
        f = ast.literal_eval(f.iloc[0])
        # Extract the genres we are looking for
        lt = [{k: f[k]} for k in f if k in genre]
        # Save them in a dictionary
        lst[j] = {ID:lt}

    lst = [l for l in lst if l != None]

    dic={i:lst}
    fin.append(dic)

#Writing into output file
outF = open("../output2.txt", "w")
for line in fin:
  outF.write(str(line))
  outF.write("\n")
outF.close()

'''