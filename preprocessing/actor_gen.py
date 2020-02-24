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
for l in range(0,len(actors)):
    for i,a in enumerate(actors[l]):
        if a == ',':
            gender.append(actors[l][1:i])
            ag.append(int(actors[l][i+2:-1]))    
actor=pd.DataFrame({'gender': gender, 'age': ag}) 
actor['gender'].loc[actor['gender'] == 'Male'] = 0.0
actor['gender'].loc[actor['gender'] == 'Female'] = 1.0

#print(actor)
#print(actor_data)

#print(actor_data.loc[(actor_data['gender'] == actor.values[0][0]) & (actor_data["age"]==actor.values[0][1])])

#print(actor.values[0][0])


#Comparing input actor dataframe and names.tsv dataframe 
match=[]
for i in range(0,len(actor.values)):
    match=actor_data.loc[(actor_data['gender'] == actor.values[i][0]) & (actor_data["age"]==actor.values[i][1])]
    actor_id=match.index
    #for ID in actor_id:

li=['nm9735552', 'nm9737044', 'nm9806928', 'nm9818684']
for l in li:
    f=(genre_data.loc[(genre_data.index == (l))])

f['genre_score']=f['genre_score'].apply(ast.literal_eval)

#print(f['genre_score'][0].find('Action'))
print(type(f['genre_score'][0]))

for g in genre:
    print(str(g))
    #if str(g) in f
    #i=((f['genre_score'])
    #print(i)
    #print(f['genre_score'][0][i+len(g)+3:i+len(g)+6])



#f['score'] = [x[0]['Adventure'] for x in f['genre_score']]

#i=((f['genre_score'][0].find('Drama')))
#print(f['genre_score'][0][i+len('Drama')+3:i+len('Drama')+6])


