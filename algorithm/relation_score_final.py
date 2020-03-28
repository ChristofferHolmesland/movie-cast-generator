#Evaluates the relationship between {}_actors.tsv
#Requires all {}_actor.tsv files, principals.tsv, input.txt
#Outputs into final.tsv


import pandas as pd 
import numpy as np 
import ast
import os
import re
import sys
import ast
import glob
import itertools

#Reading graph.tsv into graph dataframe
graph=pd.read_csv('../data/graph.tsv', sep='\t')
graph=graph.loc[:, ~graph.columns.str.contains('^Unnamed')]

#Converting values to dictionary and to list
graph['values']=graph['values'].apply(lambda x: ast.literal_eval(x))
graph['values']=graph['values'].apply(lambda x: list(x.items()))

#Exploding/Flattening out
new_graph=graph.explode('values')

#Multi-indexing 
new_graph[["to_node", "value"]]= pd.DataFrame(new_graph["values"].values.tolist(), index=new_graph.index)
new_graph.drop(columns=["values"], inplace=True)
new_graph.set_index(keys=["key", "to_node"], drop= True, inplace=True)

#Reading all {}_actor.tsv intoa list od dataframes
path = r'../data' # use your path
all_files = glob.glob(path + "/*_actor3.tsv")
li = []
for filename in all_files:
    df = pd.read_csv(filename, sep="\t", header=0)
    li.append(df)

#Collecting only the Actor_IDs
IDs=[]
for i in range(len(li)):
    ids=[]
    for j in range(len(li[i])):
        ids.append((li[i].iloc[j])[0])
    IDs.append(ids)
IDs

#Function to find the relation score between 2 actors
def relation_score(a,b,new_graph):
    
    #Checking if a and b have acted together
    d=(new_graph.loc[a]).to_dict()
    to_node=[]
    for k,v in d.items():
        for l,m in v.items():
            to_node.append(l)

    if b in to_node:
        #This is the starting step
        step = {
            "node": a,
            "value": 0,
            "distance": 0,
            "values": [],
            "cost": 0,
            "prev": ""
        }

        # This is all of the nodes we already found the shortest path to
        finished = {}
        # This is the next steps to consider
        queue = [step]
        # This is basically Dijkstra's shortest path algorithm
        while len(queue) > 0:
            # Take the node with the lowest cost
            queue.sort(key=lambda x: x["cost"], reverse=True)
            current = queue.pop()
            # When you take a node from the queue it means that you have found the shortest path to that node.
            finished[current["node"]] = current
            #print("Looking at " + current["node"] + ", distance: " + str(current["distance"]) + ", cost: " + str(current["cost"]))
            # Stop when we find b
            if current["node"] == b:
                #print("Found path to {} :D".format(b))
                break
            # The distance to the next nodes is 1 more than the distance to this node
            new_dist = current["distance"] + 1
            # Values is a list of the graph values so that we can calculate the average when we are calculating the cost
            new_values = current["values"]
            # Find all of the edges from this node to it's neighbours as dict
            d=(new_graph.loc[a]).to_dict()
            for k,v in d.items():
                edges=v   
            for edge in edges:
                # If the edge is in finished it means that we already found a shorter path to that node
                if finished.get(edge): continue
                val = edges[edge]
                vals = new_values + [val]
                # Our scores (genre_score, similarity) are between 0 and 10. So we want this score to be in that range aswell.
                # The distance (11 - new_dist) means that shorter paths are preferred, A->B->C=9 instead of A->E->F->C=8.
                # (10 - sum(vals)/new_dist) is 10 - avg of the scores from the graph. The graph values are the opposite
                # of the rating, so a rating of 7 => 3, 9.5 => 0.5. This is done because we want to find the shortest path between actors.
                # If we didn't do this then the path would always try to visit every actor before finding christian. This is why we
                # subtract the average of the two numbers from 10. 
                cost = 10 - (11 - new_dist + 10 - sum(vals)/new_dist) / 2

                new_step = {
                    "node": edge,
                    "value": val,
                    "distance": new_dist,
                    "values": vals,
                    "cost": cost,
                    "prev": current["node"]
                }

                in_queue = False
                # Check if it is already in the queue, if it is and this cost is lower it should be updated
                for i in range(len(queue)):
                    if queue[i]["node"] == edge:
                        if queue[i]["cost"] > new_step["cost"]:
                            queue[i] = new_step
                        in_queue = True
                        break
                if in_queue: continue
                # If the edge was not in the queue it is added.
                queue.append(new_step)


        actor = b
        path = []
        while actor != "":
            path.append(actor)
            actor = finished[actor]["prev"]
        path = list(reversed(path))
        score=(10 - finished[b]["cost"])
        #print("Leonardo-Christian score: " + str(10 - finished[christian]["cost"]))
    
    else:
        score=0

    return(score)


#All of the IDs from {}_actor.tsv with each other 
Id_list = list(itertools.product(*IDs))

#Id_list as a dataframe
data=pd.DataFrame(Id_list)

#Computing scoresfor all combinations in Id_list as average of all {}_actors
Id1=[]
Id2=[]
score=[]
for i in range(len(IDs)):
    for j in range(len(IDs[i])):
        for m in range(i+1,len(IDs)):
            if (i+1)<(len(IDs)):
                for k in range(len(IDs[m])):
                    a=IDs[i][j]
                    b=IDs[m][k]
                    Id1.append(a)
                    Id2.append(b)
                    score.append(relation_score(a,b,new_graph))


#Making it to a dataframe
d1=pd.DataFrame({'ID1': Id1, 'ID2' : Id2, 'Score': score})

#Making a copy of data 
new_data=data

#Initialising r_score to 0
new_data['r_score']=0.0

#Iteratively merging {}_actors to new_data and summing score
for i in range(len(li)):
    for j in range(i,len(li)):
        if (j+1)<(len(IDs)):
            new_data=pd.merge(new_data,d1, how='inner', left_on=[i,j+1], right_on=['ID1','ID2'])
            new_data['r_score']=new_data['r_score']+new_data['Score']
            new_data=new_data.drop(['ID1','ID2','Score'],axis=1)


#Finding average of r_score
new_data['r_score']=new_data['r_score']/len(li)

#Removing all zero scored values
new_data=new_data.loc[(new_data['r_score']!=0)]
#Sorting 
new_data=new_data.sort_values(by='r_score', ascending=False)

#Merging r_score and gs_score
df=new_data
for i in range(len(li)):
    df=pd.merge(df,li[i], how='inner', left_on=i, right_on='Actor ID')
    sname=('score_{}'.format(i))
    df[sname]=df['score']
    df=df.drop(['Actor ID', 'Avg Genre Score', 'summary_score', 'score'], axis=1)

#Finding mean of the gs_score of the {}_actors 
sname=[]
for i in range(len(li)):
    sname.append(('score_{}'.format(i)))
col = df.loc[: , sname]
df['gs_score'] = col.mean(axis=1)

#Finding average of the r_score and gs_score and making the final list
df['final'] = df[["r_score","gs_score"]].mean(axis=1)
col_list=[i for i in range(len(li))]
col_list.append('final')
final_df=df[col_list]
final_df=final_df.sort_values(by='final', ascending=False)
final_df

#Outputting the final list as a tsv
final_df.to_csv('../data/final3_top25_latest.tsv', sep= '\t', header=True)


