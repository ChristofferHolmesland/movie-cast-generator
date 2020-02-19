import pandas as pd
import numpy as np
import gzip
from imdb import IMDb


from bs4 import BeautifulSoup
import requests
import re
import random

im = IMDb()

#title_filepath='name.basics.tsv.gz'
file_path='data/title.tsv'


#Extracting titleIDs from title_filepath
t_ids=[]
def get_ids(file_path):
    data=pd.read_csv(file_path,delimiter='\t')
    for i in data['tconst']:
        t_ids.append(str(i[2:9]))
    return(t_ids)

#Title summary (web scraped)
def get_summary(url):
    s=[]
    movie_page = requests.get(url)
    soup = BeautifulSoup(movie_page.text, 'html.parser')
    if (soup.find("div", class_="summary_text").contents):
        for i,text in enumerate(soup.find("div", class_="summary_text").contents):
            if (type(text)) != bs4.element.NavigableString:
                s.append((soup.find("div", class_="summary_text").contents)[i].contents[0])
            else:
                s.append((soup.find("div", class_="summary_text").contents)[i])
    return ("".join(s))

#Box office collection (web scraped)
def box_office(url):
    movie_page = requests.get(url)
    soup = BeautifulSoup(movie_page.text, 'html.parser')
    for a in soup.find_all("div", class_="txt-block"):
        if len(a.contents) > 2:
            for i in a.contents[1]:
                if i == 'Cumulative Worldwide Gross:':
                    return(a.contents[2])
                else:
                    return('-')

data=[]

# 3) get_summary() and box_office()


for ID in t_ids:
    name=(im.get_movie(ID)['title'])
    url='https://www.imdb.com/title/tt'+ID
    summ=(get_summary(url))
    bo=(box_office(url))
    data.append(['tt'+ID,name,url,summ,bo])

df=pd.DataFrame(data)
df.columns=['MovieID','Movie Name','Movie URL','Movie Summary', 'Box Office Collection']
df.to_csv('C:/Users/malav/Desktop/dat500-project/Summary_BO.tsv', sep = '\t')
