import tensorflow as tf
import tensorflow_hub as hub
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import re
import seaborn as sns

module_url = "https://tfhub.dev/google/universal-sentence-encoder/4" 
model = hub.load(module_url)
print ("module %s loaded" % module_url)

def embed(input):
    return model(input)

data = pd.read_csv("./data/summary_box_office.tsv", sep="\t", header=0, index_col=0)
summaries = data.summary.dropna()

s = ["A romantic doctor Gary is left by his love. A school teacher tries to help him to get back on track."]
s.extend(summaries.tolist())

s = s[:50000]

sims = embed(s)
sims2 = []
for i in range(len(s)):
    sims2.append((i, np.dot(sims[0], sims[i])))

sims2.sort(key=lambda x: x[1], reverse=True)

for i in range(10):
    print("{}. {}. {}".format(i, sims2[i][1], s[sims2[i][0]]))