# Movie cast generation using a search engine
This repository contains the work we did on our project for the DAT500 - Data intensive systems course at the University of Stavanger. The project is a search engine that can be used by movie directors to find cast suggestions based on actor descriptions, movie genres and movie plot. 

The required data can be downloaded from [IMDb](https://www.imdb.com/interfaces/), except the movie plots which are available from the [OMDb API](http://www.omdbapi.com/). Your OMDb API keys should be put in a file called omdb_api_keys.secret in the secrets folder, each key on a seperate line. The movie plots can then be downloaded using the preprocessing/omdbpy_downloader.py script. If you do not have API keys then they can be generated using the preprocessing/generate_api_keys.py script. This script relies on two APIs which might change between now and when you read this. If the script does not give you any keys then you need to register on the OMDb site instead.

The search engine is implemented in Python and on Spark using PySpark. If you are running the Python version the data files should be in the data folder. If you are running on Spark then they should be available on the HDFS in the ~/project/data folder. We assume you have a working Hadoop cluster running Spark. Check the report in report/main.pdf if you want to mirror our cluster setup. The preprocessing scripts that should run on Spark are written using the MRJob library. There are also two scripts that run on Spark. They can be found in the mrjob folder, and in the spark folder. A list of all the commands required to run the preprocessing are found in spark/commands.txt. These commands assume the raw data is available here: hdfs:///user/ubuntu/project/data/. The Python implementation uses Pandas to perform the preprocessing. The scripts are located in the preprocessing folder.

# Instructions:
## Run search on Spark/Hadoop:
- Delete everything from hdfs:///user/ubuntu/project/spark/, except actors.tsv and movies.tsv.
  - `hdfs dfs -rm -r project/spark/cand*`
  - `hdfs dfs -rm -r project/spark/result.tsv`
  - `hdfs dfs -rm -r project/spark/movies_score.tsv/`
- If you are not able to connect to the hdfs file system then you need to start it:
  - `stop-all.sh`
  - `start-all.sh`
- If you deleted actors.tsv or movies.tsv then you can recreate them by running:
  - `spark-submit ~/project/spark/combiner.py`
- Write the search query in ~/project/spark/input.txt.
- Run the search script.
  - `python3 ~/project/spark/run_search.py`
- Execution time is ~15-17 minutes.
- Transfer result from the hdfs file system to the local file system.
  - `hdfs dfs -text project/spark/result.tsv/par* > result.tsv`

## Run search using Python:
- Note: This could take several days to run depending on your hardware. See the report for an explanation.
- Execute algorithm/actor_gen.py
- Execute algorithm/actor_ranked.py
- Execute algorithm/relation_score_final.py
- The result can be found in data/final3_top25_latest.tsv

# Accessing our Hadoop cluster (assuming you have the IP :=))
- Connect to master node.
  - `ssh ubuntu@ip`
- From the master node you can connect to slave-1, slave-2 and slave-3.
  - `ssh ubuntu@slave-1`
- File structure:
  - hdfs data is stored in ~/data.
  - hadoop is installed in ~/hadoop.
  - spark is installed in  ~/spark.
  - The project files are in ~/project.
    - /assets and /variables are used by tensorflow for caching
    - /data is a local copy of some of the data in the hdfs filesystem
    - /mrjob contains the mrjob preprocessing scripts
    - /spark contains the spark implementation of the search algorithm
