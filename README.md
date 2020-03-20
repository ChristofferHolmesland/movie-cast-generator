# DAT500 Project

# Instructions:
## Hadoop cluster
- Connect to master node.
  - `ssh ubuntu@152.94.169.179`
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
