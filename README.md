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

# Tasks:
## Task 1.
Suggested changes to the python algorithm:
- [1] actor_relation.py writes to final.tsv several times.{Looking into it. Now that we are using actor_ranked.py (Read below [3]), I am thinking of using task 2 directly instead of this method}
- [1] Make it possible to have between 1-3 actors. (DONE)
- [2] Use max(similarity_score). I made this mistake on spark and compared to the other results, it looks better because the groups have a more varied combination of actors. (DONE)
- [3] IO operations are slow. Instead of reading the same file several times, it should only happen once. The same is true for tensorflow. (DONE)
- [3] Lists are slow because they are basically a copy of the data that can be found in the dataframe. We should at least avoid using list.append(). Since we usually know the length of the data it should be possible to pre-allocate the memory. {ummm, I am not sure if we can do that in actor_ranked.py. But we can do that in actor_relation.py and I shall look into it}
- [3] *_actor.py files contain the same logic with different input/output. Would be better to have it as a function. That would also help with 3. (DONE) {Now there is actor_ranked.py that will replace primary,secondary,tertiary_actor.py}

## Task 2.
Expand on algorithm by creating a graph with the relationship between actors.
General idea:
- Pre-processing
    - Read principals.tsv and ratings.tsv.
    - Create dataframe with columns [tconst, rating, [nconsts...]].
    - For every movie, create an edge between each pair of actors with value 10-rating.
    - Prune graph such that only the lowest valued edge is left between two nodes.
    - Store graph to disk.

- Algorithm
    - For each actor pair in the group
        - Find optimal path between the pair
        - Their relation score should be something like: 10 - cost, when cost = 10 - (11 - distance + 10 - avg(value along path)) / 2
    - The group score is then (avg(score) + avg(relation score)) / 2
    - If the path finding is slow then maybe we want to do it in the pre-processing instead.

## Task 3.
Task 2 in MRJob/Spark/Hadoop

## Task 4.
Change file paths to be relative to the executing script, not the working directory of the executor.

## Task 5.
Make a script that executes the other python scripts (so the user can run a search with on command).

## Task 6.
Implement the bfs_multigrouped function in spark

## Task 7.
Write report
- [x] Background
    - Short self-contained summary of neccessary background information.
    - What is IMDB
    - User rating
    - Movie casts
    - Box office
- Data
    - Sources
    - Attributes
    - Date of download because later data may be missing
- [x] Describe how algorithm results can be evaluated
    - How do we decide if the result is good?
    - What is a good result?
    - Assumptions
        - Any movie with a high user rating on IMDB has a good cast
    - Goal
        - If you do a search with the attributes from IMDB the original cast should be early in the result
- Experimental evaluation
    - Result of search query
    - Graph of runtime of different inputs
- Conclusion
- Reference list
    - Figure out why the reference numbers are not showing