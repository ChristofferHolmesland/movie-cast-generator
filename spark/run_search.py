import os
import time
import subprocess

genres_raw = ""
actors_raw = ""
plot_raw = ""

file_path = "./input.txt"
file_name = os.path.join(os.path.dirname(__file__), file_path)

# Read input file
with open(file_name, "r") as f:
    genres_raw = f.readline().split(":")[1]
    actors_raw = f.readline().split(":")[1]
    plot_raw = f.readline().split(":")[1:]

# Convert values to a simpler format
genres = ",".join([genre.strip() for genre in genres_raw.split(",") if len(genre.strip()) > 0])
actors = ",".join([actor.strip().replace(",", "") for actor in actors_raw.split(";") if len(actor.strip()) > 0])
plot = (":".join(plot_raw)).strip()

# Store values in environment variables that can be read by spark.
os.environ["search_genres"] = genres
os.environ["search_actors"] = actors
os.environ["search_plot"] = plot

start_time = time.time()

# Execute search.py on spark.
program = "spark-submit --master yarn --deploy-mode client {}".format(os.path.join(os.path.dirname(__file__), "search.py"))
subprocess.run(program, shell=True)

end_time = time.time()

print("Elapsed time: {} seconds".format(end_time - start_time))