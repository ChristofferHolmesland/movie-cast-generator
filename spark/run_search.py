import os
import subprocess

genres_raw = ""
actors_raw = ""
plot_raw = ""

file_path = "./input.txt"
file_name = os.path.join(os.path.dirname(__file__), file_path)

with open(file_name, "r") as f:
    genres_raw = f.readline().split(":")[1]
    actors_raw = f.readline().split(":")[1]
    plot_raw = f.readline().split(":")[1:]

genres = ",".join([genre.strip() for genre in genres_raw.split(",") if len(genre.strip()) > 0])
actors = ",".join([actor.strip().replace(",", "") for actor in actors_raw.split(";") if len(actor.strip()) > 0])
plot = (":".join(plot_raw)).strip()

os.environ["search_genres"] = genres
os.environ["search_actors"] = actors
os.environ["search_plot"] = plot

program = "spark-submit {}".format(os.path.join(os.path.dirname(__file__), "search.py"))
subprocess.run(program, shell=True)