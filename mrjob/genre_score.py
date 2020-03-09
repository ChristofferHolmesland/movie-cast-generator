from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextValueProtocol

class PreprocessGenreScore(MRJob):
    INPUT_PROTOCOL = TextValueProtocol
    OUTPUT_PROTOCOL = TextValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper_collect_title,
                   reducer=self.reducer_title),
            MRStep(reducer=self.reducer_actor)
        ]

    def reducer_actor(self, actor_id, values):
        if actor_id == "header":
            yield "", list(values)[0]
            return

        scores = {}
        for value in values:
            for key in value:
                if key not in scores:
                    scores[key] = []
                scores[key].append(value[key])

        avg_scores = {}
        for key in scores:
            s = sum([float(score) for score in scores[key]])
            n = len(scores[key])
            avg_scores[key] = round(s / n, 4)

        yield actor_id, "{}\t{}".format(actor_id, avg_score)

    def reducer_title(self, key, values):
        if key == "header":
            yield key, list(values)[0]
            return

        rating = None
        genres = None
        for value in values:
            if value[0] == "rating":
                rating = value[1]
            elif value[0] == "genres":
                genres = value[1]

            if rating != None and genres != None:
                break

        if rating == None or genres == None:
            return

        genre_ratings = {genre: rating for genre in genres}
        for value in values:
            if value[0] == "actor":
                yield value[1], genre_ratings

    def mapper_collect_title(self, _, line):
        if len(line.strip()) == 0:
            return
        
        values = line.split("\t")
        
        if values[0] == "tconst" and values[1] == "nconst":
            yield "header", "nconst\tgenre_score"
            return

        # ratings.tsv
        if len(values) == 3:
            yield values[0], ["rating", values[1]]
            return

        # principals.tsv
        if values[1][:2] == "nm":
            yield values[0], ["actor", values[1]]
            return

        # title.tsv
        yield values[0], ["genres", values[1].split(",")]

if __name__ == '__main__':
    PreprocessGenreScore.run()