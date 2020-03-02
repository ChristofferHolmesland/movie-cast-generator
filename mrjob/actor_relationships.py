from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextValueProtocol

class PreprocessActorRelationship(MRJob):
    INPUT_PROTOCOL = TextValueProtocol
    OUTPUT_PROTOCOL = TextValueProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper_group_actors_by_movie,
                   reducer=self.reducer_group_actors_by_movie),
            MRStep(reducer=self.reducer_calculate_relationship)
        ]

    def reducer_group_actors_by_movie(self, key, values):
        if key == "header":
            yield key, list(values)[0]
            return

        values = list(values)
        for value in values:
            yield value, values

    def mapper_group_actors_by_movie(self, _, line):
        values = line.split("\t")

        if values[0] == "tconst":
            yield "header", "nconst\tunique\ttotal"
            return

        yield values[0], values[1]

    def reducer_calculate_relationship(self, key, values):
        if key == "header":
            yield key, list(values)[0]
            return

        # Values is a list of lists
        values = [value for value_list in values for value in value_list if value != key]

        unique = len(set(values))
        total = len(values)
        
        yield total, f"{key}\t{unique}\t{total}\t"

if __name__ == '__main__':
    PreprocessActorRelationship.run()