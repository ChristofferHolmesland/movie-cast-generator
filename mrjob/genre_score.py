from mrjob.job import MRJob
from mrjob.protocol import TextValueProtocol

class PreprocessGenreScore(MRJob):
    INPUT_PROTOCOL = TextValueProtocol
    OUTPUT_PROTOCOL = TextValueProtocol

    def reducer(self, _, values):
        yield _, list(values)[0]

    def mapper(self, _, line):
        values = line.split("\t")

        

if __name__ == '__main__':
    PreprocessGenreScore.run()