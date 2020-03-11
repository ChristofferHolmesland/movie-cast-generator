from mrjob.job import MRJob
from mrjob.protocol import TextValueProtocol

class PreprocessRatings(MRJob):
    INPUT_PROTOCOL = TextValueProtocol
    OUTPUT_PROTOCOL = TextValueProtocol

    def reducer(self, _, values):
        yield _, list(values)[0]

    def mapper(self, _, line):
        values = line.split("\t")
        yield values[0], line

if __name__ == '__main__':
    PreprocessRatings.run()