from mrjob.job import MRJob
from mrjob.protocol import TextValueProtocol

class PreprocessTitle(MRJob):
    INPUT_PROTOCOL = TextValueProtocol
    OUTPUT_PROTOCOL = TextValueProtocol

    def reducer(self, _, values):
        yield _, list(values)[0]

    def mapper(self, _, line):
        values = line.split("\t")
        
        # Save header
        if values[0] == "tconst":
            yield "header", "{}\t{}".format(values[0], values[8])
            return
        
        # Remove titles that are not a movie
        if values[1] not in ("movie", "tvMovie"):
            return
 
        # Remove movies with missing genre information
        if values[8] == "\\N":
            return

        yield values[0], "{}\t{}".format(values[0], values[8])

if __name__ == '__main__':
    PreprocessTitle.run()