from mrjob.job import MRJob
from mrjob.protocol import TextValueProtocol

class PreprocessPrincipals(MRJob):
    INPUT_PROTOCOL = TextValueProtocol
    OUTPUT_PROTOCOL = TextValueProtocol

    def reducer(self, key, values):
        if key == "header":
            yield key, list(values)[0]
        else:
            values = list(values)
            if "keep" in values:
                values.remove("keep")
                for value in values:
                    yield key, value

    def mapper(self, _, line):
        values = line.split("\t")

        # Line belongs to title.tsv
        if len(values) == 2:
            if values[0] != "tconst":
                yield values[0], "keep"
            return
        
        # Line belongs to title.principals.tsv

        # Save header
        if values[0] == "tconst":
            yield "header", "{}\t{}".format(values[0], values[2])
            return
        
        yield values[0], "{}\t{}".format(values[0], values[2])

if __name__ == '__main__':
    PreprocessPrincipals.run()