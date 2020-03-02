from mrjob.job import MRJob
from mrjob.protocol import TextValueProtocol

class PreprocessName(MRJob):
    INPUT_PROTOCOL = TextValueProtocol
    OUTPUT_PROTOCOL = TextValueProtocol

    def reducer(self, _, values):
        yield _, list(values)[0]

    def mapper(self, _, line):
        values = line.split("\t")

        # Save header
        if values[0] == "nconst":
            yield "header", "{}\t{}\t{}\t{}\t{}\t{}".format(values[0], values[1], values[2], values[3], values[5], "gender")
            return
        
        # Remove people without a birth year
        if values[2] == "\\N":
            return

        # Remove dead people
        if values[3] != "\\N":
            return

        # Remove people who are not known for anything
        if values[5] == "\\N":
            return
        
        gender = -1
        if "actor" in values[4]:
            gender = 0
        elif "actress" in values[4]:
            gender = 1
        else:
            return

        yield values[0], "{}\t{}\t{}\t{}\t{}\t{}".format(values[0], values[1], values[2], values[3], values[5], gender)

if __name__ == '__main__':
    PreprocessName.run()