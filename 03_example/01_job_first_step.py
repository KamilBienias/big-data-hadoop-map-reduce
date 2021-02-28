# Etap7. Odcinek: Map + Combine + Reduce

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r'[\w]+')

class MRJobFirstStep(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        words = WORD_RE.findall(line)
        for word in words:
            # nie zamienia slow na male
            yield word, 1

    # combiner to taki reduktor ale dzialajacy na kazdej linii osobno
    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRJobFirstStep.run()