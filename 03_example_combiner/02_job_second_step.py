from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r'[\w]+')

class MRJobFirstStep(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper_get_keys,
                   reducer=self.reducer_find_most_common_word)
        ]

    def mapper(self, _, line):
        words = WORD_RE.findall(line)
        for word in words:
            yield word, 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

    def mapper_get_keys(self, key, value):
        # najpierw jest None abysmy mogli wszystko pogrupowac.
        # Wyswietli na przyklad null [2, "She"]
        yield None, (value, key)

    def reducer_find_most_common_word(self, key, values):
        # jest tylko klucz null wiec dostaniemy liste dwuelementowych
        # list takich jak np [2, "She"]. Natomiast max bierze maksimum
        # z pierwszych elementow z kazdej dwuelementowej listy
        yield max(values)


if __name__ == '__main__':
    MRJobFirstStep.run()