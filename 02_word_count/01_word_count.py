from mrjob.job import MRJob
from mrjob.step import MRStep


class MRWordCount(MRJob):

    # kazde slowo mapuje na 1
    def mapper(self, _, line):
        # zmienna words to lista slow z danej linii
        words = line.split()
        for word in words:
            # nie bierze pod uwage ze slowo moze miec na koncu kropke
            # i wtedy jest traktowane jako inny klucz
            yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordCount.run()