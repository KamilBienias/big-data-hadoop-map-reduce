from mrjob.job import MRJob
from mrjob.step import MRStep


class MRSimpleJob(MRJob):

    # to jest funkcja (nie generator)
    # Mozna kilkukrotnie mapowac i redukowac.
    # Jak wykomentuje np parametr reducer to wykona sie tylko mapper
    def steps(self):
        return [
            # po self wpisuje nazwy generatorow
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    # para key value. Ale my tylko value przekazujemy czyli linie w pliku
    def mapper(self, _, line):
        # liczba linii
        yield 'lines', 1
        # liczba wszystkich slow
        yield 'words', len(line.split())
        # liczba znakow
        yield 'chars', len(line)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRSimpleJob.run()