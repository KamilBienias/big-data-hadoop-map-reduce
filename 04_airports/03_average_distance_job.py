# Etap9. Odcinek: MapReduce: Srednia odleglosc lotu

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRFlights(MRJob):

    def steps(self):
        return [
            # MRStep(mapper=self.mapper,
            #        reducer=self.reducer),
            # moje stepy
            MRStep(mapper=self.moje_mapper_month,
                   reducer=self.moje_reducer_month),

        ]

    # najpierw z pliku test.txt bo o wiele mniejszy niz preprocessed_data.csv
    def mapper(self, _, line):
        # klucz jest oddzielony od wartosci tabulatorem
        year, items = line.split('\t')
        # nie chcemy zeby cudzyslowy byly brane jako dane czyli
        # nie chcemy "\"2015\""     "[7, 3, \"WN\", 507]"
        year = year[1:-1]
        # usuwa nawiasy kwadratowe z listy items
        items = items[1:-1]
        month, day, airline, distance = items.split(', ')
        # zeby distance nie bylo w cudzyslowie
        distance = int(distance)
        # zamiast year kluczem jest None, bo wszedzie rok 2015
        yield None, distance

    # sam licze srednia dla kazdego miesiaca (wrzesnia i lipca)
    def moje_mapper_month(self, _, line):
        # klucz jest oddzielony od wartosci tabulatorem
        year, items = line.split('\t')
        # nie chcemy zeby cudzyslowy byly brane jako dane czyli
        # nie chcemy "\"2015\""     "[7, 3, \"WN\", 507]"
        year = year[1:-1]
        # usuwa nawiasy kwadratowe z listy items
        items = items[1:-1]
        month, day, airline, distance = items.split(', ')
        # zeby distance nie bylo w cudzyslowie
        distance = int(distance)
        # sam zamieniam month na liczbe
        month = int(month)
        # kluczem jest month
        yield month, distance

    def reducer(self, key, values):
        total = 0
        num_elements = 0
        for value in values:
            total += value
            num_elements += 1
        # wyjdzie null    845.578947368421
        yield None, total / num_elements

    # sam robie reducer zeby pokazywal po kluczu ktorym jest miesiac
    def moje_reducer_month(self, month, values):
        total = 0
        num_elements = 0
        for value in values:
            total += value
            num_elements += 1
        # klucz zmienilem z null na month, bo chce grupowac po miesiacu
        yield month, total / num_elements


if __name__ == '__main__':
    MRFlights.run()