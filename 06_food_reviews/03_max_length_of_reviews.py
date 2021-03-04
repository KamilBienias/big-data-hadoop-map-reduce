# Etap13. Odcinek: Glowne zadanie: Top 20 przymiotnikow dla skrajnych recenzji cz. 1

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# wybiera same slowa bez znakow interpunkcyjnych
WORD_RE = re.compile(r"[\w]+")

# uruchomienie na testowym:
# python3 03_max_length_of_reviews.py test.txt
class MRFood(MRJob):

    def mapper(self, _, line):
        (Id, ProductId, UserId, ProfileName, HelpfulnessNumerator, HelpfulnessDenominator,
         Score, Time, Summary, Text) = line.split('\t')
        words = WORD_RE.findall(Text)
        # klucz None czyli dzialamy na wszystkich recenzjach (bez grupowania)
        yield None, len(words)

    # maksymalna miala 3529 slow
    def reducer(self, key, values):
        yield key, max(values)


if __name__ == '__main__':
    MRFood.run()