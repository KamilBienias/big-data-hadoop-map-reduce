# Etap7. Odcinek: MapRedude: Iliada - word count + regex cz.2

import re

# takiego typu wyrazenie szukamy
# W nawiasie jest wzor czyli pattern.
# Z przodu r oznacza ze jest to wyrazenie regularne
# \w to word character, czyli male i duze litery, cyfry oraz podkreslnik
# Plus na koncu oznacza co najmniej 1 element.
# Chcemy przynajmniej jednoliterowe wyrazy
WORD_RE = re.compile(r'[\w]+')

# uzywanie patternu
words = WORD_RE.findall('Big data, hadoop and map reduce. (hello world!)')
print(words)
# zwroci liste slow:
# ['Big', 'data', 'hadoop', 'and', 'map', 'reduce', 'hello', 'world']