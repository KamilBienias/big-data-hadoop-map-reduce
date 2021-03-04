# Etap13. Odcinek: Wstepne przetworzenie danych

import pandas as pd

df = pd.read_csv('Reviews.csv')
print(df.info())

# tu mozna debugg, czyli czerwona kropka obon print() i potem debugg.
# Potem w Variables prawym na df i View as DataFrame
print()
# Z powrotem z df do pliku
# zamiast przecinka seperatorem bedzie tabulator, bo kolumnie
# Text jest duzo przecinkow. Rozszerzenie tsv to plik rozdzielany tabulatorem,
# czyli tab seperated values. Nie zapisuje naglowka header i index do pliku tsv
df.to_csv('prep_reviews.tsv', sep='\t', header=False, index=False)