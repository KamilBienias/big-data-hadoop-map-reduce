# Etap13. Odcinek: Uruchomienie MapReduce lokalnie

import pandas as pd

df = pd.read_csv('prep_reviews.tsv', sep='\t', header=None)
df = df[:1000]

df.to_csv('prep_1000.tsv', sep='\t', header=False, index=False)