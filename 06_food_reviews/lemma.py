# Etap12. Odcinek: Glowne zadanie: Top 20 przymiotnikow dla skrajnych recenzji cz. 2

from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk import pos_tag

# nltk to natural language toolkit

lemmatizer = WordNetLemmatizer()
# szuka zaimkow osobowych np we, are, was, what, which, ...
stop_words = stopwords.words('english')

print(stop_words)

# zwroci liczbe pojedyncza computer
# print(lemmatizer.lemmatize('computers'))
# zwroci liczbe pojedyncza mouse
# print(lemmatizer.lemmatize('mice'))
# zwroci liczbe pojedyncza book
# print(lemmatizer.lemmatize('books'))

# part of speech tag
# zwraca slowo zmapowane na kod
# tu zwroci JJ czyli przymiotnik
print(pos_tag(['big']))
# tu zwroci NN czyli rzeczownik
print(pos_tag(['book']))