# Etap9. Odcinek: JupyterNotebook: Eksploracja wynikow (Plotly)

# oryginalnie bylo w jupyter notebook

import pandas as pd

# seperatorem jest tabulator
# to_split jest kolumna, ktora trzeba podzielic na dwie kolumny.
# Bez names= to wtedy pierwszy wiersz odczytuje jako naglowek tabeli
df = pd.read_csv('/home/dell/PycharmProjects/big-data-hadoop-map-reduce/04_airports/average_delay.csv', sep='\t', names=['month', 'to_split'])
# wyciecie [1:-1] pozbywa sie nawiasow kwadratowych.
# Robi z jednej kolumny dwie Series
df[['dep_delay', 'arr_delay']] = df.to_split.str[1:-1].str.split(', ').apply(pd.Series)
# usuwa stara kolumne
df = df.drop(columns='to_split')
print(df)

import plotly.express as px

# czemu zle slupki pokazuje ?
fig = px.bar(df, x='month', y='dep_delay', orientation='v', color='month')
fig.show()

fig = px.bar(df, x='month', y='arr_delay', orientation='v', color='month')
fig.show()

import plotly.graph_objects as go

fig = go.Figure(data=[
                     # pierwszy slad
                     go.Bar(x=df.month, y=df.dep_delay, name='departure delay', marker_color='#517CAA'),
                     # drugi slad
                     go.Bar(x=df.month, y=df.arr_delay, name='arrival delay', marker_color='#E09F1F')
                     ],
               layout=go.Layout(title='Average Departure and Arrival Delay by Airline USA 2015'))
fig.show()