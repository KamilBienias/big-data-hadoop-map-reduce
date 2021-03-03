# Etap12. Odcinek: JupyterNotebook: Eksploracja wynikow (Plotly)

import pandas as pd
df = pd.read_csv('total_pickups_by_hour.csv', sep='\t', names=['hour', 'total'])
print(df)

import plotly.express as px

fig = px.bar(df, x='hour', y='total')
fig.show()

fig = px.area(df, x='hour', y='total')
fig.show()

import plotly.graph_objects as go

fig = go.Figure(data=[go.Pie(labels=df.hour, values=df.total)])
fig.show()