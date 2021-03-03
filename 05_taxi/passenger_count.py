import pandas as pd

df = pd.read_csv('passenger_count.csv', sep='\t', names=['passengers', 'value'])
print(df)

import plotly.express as px

fig = px.bar(df, x='passengers', y='value')
fig.show()

import plotly.graph_objects as go

fig = go.Figure(data=[go.Pie(labels=df.passengers, values=df.value)])
fig.show()