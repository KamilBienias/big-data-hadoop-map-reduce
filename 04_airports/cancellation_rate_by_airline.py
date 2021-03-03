# Etap9. Odcinek: Rozwiazanie: JupyterNotebook: Eksploracja wynikow (Protly) trzeci

import pandas as pd

# cr to cancellation rate
df = pd.read_csv('cancellation_rate.csv', sep='\t', names=['airline', 'cr'])
print(df)

import plotly.express as px

fig = px.bar(df, x='airline', y='cr', color='airline')
fig.show()

import plotly.graph_objects as go
# wykres kolowy, hole to dziura w srodku
fig = go.Figure(data=go.Pie(labels=df.airline, values=df.cr, hole=0.5))
fig.show()