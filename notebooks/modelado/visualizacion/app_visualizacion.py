import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Se realiza la lectura de los datos
df = pd.read_csv("../../../data/final/datos_finales.csv", sep=";")

# Título del dashboard
st.write("# 13MBID - Visualización de datos")
st.write("## Panel de visualización generado sobre los datos de créditos y tarjetas emitidas a clientes de la entidad")
st.write("#### Autores: ")
st.write("##### Calampa Tantachuco, Colbert Moises Bryan")
st.write("##### Miranda Villalón, Elena")
st.write("----")

# Histograma de los créditos otorgados por objetivo
st.write("### Caracterización de los créditos otorgados")

# Se tienen que agregar las definiciones de gráficos desde la libreta
creditos_x_objetivo = px.histogram(df, x='objetivo_credito', 
                                   title='Conteo de créditos por objetivo')
creditos_x_objetivo.update_layout(xaxis_title='Objetivo del crédito', yaxis_title='Cantidad')

# Se realiza la "impresión" del gráfico en el dashboard
st.plotly_chart(creditos_x_objetivo)


# Histograma de los importes de créditos otorgados
histograma_importes = px.histogram(df, x='importe_solicitado', nbins=10, title='Importes solicitados en créditos')
histograma_importes.update_layout(xaxis_title='Importe solicitado', yaxis_title='Cantidad')

st.plotly_chart(histograma_importes)

# Cantidad de créditos por situacion vivienda del cliente
creditos_x_situacion_vivienda = px.histogram(df, x='situacion_vivienda', 
                                   title='Conteo de créditos por situacion vivienda de cliente')
creditos_x_situacion_vivienda.update_layout(xaxis_title='Situacion vivienda', yaxis_title='Cantidad')

st.plotly_chart(creditos_x_situacion_vivienda)

# Gráfico distribución de créditos por edad 
edad_counts = df['edad'].value_counts()

# Gráfico de torta de estos valores
fig_creditos_x_edad = go.Figure(data=[go.Pie(labels=edad_counts.index, values=edad_counts)])
fig_creditos_x_edad.update_layout(title_text='Distribución de créditos por edad')
st.plotly_chart(fig_creditos_x_edad)


# Filtros objetivo_credito y estado_credito_N
objetivo_credito_uniq = df['objetivo_credito'].unique()
option = st.selectbox(
    'Qué objetivo de crédito desea filtrar?',
     objetivo_credito_uniq )

df_filtrado = df[df['objetivo_credito'] == option]

st.write(f"Tipo de crédito seleccionado: {option}")

credito_finalizado_checkbox = st.checkbox('Mostrar créditos finalizados?', value=True)

if credito_finalizado_checkbox:

    # Conteo de ocurrencias por estado
    estado_credito_counts = df_filtrado['estado_credito_N'].value_counts()

    # Gráfico de torta de estos valores
    fig = go.Figure(data=[go.Pie(labels=estado_credito_counts.index, values=estado_credito_counts)])
    fig.update_layout(title_text='Distribución de créditos por estado registrado')
else:
    df_filtrado = df_filtrado[df_filtrado['estado_credito_N'] == 'P']
    # Conteo de ocurrencias por caso
    falta_pago_counts = df_filtrado['falta_pago'].value_counts()

    # Create a Pie chart
    fig = go.Figure(data=[go.Pie(labels=falta_pago_counts.index, values=falta_pago_counts)])
    fig.update_layout(title_text='Distribución de créditos en función de registro de mora')

st.write(f"Cantidad de créditos con estas condiciones: {df_filtrado.shape[0]}")
st.plotly_chart(fig)
