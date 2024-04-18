# %% [markdown]
# ## Máster en Big Data y Data Science
#
# ### Metodologías de gestión y diseño de proyectos de big data
#
# #### AP1 - Verificación de la calidad de los datos (versión completa)
#
# ---
#
# En esta libreta se realiza una verificación de calidad de datos con base en los planteado en el Anexo SP4.
#
# ---

# %%
# Se importan las librerias a utilizar

import pandas as pd

# %% [markdown]
# ----
#
# ##### Lectura de los datasets

# %%
df_creditos = pd.read_csv("../../data/processed/datos_creditos_mc.csv", sep=";")
display(df_creditos.head(1))

df_tarjetas = pd.read_csv("../../data/processed/datos_tarjetas_mc.csv", sep=";")
display(df_tarjetas.head(1))

# %% [markdown]
# ---
# #### Verificación de calidad de datos
#
# **Análisis a realizar**
#
# 1. Evaluación de valores nulos (filas y columnas)
# 2. Evaluación de formato válido
# 3. Valores ajustados en rangos (ver anexos)
# 4. Claves únicas
# 5. Integridad referencial
# 6. Cumplimiento de reglas en valores

# %%
# Establecimiento de los umbrales de aceptación

FORMATEO_VALORES = 0.1
RANGOS_VALORES = 0.0
INTEGRIDAD_REF = 0.1
REGLAS_VALORES = 0.1

# Valores globales

cantidad_filas_creditos = df_creditos.shape[0]
cantidad_filas_tarjetas = df_tarjetas.shape[0]

# %% [markdown]
# ## Dimensión: completitud
#
# ### (1a) Filas

# %%
# Se obtienen las cantidades de valores nulos por columna

nulos_x_columna_c = df_creditos.isna().sum()
nulos_x_columna_t = df_tarjetas.isna().sum()

print(f"Cantidad de filas que tienen valores nulos por atributo:\n{nulos_x_columna_c}\n")
print(f"Cantidad de filas que tienen valores nulos por atributo:\n{nulos_x_columna_t}")

# %%
# De cualquier manera se establece el cálculo a realizar

cantidad_columnas = len(df_creditos.axes[1])

df_creditos['completitud_fila'] = (df_creditos.isnull().sum(axis=1) / cantidad_columnas)

problemas = df_creditos[df_creditos['completitud_fila'] >= 0.2]

completitud_f = problemas.shape[0]

print(f"Filas que incumplen el umbral de nulos en columnas [completitud_f] - créditos - :")
print(f"{completitud_f} ({round((completitud_f / cantidad_filas_creditos) * 100, 2)})%")

# %%
# De cualquier manera se establece el cálculo a realizar

cantidad_columnas = len(df_tarjetas.axes[1])

df_tarjetas['completitud_fila'] = (df_tarjetas.isnull().sum(axis=1) / cantidad_columnas)

problemas = df_tarjetas[df_tarjetas['completitud_fila'] >= 0.2]

completitud_f = problemas.shape[0]

print(f"Filas que incumplen el umbral de nulos en columnas [completitud_f] - tarjetas - :")
print(f"{completitud_f} ({round((completitud_f / cantidad_filas_creditos) * 100, 2)})%")

# %% [markdown]
# ### (1b) Dataset

# %%
completitud_dc = df_creditos.isnull().any(axis=1).sum()

print(f"Filas que presentan nulos en el dataset [completitud_d] - creditos - :")
print(f"{completitud_dc} ({round((completitud_dc / cantidad_filas_creditos) * 100, 2)})%\n")

completitud_dt = df_tarjetas.isnull().any(axis=1).sum()

print(f"Filas que presentan nulos en el dataset [completitud_d] - tarjetas - :")
print(f"{completitud_dt} ({round((completitud_dt / cantidad_filas_tarjetas) * 100, 2)})%")

# %% [markdown]
# ----
#
# ## Dimensión: exactitud
#
# ### (2) Formato válido

# %%
# No se encuentran atributos con formato específico

# %% [markdown]
# ----
#
# ## Dimensión: exactitud
#
# ### (3) Valores ajustados

# %% [markdown]
# Atributo: **edad**

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: edad

valores = df_creditos['edad'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_creditos['edad']) - df_creditos['edad'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_creditos[df_creditos['edad'] > 90]

print("Se visualizan las filas con errores de rango:")
display(resultado)  # Para visualizar las tuplas con valores nulos o erróneos

print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_edad():
    edad_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(f"Cantidad de filas con valores fuera de rango en atributo edad: {edad_valores_fuera_rango}")

    indicador = (edad_valores_fuera_rango / cantidad_filas_creditos)
    print(f"Porcentaje de filas con errores de rango de valores (atributo edad): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_edad()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: importe_solicitado

valores = df_creditos['importe_solicitado'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_creditos['importe_solicitado']) - df_creditos['importe_solicitado'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_creditos[df_creditos['importe_solicitado'] < 0]

print("Se visualizan las filas con errores de rango:")
display(resultado)  # Para visualizar las tuplas con valores nulos o erróneos

print(f"Cantidad detectada: {resultado.shape[0]}")

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: duracion_credito

valores = df_creditos['duracion_credito'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_creditos['duracion_credito']) - df_creditos['duracion_credito'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_creditos[df_creditos['duracion_credito'] < 0]

print("Se visualizan las filas con errores de rango:")
display(resultado)  # Para visualizar las tuplas con valores nulos o erróneos

print(f"Cantidad detectada: {resultado.shape[0]}")

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: antiguedad_empleado

valores = df_creditos['antiguedad_empleado'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_creditos['antiguedad_empleado']) - df_creditos['antiguedad_empleado'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_creditos[df_creditos['antiguedad_empleado'] > 50]

print("Se visualizan las filas con errores de rango:")
display(resultado)  # Para visualizar las tuplas con valores nulos o erróneos

print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_antiguedad_empleado():
    antiguedad_empleado_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(f"Cantidad de filas con valores fuera de rango en atributo edad: {antiguedad_empleado_valores_fuera_rango}")

    indicador = (antiguedad_empleado_valores_fuera_rango / cantidad_filas_creditos)
    print(
        f"Porcentaje de filas con errores de rango de valores (atributo antiguedad_empleado): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_antiguedad_empleado()

# %% [markdown]
# Se procesa el atributo: **situacion_vivienda**
#

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: situacion_vivienda

valores = df_creditos['situacion_vivienda'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_creditos['situacion_vivienda']) - df_creditos['situacion_vivienda'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

valores_validos = 'ALQUILER|PROPIA|HIPOTECA|OTROS'  # Se define una re de los valores validos según el anexo

df_creditos['situacion_vivienda_ok'] = df_creditos['situacion_vivienda'].astype(str).str.match(valores_validos)

print("Se visualizan las filas con errores de rango:")
display(df_creditos[
            df_creditos['situacion_vivienda_ok'] == False])  # Para visualizar las tuplas con valores nulos o erróneos

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_creditos[df_creditos['situacion_vivienda_ok'] == False]
print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_situacion_vivienda():
    situacion_vivienda_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(
        f"Cantidad de filas con valores fuera de rango en atributo situacion_vivienda: {situacion_vivienda_valores_fuera_rango}")

    indicador = (situacion_vivienda_valores_fuera_rango / cantidad_filas_creditos)
    print(
        f"Porcentaje de filas con errores de rango de valores (atributo situacion_vivienda): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_situacion_vivienda()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: objetivo_credito

valores = df_creditos['objetivo_credito'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_creditos['objetivo_credito']) - df_creditos['objetivo_credito'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

valores_validos = 'EDUCACIÓN|SALUD|INVERSIONES|PAGO_DEUDAS|PERSONAL|MEJORAS_HOGAR'

# Se define una re de los valores validos según el anexo

df_creditos['objetivo_credito_ok'] = df_creditos['objetivo_credito'].astype(str).str.match(valores_validos)

print("Se visualizan las filas con errores de rango:")
display(
    df_creditos[df_creditos['objetivo_credito_ok'] == False])  # Para visualizar las tuplas con valores nulos o erróneos

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_creditos[df_creditos['objetivo_credito_ok'] == False]
print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_objetivo_credito():
    objetivo_credito_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(
        f"Cantidad de filas con valores fuera de rango en atributo objetivo_credito: {objetivo_credito_valores_fuera_rango}")

    indicador = (objetivo_credito_valores_fuera_rango / cantidad_filas_creditos)
    print(
        f"Porcentaje de filas con errores de rango de valores (atributo objetivo_credito): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_objetivo_credito()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: ingresos

valores = df_creditos['ingresos'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_creditos['ingresos']) - df_creditos['ingresos'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_creditos[df_creditos['ingresos'] < 0]

print("Se visualizan las filas con errores de rango:")
display(resultado)  # Para visualizar las tuplas con valores nulos o erróneos

print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_ingresos():
    ingresos_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(f"Cantidad de filas con valores fuera de rango en atributo ingresos: {ingresos_valores_fuera_rango}")

    indicador = (ingresos_valores_fuera_rango / cantidad_filas_creditos)
    print(f"Porcentaje de filas con errores de rango de valores (atributo ingresos): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_ingresos()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: pct_ingreso

valores = df_creditos['pct_ingreso'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_creditos['pct_ingreso']) - df_creditos['pct_ingreso'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_creditos[(df_creditos['pct_ingreso'] > 1) | (df_creditos['pct_ingreso'] < 0)]

print("Se visualizan las filas con errores de rango:")
display(resultado)  # Para visualizar las tuplas con valores nulos o erróneos

print(f"Cantidad detectada: {resultado.shape[0]}")

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: tasa_interes

valores = df_creditos['tasa_interes'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_creditos['tasa_interes']) - df_creditos['tasa_interes'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_creditos[(df_creditos['tasa_interes'] > 20)]

print("Se visualizan las filas con errores de rango:")
display(resultado)  # Para visualizar las tuplas con valores nulos o erróneos

print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_tasa_interes():
    ingresos_valores_fuera_tasa_interes = resultado.shape[0] + cantidad_nulos
    print(f"Cantidad de filas con valores fuera de rango en atributo ingresos: {ingresos_valores_fuera_tasa_interes}")

    indicador = (ingresos_valores_fuera_tasa_interes / cantidad_filas_creditos)
    print(f"Porcentaje de filas con errores de rango de valores (atributo tasa_interes): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_tasa_interes()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: estado_credito

valores = df_creditos['estado_credito'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_creditos['estado_credito']) - df_creditos['estado_credito'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_creditos[(df_creditos['estado_credito'] != 0) & (df_creditos['estado_credito'] != 1)]

print("Se visualizan las filas con errores de rango:")
display(resultado)  # Para visualizar las tuplas con valores nulos o erróneos

print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_estado_credito():
    ingresos_valores_fuera_estado_credito = resultado.shape[0] + cantidad_nulos
    print(f"Cantidad de filas con valores fuera de rango en atributo ingresos: {ingresos_valores_fuera_estado_credito}")

    indicador = (ingresos_valores_fuera_estado_credito / cantidad_filas_creditos)
    print(f"Porcentaje de filas con errores de rango de valores (atributo tasa_interes): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_estado_credito()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: falta_pago

valores = df_creditos['falta_pago'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_creditos['falta_pago']) - df_creditos['falta_pago'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

valores_validos = 'Y|N'  # Se define una re de los valores validos según el anexo

df_creditos['falta_pago_ok'] = df_creditos['falta_pago'].astype(str).str.match(valores_validos)

print("Se visualizan las filas con errores de rango:")
display(df_creditos[df_creditos['falta_pago_ok'] == False])  # Para visualizar las tuplas con valores nulos o erróneos

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_creditos[df_creditos['falta_pago_ok'] == False]
print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_falta_pago():
    falta_pago_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(f"Cantidad de filas con valores fuera de rango en atributo falta_pago: {falta_pago_valores_fuera_rango}")

    indicador = (falta_pago_valores_fuera_rango / cantidad_filas_creditos)
    print(f"Porcentaje de filas con errores de rango de valores (atributo efalta_pago): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_falta_pago()

# %% [markdown]
# ----
#
# Tarjetas

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: antiguedad_cliente

valores = df_tarjetas['antiguedad_cliente'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_tarjetas['antiguedad_cliente']) - df_tarjetas['antiguedad_cliente'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_tarjetas[df_tarjetas['antiguedad_cliente'] < 0]

print("Se visualizan las filas con errores de rango:")
display(resultado)  # Para visualizar las tuplas con valores nulos o erróneos

print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_antiguedad_cliente():
    antiguedad_cliente_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(
        f"Cantidad de filas con valores fuera de rango en atributo antiguedad_cliente: {antiguedad_cliente_valores_fuera_rango}")

    indicador = (antiguedad_cliente_valores_fuera_rango / cantidad_filas_creditos)
    print(
        f"Porcentaje de filas con errores de rango de valores (atributo antiguedad_cliente): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_antiguedad_cliente()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: personas_a_cargo

valores = df_tarjetas['personas_a_cargo'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_tarjetas['personas_a_cargo']) - df_tarjetas['personas_a_cargo'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_tarjetas[df_tarjetas['personas_a_cargo'] < 0]

print("Se visualizan las filas con errores de rango:")
display(resultado)  # Para visualizar las tuplas con valores nulos o erróneos

print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_personas_a_cargo():
    personas_a_cargo_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(
        f"Cantidad de filas con valores fuera de rango en atributo personas_a_cargo: {personas_a_cargo_valores_fuera_rango}")

    indicador = (personas_a_cargo_valores_fuera_rango / cantidad_filas_creditos)
    print(
        f"Porcentaje de filas con errores de rango de valores (atributo personas_a_cargo): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_personas_a_cargo()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: gastos_ult_12m

valores = df_tarjetas['gastos_ult_12m'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_tarjetas['gastos_ult_12m']) - df_tarjetas['gastos_ult_12m'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_tarjetas[df_tarjetas['gastos_ult_12m'] < 0]

print("Se visualizan las filas con errores de rango:")
display(resultado)  # Para visualizar las tuplas con valores nulos o erróneos

print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_gastos_ult_12m():
    gastos_ult_12m_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(
        f"Cantidad de filas con valores fuera de rango en atributo gastos_ult_12m: {gastos_ult_12m_valores_fuera_rango}")

    indicador = (gastos_ult_12m_valores_fuera_rango / cantidad_filas_creditos)
    print(
        f"Porcentaje de filas con errores de rango de valores (atributo gastos_ult_12m): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_gastos_ult_12m()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: limite_credito_tc

valores = df_tarjetas['limite_credito_tc'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_tarjetas['limite_credito_tc']) - df_tarjetas['limite_credito_tc'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_tarjetas[df_tarjetas['limite_credito_tc'] < 0]

print("Se visualizan las filas con errores de rango:")
display(resultado)  # Para visualizar las tuplas con valores nulos o erróneos

print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_limite_credito_tc():
    limite_credito_tc_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(
        f"Cantidad de filas con valores fuera de rango en atributo gastos_ult_12m: {limite_credito_tc_valores_fuera_rango}")

    indicador = (limite_credito_tc_valores_fuera_rango / cantidad_filas_creditos)
    print(
        f"Porcentaje de filas con errores de rango de valores (atributo limite_credito_tc): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_limite_credito_tc()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: operaciones_ult_12m

valores = df_tarjetas['operaciones_ult_12m'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_tarjetas['operaciones_ult_12m']) - df_tarjetas['operaciones_ult_12m'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_tarjetas[df_tarjetas['operaciones_ult_12m'] < 0]

print("Se visualizan las filas con errores de rango:")
display(resultado)  # Para visualizar las tuplas con valores nulos o erróneos

print(f"Cantidad detectada: {resultado.shape[0]}")

# %% [markdown]
# - nominales

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: estado_civil

valores = df_tarjetas['estado_civil'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_tarjetas['estado_civil']) - df_tarjetas['estado_civil'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

valores_validos = 'CASADO|SOLTERO|DESCONOCIDO|DIVORCIADO'  # Se define una re de los valores validos según el anexo

df_tarjetas['estado_civil_ok'] = df_tarjetas['estado_civil'].astype(str).str.match(valores_validos)

print("Se visualizan las filas con errores de rango:")
display(df_tarjetas[df_tarjetas['estado_civil_ok'] == False])  # Para visualizar las tuplas con valores nulos o erróneos

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_tarjetas[df_tarjetas['estado_civil_ok'] == False]
print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_estado_civil():
    estado_civil_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(f"Cantidad de filas con valores fuera de rango en atributo estado_civil: {estado_civil_valores_fuera_rango}")

    indicador = (estado_civil_valores_fuera_rango / cantidad_filas_creditos)
    print(f"Porcentaje de filas con errores de rango de valores (atributo estado_civil): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_estado_civil()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: estado_cliente

valores = df_tarjetas['estado_cliente'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_tarjetas['estado_cliente']) - df_tarjetas['estado_cliente'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

valores_validos = 'ACTIVO|PASIVO'  # Se define una re de los valores validos según el anexo

df_tarjetas['estado_cliente_ok'] = df_tarjetas['estado_cliente'].astype(str).str.match(valores_validos)

print("Se visualizan las filas con errores de rango:")
display(
    df_tarjetas[df_tarjetas['estado_cliente_ok'] == False])  # Para visualizar las tuplas con valores nulos o erróneos

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_tarjetas[df_tarjetas['estado_cliente_ok'] == False]
print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_estado_cliente():
    estado_cliente_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(
        f"Cantidad de filas con valores fuera de rango en atributo estado_cliente: {estado_cliente_valores_fuera_rango}")

    indicador = (estado_cliente_valores_fuera_rango / cantidad_filas_creditos)
    print(
        f"Porcentaje de filas con errores de rango de valores (atributo estado_cliente): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_estado_cliente()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: genero

valores = df_tarjetas['genero'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_tarjetas['genero']) - df_tarjetas['genero'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

valores_validos = 'F|M'  # Se define una re de los valores validos según el anexo

df_tarjetas['genero_ok'] = df_tarjetas['genero'].astype(str).str.match(valores_validos)

print("Se visualizan las filas con errores de rango:")
display(df_tarjetas[df_tarjetas['genero_ok'] == False])  # Para visualizar las tuplas con valores nulos o erróneos

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_tarjetas[df_tarjetas['genero_ok'] == False]
print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_genero():
    genero_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(f"Cantidad de filas con valores fuera de rango en atributo genero: {genero_valores_fuera_rango}")

    indicador = (genero_valores_fuera_rango / cantidad_filas_creditos)
    print(f"Porcentaje de filas con errores de rango de valores (atributo genero): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_genero()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: nivel_educativo

valores = df_tarjetas['nivel_educativo'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_tarjetas['nivel_educativo']) - df_tarjetas['nivel_educativo'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

valores_validos = 'SECUNDARIO_COMPLETO|UNIVERSITARIO_INCOMPLETO|UNIVERSITARIO_COMPLETO|POSGRADO_INCOMPLETO|POSGRADO_COMPLETO|DESCONOCIDO'  # Se define una re de los valores validos según el anexo

df_tarjetas['nivel_educativo_ok'] = df_tarjetas['nivel_educativo'].astype(str).str.match(valores_validos)

print("Se visualizan las filas con errores de rango:")
display(
    df_tarjetas[df_tarjetas['nivel_educativo_ok'] == False])  # Para visualizar las tuplas con valores nulos o erróneos

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_tarjetas[df_tarjetas['nivel_educativo_ok'] == False]
print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_nivel_educativo():
    nivel_educativo_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(
        f"Cantidad de filas con valores fuera de rango en atributo nivel_educativo: {nivel_educativo_valores_fuera_rango}")

    indicador = (nivel_educativo_valores_fuera_rango / cantidad_filas_creditos)
    print(
        f"Porcentaje de filas con errores de rango de valores (atributo nivel_educativo): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_nivel_educativo()

# %%
# Verificar que los valores de cada atributo se encuentren dentro de los listados anexos

# Atributo: nivel_tarjeta

valores = df_tarjetas['nivel_tarjeta'].value_counts()  # Conteo de ocurrencias por valor (not-null)
print(f"Distribución inicial del atributo: \n{valores}\n")

cantidad_nulos = len(df_tarjetas['nivel_tarjeta']) - df_tarjetas['nivel_tarjeta'].count()  # Conteo de nulos

if cantidad_nulos > 0:
    print(f"Cantidad de nulos en el atributo: {cantidad_nulos}\n")  # Impresión de la cantidad de nulos
else:
    print("No existen filas con valores nulos para este atributo.\n")

# Se identifica y cuenta a los valores que no cumplen la condición definida

valores_validos = 'Blue|Gold|Silver|Platinum'  # Se define una re de los valores validos según el anexo

df_tarjetas['nivel_tarjeta_ok'] = df_tarjetas['nivel_tarjeta'].astype(str).str.match(valores_validos)

print("Se visualizan las filas con errores de rango:")
display(
    df_tarjetas[df_tarjetas['nivel_tarjeta_ok'] == False])  # Para visualizar las tuplas con valores nulos o erróneos

# Se identifica y cuenta a los valores que no cumplen la condición definida

resultado = df_tarjetas[df_tarjetas['nivel_tarjeta_ok'] == False]
print(f"Cantidad detectada: {resultado.shape[0]}")


# %%
def calcular_rangos_valores_nivel_tarjeta():
    nivel_tarjeta_valores_fuera_rango = resultado.shape[0] + cantidad_nulos
    print(
        f"Cantidad de filas con valores fuera de rango en atributo nnivel_tarjeta: {nivel_tarjeta_valores_fuera_rango}")

    indicador = (nivel_tarjeta_valores_fuera_rango / cantidad_filas_creditos)
    print(
        f"Porcentaje de filas con errores de rango de valores (atributo nivel_tarjeta): {round(indicador * 100, 2)} %")

    if (indicador > RANGOS_VALORES):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_rangos_valores_nivel_tarjeta()

# %% [markdown]
# ---
#
# ## Dimensión: consistencia
#
# ### (4) Claves únicas
#
# Dataset: **datos_creditos**

# %%
# Se obtiene el valor de la cantidad de filas actual
cant_antes = df_creditos.shape[0]

# Se ordena el dataset según el atributo que se desee evaluar (requerido para el paso siguiente)
df_creditos.sort_values("id_cliente", inplace=True)

# Se detectan y eliminan los duplicados en un atributo dejando la última ocurrencia
df_creditos.drop_duplicates(subset="id_cliente", keep='last', inplace=True)

# Se obtiene el valor posterior a la operación
cant_despues = df_creditos.shape[0]

# Se imprimen ambos valores
print('Dataset: creditos')
print(f"Antes del análisis de duplicados: {cant_antes} - Despues del filtrado de duplicados: {cant_despues}")
if cant_antes > cant_despues:
    diferencia = cant_antes - cant_despues
    pct_diferencia = ((cant_antes - cant_despues) / cant_antes) * 100
    print(f"Se detectaron claves duplicadas en {diferencia} fila(s) un {round(pct_diferencia, 2)}%.")
else:
    print("No se detectaron claves duplicadas")

# %% [markdown]
# Dataset: **datos_tarjetas**

# %%
# Se obtiene el valor de la cantidad de filas actual
cant_antes = df_tarjetas.shape[0]

# Se ordena el dataset según el atributo que se desee evaluar (requerido para el paso siguiente)
df_tarjetas.sort_values("id_cliente", inplace=True)

# Se detectan y eliminan los duplicados en un atributo dejando la última ocurrencia
df_tarjetas.drop_duplicates(subset="id_cliente", keep='last', inplace=True)

# Se obtiene el valor posterior a la operación
cant_despues = df_tarjetas.shape[0]

# Se imprimen ambos valores
print('Dataset: tarjetas')
print(f"Antes del análisis de duplicados: {cant_antes} - Despues del filtrado de duplicados: {cant_despues}")
if cant_antes > cant_despues:
    diferencia = cant_antes - cant_despues
    pct_diferencia = ((cant_antes - cant_despues) / cant_antes) * 100
    print(f"Se detectaron claves duplicadas en {diferencia} fila(s) un {round(pct_diferencia, 2)}%.")
else:
    print("No se detectaron claves duplicadas")

# %% [markdown]
# ### (5) Integridad referencial

# %%
# Las uniones se hacen de a pares - revisar nombres de atributos

df_integrado = pd.merge(df_creditos, df_tarjetas, on='id_cliente', how='inner')
coincidencias = df_integrado.shape[0]

print(f"Datos de créditos: {cantidad_filas_creditos} - Coincidencias con datos de tarjetas: {coincidencias}")

print("\nSe visualiza el dataset resultante:")
display(df_integrado.head(5))

print(f"Reporte general:\n \
- Filas del dataset creditos (inicial): {cantidad_filas_creditos}\n \
- Filas del dataset tarjetas (inicial): {cantidad_filas_tarjetas}\n \
- Errores detectados en la operación de unión: {abs(coincidencias - cantidad_filas_creditos)} \n \
- Filas del dataset unificado: {df_integrado.shape[0]}")


# %%
def calcular_integridad_referencial():
    cant_problemas = cantidad_filas_creditos - df_integrado.shape[0]  # Se calcula sobre el inicio (foco)
    print(f"Casos de problemas de integridad referencial: {cant_problemas}")

    indicador = (cant_problemas / cantidad_filas_creditos)
    print(f"Porcentaje de filas con problemas de integridad referencial: {round(indicador * 100, 2)} %")

    if (indicador > INTEGRIDAD_REF):
        print('Evaluación: no cumplimiento')
    else:
        print('Evaluación: ok')


calcular_integridad_referencial()


# %% [markdown]
# ---
#
# ## Dimensión exactitud (bis)
# ### (6) Reglas en valores
#
# Regla 1: Para aquellos casos en que los créditos constituyan un porcentaje de los ingresos del cliente mayor al 50% sus ingresos deberán ser mayores a 20.000.
#
# Regla 2: Para aquellos créditos cuya duración sea la mínima permitida el porcentaje de los ingresos del cliente (con respecto al importe solicitado) no podrá exceder el 60% salvo en los casos en los que sea propietario de su vivienda.

# %%
# Se puede definir una función para aplicar los cálculos
def regla_pct_ingresos_credito(row):
    pct_ingreso = row.pct_ingreso
    ingresos = row.ingresos

    if pct_ingreso > 0.5 and ingresos <= 20000:
        # Es un error, no cumple la regla definida
        return 'err'
    else:
        return 'ok'


# Se aplica la función para todos los elementos del dataset
regla_pct_ingresos = df_integrado.apply(lambda row: regla_pct_ingresos_credito(row), axis=1).rename(
    "regla_pct_ingresos")

# Se unen los resultados al dataset inicial
df_resultado = pd.concat([df_integrado, regla_pct_ingresos], axis=1)

# Se visualizan los datos
print("Se visualizan las tuplas que no cumplen con la regla:\n")
display(df_resultado[df_resultado.regla_pct_ingresos == 'err'].head())

# Se verifica la cantidad de elementos
aux = df_resultado[df_resultado.regla_pct_ingresos == 'err']
print(f"Cantidad de filas que no cumplen la regla: {aux.shape[0]}")


# %%
# Se puede definir una función para aplicar los cálculos
def regla_pct_ingresos_credito(row):
    pct_ingreso = row.pct_ingreso
    ingresos = row.ingresos

    if pct_ingreso > 0.5 and ingresos <= 20000:
        # Es un error, no cumple la regla definida
        return 'err'
    else:
        return 'ok'


# Se aplica la función para todos los elementos del dataset
regla_pct_ingresos = df_integrado.apply(lambda row: regla_pct_ingresos_credito(row), axis=1).rename(
    "regla_pct_ingresos")

# Se unen los resultados al dataset inicial
df_resultado = pd.concat([df_integrado, regla_pct_ingresos], axis=1)

# Se visualizan los datos
print("Se visualizan las tuplas que no cumplen con la regla:\n")
display(df_resultado[df_resultado.regla_pct_ingresos == 'err'].head())

# Se verifica la cantidad de elementos
aux = df_resultado[df_resultado.regla_pct_ingresos == 'err']
print(f"Cantidad de filas que no cumplen la regla: {aux.shape[0]}")


