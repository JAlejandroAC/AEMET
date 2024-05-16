import requests
import polars as pl
import json
from datetime import datetime
import pytz
from functools import reduce
from utils import *
import polars as pl
from polars import col, lit, expr
from io import StringIO



def explode(data, field):
    df = (data.with_column(pl.col(field).explode().alias(f"{field}_nest"))
              .select(['id', 'fecha', f"{field}_nest['periodo']", f"{field}_nest['value']"])
              .with_column(pl.col("value").alias(field))
              .with_column(pl.col("periodo").alias("hora")))
    return df

def probabilidanested(pdata, field):
    df_prob = (pdata.with_column(pl.col(field).explode().alias(f"{field}_nest"))
                   .select(['id', 'fecha', f"{field}_nest['periodo']", f"{field}_nest['value']"])
                   .with_column(pl.col("periodo").str.slice(0, 2).alias("startperiodo"))
                   .with_column(pl.col("periodo").str.slice(2, 2).alias("endperiodo"))
                   .select(['id', 'fecha', "startperiodo", "endperiodo", "value"]))

    df_hourly = df_prob.with_column(pl.col("periodo").apply(list(range(24)), return_dtype=pl.DataType.LIST).explode().alias("periodo"))

    df_prob_hourly = (df_hourly.filter((pl.col("startperiodo") < pl.col("endperiodo")) & (pl.col("periodo") >= pl.col("startperiodo")) & (pl.col("periodo") < pl.col("endperiodo")))
                                .select("*")
                                .with_column(pl.col("value").alias(field))
                                .with_column(pl.col("periodo").alias("hora")))

    df_prob_hourly = (df_prob_hourly.with_column(
                                    "fecha", 
                                    expr.when(
                                        df_prob_hourly["hora"] < df_prob_hourly["startperiodo"], 
                                        pl.col("fecha").shift_and_fill(1)
                                    ).otherwise(df_prob_hourly["fecha"])
                                    ).sort("Fecha", "hora")
                                     .select('id','fecha','hora',f"{field}"))

    return df_prob_hourly

def vientonested(vdata):
    dfviento = (vdata.with_column(pl.col('Viento').explode().alias("viento_nest"))
                    .select(['id', 'fecha', "viento_nest['periodo']", "viento_nest['velocidad'][0]", "viento_nest['direccion'][0]"])
                    .drop_nulls(["vviento","vdireccion"])
                    .with_column(pl.col("Viento (km/h)").apply(lambda vdireccion, vviento: f"{vdireccion} {vviento}", args=["vdireccion", "vviento"]))
                    .with_column(pl.col("periodo").alias("hora"))
                    .select('id', 'fecha','hora','Viento (km/h)'))

    dfrmax = (vdata.with_column(pl.col('Viento').explode().alias("viento_nest"))
                   .select(['id', 'fecha', "viento_nest['periodo']", "viento_nest['value']"])
                   .drop_nulls(["value"])
                   .with_column(pl.col("periodo").alias("hora"))
                   .with_column(pl.col("value").alias("rachamax")))

    dfvientormax = dfviento.join(dfrmax, on=['id', 'fecha', 'hora'])

    return dfvientormax

def cielonested(cdata):
    cdf = (cdata.with_column(pl.col('Cielo').explode().alias("Cielo_nest"))
                .select(['id', 'fecha', "Cielo_nest['periodo']", "Cielo_nest['descripcion']"])
                .with_column(pl.col("descripcion").alias("Cielo"))
                .with_column(pl.col("periodo").alias("hora")))
    return cdf


decimalFields = ['temperatura','Senstermica','Precipitación','Nieve','Humedadrelativa']

def createdf(jdata):
    df_temp = pl.read_json(StringIO(jdata))
    #########################################################################################################
    #explode nested strings
    df_temperatura = explode(df_temp,'temperatura')
    df_Senstermica = explode(df_temp,'Senstermica')
    df_Precipitacion = explode(df_temp,'Precipitación')
    df_Nieve = explode(df_temp,'Nieve')
    df_Humedadrelativa = explode(df_temp,'Humedadrelativa')
    # explode nest for viento
    df_Viento = vientonested(df_temp)
    # explode nest cielo
    df_Cielo = cielonested(df_temp)
    # explode nest for probabilidad fields
    df_probPrecipitacion = probabilidanested(df_temp,'probPrecipitacion')
    df_probNieve = probabilidanested(df_temp,'probNieve')
    df_probTormenta = probabilidanested(df_temp,'probTormenta')
    #########################################################################################################
    #List of DF to join them
    df_list = [df_temperatura, df_Cielo, df_Senstermica, df_Viento,
               df_Precipitacion, df_Nieve, df_Humedadrelativa, df_probPrecipitacion, df_probNieve, df_probTormenta]
    #########################################################################################################
    ## renaming mapping
    rename_map = {"fecha": "Fecha", "hora": "Hora","temperatura": "Temp. (°C)" , "Senstermica": "Sen. térmica (°C)","rachamax": "Racha máx. (km/h)",
                  "Precipitación": "Precipitación (mm)","Nieve": "Nieve (mm)","Humedadrelativa": "Humedad relativa (%)",
                  "probPrecipitacion": "Prob. precip. (%)","probNieve": "Prob. de nieve (%)","probTormenta": "Prob. de tormenta (%)"}
     ##############################################################
    #Join DF's
    df_final_temp = reduce(join_df, df_list)
    #add today
    df_final_temp = add_date(df_final_temp)
    ## change type
    df_final_temp = convert_to_decimal(df_final_temp,decimalFields)
    ##ordenando el df
    df_final_temp = df_final_temp.select('fecha','hora','Cielo','temperatura','Senstermica','Viento (km/h)','rachamax',
                                    'Precipitación','Nieve','Humedadrelativa','probPrecipitacion','probNieve','probTormenta','Avisos','DateLoad')
    df_final = df_final_temp.rename(rename_map)
    return df_final


url = "https://opendata.aemet.es/opendata/api/prediccion/especifica/municipio/horaria/31069"
headers = {
    "api_key": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGVqYW5kcm9hZ3VpbGFyYzk1QGdtYWlsLmNvbSIsImp0aSI6IjIyODRmMjk5LWY5MmUtNDQyZC04MTY2LWZhYmJjNzQzZjk1OSIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNzEwODQ1ODIxLCJ1c2VySWQiOiIyMjg0ZjI5OS1mOTJlLTQ0MmQtODE2Ni1mYWJiYzc0M2Y5NTkiLCJyb2xlIjoiIn0.72-kfNL9CqxXoeRLLVrfWZsq3HFtcLwZ7aOb6FSNktQ"
}
data = obtener_datos(url, headers)

df_temp = jsonclean(data)

print(df_temp)

df_temp = pl.read_json(StringIO(df_temp))

print(df_temp)

df = (df_temp.select(['id', 'Fecha', "Temperatura"])
              .explode("Temperatura"))
            #   .select(['id', 'fecha', "temperatura_nest['periodo']", "temperatura_nest['value']"])
            #   .with_columns(pl.col("value").alias("temperatura"))
            #   .with_columns(pl.col("periodo").alias("hora")))


print(df)
# df_final = createdf(df_temp)
# print(df_final.orderBy("Fecha", "Hora", "DateLoad"))