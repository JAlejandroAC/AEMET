import requests
import polars as pl
import json
from datetime import datetime
import pytz


def obtener_datos(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  
        request_data = response.json()
        
        if isinstance(request_data, dict) and 'datos' in request_data:
            data_url = request_data['datos']
            data_response = requests.get(data_url)
            print(data_url)
            data_response.raise_for_status()  
            data = data_response.json()[0]
            return data
        else:
            print("El JSON principal no contiene la clave 'datos' o no es un diccionario válido.")
            
    except requests.exceptions.RequestException as e:
        print("Error en la solicitud:", e)
    except json.decoder.JSONDecodeError as e:
        print("Error al decodificar JSON:", e)


#####restructure the json
def jsonclean(data):
    prediccion = data.get('prediccion')
    predicciones = []
    for diait in prediccion.get('dia'):
        predicciones.append({
            'Fecha': diait.get('fecha'),
            'id': data.get('id'),
            'Temperatura': diait.get('temperatura'),
            'Cielo': diait.get('estadoCielo'),
            'Senstermica': diait.get('sensTermica'),
            'Viento': diait.get('vientoAndRachaMax'),
            'Precipitación': diait.get('precipitacion'),
            'Nieve': diait.get('nieve'),
            'Humedadrelativa': diait.get('humedadRelativa'),
            'probPrecipitacion': diait.get('probPrecipitacion'),
            'probNieve': diait.get('probNieve'),
            'Humedadrelativa': diait.get('humedadRelativa'),
            'probTormenta': diait.get('probTormenta')

        })
    jdata = json.dumps(predicciones)
    return jdata


#########################################
# add today as updatedate/DateLoad


def add_date(df):
    # timezone Madrid
    madrid_tz = pytz.timezone('Europe/Madrid')
    current_date = datetime.now(madrid_tz).strftime('%Y-%m-%d %H:%M')

    # add currentdate to df
    df = df.with_column(pl.lit(current_date).alias('DateLoad').cast(pl.Date32))
    
    return df


def convert_to_decimal(df,decimalFields):
    for field in decimalFields:
        df = df.with_column(pl.col(field).cast(pl.Float64))
    return df

def join_df(df1, df2):
    return df1.join(df2, on=['id', 'fecha', 'hora'], how='left')
