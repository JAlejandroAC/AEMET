import pandas as pd
from datetime import datetime, timedelta

today = datetime.today()

start_date = datetime(today.year, 1, 1)
print(today)


def create_date_dim(start_date, num_years):
    # Create date range
    start_date = start_date - timedelta(days=num_years*365)
    fecha = pd.date_range(start_date, periods=num_years*365)

    # Create DataFrame
    df = pd.DataFrame(fecha, columns=['fecha'])

    # Create additional fields
    df['Fecha_Key'] = df['fecha'].dt.strftime('%Y%m%d')
    df['DiaDesemana_Cod'] = df['fecha'].dt.weekday
    df['DiaDeSemana'] = df['fecha'].dt.day_name(locale='es')
    df['DiaDeMes'] = df['fecha'].dt.day
    df['DiaMes_cod'] = df['fecha'].dt.strftime('%m%d').astype(int)
    df['SemanaDelAnio_Cod'] = df['fecha'].dt.isocalendar().week
    df['SemanaDelAnio'] = "S" + df['fecha'].dt.strftime('%U')
    df['Semana'] = df['fecha'].dt.strftime('%Y') + "-S" + df['fecha'].dt.strftime('%U')
    df['MesDelAnio_Cod'] = df['fecha'].dt.month
    df['MesDelAnio'] = df['fecha'].dt.month_name(locale='es')
    df['Mes_cod'] = df['fecha'].dt.strftime('%Y%m').astype(int)

    # Reorder columns
    cols = df.columns.tolist()
    cols.insert(0, cols.pop(cols.index('Fecha_Key')))
    df = df[cols]


    return df

# Usage
num_years = 3
date_dim = create_date_dim(start_date, num_years)
print(date_dim)
