import requests
import pandas as pd
from datetime import datetime

# Defino URL

def get_crypto_data():

    url = 'https://api.coingecko.com/api/v3/coins/markets'

    params = {
        'vs_currency': 'usd',
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False
    }

    try:

        resp = requests.get(url, params=params)  # Prueba conexion

    except requests.exceptions.ConnectionError:

        print(f"Codigo Rechazo: {resp.status_code}") # Mensaje de error
    
    else:  

        print(f"Codigo respuesta: {resp.status_code}") # Mensaje de aprobacion
   

    # Transformamos la data a json para poder guardarlo en un DataFrame
    resp_json = resp.json()

    df = pd.DataFrame(resp_json)

    print(f"\nPrimeras 5 lineas del DataFrame: \n{df.head()}")

    # Analizamos las columnas

    print(f"\nColumnas iniciales: \n{df.columns}")

    # Actualizamos el DataFrame y solo dejamos las columnas que nos interesa y creamos columna nueva de ingestion time

    df = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume', 'last_updated']]

    df['ingestion_time'] = datetime.utcnow().isoformat()

    print(f"\nColumnas finales: \n{df.columns}")

    return df

def to_csv(data):  

    data.to_csv("data/crypto_data.csv", index=False)

if __name__ == '__main__':

    df = get_crypto_data()

    to_csv(df)
