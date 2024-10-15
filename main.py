
#Se importan las librerias necesarias

import pandas as pd
from fastapi import FastAPI, HTTPException

app = FastAPI()

# 1. developer(): Cantidad de items y porcentaje de contenido Free por año según desarrolladora

df_games = pd.read_parquet("df_games.parquet")
df_items = pd.read_parquet("df_items.parquet")


@app.get("/developer/{desarrollador}")
def developer(desarrollador: str):
    # Se filtran los datos por el desarrollador
    dev_games = df_games[df_games['developer'] == desarrollador]
    # Se verifica si hay datos para ese desarrollador
    if dev_games.empty:
        return {"error": f"No se encontraron datos para el desarrollador: {desarrollador}"}
    # Se convierte la columna 'release_date' a formato datetime para extraer el año
    dev_games['release_date'] = pd.to_datetime(dev_games['release_date'], errors='coerce')
    dev_games['year'] = dev_games['release_date'].dt.year
    # Se añade una columna para identificar si el juego es gratuito (price == 0)
    dev_games['is_free'] = dev_games['price'] == 0
    # Se agrupa por año y calculamos la cantidad de items y el porcentaje de contenido gratuito
    summary_by_year = dev_games.groupby('year').agg(
        cantidad_items=('title', 'count'),  # Cantidad de juegos por año
        contenido_free=('is_free', 'mean')  # Promedio de juegos gratuitos
    ).reset_index()
    # Se convierte el porcentaje a formato %
    summary_by_year['contenido_free'] = (summary_by_year['contenido_free'] * 100).round(2).astype(str) + '%'
    # Se formatea los datos para la respuesta
    response_data = summary_by_year.rename(columns={
        'year': 'Año',
        'cantidad_items': 'Cantidad de Items',
        'contenido_free': 'Contenido Free'
    }).to_dict(orient='records')

    # Se devuelve los datos en formato JSON
    return {"desarrollador": desarrollador, "datos": response_data}

#2. userdata(): Cantidad de dinero gastado, porcentaje de recomendación y cantidad de items por usuario

df_games = pd.read_parquet("df_games.parquet")
df_reviews = pd.read_parquet("df_reviews_sentiment.parquet")
df_items = pd.read_parquet("df_items.parquet")

@app.get("/userdata/{user_id}")
def userdata(user_id: str):
    # Se filtra los datos por usuario
    user_items = df_items[df_items['user_id'] == user_id]
    if user_items.empty:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    # Se calcula la cantidad de dinero gastado
    total_spent = df_games[df_games['id'].isin(user_items['item_id'])]['price'].sum()
    # Se calcula el porcentaje de recomendación
    user_reviews = df_reviews[df_reviews['user_id'] == user_id]
    recommendation_percentage = (user_reviews['recommend'].mean() * 100) if not user_reviews.empty else 0
    return {
        "Usuario": user_id,
        "Dinero gastado": f"{total_spent} USD",
        "% de recomendación": f"{recommendation_percentage}%",
        "Cantidad de items": len(user_items)
    }


@app.get("/")
def root():
    return {"message": "Bienvenido a la API de FastAPI"}
