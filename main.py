
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

#4. best_developer_year(): Top 3 desarrolladores más recomendados por usuarios en un año

df_games = pd.read_parquet("df_games.parquet")
df_reviews = pd.read_parquet("df_reviews_sentiment.parquet")

@app.get("/bestdeveloper/{year}")
def best_developer(year: int):
    # Se convierte la columna 'release_date' en formato datetime para extraer el año
    df_games['release_date'] = pd.to_datetime(df_games['release_date'], errors='coerce')
    df_games['year'] = df_games['release_date'].dt.year

    # Se filtra por el año proporcionado
    games_in_year = df_games[df_games['year'] == year]
    
    # Se verifica si hay datos para ese año
    if games_in_year.empty:
        return {"error": f"No se encontraron datos para el año {year}"}
    
    # Se agrupa  por desarrollador y contar la cantidad de juegos
    developer_count = games_in_year.groupby('developer').size().reset_index(name='cantidad_juegos')
    
    # Se ordena por la cantidad de juegos publicados y tomar los primeros 3 desarrolladores
    top_developers = developer_count.sort_values(by='cantidad_juegos', ascending=False).head(3)
    
    # Se crea un diccionario para los resultados
    ranking = {f"Puesto {i+1}": row['developer'] for i, row in top_developers.iterrows()}
    
    return ranking




@app.get("/")
def root():
    return {"message": "Bienvenido a la API de FastAPI"}
