
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

#3. UserForGenre(): Usuario con más horas jugadas para un género y horas acumuladas por año

df_games = pd.read_parquet("df_games.parquet")
df_items = pd.read_parquet("df_items.parquet")

@app.get("/userforgenre/{genero}")
def user_for_genre(genero: str):
    # Se filtra por género
    genre_games = df_games[df_games['genres'].str.contains(genero, case=False, na=False)]
    
    if genre_games.empty:
        raise HTTPException(status_code=404, detail="Género no encontrado")
    
    # Se cambia de tipo de dato
    genre_games['id'] = genre_games['id'].astype('int64') 
    df_items['item_id'] = df_items['item_id'].astype('int64')
    
    # Se une con df_items para obtener el tiempo de juego por usuario
    merged_data = genre_games.merge(df_items, left_on='id', right_on='item_id', how='inner')
    
    # Se encuentra el usuario con más horas jugadas
    user_with_most_playtime = merged_data.groupby('user_id')['playtime_forever'].sum().idxmax()
    
    # Horas acumuladas por año de lanzamiento
    playtime_by_year = merged_data.groupby(merged_data['release_date'].dt.year)['playtime_forever'].sum().reset_index()
    
    return {
        "Usuario con más horas jugadas para Género": user_with_most_playtime,
        "Horas jugadas": playtime_by_year.to_dict(orient="records")
    }


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





#5. developer_reviews_analysis(): Análisis de reseñas según desarrolladora (positivas vs negativas)

# Cargar los DataFrames
df_games = pd.read_parquet("df_games.parquet")
df_reviews = pd.read_parquet("df_reviews_sentiment.parquet")

@app.get("/developerreviews/{developer_name}")
def developer_reviews_analysis(developer_name: str):
    # Se filtra juegos por desarrollador
    developer_games = df_games[df_games['developer'] == developer_name]
    if developer_games.empty:
        return {developer_name: "No se encontraron juegos para este desarrollador."}
    # Se filtra reseñas por los juegos del desarrollador
    reviews = df_reviews[df_reviews['item_id'].isin(developer_games['id'])]
    # Contar reseñas positivas y negativas
    positive_count = reviews[reviews['sentiment_analysis'] == 2].shape[0]
    negative_count = reviews[reviews['sentiment_analysis'] == 0].shape[0]

    return {developer_name: {"Negative": negative_count, "Positive": positive_count}}





@app.get("/")
def root():
    return {"message": "Bienvenido a la API de FastAPI"}
