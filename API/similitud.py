import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import scipy as sp



def cargar_datos():
    # Cargar los datos de los archivos .parquet
    df_items = pd.read_parquet('df2_items.parquet')
    df_reviews = pd.read_parquet('df_reviews_sentiment.parquet')
    
    # Preprocesar los datos
    df_items = df_items.drop(["steam_id", "user_url", "playtime_2weeks"], axis=1)
    df_items_filtered = df_items[['user_id', 'item_id', 'item_name', 'playtime_forever']]
    df_reviews_filtered = df_reviews[['user_id', 'item_id', 'sentiment_analysis']]
    df_items_filtered = df_items_filtered[df_items_filtered['playtime_forever'] != 0]
    
    # Combinar los DataFrames
    df_combined = pd.merge(df_items_filtered, df_reviews_filtered, on=['user_id', 'item_id'])
    
    return df_combined

def calcular_similitud(df_combined):
    # Crear la tabla pivot
    items_pivot = df_combined.pivot_table(index="user_id", columns="item_name", values="sentiment_analysis", aggfunc="mean")
    items_pivot.fillna(0, inplace=True)

    # Convertir a matriz dispersa
    df_dispersa = sp.sparse.csr_matrix(items_pivot.values)
    
    # Calcular similitudes
    user_similarity = cosine_similarity(df_dispersa)
    item_similarity = cosine_similarity(df_dispersa.T)
    
    # Convertir a DataFrames
    user_similarity_df = pd.DataFrame(user_similarity, index=items_pivot.index, columns=items_pivot.index)
    item_similarity_df = pd.DataFrame(item_similarity, index=items_pivot.columns, columns=items_pivot.columns)
    
    return items_pivot, user_similarity_df, item_similarity_df

def user_item_similarity(user_id, items_pivot, user_similarity_df, n_recommendations=5):
    user_idx = items_pivot.index.get_loc(user_id)
    similar_users = list(enumerate(user_similarity_df.iloc[user_idx]))
    similar_users = sorted(similar_users, key=lambda x: x[1], reverse=True)
    similar_users = [items_pivot.index[i] for i, score in similar_users[1:n_recommendations + 1]]
    recommended_games = items_pivot.loc[similar_users].mean().sort_values(ascending=False).index[:n_recommendations]
    return recommended_games.tolist()

def item_item_similarity(item_id, items_pivot, item_similarity_df, n_recommendations):
    item_idx = items_pivot.columns.get_loc(item_id)  # Obtener el índice del juego
    similar_items = list(enumerate(item_similarity_df.iloc[item_idx]))  # Obtener similitudes
    similar_items = sorted(similar_items, key=lambda x: x[1], reverse=True)  # Ordenar por similitud
    similar_games = [items_pivot.columns[i] for i, score in similar_items[1:n_recommendations + 1]]
    return similar_games  

