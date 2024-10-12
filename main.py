{
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "from fastapi import FastAPI, HTTPException\n",
    "import pandas as pd\n",
    "from typing import List, Optional, Dict\n"
   ]
  },
{
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Se cargan los datos desde archivos .parquet\n"
   ]
  },
{
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
        "df_reviews = pd.read_parquet(\"df_reviews.parquet\")\n",
        "df_items = pd.read_parquet(\"df_items.parquet\")\n",
        "df_games = pd.read_parquet(\"df_games.parquet\")\n",
    "except Exception as e:\n",
    "    print(f\"Error al cargar los archivos: {e}\")\n"
   ]
  },
{
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Definir la aplicación FastAPI\n",
    "Se crea una instancia de FastAPI que permitirá definir los endpoints."
   ]
  },
{
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
    "app = FastAPI()\n"
   ]
  },
{
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Definición de Endpoints\n",
    "Se definen los endpoints según las especificaciones del proyecto 1 individual Henry."
   ]
  },
{
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Endpoint 1: Información de la empresa desarrolladora del juego\n",
    "Este endpoint devuelve la cantidad de items y porcentaje de contenido Free por año según empresa desarrolladora."
   ]
  },
{
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
        "@app.get(\"/developer/{developer}\")\n",
        "def developer(developer: str):\n",
    "         developer_games = df_games[df_games['developer'] == developer]\n", #se filtran los juegos de la empresa desarrolladora 
    "         item_count = developer_games.groupby('release_year').size().reset_index(name='Cantidad de Items')\n", #se cuentan los items (juegos ) por año
    "         free_content_count = developer_games[developer_games['is_free']].groupby('release_year').size().reset_index(name='Contenido Free')\n", # Se cuentan los items gratuitos
    "         result = pd.merge(item_count, free_content_count, on='release_year', how='left').fillna(0)\n", # se combinan los resultados
    "         result['Contenido Free'] = (result['Contenido Free'] / result['Cantidad de Items'] * 100).round(2).astype(str) + '%'\n", # se saca el procentaje del contenido gratuito
    "         return result.to_dict(orient='records')\n"
   ]
  },
{
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Endpoint 2: Información de la cantidad de dinero gastado por el usuario, el porcentaje de recomendación en base a reviews.recommend y cantidad de items\n",
    "Este endpoint devuelve la cantidad de dinero gastado por el usuario, el porcentaje de recomendación en base a reviews.recommend y cantidad de item."
   ]
  },
{
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
        "@app.get(\"/userdata/{User_id}\")\n",
        "def userdata(User_id : str ):\n",
    "        user_data = df_reviews[df_reviews['user_id'] == User_id]\n\n",  # se filtra las reseñas correspondientes al usuario dado        
    "        if user_data.empty:\n",
    "        return {\"error\": \"No se encontraron datos para este usuario\"}\n\n",
    "        total_spent = user_data['price'].sum()\n\n", # se calcula el dinero total gastado por el usuario\n"
    "        recommendation_percentage = (user_data['recommend'].mean()) * 100\n\n",   # se calcula el porcentaje de recomendación\n"
    "        item_count = user_data.shape[0]\n\n",    # Se cuenta la cantidad de items adquiridos por el usuario\n",
    "        result = {\n",                               # Se estructura la respuesta en un diccionario\n",
    "             \"Usuario X\": User_id,\n",
    "             \"Dinero gastado\": f\"{total_spent} USD\",\n",
    "             \"% de recomendación\": f\"{recommendation_percentage:.2f}%\",\n",
    "             \"cantidad de items\": item_count\n",
    "    }\n\n",
        
    "    return result\n"
   ]
},  
{
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Endpoint 3: Informacion del usuario que acumula más horas jugadas para el género y una lista de la acumulación de horas jugadas por año de lanzamiento.\n",
    "Este endpoint Debe devolver el usuario que acumula más horas jugadas para el género dado y una lista de la acumulación de horas jugadas por año de lanzamiento."
   ]
  },
{
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
        "@app.get(\"/userforgenre/{genero}\")\n",
        "def UserForGenre(genero: str) -> Dict[str, List[Dict[str, int]]]:\n",
    "         genre_games = df_games[df_games['genre'] == genero]\n\n", #se filtran los juegos de la empresa desarrolladora 
    "         merged_data = pd.merge(df_reviews, genre, on='id')\n\n", # Se unen las reseñas de usuarios con los juegos filtrados por género\n",
    "         user_hours = merged_data.groupby('user_id')['playtime_forever'].sum().reset_index()\n",  # Se agrupa por usuario y sumamos las horas jugadas
    "         top_user = user_hours.loc[user_hours['playtime_forever'].idxmax()]['user_id']\n\n",
    "         hours_by_year = merged_data.groupby('release_date')['playtime_forever'].sum().reset_index()\n",
    "         hours_by_year_list = [\n",
    "              {'Año': int(row['release_date']), 'Horas': int(row['playtime_forever'])} for _, row in hours_by_year.iterrows()\n",
    "         ]\n\n",
    "         result = {\n",  # Se estructura la respuesta\n"
    "        'Usuario con más horas jugadas para Género X': top_user,\n",
    "        'Horas jugadas': hours_by_year_list\n",
    "    }\n\n",
        
    "    return result\n"            
   ]
  },       
{
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Endpoint 4: Top de desarrolladores con los juegos mas recomendados por usuario para el año asignado\n",
    "Este endpoint devuelve el top 3 de desarrolladores con juegos más recomendados por usuarios para un año asignado.\n",
   ]
  },
{
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
        "@app.get(\"/best_developer_year/{año}\")\n",
        "def best_developer_year(año: int) -> List[Dict[str, str]]:\n",
    "        year_games = df_games[df_games['release_date'] == año]\n\n",  # Se filtran los juegos por el año de lanzamiento\n"
    "        merged_data = pd.merge(df_reviews, year_games, on='id')\n\n", # se unen las reseñas con los juegos filtrados por el año\n"
    "        recommended_reviews = merged_data[merged_data['recommend'] == True]\n\n",  # se filtran reseñas recomendadas (donde reviews.recommend es True)\n",  
    "        developer_recommendations = recommended_reviews.groupby('developer')['recommend'].count().reset_index(name='recommend_count')\n\n", # Agrupamos por desarrollador y contamos las recomendaciones\n"
    "        top_developers = developer_recommendations.sort_values(by='recommend_count', ascending=False).head(3)\n\n",   # Se ordena los desarrolladores por cantidad de recomendaciones\n",
    "             result = [\n",
    "                   {f'Puesto {i+1}': developer} for i, developer in enumerate(top_developers['developer'])\n",
    "             ]\n\n",
        
    "    return result\n"           
   ]
  }, 
{
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Endpoint 5: Análisis de las reseñas de un desarrollador según el sentimiento (positivo o negativo).\n",
    "Este endpoint devuelve un análisis de las reseñas de un desarrollador según el sentimiento (positivo o negativo)."
   ]
  },
{
   "cell_type": "code",
   "execution_count": None,
   "metadata": {},
   "outputs": [],
   "source": [
        "@app.get(\"/developer_reviews_analysis/{desarrolladora}\")\n",
        "def developer_reviews_analysis(desarrolladora: str) -> Dict[str, List[str]]:\n",
    "        developer_games = df_games[df_games['developer'] == desarrollador]\n\n",  # Se filtra los juegos por el desarrollador\n",   
    "        merged_data = pd.merge(df_reviews, developer_games, on='id')\n\n",  # Se une las reseñas con los juegos de ese desarrollador\n",  
    "        positive_reviews = merged_data[merged_data['sentiment'] == 'positive'].shape[0]\n", # Se filtra por sentimiento positivo y negativo\n",
    "        negative_reviews = merged_data[merged_data['sentiment'] == 'negative'].shape[0]\n\n",
    "        result = {desarrolladora: [f'Negative = {negative_reviews}', f'Positive = {positive_reviews}']}\n\n",
        
    "        return result\n"            
   ]
  }, 
{
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.8"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
