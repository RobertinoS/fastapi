from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd 
from typing import Optional

app = FastAPI()

#http://127.0.0.1:8000 (ruta raiz)
@app.get("/")                       #ruta
def read_root():                    #FUNCION EN ESTA RUTA
    return {"Hello": "World"}
    
df_play=pd.read_parquet('data/df_playtime.parquet')
df_merge_g2_group=pd.read_parquet('data/df_useforgenre.parquet')
tabla_pivote_norm=pd.read_parquet('data\tabla_pivote_norm.parquet')
df_users_sim=pd.read_parquet('data\df_items_sim.parquet')


@app.get('/PlayTimeGenre')
def PlayTimeGenre(genero: str):
    # Realizar el merge de los DataFrames
    #df_merge = pd.merge(df_games[['genres', 'release_anio', 'item_id']], df_items[['playtime_forever', 'item_id']], on='item_id')
    
    # Filtrar por el género especificado
    df_genre = df_play[df_play['genres'] == genero]
    
    # Si no hay datos para el género especificado, retorna un mensaje
    if df_genre.empty:
        return f"No hay datos para el género '{genero}'"
    
    # Agrupar por año y calcular las horas jugadas sumando los valores
    grouped = df_genre.groupby('release_anio')['playtime_forever'].sum()
    
    # Encontrar el año con más horas jugadas
    max_playtime_year = grouped.idxmax()
    #max_playtime = grouped.max()
    
    # Retornar el resultado como un diccionario
    return {"Año de lanzamiento con más horas jugadas para Género {}".format(genero): max_playtime_year}
    

@app.get('/UserForGenre')
def UserForGenre(genero: str):
    # Realizar el merge de los DataFrames
    #df_merge = pd.merge(df_games[['genres', 'item_id', 'release_anio']], df_items[['playtime_forever', 'item_id']], on='item_id')
    
    # Filtrar por el género especificado
    df_genre = df_merge_g2_group[df_merge_g2_group['genres'] == genero]
    
    # Si no hay datos para el género especificado, retorna un mensaje
    if df_genre.empty:
        return f"No hay datos para el género '{genero}'"
    
    # Agrupar por usuario y género y calcular las horas jugadas sumando los valores
    grouped = df_genre.groupby(['user_id'])['playtime_forever'].sum()
    
    # Encontrar el usuario con más horas jugadas
    max_playtime_user = grouped.idxmax()
    
    # Filtrar por el usuario con más horas jugadas
    df_user_max_playtime = df_genre[df_genre['user_id'] == max_playtime_user]
    
    # Agrupar por año y calcular las horas jugadas sumando los valores
    grouped_by_year = df_genre.groupby('release_anio')['playtime_forever'].sum()
    
    # Crear lista de acumulación de horas jugadas por año
    acumulacion_horas = [{'Año': year, 'Horas': hours} for year, hours in grouped_by_year.items()]
    
    # Retornar el resultado como un diccionario
    return {"Usuario con más horas jugadas para Género {}".format(genero): max_playtime_user, "Horas jugadas": acumulacion_horas}

@app.get('/recomendacion_usuario')
def recomendacion_usuario(user):
   
    '''
    Muestra una lista de los usuarios más similares a un usuario dado y sus valores de similitud.

    Args:
        user (str): El nombre o identificador del usuario para el cual se desean encontrar usuarios similares.

    Returns:
        None: Esta función imprime la lista de usuarios similares y sus valores de similitud en la consola.

    '''
    # Verifica si el usuario está presente en las columnas de piv_norm (si no está, devuelve un mensaje)
    if user not in tabla_pivote_norm.columns:
        return('No data available on user {}'.format(user))
    
    print('Most Similar Users:\n')
    # Ordena los usuarios por similitud descendente y toma los 5 usuarios más similares (excluyendo el propio 'user')
    sim_values = df_users_sim.sort_values(by=user, ascending=False).loc[:,user].tolist()[1:6]
    sim_users = df_users_sim.sort_values(by=user, ascending=False).index[1:11]
    # Combina los nombres de usuario y los valores de similitud en una lista de tuplas
    zipped = zip(sim_users, sim_values,)
    
    # Itera a través de las tuplas y muestra los usuarios similares y sus valores de similitu
    for user, sim in zipped:
        print('User #{0}, Similarity value: {1:.2f}'.format(user, sim)) 
    


