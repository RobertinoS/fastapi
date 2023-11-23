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
df_useforgenre=pd.read_parquet('data/df_useforgenre.parquet')
tabla_pivote_norm=pd.read_parquet('data/tabla_pivote_norm.parquet')
df_users_sim=pd.read_parquet('data/df_items_sim.parquet')
df_items_sim=pd.read_parquet('data/df_items_sim.parquet')


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
def UserForGenre(genero):
    # Realizar el merge de los DataFrames
    #df_merge = pd.merge(df_games[['genres', 'item_id', 'release_anio']], df_items[['playtime_forever', 'item_id']], on='item_id')    
    # Filtrar por el género especificado
    df_genre = df_useforgenre[df_useforgenre['genres'] == genero]    
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

@app.get('/recomendacion_juego')
def recomendacion_juego(game):
    similar_games = df_items_sim.sort_values(by=game, ascending=False).iloc[1:6]
    count = 1
    contador = 1
    recomendaciones = {}
    
    for item in similar_games:
        if contador <= 5:
            item = str(item)
            recomendaciones[count] = item
            count += 1
            contador += 1 
        else:
            break
    return recomendaciones

def recomendacion_usuario(user):
    # Verifica si el usuario está presente en las columnas de piv_norm (si no está, devuelve un mensaje)
    if user not in tabla_pivote_norm.columns:
        return('No data available on user {}'.format(user))
    
    # Obtiene los usuarios más similares al usuario dado
    sim_users = df_users_sim.sort_values(by=user, ascending=False).index[1:11]
    
    best = [] # Lista para almacenar los juegos mejor calificados por usuarios similares
    most_common = {} # Diccionario para contar cuántas veces se recomienda cada juego
    
    # Para cada usuario similar, encuentra el juego mejor calificado y lo agrega a la lista 'best'
    for i in sim_users:
        i = str(i)
        max_score = tabla_pivote_norm.loc[:, i].max()
        best.append(tabla_pivote_norm[tabla_pivote_norm.loc[:, i]==max_score].index.tolist())
    
    # Cuenta cuántas veces se recomienda cada juego
    for i in range(len(best)):
        for j in best[i]:
            if j in most_common:
                most_common[j] += 1
            else:
                most_common[j] = 1
    
    # Ordena los juegos por la frecuencia de recomendación en orden descendente
    sorted_list = sorted(most_common.items(), key=operator.itemgetter(1), reverse=True)
    recomendaciones = {} 
    contador = 1 
    # Devuelve los 5 juegos más recomendados
    for juego, _ in sorted_list:
        if contador <= 5:
            recomendaciones[contador] = juego 
            contador += 1 
        else:
            break
    
    return recomendaciones
