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
#df_usegenre=pd.read_parquet('data/df_useforgenre_parquet.parquet')

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
    
'''
@app.get('/UserForGenre')
def UserForGenre(genero):
        # Filtrar por el género especificado
    df_genre = df_usegenre[df_usegenre['genres'] == genero]
    
    # Si no hay datos para el género especificado, retorna un mensaje
    if df_genre.empty:
        return f"No hay datos para el género '{genero}'"
    
    # Agrupar por usuario y género y calcular las horas jugadas sumando los valores
    grouped = df_genre.groupby(['item_id'])['playtime_forever'].sum()
    
    # Encontrar el usuario con más horas jugadas
    max_playtime_user = grouped.idxmax()
    
    # Filtrar por el usuario con más horas jugadas
    df_user_max_playtime = df_genre[df_genre['item_id'] == max_playtime_user]
    
    # Agrupar por año y calcular las horas jugadas sumando los valores
    grouped_by_year = df_genre.groupby('release_anio')['playtime_forever'].sum()
    
    # Crear lista de acumulación de horas jugadas por año
    acumulacion_horas = [{'Año': year, 'Horas': hours} for year, hours in grouped_by_year.items()]
    
    # Retornar el resultado como un diccionario
    return {"Usuario con más horas jugadas para Género {}".format(genero): max_playtime_user, "Horas jugadas": acumulacion_horas}'''
    


