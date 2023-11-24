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
#tabla_pivote_norm=pd.read_parquet('data/tabla_pivote_norm.parquet')
#df_users_sim=pd.read_parquet('data/df_items_sim.parquet')
#df_items_sim=pd.read_parquet('data/df_items_sim.parquet')
df_worst_1=pd.read_parquet('data/df_worst.parquet')
df_senti=pd.read_parquet('data/df_senti.parquet')
#df_recom=pd.read_parquet('data/df_recom.parquet')
df_merge_id=pd.read_parquet('data/df_recomendacion.parquet')

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
@app.get('/UsersRecommend') 

def UsersRecommend( año : int):
    # Filtrar el DataFrame df_top3 por el año proporcionado
    top3_by_year = df_recom[df_recom['release_anio'] == año]

#Crear la lista de diccionarios
    resultado = []
    for index, row in top3_by_year.iterrows():
        puesto = row['rank']
        titulo = row['title']
        año = int(row['release_anio'])
        resultado.append({f"Puesto {puesto}": f"{titulo}"})
    return resultado
    # retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]
    
@app.get('/UsersWorstDeveloper')  
def UsersWorstDeveloper( año : int ):
    # Filtrar el DataFrame df_developer por el año proporcionado
    developer_by_year = df_worst_1[df_worst_1['release_anio'] == año]

    # Obtener el top 3 de desarrolladoras con juegos MENOS recomendados y sus valores según rank
    top3_worst_developer = developer_by_year.sort_values(by='rank', ascending=True).head(3)

    # Formatear el resultado como lista de diccionarios
    result = [{"Puesto {}: {}".format(rank, developer)} for rank, developer in zip(top3_worst_developer['rank'], top3_worst_developer['developer'])]

    return result


    #retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]
    
    
@app.get('/sentiment_analysis')    
def sentiment_analysis( empresa_desarrolladora : str): 
    # Filtrar el DataFrame por la empresa desarrolladora proporcionada
    developer_df = df_senti[df_senti['developer'] == empresa_desarrolladora]

    #Crear el diccionario de retorno
    result = {empresa_desarrolladora: {'Negative': 0, 'Neutral': 0, 'Positive': 0}}

    # Llenar el diccionario con la cantidad de registros para cada categoría de sentimiento
    for sentiment, count in zip(developer_df['sentiment_analysis'], developer_df['reviews_recommend_count']):
        sentiment_mapping = {0: 'Negative', 1: 'Neutral', 2: 'Positive'}
        sentiment_category = sentiment_mapping[sentiment]
        result[empresa_desarrolladora][sentiment_category] += count

    return result

@app.get('/recomendacion_juego')
def recomendacion_juego(id:int):
    # Filtrar el DataFrame por la empresa desarrolladora proporcionada
    modelo = df_merge_id[df_merge_id['id'] == id]['model'].iloc[0]
    if len(modelo) > 0:
        recomendaciones_dict = {i + 1: juego for i, juego in enumerate(modelo)}
        return recomendaciones_dict
    else:
        return f"No se encontró un modelo para el id {id}"


'''def recomendacion_usuario(user):
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
    
    return recomendaciones'''
