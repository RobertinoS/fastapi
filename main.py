from pydantic import BaseModel
import pandas as pd 
from typing import Optional
from fastapi import FastAPI, Response, Query
from fastapi.responses import HTMLResponse

#http://127.0.0.1:8000 (ruta raiz)

app = FastAPI()
@app.get("/", response_class=HTMLResponse)  # Ruta de la página inicial
def presentacion():
    '''
    Genera una página de presentación HTML para la API del proyecto individual 1 sobre la plataforma Steam.
    
    Returns:
    str: Código HTML que muestra la página de presentación.
    '''
    return '''
    <html>
        <head>
            <title>API Proyecto Individual 1</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    padding: 20px;
                    background-image: url('https://cdn.businessinsider.es/sites/navi.axelspringer.es/public/media/image/2021/01/steam-juegos-2203341.jpg?tf=1200x');
                    background-size: cover;
                    color: white;
                    text-shadow: 2px 2px 4px #000000;
                }
                h1 {
                    color: #fff;
                    text-align: center;
                }
                p {
                    color: #fff;
                    text-align: center;
                    font-size: 18px;
                    margin-top: 20px;
                }
            </style>
        </head>
        <body>
            <h1>Bienvenidos a mi API sobre el proyecto individual 1 sobre la plataforma Steam</h1>
            <p>Mi nombre es Robertino Sanguedolce del cohorte nº17.</p>
            <p>INSTRUCCIONES:</p>
            <p>Escriba <span style="background-color: lightgray; color: black;">/docs</span> a continuación de la URL actual de esta página para interactuar con la API</p>
        </body>
    </html>
    '''



#-------------------------------------------------------------------------------------------------------
    
df_play=pd.read_parquet('data/df_playtime.parquet')
df_useforgenre=pd.read_parquet('data/df_useforgenre.parquet')
df_worst_1=pd.read_parquet('data/df_worst.parquet')
df_senti=pd.read_parquet('data/df_senti.parquet')
df_recom=pd.read_parquet('data/df_recom.parquet')
df_merge_id=pd.read_parquet('data/df_recomendacion.parquet')
#---------------------------------------------------------------------------------------------------------
@app.get('/PlayTimeGenre')

def PlayTimeGenre(genero: str = Query(..., description="Ingrese el género del videojuego. Por ejemplo, un género válido podría ser 'Action'.")):
    # Tu código aquí
    #pass
    # Filtrar por el género especificado
    df_genre = df_play[df_play['genres'] == genero]    
    # Si no hay datos para el género especificado, retorna un mensaje
    if df_genre.empty:
        raise HTTPException(status_code=404, detail=f"No hay datos para el género '{genero}'")   
    # Agrupar por año y calcular las horas jugadas sumando los valores
    grouped = df_genre.groupby('release_anio')['playtime_forever'].sum()    
    # Encontrar el año con más horas jugadas
    max_playtime_year = grouped.idxmax()
    #max_playtime = grouped.max()    
    # Retornar el resultado como un diccionario
    return {"Año de lanzamiento con más horas jugadas para Género {}".format(genero): max_playtime_year}
        
@app.get('/UserForGenre')
def UserForGenre(genero: str = Query(..., description="Ingrese el género del videojuego. Por ejemplo, un género válido podría ser 'Action'.")):
    # Realizar el merge de los DataFrames
    #df_merge = pd.merge(df_games[['genres', 'item_id', 'release_anio']], df_items[['playtime_forever', 'item_id']], on='item_id')    
    # Filtrar por el género especificado
    df_genre = df_useforgenre[df_useforgenre['genres'] == genero]    
    # Si no hay datos para el género especificado, retorna un mensaje
    if df_genre.empty:
        return HTTPException(status_code=404, detail=f"No hay datos para el género '{genero}'")     
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
def UsersRecommend( año :int = Query(..., description="Ingrese un año que este en el rango entre el 2010 y 2015")):
    # Filtrar el DataFrame df_top3 por el año proporcionado
    top3_by_year = df_recom[df_recom['year'] == año]

#Crear la lista de diccionarios
    resultado = []
    for index, row in top3_by_year.iterrows():
        puesto = row['rank']
        titulo = row['title']
        año = int(row['year'])
        resultado.append({f"Puesto {puesto}": f"{titulo}"})
    return resultado
    # retorno: [{"Puesto 1" : X}, {"Puesto 2" : Y},{"Puesto 3" : Z}]
    
@app.get('/UsersWorstDeveloper')  
def UsersWorstDeveloper( año : int = Query(..., description="Ingrese un año que este en el rango entre el 2011 y 2015")):
    # Filtrar el DataFrame df_developer por el año proporcionado
    developer_by_year = df_worst_1[df_worst_1['year'] == año]

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
    recomendaciones_dict = {}
    if len(modelo) > 0:
        for i in range(len(modelo)):
            recomendaciones_dict[i + 1] = modelo[i]
        return recomendaciones_dict
    else:
        return f"No se encontró un modelo para el id {id}"

