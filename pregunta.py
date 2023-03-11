"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():

    colspecs = [(0, 8), (8, 24), (24, 40), (40, -1)]
    df = pd.read_fwf('clusters_report.txt', index_col=False, header=None,skiprows=4, 
                 names=['cluster', 'cantidad_de_palabras_clave', 
                        'porcentaje_de_palabras_clave', 'principales_palabras_clave'])

    cols_to_fill=['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave']
    df[cols_to_fill] = df[cols_to_fill].fillna(method='ffill')
    df=df.groupby(cols_to_fill)['principales_palabras_clave'].agg(list).reset_index()
    
    df['porcentaje_de_palabras_clave']=df['porcentaje_de_palabras_clave'].str.replace(',', '.').str.rstrip(' %').astype(float)
    
    df['principales_palabras_clave']=df['principales_palabras_clave'].apply(lambda x: ' '.join(x))
    df['principales_palabras_clave']=df['principales_palabras_clave'].str.replace(r'\s{2,}', ' ', regex=True)
    df['principales_palabras_clave']=df['principales_palabras_clave'].str.replace('.','',regex=True)

    return df
