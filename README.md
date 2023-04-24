# prueba2
Documentación: sprint 2
HU1
BASE DE DATOS
Nombre de la base de datos: postgres
Descripción: Esta base de datos postgres, tiene información sobre el tráfico vehicular de la ciudad de Medellín almacenada en las tablas: “stn_dtch “, “fc_dtch” y “dm_carril”. Estas últimas 2 creadas en este sprint


Ubicación física:
Servidor: 10.5.3.99
Sistema de gestión de bases de datos: postgreSQL
Información de conexión:
Nombre del servidor: 10.5.3.99
Nombre de la base de datos: postgres
Nombre de usuario: postgres
Contraseña: postgres
Puerto: 5432 (predeterminado)
Descripción de tablas:
Tabla "stn_dtch ":
Carril (int): contiene la identificación numérica de cada carril
Carril Nombre (tex): contiene el nombre del carril
 Fecha (datatime): contiene la fecha exacta de cuando se creó el registro
Intensidad (int): contiene la cantidad de vehículos que transitan por cada carril
VehiculoLongitud1 (int):  contiene la cantidad de vehículos de longitud *** que transitan por cada carril 
VehiculoLongitud2 (int):  contiene la cantidad de vehículos de longitud *** que transitan por cada carril 
VehiculoLongitud3 (int):  contiene la cantidad de vehículos de longitud *** que transitan por cada carril 
Velocidad (km/h): contiene la velocidad promedio de los vehículos que pasan por cada carril
Ocupación (int): 
Carril_M ():



GENERACION DE TABLA MACRO - DATOS DE TRAFICO ATIPICOS
Objetivo: creación de tabla macro en postgres con las variables de tráfico atípico de CCTV y ARS.

Desarrollo:
Tablas creadas:
fc_dtch:
Descripción: la tabla fc_dtch contiene información del tráfico vehicular de Medellín, contiene los campos:
Carril (int): contiene la identificación numérica de cada carril 
Fecha_Hora (datetime): contiene la fecha en la cual fue creado el registro y redondeado por hora
Intensidad (int): contiene la cantidad de vehículos que transitan por cada carril
velocidad (int): contiene la velocidad promedio de los vehículos que pasan en esa hora
intensidad_0 (int): el campo intessidad_0 es cero, si existe el campo de intensidad 0 mayor a cero y 1 si el campo de intensidad es cero
sin_velocidad (int): el campo sin_velocidad es cero, si el campo velocidad es mayor a cero y 1 si el campo de velocidad es cero
registro(int): el campo registro es cero si el registro fue agregado manualmente (en la transformación de datos) y 1 en otro caso

 

dm_carril:
Descripción: contiene información del nombre y la identificación numérica de cada carril
carril (int): contiene la identificación de cada carril
carril_nombre: contiene el nombre del carril
 

CODIGO DE LA CREACION DE LA TABLA 
Librerías necesarias:
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
import sqlalchemy

Conexión a la base de datos postgres:

database = 'postgres'
user = 'postgres'
password = 'postgres'
host = '10.5.3.99'
port = '5432'

database_uri = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'
psql_engine = sqlalchemy.create_engine(database_uri)
Definición de funciones 
Psql_2_df: esta función se encarga de hacer una consulta y volverla un dataframe de pandas, para la manipulación y posteriormente la cargar los datos manipulados a la base de datos
def psql_2_df(query):
    df = pd.read_sql_query(sql=text(query), con=psql_engine.connect())
    return df

Df_2_psql: esta función se encarga de escribir los datos de un dataframe en una tabla de postgres, creándola si esta no existe y adicionando los datos si ya existe. Principalmente se la utilizara para actualizar los datos de las tablas fc_dtch y dm_carril

def df_2_psql(df,table_name):
    df.to_sql(table_name, psql_engine, if_exists='append', index=False)
    psql_engine.dispose()

Limpiar_psql:  esta función se la utiliza para finalizar todas las conexiones de backend a la base de datos "postgres", excepto la conexión actual que está ejecutando la consulta

 def limpiar_psql():
    query = "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM  pg_stat_activity WHERE pg_stat_activity.datname = 'postgres' AND pid <> pg_backend_pid()"
    df = psql_2_df(query)
    return  df


Update_dim: esta función se encarga de actualizar la dimensión de la tabla (carril) en caso de que existan carriles no agregados

def update_dim(dim_table_name):
    try:
        query = "select distinct carril, carril_nombre from stn_dtch "\
                " where carril not in (select distinct carril from dm_carril)" 
        df1 = psql_2_df(query)
                
    except:
        query = "select distinct carril, carril_nombre from stn_dtch" 
        df1 = psql_2_df(query)
    df_2_psql(df1, dim_table_name)


Atípicos: esta función se encarga de sacar y analizar los datos atípicos como ausencia de intensidad y velocidad 0 de un dataframe

def atipicos(df1):
    #query_0 = 'SELECT carril, carril_nombre, fecha, intensidad, velocidad FROM stn_dtch '\
    #    +'where carril = ' +"'" +str(carril) + "'"

    #df1= psql_query(query_0)
    df1['fecha_hora'] = df1['fecha'].dt.round('H')

    #int_0 es 1, si hay un registro de intensidad de cero, es decir la cámara registró 0 vehiculos
    df1['intensidad_0'] = (df1['intensidad'] == 0).astype(int) #cuando la cámara registra intensidad como cero, int_0 es uno

    # Función lambda para evaluar si el valor es no real
    es_no_real = lambda x: 1 if pd.isna(x) or x is None else 0
    # Aplicar la función lambda a la columna "velocidad" y asignar el resultado a una nueva columna "es_no_real"
    df1['sin_velocidad'] = df1['velocidad'].apply(es_no_real) #si hay ausencia de velocidad este campo es igual a 1, sino es igual a cero
    df1['registrado'] = 1 #censado es 1 si el registro es tomado por la cámara.


    #crear un dataframe con vacios
    dm_carril = df1[['carril']].drop_duplicates()
    update_dim("dm_carril")
    min_date = df1['fecha_hora'].min()
    max_date = df1['fecha_hora'].max()
    #crear un dataframe que contenga de fechas y horas
    dm_fecha_hora = pd.DataFrame(pd.date_range(start=min_date, end=max_date,  freq="H"), columns =['fecha_hora']) 

    #crear un dataframe que contenga de fechas, horas y carriles
    dfm = dm_carril.merge(dm_fecha_hora,how='cross')

    #se crea un dataframe con registros en 0 y las conlumnas indicadas
    dfm[['intensidad', 'velocidad', 'intensidad_0','sin_velocidad','registrado']] = 0
    df2 = pd.concat([dfm, df1]).groupby(['carril','fecha_hora']).sum().reset_index()
    return df2






Main: 
def main():
    query_0 = "select distinct carril from stn_dtch"
    df_carriles = psql_2_df(query_0)
    
    #seleccionar la ultima fecha de cada carril en la tabla destino
    for carril in df_carriles['carril']:  
    #for carril in ['0302566']:    
        print("carril",str(carril))
        
        query_1 = "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = 'fc_dtch')"
        table_exist = psql_2_df(query_1)['exists'][0]
            
        #revisar si el carril existe en la tabla fc_dtch
        query_2 = "select * FROM fc_dtch where carril = '" +str(carril)+"'"
        carril_exist = len(psql_2_df(query_2)) >= 1
        
        if table_exist == False or carril_exist==False:
            print("table_exist")
            query_3 = 'SELECT carril, fecha, intensidad, velocidad FROM stn_dtch '\
            + 'where carril = ' +"'" +str(carril) + "'"
            df1= psql_2_df(query_3)            
                        
        if carril_exist == True: 
            print("carril_exist")
            query_4 = 'SELECT carril, fecha, intensidad, velocidad FROM stn_dtch '\
                + 'where carril = ' +"'" +str(carril) + "'"\
                + ' and fecha_hora > (select max(fecha_hora) FROM fc_dtch where carril = ' +"'" +str(carril)+"')"
            df1= psql_2_df(query_4)

        if len(df1)>0:            
            print("inicio calculo atipicos")
            dfo = atipicos(df1)
            df_2_psql(dfo,"fc_dtch")
            print("cargados ", len(dfo), " registros del carril: ", str(carril))




HU2
TABLERO DATOS FALTANTES DE INTENCISIDAD

DESCRIPCIÓN: Este tablero muestra una matriz con el registro de ausencia de intensidad, organizado por día, mes y año con la información que proporcionan los dispositivos del proyecto SIMM. Además, proporciona un conteo de los carriles en los cuales se identificó ausencia de datos

 
Filtros:
Año: en la matriz se muestran el total de horas sin registro de intensidad por año en cada día del mes 
Mes: en la matriz se muestran el total de horas sin registro de intensidad por mes en cada día del mes
Regostro: se muestra en la matriz, las horas sin registro de intensidad por cada día  
Carril_nombre: nos filtra la información por nombre de carril, en la matriz aparesera un solo carril con la cantidad de horas no registradas en cada dia


FUENTE DE DATOS: El tablero se alimenta de la base de datos PostgreSQL, que contiene las tablas fc_dtch, dm_carril donde se encuentran todos los datos utilizados
Nombre del servidor: 10.5.3.99
Nombre de la base de datos: postgres
Nombre de usuario: postgres
Contraseña: postgres

