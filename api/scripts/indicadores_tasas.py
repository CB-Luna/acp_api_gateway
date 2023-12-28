import pyspark
import requests
import json
import os
from supabase import create_client, Client
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, IntegerType, StringType
from pyspark.sql.functions import col, avg
from pyspark.sql import Row
from pyspark.sql import SQLContext
from datetime import datetime
from dateutil import tz

# Crea una sesión de Spark
spark = SparkSession.builder.appName("IndicadoresTasas").getOrCreate()
# Crea un SQLContext a partir de la sesión de Spark
sqlContext = SQLContext(spark)

# Salida
output_api_url = "https://jjowpbgpiznxequndnbt.supabase.co"
output_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Impqb3dwYmdwaXpueGVxdW5kbmJ0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTgzNDUwMzYsImV4cCI6MjAxMzkyMTAzNn0.c9XF7TqAm2rphYNKyLAIvqWeGr9dHIpBv2Cbf-klBGE"
supabase_client_out: Client = create_client(output_api_url, output_api_key)

def actualizar_registro_todos(id_existing_record, tasa_descuento_minima, tasa_descuento_maxima, tasa_descuento_media, tasa_descuento_moda, tasa_descuento_promedio, id_existing_records, supabase_table):
    """
    Función para actualizar o insertar un registro en Supabase para la tabla "indicadores_tasas_dashboard".

    Args:
        id_existing_record: valor del registro existente que se va a actualizar (si aún no existe en la tabla su valor es 0) (int) 
        tasa_descuento_minima: valor minímo de tasa de descuento con enfoque general (int).
        tasa_descuento_maxima: valor máximo de tasa de descuento con enfoque general (int).
        tasa_descuento_media: valor de media de tasa de descuento con enfoque general (int).
        tasa_descuento_moda: valor de moda de tasa de descuento con enfoque general (int).
        tasa_descuento_promedio: valor promedio de tasa de descuento con enfoque general (float).
        id_existing_records: lista de id records ya existentes
        supabase_table: nombre de la tabla de supabase donde se hace el CRUD de datos
    Returns:
        Null.
    """
    # Realiza una consulta para verificar si el registro existe
    condition = "TODOS"
    # Verifica si ya existe el registro con "id_existing_record" actual en Supabase
    if id_existing_record != 0:
        # Se realiza el UPDATE del registro
        responseUpdate = supabase_client_out.table(supabase_table).update({"tasa_minima": tasa_descuento_minima, "tasa_maxima": tasa_descuento_maxima, "moda": tasa_descuento_moda, "media": tasa_descuento_media, "promedio": tasa_descuento_promedio}).eq("id", id_existing_record).execute()
        # Verifica la respuesta exitosa y maneja errores si es necesario
        if responseUpdate.data:
            # Se quita el valor del "id" del registro, con la condición que exista en la lista "id_existing_records"
            if id_existing_record in id_existing_records:
                id_existing_records.remove(id_existing_record)
            print(f"Registro actualizado: {condition}")
        else:
            print(f"Error al actualizar registro {condition}: {error}")
    else:
        # Se realiza el CREATE del registro
        responseCreate = supabase_client_out.table(supabase_table).insert({"tasa_minima": tasa_descuento_minima, "tasa_maxima": tasa_descuento_maxima, "moda": tasa_descuento_moda, "media": tasa_descuento_media, "promedio": tasa_descuento_promedio, "id_analisis": condition, "cliente": "Todos"}).execute()
        # Verifica la respuesta exitosa y maneja errores si es necesario
        if responseCreate.data:
            print(f"Registro creado: {condition}")
        else:
            print(f"Error al crear registro {condition}: {error}")

# Función principal del Proceso
def main():
    try:
        # Define el esquema personalizado
        schema = StructType([
            StructField("factura_id", IntegerType(), False),
            StructField("cliente_id", IntegerType(), False),
            StructField("tasa_descuento", IntegerType(), False),
            # El valor puede ser Null (True)
            StructField("created_at", StringType(), True)
        ])

        id_existing_records = []
        id_existing_record = 0

        # Define la tabla donde se va a hacer el CRUD de Datos
        supabase_table = "indicadores_tasas_dashboard"
        # Entrada
        # API de Entrada de Supabase
        input_api_url = "https://jjowpbgpiznxequndnbt.supabase.co/rest/v1/graf_td?select=*"
        # Clave de Entrada de Supabase
        input_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Impqb3dwYmdwaXpueGVxdW5kbmJ0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTgzNDUwMzYsImV4cCI6MjAxMzkyMTAzNn0.c9XF7TqAm2rphYNKyLAIvqWeGr9dHIpBv2Cbf-klBGE"
        # Configura los Headers con la clave de autenticación
        input_headers = {
            "apikey": f"{input_api_key}"
        }
        # Realiza una solicitud GET para obtener datos de Entrada
        input_response = requests.get(input_api_url, headers=input_headers)

        # Entrada
        input_api_url = "https://jjowpbgpiznxequndnbt.supabase.co"
        input_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Impqb3dwYmdwaXpueGVxdW5kbmJ0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTgzNDUwMzYsImV4cCI6MjAxMzkyMTAzNn0.c9XF7TqAm2rphYNKyLAIvqWeGr9dHIpBv2Cbf-klBGE"
        supabase_client_in: Client = create_client(input_api_url, input_api_key)

        # <<< EXTRACCIÓN >>>
        # Verifica si la solicitud de Entrada fue exitosa
        if input_response.status_code == 200:
            # Guarda la respuesta JSON como una lista de diccionarios de usuarios
            responseJSON = input_response.json()
            # Convierte cada respuesta JSON en un objeto Row con el esquema definido
            # 'input_response.get("clave", None)' Intenta recuperar el valor con la clave de la primera posición, en caso de que no exista regresa "None" 
            responseRow = [Row(
                facturaId=response.get("factura_id", None), 
                clienteId=response.get("cliente_id", None), 
                tasaDescuento=response.get("tasa_descuento", None),
                fechaCreacion= response.get("created_at", None))
                for response in responseJSON]
            # <<< TRANSFORMACIÓN >>>
            # Crea un DataFrame a partir de la lista de objetos Row y el esquema
            responseDF = sqlContext.createDataFrame(responseRow, schema=schema)
            randomDF = responseDF.sample(False, 0.5).limit(10)
            # Muestra 10 registros aleatorios del DataFrame
            randomDF.show()
            # Verifica si el DataFrame tiene contenido
            if responseDF.isEmpty() == False:
                # <<< CÁLCULOS >>>
                tasa_descuento_minima = responseDF.agg({"tasa_descuento": "min"}).collect()[0][0]
                tasa_descuento_maxima = responseDF.agg({"tasa_descuento": "max"}).collect()[0][0]
                # Agrupa por la columna 'tasa_descuento' y cuenta la frecuencia de cada valor
                moda_result = responseDF.groupBy("tasa_descuento").count()
                # Encuentra el valor con la frecuencia máxima (moda)
                tasa_descuento_moda = moda_result.orderBy(col("count").desc()).first()["tasa_descuento"]
                # Calcula la media de la columna 'tasa_descuento'
                media_result = responseDF.agg(avg("tasa_descuento").alias("media_tasa_descuento"))
                # Recupera el valor de la media
                tasa_descuento_media = media_result.first()["media_tasa_descuento"]
                # Calcula el promedio de la columna 'tasa_descuento'
                promedio_result = responseDF.agg(avg("tasa_descuento").alias("promedio_tasa_descuento"))
                # Recupera el valor del promedio
                tasa_descuento_promedio = promedio_result.first()["promedio_tasa_descuento"]
                # <<< INSERCIÓN >>>
                # Actualiza, inserta o elimina datos en la tabla de Supabase
                # Obtener la fecha actual en el formato correcto
                fecha_actual_inicio = datetime.now(tz.tzutc()).strftime('%Y-%m-%dT00:00:00.%f%z')
                fecha_actual_fin = datetime.now(tz.tzutc()).strftime('%Y-%m-%dT23:59:59.%f%z')
                # Realiza una consulta SQL para recuperar los valores de "id" de los registros en Supabase
                queryStoredData = "id"
                responseStoredData = supabase_client_out.table(supabase_table).select(queryStoredData).eq("id_analisis", "TODOS").gte("created_at", f'{fecha_actual_inicio}').lte("created_at", f'{fecha_actual_fin}').execute()
                # Verifica si hubo un error en la consulta
                if responseStoredData.data:
                    # Almacena los valores de "id" en una lista
                    id_existing_records = [record["id"] for record in responseStoredData.data]
                    print(f"Lista recuperada de los id registrados existentes: {id_existing_records}")
                    id_existing_record = responseStoredData.data[0]["id"]
                else:
                    print(f"No se pudieron recuperar valores {queryStoredData} de la tabla {supabase_table}")
                actualizar_registro_todos(id_existing_record, tasa_descuento_minima, tasa_descuento_maxima, tasa_descuento_media, tasa_descuento_moda, tasa_descuento_promedio, id_existing_records, supabase_table)
                print(f"Tamaño de Lista de registros a eliminar: {len(id_existing_records)}")
                if len(id_existing_records) > 0:
                    for record in id_existing_records:
                        supabase_client_out.table(supabase_table).delete().eq("id", record).execute() # Elimina registro
            else:
                print("El DataFrame no contiene datos.")
            # spark.stop()
            return "Successfull Process", True
        else:
            resultado = "Fail to recover data from Supabase Client"
            return resultado, False
    except Exception as e:
        # Manejo de otras excepciones
        resultado = f"Error: {str(e)}"
        return resultado, False