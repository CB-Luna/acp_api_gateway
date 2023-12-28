import pyspark
import requests
import json
import os
from supabase import create_client, Client
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, avg, sum
from pyspark.sql import Row
from pyspark.sql import SQLContext
from datetime import datetime
from dateutil import tz

# Crea una sesión de Spark
spark = SparkSession.builder.appName("GraficaClientesCondicionPago").getOrCreate()
# Crea un SQLContext a partir de la sesión de Spark
sqlContext = SQLContext(spark)

# Salida
output_api_url = "https://jjowpbgpiznxequndnbt.supabase.co"
output_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Impqb3dwYmdwaXpueGVxdW5kbmJ0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTgzNDUwMzYsImV4cCI6MjAxMzkyMTAzNn0.c9XF7TqAm2rphYNKyLAIvqWeGr9dHIpBv2Cbf-klBGE"
supabase_client_out: Client = create_client(output_api_url, output_api_key)

def actualizar_registro_todos(id_existing_record, key, value, id_existing_records, supabase_table):
    """
    Función para actualizar o insertar un registro en Supabase para la tabla "grafica_clientes_x_condicion_pago".

    Args:
        id_existing_record: valor del registro existente que se va a actualizar (si aún no existe en la tabla su valor es 0) (int)
        key: rango de dias (string).
        value: número de clientes (int).
        conditionStoredData: id del analisis que se está realizando (string)
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
        responseUpdate = supabase_client_out.table(supabase_table).update({"dias":key, "no_clientes": value}).eq("id", id_existing_record).execute()
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
        responseCreate = supabase_client_out.table(supabase_table).insert({"dias":key, "no_clientes": value, "id_analisis": condition, "cliente": "Todos"}).execute()
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
            StructField("anexo_id", IntegerType(), False),
            StructField("cliente_id", IntegerType(), False),
            StructField("comision", FloatType(), False),
            StructField("dias_pago", IntegerType(), False),
            StructField("created_at", StringType(), False)
        ])
        # Define la tabla donde se va a hacer el CRUD de Datos
        supabase_table = "grafica_clientes_x_condicion_pago"
        # Define las listas y variables a ocupar
        id_existing_records = []
        dictionary_existing_records = {"0-30" : 0, "31-45" : 0, "46-89" : 0, "90" : 0, ">90" : 0}
        id_existing_record = 0
        # Entrada
        # API de Entrada de Supabase
        input_api_url = "https://jjowpbgpiznxequndnbt.supabase.co/rest/v1/graf_anexo?select=*"
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
            responseRow = []
            for response in responseJSON:
                anexoId = response.get("anexo_id", None)
                for jsonb_agg_item in response.get("jsonb_agg", []):
                    clienteId = response.get("cliente_id", None)
                    comision = jsonb_agg_item.get("cant_comision", None)
                    dias_pago = jsonb_agg_item.get("dias_pago", None)
                    fechaCreacion = jsonb_agg_item.get("fecha_registro", None)

                    # Se crea el objeto Row y se agrega a la lista
                    row = Row(
                        anexoId=anexoId, 
                        clienteId=clienteId, 
                        comision=comision, 
                        dias_pago=dias_pago, 
                        fechaCreacion=fechaCreacion)
                    responseRow.append(row)
            # <<< TRANSFORMACIÓN >>>
            # Crea un DataFrame a partir de la lista de objetos Row y el esquema
            responseDF = sqlContext.createDataFrame(responseRow, schema=schema)
            randomDF = responseDF.sample(False, 0.5).limit(10)
            # Muestra 10 registros aleatorios del DataFrame
            randomDF.show()
            # <<< CÁLCULOS >>>
            # Filtrar los registros según la condición "dias_pago" mayor a 0 y menor o igual a 30
            filtered0_30DF = responseDF.filter((col("dias_pago") > 0) & (col("dias_pago") <= 30))
            # Obtener clientes únicos
            clientes0_30 = filtered0_30DF.select("cliente_id").distinct().count()
            dictionary_existing_records["0-30"] = clientes0_30
            # Filtrar los registros según la condición "dias_pago" mayor o igual a 31 y menor o igual a 45
            filtered31_45DF = responseDF.filter((col("dias_pago") >= 31) & (col("dias_pago") <= 45))
            # Obtener clientes únicos
            clientes31_45 = filtered31_45DF.select("cliente_id").distinct().count()
            dictionary_existing_records["31-45"] = clientes31_45
            # Filtrar los registros según la condición "dias_pago" mayor o igual a 46 y menor o igual a 89
            filtered46_89DF = responseDF.filter((col("dias_pago") >= 46) & (col("dias_pago") <= 89))
            # Obtener clientes únicos
            clientes46_89 = filtered46_89DF.select("cliente_id").distinct().count()
            dictionary_existing_records["46-89"] = clientes46_89
            # Filtrar los registros según la condición "dias_pago" igual a 90
            filtered_90DF = responseDF.filter((col("dias_pago") == 90))
            # Obtener clientes únicos
            clientes_90 = filtered_90DF.select("cliente_id").distinct().count()
            dictionary_existing_records["90"] = clientes_90
            # Filtrar los registros según la condición "dias_pago" mayor a 90
            filteredMore90DF = responseDF.filter((col("dias_pago") > 90))
            # Obtener clientes únicos
            clientes_More_90 = filteredMore90DF.select("cliente_id").distinct().count()
            dictionary_existing_records[">90"] = clientes_More_90
            # <<< INSERCIÓN >>>
            # Actualiza, inserta o elimina datos en la tabla de Supabase
            # Obtener la fecha actual en el formato correcto
            fecha_actual_inicio = datetime.now(tz.tzutc()).strftime('%Y-%m-%dT00:00:00.%f%z')
            fecha_actual_fin = datetime.now(tz.tzutc()).strftime('%Y-%m-%dT23:59:59.%f%z')
            for key in dictionary_existing_records:
                queryStoredData = "id"
                responseStoredData = supabase_client_out.table(supabase_table).select(queryStoredData).eq("dias", key).eq('id_analisis', "TODOS").gte("created_at", f'{fecha_actual_inicio}').lte("created_at", f'{fecha_actual_fin}').execute()
                # Verifica si hubo un error en la consulta
                if responseStoredData.data:
                    # Almacena los valores de "id" en una lista
                    id_existing_records = [record["id"] for record in responseStoredData.data]
                    print(f"Lista recuperada de los id registrados existentes: {id_existing_records}")
                    id_existing_record = responseStoredData.data[0]["id"]
                else:
                    print(f"No se pudieron recuperar valores {queryStoredData} de la tabla {supabase_table}")
                actualizar_registro_todos(id_existing_record, key, dictionary_existing_records[key], id_existing_records, supabase_table)
                print(f"Tamaño de Lista de registros a eliminar: {len(id_existing_records)}")
                if len(id_existing_records) > 0:
                    for record in id_existing_records:
                        supabase_client_out.table(supabase_table).delete().eq("id", record).execute() # Elimina registro
                id_existing_record = 0
            # spark.stop()
            return "Successfull Process", True
        else:
            resultado = "Fail to recover data from Supabase Client"
            return resultado, False
    except Exception as e:
        # Manejo de otras excepciones
        resultado = f"Error: {str(e)}"
        return resultado, False