import pyspark
import requests
import json
import os
from supabase import create_client, Client
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import Row
from pyspark.sql import SQLContext

# Crea una sesión de Spark
spark = SparkSession.builder.appName("EjemploSupabaseJSON").getOrCreate()
# Crea un SQLContext a partir de la sesión de Spark
sqlContext = SQLContext(spark)
# Define el esquema personalizado
schemaUsers = StructType([
    StructField("id", StringType(), False),
    StructField("email", StringType(), False),
    # El valor puede ser Null (True)
    StructField("status", StringType(), True),
    StructField("license_plates", StringType(), True)
])

# Registros existentes
id_existing_records = []

# Define la tabla donde se va a hacer el CRUD de Datos
supabase_table = "users_spark_test"
# Salida
output_api_url = "https://lagiinfnnpqlsydoriqt.supabase.co"
output_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxhZ2lpbmZubnBxbHN5ZG9yaXF0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE2OTc3NDM1MzEsImV4cCI6MjAxMzMxOTUzMX0.3nZOm4gzHdM27FqxUUOSzYl5eBuHQXzCNPof3Fk83po"
supabase_client_out: Client = create_client(output_api_url, output_api_key)

# Función para actualizar un registro en Supabase
def actualizar_registro(row):
    # Define el valor que permite verificar si existe el registro (por ejemplo, el valor de la columna "id")
    registro_id = row["id"]
    # Define los valores que se van a INSERTAR o ACTUALIZAR
    registro_email = row["email"]
    registro_license_plates = row["license_plates"]
    registro_status = row["status"]
    # Realiza una consulta para verificar si el registro existe
    # En este ejemplo, estamos verificando si existe un registro con un valor específico en la columna "id_user_sb"
    condition = registro_id
    queryExistingRow = "id"
    responseExistingRow = supabase_client_out.table(supabase_table).select(queryExistingRow).eq('id_user_sb', condition).execute()
    # Verifica si ya existe el registro de la iteración actual en Supabase
    if responseExistingRow.data:
        # Se realiza el UPDATE del registro
        responseUpdate = supabase_client_out.table(supabase_table).update({"email": registro_email, "license_plates": registro_license_plates, "status": registro_status}).eq("id", responseExistingRow.data[0]["id"]).execute()
        # Verifica la respuesta exitosa y maneja errores si es necesario
        if responseUpdate.data:
            print(f"Registro actualizado: {registro_id}")
        else:
            print(f"Error al actualizar registro {registro_id}: {error}")
    else:
        # Se realiza el CREATE del registro
        responseCreate = supabase_client_out.table(supabase_table).insert({"id_user_sb": registro_id, "email": registro_email, "license_plates": registro_license_plates, "status": registro_status}).execute()
        # Verifica la respuesta exitosa y maneja errores si es necesario
        if responseCreate.data:
            print(f"Registro creado: {registro_id}")
        else:
            print(f"Error al crear registro {registro_id}: {error}")

 #Programa ejemplo de pyspark (Conexión API)
if __name__ == "__main__":
    # Entrada
    # API de Entrada de Supabase
    input_api_url = "https://supa43.rtatel.com/rest/v1/users?select=*"
    # Clave de Entrada de Supabase
    input_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.ewogICAgInJvbGUiOiAiYW5vbiIsCiAgICAiaXNzIjogInN1cGFiYXNlIiwKICAgICJpYXQiOiAxNjg0ODI1MjAwLAogICAgImV4cCI6IDE4NDI2NzgwMDAKfQ.Atj9wTNbdEEVPOjstsO14DtxbY2SEpnr50elVXBgAmM"
    # Configura los Headers con la clave de autenticación
    input_headers = {
        "apikey": f"{input_api_key}"
    }
    # Realiza una solicitud GET para obtener datos de Entrada
    input_response = requests.get(input_api_url, headers=input_headers)

    # Entrada
    input_api_url = "https://supa43.rtatel.com"
    input_api_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.ewogICAgInJvbGUiOiAiYW5vbiIsCiAgICAiaXNzIjogInN1cGFiYXNlIiwKICAgICJpYXQiOiAxNjg0ODI1MjAwLAogICAgImV4cCI6IDE4NDI2NzgwMDAKfQ.Atj9wTNbdEEVPOjstsO14DtxbY2SEpnr50elVXBgAmM"
    supabase_client_in: Client = create_client(input_api_url, input_api_key)

    # <<< EXTRACCIÓN >>>
    # Verifica si la solicitud de Entrada fue exitosa
    if input_response.status_code == 200:
        # Guarda la respuesta JSON como una lista de diccionarios de usuarios
        usersJSON = input_response.json()
        # Convierte cada respuesta JSON en un objeto Row con el esquema definido
        # 'input_response.get("clave", None)' Intenta recuperar el valor con la clave de la primera posición, en caso de que no exista regresa "None" 
        usersRow = [Row(
            id=response.get("id", None), 
            email=response.get("email", None), 
            status=response.get("status", None),
            license_plates=response.get("license_plates", None))
            for response in usersJSON]
        # <<< TRANSFORMACIÓN >>>
        # Crea un DataFrame a partir de la lista de objetos Row y el esquema
        usersDF = sqlContext.createDataFrame(usersRow, schema=schemaUsers)
        randomDF = usersDF.sample(False, 0.5).limit(10)
        # Muestra 10 registros aleatorios del DataFrame
        randomDF.show()
        # <<< CARGA >>>
        # Actualiza, inserta o elimina datos en la tabla de Supabase
        # Realiza una consulta SQL para recuperar los valores de "id" de los registros en Supabase
        queryStoredData = "id_user_sb"
        responseStoredData = supabase_client_out.table(supabase_table).select(queryStoredData).execute()
        # Verifica si hubo un error en la consulta
        if responseStoredData.data:
            # Almacena los valores de "id" en una lista
            id_existing_records = [record["id_user_sb"] for record in responseStoredData.data]
            print(f"Lista recuperada de los id registrados existentes: {id_existing_records}")
        else:
            print(f"No se pudieron recuperar valores {queryStoredData} de la tabla {supabase_table}")
        # Itera a través de las filas del DataFrame y aplica la función de actualización
        rows = usersDF.rdd.map(lambda row: row.asDict()).collect() # NO HACER collet()
        for row in rows:
            actualizar_registro(row)
            if row["id"] in id_existing_records:
                id_existing_records.remove(row["id"])
        print(f"Tamaño de Lista de registros a eliminar: {len(id_existing_records)}")
        if len(id_existing_records) > 0:
            for record in id_existing_records:
                supabase_client_out.table(supabase_table).delete().eq("id_user_sb", record).execute() # Elimina registro
        spark.stop()
    else:
        print("Error al obtener datos de usuarios desde Supabase.")