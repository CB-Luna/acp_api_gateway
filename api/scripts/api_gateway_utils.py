import pyspark
from supabase import create_client
from pyspark.sql import SQLContext, SparkSession

def init_spark_session(app_name=""):
    return SparkSession.builder.appName(app_name).getOrCreate()

def sqlcontext_spark_session(spark):
    return SQLContext(spark)

def create_supabase_client(api_url, api_key):
    return create_client(api_url, api_key)

# Agrega otras funciones comunes aquí