# Usa una imagen base de Python
FROM python:3.9-slim

# Instalar Java
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get clean

# Configurar la variable de entorno JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/default-java

# # Configura JAVA_HOME
# ENV JAVA_HOME /usr/bin/java

# Establece el directorio de trabajo
WORKDIR /app/

# Copia archivos y directorios desde el contexto de construcción (actual) al sistema de archivos del contenedor /app/.
COPY . .

# Instala las dependencias si es necesario
RUN pip install -r requirements.txt
# RUN pip install flask

# Expone el puerto 8089
EXPOSE 8089

# Comando para ejecutar tu script
CMD ["python", "api_gateway.py"]