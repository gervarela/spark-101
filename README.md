# Abordando el Big Data con Apache Spark

Este repositorio contiene el código, datasets y ejemplos utilizados en la charla "Abordando el Big Data con Apache Spark" realizada por Gervasio Varela, responsable innovación en Redegal SL, en las jornadas DojoESEI e la Escosa Superior de Exeñería Informática de la Universidade de Vigo, en Ourense.


## Descripción del contenido

 * *Structured API - Basic operations.ipynb*: Notebook con ejemplos introductorios a la API estructurada de Spark.
 * *Structured API - Generate user profile by genre.ipynb*: Notebook con un ejemplo completo de transformación de datasets utilizando la API estructurada.
 * *Stream ratings to Kafka.ipynb*: Programa python que simula un stream continuo de puntuaciones a partir del dataset de ratings.
 * *Streaming - Popular movie genres.ipynb*: Notebook con un ejemplo completo de procesamiento de datos en streaming para generar un dashboard en tiempo real.
 * *Streaming - Popular movies by title.ipynb*: Otro ejemplo de streaming para monitorizar las películas más populares.
 * *MLlib - Movie recommendations.ipynb*: Notebook con un ejemplo sencillo de uso de la MLlib para la generación automática de recomendaciones de películas.
 * *MLlib - User profile segmentation.ipynb*: Notebook con un ejemplo sencillo de uso de la MLlib para la segmentación automática de usuarios a partir de perfiles.


## Ejecución de los ejemplos

Los ejemplos están implementados en lenguage Python utilizando Spark 2.4.x y Jupyter-Notebook.

### Configurar entorno y dependencias

Necesitaremos tener instalado Python 3.x y Apache Spark 2.4.x.

Se asumen que Python ya estará instalado, y no se darán instrucciones.

### Instalación de Apache Spark

1. Descargar la versión "prebuilt with hadoop 2.7 and later" de la web oficioal: https://www.apache.org/dyn/closer.lua/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz

2. Descomprimirlo localmente en el directorio preferido para instalación de paquetes en local, por ejemplo /opt/spark

3. Añadir Spark al path agregando las siguientes dos líneas al fichero ~/.baschrc
```sh
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH
```

### Configuración de entorno virtual Python

Utilizaremos un virtualenv de Python para encapsular las dependencias de ejecución de MAPIT-Analytics

1. Instalar virtualenv a nivel de sistema
```sh
pip install virtualenv
```

2. Crear entorno virtual
```sh
cd src
virtualenv env --python=python3
```

3. Activar el entorno virtual
```sh
source env/bin/activate
```

4. Instalar dependencias
```sh
pip install pymongo pyspark findspark numpy pandas ploty
```

### Ejecución de procesos

Antes de poder ejecutar los procesos es necesario cargar el entorno virtual de Python correspondiente.

```sh
cd src
source env/bin/activate
```

A partir de aquí el entorno virtual está activo y se pueden ejecutar los scripts con los commandos indicados en la siguientes secciones.

### Ejemplos de streaming

Para los ejemplos de streaming será necesarios disponer de una instancia de Apache Kafka operativa, y seguír las instrucciones de este enlace: 

https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html

Antes de ejecutar jupyter-notebook hay que configurar las dependencias de Spark:

```sh
export PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4"
export PYSPARK_SUBMIT_ARGS="--packages ${PACKAGES} pyspark-shell"
```
