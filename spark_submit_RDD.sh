#!/bin/bash -x

#CHAMADA DO SCRIPT PYSPARK
echo "INICIANDO PROCESSAMENTO SPARK - RDD"
spark-submit \
--master yarn \
--deploy-mode cluster \
--name "STREAMING MOVIES PLATAFORM - RDD" \
--files HQLS_Scripts/hive-site.xml \
--py-files PySpark_Application/methods.py \
PySpark_Application/main.py "RDD"

if [ $? -ne 0 ]; then
    echo "ERRO AO REALIZAR PROCESSAMENTO"
else
    echo "PROCESSAMENTO FINALIZADO"
fi