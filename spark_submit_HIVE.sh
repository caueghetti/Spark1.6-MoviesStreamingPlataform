#!/bin/bash -x

#CHAMADA DO SCRIPT PYSPARK
echo "INICIANDO PROCESSAMENTO SPARK - HIVE"
spark-submit \
--master yarn \
--deploy-mode cluster \
--name "STREAMING MOVIES PLATAFORM - HIVE" \
--files HQLS_Scripts/hive-site.xml \
--py-files PySpark_Application/methods.py \
PySpark_Application/main.py "HIVE"

if [ $? -ne 0 ]; then
    echo "ERRO AO REALIZAR PROCESSAMENTO"
else
    echo "PROCESSAMENTO FINALIZADO"
fi