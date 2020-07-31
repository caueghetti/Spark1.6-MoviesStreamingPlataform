#!/bin/bash

echo "CRIANDO ESTRUTURA DE DIRETORIOS NO HDFS"

hdfs_path='/streaming_movies'

hdfs dfs -mkdir -p ${hdfs_path}
if [ $? -ne 0 ]; then
    echo "ERRO AO CRIAR DIRETORIO BASE"
    exit 1
fi

hdfs dfs -mkdir -p ${hdfs_path}/netflix
if [ $? -ne 0 ]; then
    echo "ERRO AO CRIAR DIRETORIO BASE NETFLIX"
    exit 1
fi

hdfs dfs -mkdir -p ${hdfs_path}/prime_video
if [ $? -ne 0 ]; then
    echo "ERRO AO CRIAR DIRETORIO BASE PRIME VIDEO"
    exit 1
fi

hdfs dfs -mkdir -p ${hdfs_path}/multi_plataforma
if [ $? -ne 0 ]; then
    echo "ERRO AO CRIAR DIRETORIO BASE PRIME VIDEO"
    exit 1
fi

hdfs dfs -mkdir -p ${hdfs_path}/base_dir
if [ $? -ne 0 ]; then
    echo "ERRO AO CRIAR DIRETORIO BASE BASE DIR"
    exit 1
fi

for file_ in $(ls Datasets/); 
do
    echo "INSERINDO ARQUIVO ${file_^^}"
    hdfs dfs -put -f Datasets/$file_ ${hdfs_path}/base_dir/
    if [ $? -ne 0 ]; then
        echo "ERRO AO REALIZAR PUT DE ARQUIVO"
        exit 1
    fi
done

echo "CRIANDO TABELAS HIVE"

hive -f HQLS_Scripts/CreateHiveStruct.hql
if [ $? -ne 0 ]; then
    echo "ERRO AO CRIAR TABELAS HIVE"
    exit 1
fi