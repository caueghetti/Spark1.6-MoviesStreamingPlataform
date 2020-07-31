# Spark1.6-MoviesStreamingPlataform
Processamento de Dados através de Spark1.6 (Batch Process)

- Estrutura de diretorios
    - Datasets -> Base CSV utilizada na aplicacao
    - HQLS_Scripts -> Script HQL para criacao de tabelas e arquivo de configuracao hive
    - PySpark_Application -> Scripts python
    - / -> diretorio raiz onde estao as shell de execucao spark e criacao de estrutura hive e HDFS

- Execucao
    1. executar a shell create_hdfs_struct.sh realizara criacao de tabelas hive e estrutura necessaria no HDFS e fazer put do dataset
    2. execucao de aplicacao spark onde o arquivo e lido e tratado inicialmente usando RDD -> spark_submit_RDD.sh
    3. execucao de aplicacao spark onde os dados sao carregados atraves de uma tabela Hive -> spark_submit_HIVE.sh

- Resultados
    - sao gerados 3 arquivos no hdfs onde 2 deles alimentam tabelas externas hive no formato parquet, e 1 no formato texto que esta armazenado HDFS como historico 
    - uma tabela interna hive e alimentada na aplicacao spark_submit_HIVE.sh