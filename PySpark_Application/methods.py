from pyspark import SparkContext
from pyspark.sql import SQLContext,HiveContext
from pyspark.sql.functions import trim,concat_ws

#----------------------------------------------------------------------------------------#
# ESTE MODULO POSSUI AS FUNCOES NECESSARIAS PARA REALIZAR AS TRANFORMACOES E ACOES SPARK #
#----------------------------------------------------------------------------------------#

def SparkContext_():
    try:
        #----------------------------------------------------------------#
        # INICIA O SPARK CONTEXT PERMITINDO INTERAGIR COM RECURSOS SPARK #
        #----------------------------------------------------------------#
        sc = SparkContext.getOrCreate()
        return sc
    except Exception as e:
        print('ERRO AO CRIAR SPARK CONTEXT: {0}'.format(e))
        exit(1)

def SQLContext_(sc):
    try:
        #------------------------------------------------------------------#
        # INICIA  SQLCONTEXT PERMITINDO INTERAGIR ATRAVES DE FUNCOES SPARK #
        #------------------------------------------------------------------#
        spark = SQLContext(sc)
        return spark
    except Exception as e:
        print('ERRO AO CRIAR SQLCONTEXT: {0}'.format(e))
        exit(1)

def HiveContext_(sc):
    try:
        #-------------------------------------------------------#
        # INICIA  HIVECONTEXT PERMITINDO INTERAGIR TABELAS HIVE #
        #-------------------------------------------------------#
        spark = HiveContext(sc)
        return spark
    except Exception as e:
        print('ERRO AO CRIAR HIVE CONTEXT: {0}'.format(e))
        exit(1)

def normalize(df):
    try:
        #-----------------------------#
        # EXIBE A ESTRUTURA CARREGADA #
        #-----------------------------#
        print('ESTRUTURA DO ARQUIVO DO DATAFRAME')
        df.printSchema()
        
        #----------------------------------------------------------#
        # RETIRA OS ESPACOS EXISTENTES NO INICIO E FIM DAS COLUNAS #
        #----------------------------------------------------------#
        columns_=df.columns
        for col_ in columns_:
            df = df.withColumn(col_,trim(df[col_]))

        #-------------------------------------------------------------------------------#
        # REMOVE EVENTUAIS REGISTROS DUPLICADOS                                         #
        # REPARTICIONA EM MEMORIA PARA APENAS 10 PARTICAO VISANDO OTIMIZAR A PERFORMANCE #
        #-------------------------------------------------------------------------------#
        df = df.dropDuplicates().repartition(10)
        return df
    except Exception as e:
        print('ERRO AO NORMALIZAR DF : {0}'.format(e))
        exit(1)

def load_file(File_PATH,File_Columns,sc):
    try:
        #------------------------------------------------------------#
        # REALIZA A CARGA DO ARQUIVO CSV PARA O FORMATO DE DATAFRAME #
        #------------------------------------------------------------#

        #----------------------------------------------------------------#
        # REALIZA A CARGA DO ARQUIVO DE TEXTO UTILIZANDO RDD             #
        # UTILIZA FUNCOES RDD PARA SEPARAR AS COLUNAS E REMOVER O HEADER #
        # TRANSFORMA O CONJUNTO DE ROWS RDD EM UM DATAFRAME              #
        #----------------------------------------------------------------#
        RDD_LINES = sc.textFile(File_PATH)
        HEADER=RDD_LINES.first()
        
        df = RDD_LINES.filter(lambda l: not str(1).startswith(HEADER))\
            .map(lambda line: line.split(';'))\
            .toDF(File_Columns)

        #----------------------------------------#
        # REALIZA NORMALIZACAO INICIAR DOS DADOS #
        #----------------------------------------#
        df = normalize(df)

        return df
    except Exception as e:
        print('ERRO AO CARREGAR ARQUIVO {0}'.format(e))
        exit(1)

def load_hive_table(spark,table_name):
    try:
        #------------------------------------------------------------------------------------------------------------#
        # ATRAVES DO RECURSO HIVE CONTEXT REALIZAMOS UM SELECT EM UMA TABELA HIVE E RETORNAMOS ELA PARA UM DATAFRAME #
        #------------------------------------------------------------------------------------------------------------#

        df = spark.sql('SELECT * FROM {0}'.format(table_name))
        #----------------------------------------#
        # REALIZA NORMALIZACAO INICIAR DOS DADOS #
        #----------------------------------------#
        df = normalize(df)
        return df
    except Exception as e:
        print('ERRO AO CARREGAR DADOS DA TABELA HIVE PARA DATAFRAME : {0}'.format(e))
        exit(1)

def Netflix(df):
    try:
        #-----------------------------------------------------------------------#
        # REALIZA FILTRO DE COLUNAS E LINHAS DEIXANDO SOMENTE FILMES DA NETFLIX #
        #-----------------------------------------------------------------------#
        df_netflix = df.select('ID','TITLE','NETFLIX').filter(df['NETFLIX']=='1')
        return df_netflix
    except Exception as e:
        print('ERRO AO FILTRAR FILMES DA NETFLIX {0}'.format(e))
        exit(1)

def PrimeVideo(df):
    try:
        #---------------------------------------------------------------------------#
        # REALIZA FILTRO DE COLUNAS E LINHAS DEIXANDO SOMENTE FILMES DA PRIME VIDEO #
        #---------------------------------------------------------------------------#
        df_prime = df.select('ID','TITLE','PRIME_VIDEO').filter(df['PRIME_VIDEO']=='1')
        return df_prime
    except Exception as e:
        print('ERRO AO FILTRAR FILMES DA PRIME VIDEO {0}'.format(e))
        exit(1)

def Multiplataforma(df_plataforma_1,df_plataforma_2):
    try:
        #-------------------------------------------------------------------------------------------------------#
        # CRUZA AS BASES DE NETFLIX E PRIME VIDEO COM PARA DESCOBRIR OS FILMES EXISTE EM MAIS DE UMA PLATAFORMA #
        #-------------------------------------------------------------------------------------------------------#
        df_multi = df_plataforma_1.select('ID','TITLE').join(
            df_plataforma_2.select('ID'),
            ['ID'],
            'inner'
        )
        return df_multi
    except Exception as e:
        print('ERRO CRUZAR INFORMACOES DE FILMES EM MAIS DE UMA PLATAFORMA {0}'.format(e))
        exit(1)

def save_parquet(df,path_hdfs):
    try:
        #----------------------------------------------------------------------------------------#
        # O SPARK POR PADRAO AO SALVAR ALGO NO HDFS CRIA 200 PARTICOES                           #
        # ESTAMOS REPARTICIONANDO O DATAFRAME EM APENAS 1 PARTICAO                               #
        # POIS SUA VOLUMETRIA E BAIXA COM ISSO NO MOMENTO DE SUA LEITURA MELHORARA A PERFORMANCE #
        #----------------------------------------------------------------------------------------#
        df = df.repartition(1) 
        #-----------------------------------------------------------#
        # REALIZA O SAVE DO DATAFRAME EM UM ARQUIVO PARQUET NO HDFS #
        #-----------------------------------------------------------#
        df.write.mode('overwrite').format('parquet').save(path_hdfs)
    except Exception as e:
        print('ERRO AO REALIZAR SAVE DE ARQUIVO NO FORMATO PARQUET {0}'.format(e))
        exit(1)

def save_text(df,path_hdfs):
    try:
        #----------------------------------------------------------------------------------------------------------#
        # PARA REALIZARMOS O SAVE EM UM FORMATO DE TEXTO PRECISAMOS TRANSFORMAR O DF EM UM DF DE APENAS UMA COLUNA #
        # COM A FUNCAO CONCAT_WS CONCATENAREMOS TODAS AS COLUNAS DO DATAFRAME SEPARANDO ELAS POR |                 #
        # REALIZAMOS UM SELECT PARA DELIMITARMOS O DATAFRAME A SOMENTE A COLUNA CRIADA                             #
        #----------------------------------------------------------------------------------------------------------#
        columns_ = df.columns
        df_text = df.withColumn('final_layout',concat_ws('|',*columns_)).select('final_layout')
        #--------------------------------------------------#
        # SALVA O DATAFRAME EM UM ARQUIVO DE TEXTO NO HDFS #
        #--------------------------------------------------#
        df_text.write.mode('overwrite').format('text').save(path_hdfs)
    except Exception as e:
        print('ERRO AO REALIZAR SAVE DE ARQUIVO NO FORMATO TEXTO {0}'.format(e))
        exit(1)

def insert_hive_table(df,table_name):
    try:
        #----------------------------------------------------#
        # REALIZA O INSERT INTO DOS DADOS EM UMA TABELA HIVE #
        #----------------------------------------------------#
        df.write.mode("append").insertInto(table_name)
    except Exception as e:
        print('ERRO AO INSERIR DADOS NA TABELA HIVE : {0}'.format(e))
        exit(1)