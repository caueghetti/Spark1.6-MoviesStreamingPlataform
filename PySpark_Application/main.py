from methods import *
from sys import argv

#-------------------------------------------------------------------------------------------------------#
# NESTE PROCESSAMENTO SERAO CRIADAS DIVERSAS VISOES ONDE ONTEM SERAO SALVAS NO HDFS COMO NOVOS ARQUIVOS #
#-------------------------------------------------------------------------------------------------------#

def main_(base_info):
    #------------------------------------------------------------------------------------#
    # INICIA SPARK CONTEXT NECESSARIO PARA INTERAGIR COM TABELAS HIVE E ARQUIVOS NO HDFS #
    #------------------------------------------------------------------------------------#
    sc = SparkContext_()
    
    #-----------------------------------------------------------#
    # SELECIONA MODELO DE CARGA DE ACORDO COM PARAMETRO PASSADO #
    #-----------------------------------------------------------#
    if str(argv[1]).upper().strip() == 'RDD':
        #---------------------------------------#
        # PARTICIONA OS ARQUIVOS ATRAVES DE RDD #
        #---------------------------------------#
        spark = SQLContext_(sc)
        df = load_file(
            '{0}/base_dir'.format(base_info['FILE_PATH']),
            base_info['FILE_COLUMNS'],
            sc
        )
    elif str(argv[1]).upper().strip() == 'HIVE':
        #-------------------------------------------#
        # CARREGA O ARQUIVO ATRAVES DE TABELAS HIVE #
        #-------------------------------------------#
        spark = HiveContext_(sc)
        df = load_hive_table(spark,'{0}.{1}'.format(base_info['DATABASE_HIVE'],base_info['HIVE_EXTERNAL_TABLE']))
    else:
        print('MODO DE CARGA NAO IDENTIFICADO')

    #----------------------------------------------------------------------------#
    # FIXA O DATAFRAME EM MEMORIA PARA OTIMIZAR A PERFORMACE NO MOMENTO DE ACOES #
    # O DADO SO E FIXADO EM MEMORIA NO MOMENTO DE SUA PRIMEIRA ACAO              #
    # OTIMIZANDO ASSIM EM MULTIPLAS CHAMADAS PARA AQUELE DADO                    #
    #----------------------------------------------------------------------------#
    df = df.cache()

    #-----------------------------------------------------------------------#
    # CRIA UM DATAFRAME SOMENTE COM OS TITULOS EXISTENTES NA NETFLIX        #
    # ATRAVES DO COMANDO SHOW EXIBE OS 10 PRIMEIROS RESULTADOS              #
    # ATRAVES DO COMANDO COUNT EXIBE A QUANTIDADE DE REGISTROS NO DARAFRAME #
    #-----------------------------------------------------------------------#
    df_netflix = Netflix(df)
    df_netflix.show(10,False)
    print('QUANTIDADE DE FILMES EXISTENTES NA NETFLIX : {0}'.format(df_netflix.count()))

    #-----------------------------------------------------------------------#
    # CRIA UM DATAFRAME SOMENTE COM OS TITULOS EXISTENTES NA PRIME VIDEO    #
    # ATRAVES DO COMANDO SHOW EXIBE OS 10 PRIMEIROS RESULTADOS              #
    # ATRAVES DO COMANDO COUNT EXIBE A QUANTIDADE DE REGISTROS NO DARAFRAME #
    #-----------------------------------------------------------------------#
    df_prime = PrimeVideo(df)
    df_prime.show(10,False)
    print('QUANTIDADE DE FILMES EXISTENTES NA PRIME VIDEO : {0}'.format(df_prime.count()))

    #------------------------------------------------------------------------------#
    # CRIA UM DATAFRAME SOMENTE COM OS TITULOS EXISTENTES NA NETFLIX E PRIME VIDEO #
    # ATRAVES DO COMANDO SHOW EXIBE OS 10 PRIMEIROS RESULTADOS                     #
    # ATRAVES DO COMANDO COUNT EXIBE A QUANTIDADE DE REGISTROS NO DARAFRAME        #
    #------------------------------------------------------------------------------#
    df_multi = Multiplataforma(df_netflix,df_prime)
    df_multi.show(10,False)
    print('QUANTIDADE DE FILMES EXISTENTES NA NETFLIX E NA PRIME VIDEO : {0}'.format(df_multi.count()))

    #----------------------------------------------------------------------------#
    # REALIZA O SAVE DOS ARQUIVOS SENDO                                          #
    # BASE NETFLIX - FORMATO PARQUET (LOCATION DE UMA EXTERNAL TABLE - HIVE)     #
    # BASE PRIME VIDEO - FORMATO PARQUET (LOCATION DE UMA EXTERNAL TABLE - HIVE) #
    # BASE MULTI EM DOIS FORMATOS, TEXTO E UMA INTERNAL TABLE NO HIVE            #
    #----------------------------------------------------------------------------#
    save_parquet(df_netflix,'{0}/netflix'.format(base_info['FILE_PATH']))
    save_parquet(df_prime,'{0}/prime_video'.format(base_info['FILE_PATH']))

    if str(argv[1]).upper().strip() == 'RDD':
        #---------------------------------------#
        # SALVA O DATAFRAME EM ARQUIVO DE TEXTO #
        #---------------------------------------#
        save_text(df_multi,'{0}/multi_plataforma'.format(base_info['FILE_PATH']))
    elif str(argv[1]).upper().strip() == 'HIVE':
        #--------------------------------------#
        # SALVA O DATAFRAME EM UMA TABELA HIVE #
        #--------------------------------------#
        insert_hive_table(df_multi,'{0}.{1}'.format(base_info['DATABASE_HIVE'],base_info['HIVE_INTERNAL_TABLE']))
    else:
        print('MODO DE SALVE NAO IDENTIFICADO')


if __name__ == "__main__":
    #---------------------------------------#
    # INFORMACAO REFERENTE AO PROCESSAMENTO #
    #---------------------------------------#

    info_={
        'FILE_PATH':'/streaming_movies',
        'FILE_COLUMNS':[
            'ROW_NUMBER_INDEX','ID','TITLE','DT_YEAR','AGE','IMDB',
            'ROTTEN_TOMATOES','NETFLIX','HULU','PRIME_VIDEO',
            'DISNEY_PLUS','TYPE','DIRECTORS','GENRES','COUNTRY','LANGUAGE','RUNTIME'
        ],
        'DATABASE_HIVE':"movies",
        'HIVE_EXTERNAL_TABLE':"ext_movies_streaming",
        'HIVE_INTERNAL_TABLE':"movies_multiplataforma",
        'HDFS_PATH':"/streaming_movies"
    }
    
    print('METODO DE LOAD/SAVE : {0}'.format(argv[1]))

    #-------------------------------------#
    #             METODO MAIN             #
    #-------------------------------------#
    main_(info_)