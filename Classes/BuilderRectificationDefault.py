import configparser
import logging

from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

from DbUtils.DbConfPostgres import DbConfPostgres
from Etl import EtlRequest
from SparkUtils.spark_utils import read_data_from_source, write_data_to_target
from DbUtils.DbSourceMySql import DbSourceMySql


class BuilderRectificationDefault:
    table = ""


    def __init__(self, etl_request: EtlRequest):

        self.etlRequest = etl_request
        self.banca = self.etlRequest.semaforo.abi
        self.provenienza = self.etlRequest.semaforo.provenienza
        self.id_file = self.etlRequest.semaforo.idFile
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.dbConf = DbConfPostgres(config)
        self.dbSource = DbSourceMySql(self.dbConf.getSourceParameters(self.table))
        self.spark_parameters = self.dbConf.getSparkParameters(self.table)
        self.query_ingestion, self.count_key = self.getQueryIngest
        self.additional_where = self.dbConf.getAdditionalWhere(self.table)
        self.num_partitions = self.dbConf.getNumPartitions(self.table)
        self.ingestion_table = self.dbConf.getIngestionTable(self.table)

    def ingest(self):
        try:
            logging.info(f"Start ingestion table {self.table}")
            df_source = read_data_from_source(self.spark_parameters,
                                              query_ingestion=self.query_ingestion,
                                              elements_count=self.count_key,
                                              num_partitions=self.num_partitions)
            # aggiungo le colonne id_processo, cod_id_utente_rett(None) e cod_id_file_rett al dataframe
            df_source = df_source.withColumn("id_processo", lit(self.etlRequest.processId))
            df_source = df_source.withColumn("cod_id_utente_rett", lit(None).cast(StringType()))
            df_source = df_source.withColumn("cod_id_file_rett", lit(self.id_file))
            write_data_to_target(df_source=df_source, table=self.ingestion_table)
            logging.info(f"End ingestion table {self.table}")
            return df_source
        except Exception as e:
            logging.error(e)
            logging.error(f"Error in ingest while ingesting {self.table}")
            raise e

    def getQueryIngest(self):
        where_cond, count = self.buildWhereConditionFromIdFile

        return self.query_ingestion.format(where_condition=where_cond,
                                           additional_where=self.additional_where), count

    def buildWhereConditionFromIdFile(self):
        pass
