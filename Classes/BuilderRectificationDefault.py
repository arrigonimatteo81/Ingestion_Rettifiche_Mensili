import configparser
import logging

from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

from DbUtils.DbConfPostgres import DbConfPostgres
from DbUtils.DbSourceMySql import DbSourceMySql
from Etl import EtlRequest
from Etl.EtlResponse import EtlResponse
from SparkUtils.spark_utils import read_data_from_source, write_data_to_target
from constants import ID_PROCESSO, COD_ID_FILE_RETT, COD_ID_UTENTE_RETT


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
        self.query_ingestion, self.count_elems = self.getQueryIngest()
        self.additional_where = self.dbConf.getAdditionalWhere(self.table)
        self.num_partitions = self.dbConf.getNumPartitions(self.table)
        self.ingestion_table = self.dbConf.getIngestionTable(self.table)

    def ingest(self):
        try:
            logging.info(f"Start ingestion table {self.table}")
            if self.count_elems != 0:
                df_source = read_data_from_source(self.spark_parameters,
                                                  query_ingestion=self.query_ingestion,
                                                  elements_count=self.count_elems,
                                                  num_partitions=self.num_partitions)

                df_source = df_source.withColumn(COD_ID_UTENTE_RETT, lit(None).cast(StringType()))\
                    .withColumn(COD_ID_FILE_RETT, lit(self.id_file))

                write_data_to_target(df_source=df_source, table=self.ingestion_table, process_id=self.etlRequest.processId)
                logging.info(f"End ingestion table {self.table}")
                return EtlResponse(processId=self.etlRequest.processId, status="OK")
            else:
                logging.info(f"End ingestion table {self.table} with NO DATA")
                return EtlResponse(processId=self.etlRequest.processId, status="OK - NO DATA TO PROCESS")
        except Exception as e:
            logging.error(e)
            logging.error(f"Error in ingest rectification while ingesting {self.table}")
            return EtlResponse(processId=self.etlRequest.processId, status="KO", error=e)

    def getQueryIngest(self) -> tuple[str, int]:
        where_cond, count = self.buildWhereConditionFromIdFile

        return self.query_ingestion.format(where_condition=where_cond,
                                           additional_where=self.additional_where), count

    def buildWhereConditionFromIdFile(self) -> tuple[str, int]:
        pass
