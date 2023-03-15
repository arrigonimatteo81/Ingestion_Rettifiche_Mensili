import logging
import re
from typing import Any

from pyspark.sql.functions import when, col, regexp_replace, lit
from pyspark.sql.types import StructType, StringType, FloatType, IntegerType

from Classes.BuilderRectificationDefault import BuilderRectificationDefault
from Etl.EtlResponse import EtlResponse
from SparkUtils.spark_utils import create_dataframe, write_data_to_target
from constants import TAB_READVC_RECT


def scomponiRiga(riga: tuple[Any, Any]) -> tuple[int, str, str, int, str, str, str, str, str, str, str, str, str, float]:
    return (int(riga[0][2: 7]), str(riga[0][7: 12]), str(riga[0][12: 36]),
            int(re.search("(PERIODO=([a-zA-Z0-9_.+-])*)", riga[1]).group(1).split("=")[1]),
            re.search("(PTF_SPECCHIO=([a-zA-Z0-9_.€+-])*)", riga[1]).group(1).split("=")[1],
            re.search("(PROVN=([a-zA-Z0-9_.+-])*)", riga[1]).group(1).split("=")[1],
            re.search("(COD_PRODOTTO=([a-zA-Z0-9_.+-])*)", riga[1]).group(1).split("=")[1],
            re.search("(TIPO_OPERAZIONE=([a-zA-Z0-9_.+-])*)", riga[1]).group(1).split("=")[1],
            re.search("(CANALE=([a-zA-Z0-9_.+-])*)", riga[1]).group(1).split("=")[1],
            re.search("(PRODOTTO_COMM=([a-zA-Z0-9_.+-])*)", riga[1]).group(1).split("=")[1],
            re.search("(PORTAFOGLIO=([a-zA-Z0-9_.+-])*)", riga[1]).group(1).split("=")[1],
            re.search("(DESK_RENDICONTATIVO=([a-zA-Z0-9_.+-])*)", riga[1]).group(1).split("=")[1],
            re.search("(CODICE=([a-zA-Z0-9_.+-])*)", riga[1]).group(1).split("=")[1],
            float(re.search("(VALORE=([0-9_.+-])*)", riga[1]).group(1).split("=")[1])
            )


def componiStringa(riga: tuple[int, str, str, int, str, str, str, str, str, str, str, str, str, float], process_id,
                   file_id):
    if riga[7].startswith("__"):
        periodo_comp = riga[7].replace("__", "20")
    else:
        periodo_comp = riga[3]

    return f"('{riga[0]}','{riga[1]}','{riga[2]}',{riga[3]},'{riga[4]}','{riga[5]}','{riga[6]}','{riga[7]}'," \
           f"'{riga[8]}','{riga[9]}','{riga[10]}','{riga[11]}','{riga[12]}',{riga[13]},{periodo_comp}," \
           f"'{process_id}', '{file_id}'"


class BuilderRectificationReadvc(BuilderRectificationDefault):
    table = TAB_READVC_RECT

    def getQueryIngest(self) -> tuple[str, int]:
        # TODO vedere cosa meglio:
        return "", 0
        # TODO oppure
        # pass

    def ingest(self):
        try:
            rows = self.dbSource.returnQueryContent(
                f"SELECT RecInput,DatiOut from REAEX6 where idFile = '{self.id_file}'")
            dig = list(map(lambda l: scomponiRiga(l), rows))
            # codice da tenere commentato, serve solo per fare alcuni test
            # di performance per vedere quale insert sia piu veloce
            # genera la query di insert
            # query = map(lambda l: generaQueryInsert(l), dig)

            schema = StructType() \
                .add("BANCA", IntegerType(), True) \
                .add("COD_UO", StringType(), True) \
                .add("NUM_PARTITA", StringType(), True) \
                .add("PERIODO_RIF", IntegerType(), True) \
                .add("PTF_SPECCHIO", StringType(), True) \
                .add("PROVN", StringType(), True) \
                .add("COD_PRODOTTO", StringType(), True) \
                .add("TIPO_OPERAZIONE", StringType(), True) \
                .add("CANALE", StringType(), True) \
                .add("PRODOTTO_COMM", StringType(), True) \
                .add("PORTAFOGLIO", StringType(), True) \
                .add("DESK_RENDICONTATIVO", StringType(), True) \
                .add("CODICE", StringType(), True) \
                .add("VALORE", FloatType(), True)

            first_df = create_dataframe(dig, schema)

            df_source = first_df.withColumn("PERIODO_COMP", when((col("TIPO_OPERAZIONE").startswith('__')),
                                                                 regexp_replace(col("TIPO_OPERAZIONE"), '__',
                                                                                '20').cast(
                                                                     IntegerType()))
                                            .otherwise((col("PERIODO_RIF")))) \
                .withColumn("ID_PROCESSO", lit(self.etlRequest.processId)) \
                .withColumn("COD_ID_UTENTE_RETT", lit(None).cast(StringType())) \
                .withColumn("COD_ID_FILE_RETT", lit(self.id_file))

            write_data_to_target(df_source=df_source, table=self.ingestion_table)
            logging.info(f"End ingestion table {self.table}")
            return EtlResponse(processId=self.etlRequest.processId, status="OK")

        except Exception as e:
            logging.error(e)
            logging.error(f"Error in ingest rectification while ingesting {self.table}")
            return EtlResponse(processId=self.etlRequest.processId, status="KO", error=e)

    def generaQueryInsert(self, list_value):
        query = f"insert into {self.ingestion_table} (BANCA, COD_UO, NUM_PARTITA, " \
                                                        "PERIODO_RIF, PTF_SPECCHIO, PROVN, COD_PRODOTTO, " \
                                                        "TIPO_OPERAZIONE, CANALE, " \
                                                        "PRODOTTO_COMM, PORTAFOGLIO, DESK_RENDICONTATIVO, CODICE, " \
                                                        "VALORE, PERIODO_COMP, " \
                                                        "ID_PROCESSO, COD_ID_FILE_RETT) values "
        dag = list(map(lambda t: componiStringa(t, self.etlRequest.processId, self.id_file), list_value))
        return query + ",".join(dag)
