__author__ = 'matthieu'
from pyspark.sql import HiveContext
import pandas as pd
import uuid

from ..df import DataFrame

def read_csv(sc, file_name, sep=",", storage="hive://", header=True,
             names=None, table_name=None, infer_limit=10000):
    table_name = table_name if table_name is not None else "df" + str(uuid.uuid4())
    hc = HiveContext(sc)
    df = pd.read_csv(file_name, sep=sep, nrows=infer_limit)
    names = df.columns if not names else names
    types = []
    for i in range(len(names)):
        tp = names[i] + " "
        if df.dtypes[i] == "O":
            tp += "STRING"
        elif df.dtypes[i] == "int64":
            tp += "INT"
        else:
            tp += "DOUBLE"
        types.append(tp)
    hc.sql('drop table if exists %s' %table_name)
    qw = """CREATE TABLE IF NOT EXISTS %s (%s) row format delimited fields terminated by '%s'
LINES TERMINATED BY '\n'""" %(table_name, ','.join(types), sep)
    if header:
        qw += " tblproperties ('skip.header.line.count'='1')"
    hc.sql(qw)
    hc.sql("LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE %s" %(file_name, table_name))
    rdd = hc.sql("SELECT * FROM %s" %table_name)
    ctx = hc
    if storage.startswith("parquet://"):
        path = storage.replace("parquet://", "")
        rdd.saveAsParquetFile("%s/%s" %(path, table_name))
        sq = HiveContext(sc)
        rdd = sq.parquetFile("%s/%s" %(path, table_name))
        rdd.registerTempTable(table_name)
        rdd = sq.sql("select * from %s" %table_name)
        ctx = sq
    return DataFrame(ctx, table_name, data=rdd, columns=names, dtype=types)

def read_table(hc, table):
    if table.startswith("hive://"):
        table_name = table.replace("hive://", "")
        return DataFrame(hc, table_name, hc.sql("select * from %s" %table_name))
    elif table.startswith("parquet://"):
        path = table.replace("parquet://", "")
        if path.find("/") >= 0:
            table_name = path.split("/")[-1]
        else:
            table_name = path
        rdd = hc.parquetFile(path)
        rdd.registerTempTable(table_name)
        return DataFrame(hc, table_name, hc.sql("select * from %s" %table_name))
    else:
        raise ValueError("Unsupported format " + table)


