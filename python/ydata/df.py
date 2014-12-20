__author__ = 'matthieu'
import pandas as pd
from prettytable import PrettyTable
import uuid

QUERIES = {
    "column": {
        "head": "select {column} from {table} limit {n}",
        "all": "select {column} from {table}",
        "unique": "select distinct {column} from {table}",
        "sample": "select {column} from {table} order by random() limit {n}",
        "value_counts": "select count(*) from (select {column} from {table} group by {column}) a"
    },
    "table": {
        "select": "select {columns} from {table}",
        "select_limit": "select {columns} from {table} limit {limit}",
        "head": "select * from {table} limit {n}",
        "all": "select * from {table}",
        "unique": "select distinct {columns} from {table}",
        "sample": "select * from {table} order by rand() limit {n}"
    },
}

def gen_table_name(prefix="tbl_"):
    return prefix + str(uuid.uuid4()).replace("-", "_")

def read_schema_rdd(rows):
   return pd.DataFrame(map(lambda x: x.asDict(), rows))


class Column(object):

    def __init__(self, ctx, table, name, dtype):
        self.ctx = ctx
        self.table = table
        self.name = name
        self.type = dtype

    def __repr__(self):
        return self.head().__repr__()

    def __str__(self):
        return "Column({0})<{1}>".format(self.name, self.__hash__())

    def _repr_html_(self):
        return self.head()._repr_html()

    def head(self, n=6):
        q = QUERIES['column']['head'].format(column=self.name, table=self.table, n=n)
        return read_schema_rdd(self.ctx.sql(q).collect())

    def all(self):
        q = QUERIES['column']['all'].format(column=self.name, table=self.table)
        return self.ctx.sql(q)

    def unique(self):
        q = QUERIES['column']['unique'].format(column=self.name, table=self.table)
        return read_schema_rdd(self.ctx.sql(q).collect())

    def sample(self, n=10):
        q = QUERIES['column']['sample'].format(column=self.name, table=self.table, n=n)
        return self.ctx.sql(q)

    def value_counts(self):
        res = self.all().countByKey()
        serie = pd.Series(index=res.keys(), data=res.values())
        serie.sort(ascending=False)
        return serie

    def cache(self):
        self.ctx.cacheTable(self.name)

    def uncache(self):
        self.ctx.uncacheTable(self.name)

def new_sql_view(ctx, q):
    name = gen_table_name()
    rdd = ctx.sql(q)
    rdd.registerTempTable(name)
    return rdd, name

def dataframe_from_schemardd(ctx, rdd):
    name = gen_table_name()
    rdd.registerTempTable(name)
    return DataFrame(ctx, name, data=rdd)

class DataFrame:
    def __init__(self, ctx, name, data=None, columns=None, dtype=None):
        self.ctx = ctx
        self.name = name
        self.data = data
        self.columns = columns
        self.dtype = dtype

        if data is None:
           self.data = self.ctx.sql(QUERIES['table']['all'].format(table=name))

        fields = map(lambda x: x.jsonValue(), self.data.schema().fields)

        if self.columns is None:
            self.columns = [col["name"] for col in fields]
        cols = [Column(self.ctx, name, col["name"], col["type"]) for col in fields]
        self._columns = cols
        self._columns_indexes = {}
        idx = 0

        for col in cols:
            attr = col.name
            if attr in ("name", "ctx"):
                attr = "_" + col.name
            setattr(self, attr, col)
            self._columns_indexes[col.name] = idx
            idx += 1

    def __repr__(self):
        return self.head().__repr__()

    def __str__(self):
        return "Table({0})<{1}>".format(self.name, self.__hash__())

    def _repr_html_(self):
        return self.head()._repr_html_()

    def select(self, columns=['*'], limit=None):
        if limit is not None:
            q = QUERIES['table']['select_limit'].format(columns=", ".join(columns), table=self.name,
                                                        limit=limit)
        else:
            q = QUERIES['table']['select'].format(columns=", ".join(columns), table=self.name)
        rdd, name = new_sql_view(self.ctx, q)
        return DataFrame(self.ctx, name, rdd)

    def head(self, n=6):
        q = QUERIES['table']['head'].format(table=self.name, n=n)
        return read_schema_rdd(self.ctx.sql(q).collect())

    def all(self):
        q = QUERIES['table']['all'].format(table=self.name)
        rdd, name = new_sql_view(self.ctx, q)
        return DataFrame(self.ctx, name, rdd)

    def unique(self, *args):
        if len(args)==0:
            columns = "*"
        else:
            columns = ", ".join(args)
        q = QUERIES['table']['unique'].format(columns=columns, table=self.name)
        rdd, name = new_sql_view(self.ctx, q)
        return rdd.collect()

    def sample(self, n=10):
        q = QUERIES['table']['sample'].format(table=self.name, n=n)
        rdd, name = new_sql_view(self.ctx, q)
        return DataFrame(self.ctx, name, rdd)

    def __getitem__(self, item):
        if type(item) == str:
            return self._columns[self._columns_indexes[item]]
        elif type(item) == int:
            return self._columns[item]
        elif type(item) == list:
            return self.select(columns=item)

    def schema_difference(self, df):
        schema = set(self.columns)
        schema2 = set(df.columns)
        inter = schema.intersection(schema2)
        delta = schema.difference(schema2)
        in_schema1 = {col for col in schema if col not in schema2}
        in_schema2 = {col for col in schema2 if col not in schema}
        return inter, in_schema1, in_schema2

    def append(self, df):
        inter, s1, s2 = self.schema_difference(df)
        col1 = list(inter) + list(s1) + ["NULL as %s" %col for col in s2]
        col2 = list(inter) + ["NULL as %s" %col for col in s1] + list(s2)
        q = 'select {col1} from {table1} union all select {col2} from {table2}'.format(col1=','.join(col1), table1=self.name,
                                                                                       col2=','.join(col2), table2=df.name)
        print q
        rdd, name = new_sql_view(self.ctx, q)
        return DataFrame(self.ctx, name, rdd)

    def __len__(self):
        return int(self.data.count())

    def cache(self):
        self.ctx.cacheTable(self.name)

    def uncache(self):
        self.ctx.uncacheTable(self.name)

    def to_csv(self, file_name):
        self.data.saveAsTextFile(file_name)

