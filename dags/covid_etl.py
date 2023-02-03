from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("airflow_app") \
    .config('spark.executor.memory', '6g') \
    .config('spark.driver.memory', '6g') \
    .config("spark.driver.maxResultSize", "1048MB") \
    .config("spark.port.maxRetries", "100") \
    .getOrCreate()


def unpivot(dataframe, unpivot_col_name):
    fixed_cols = ['Province/State', 'Country/Region', 'Lat', 'Long']
    renamed_columns = [col if col in fixed_cols else col.replace("/", "_") for col in dataframe.columns]
    dataframe = dataframe.toDF(*renamed_columns)
            
    unpivot_cols = [col for col in dataframe.columns if col not in fixed_cols]
        
    unpivotList = ",".join([f"'{unpivot_col}', {unpivot_col}" 
              for unpivot_col in unpivot_cols])

    n = len(unpivot_cols)

    unpivotExpr = f"stack({n}, {unpivotList}) as (Date, {unpivot_col_name})"

    unpivotDF = dataframe.select(fixed_cols + [f.expr(unpivotExpr)]) \
        .where(f"{unpivot_col_name} is not null")

    unpivotDF = unpivotDF.withColumn("Date", f.to_date(f.col("Date"), "M_d_yy").cast(t.TimestampType()))
    
    del dataframe, unpivotList, unpivotExpr, n

    return unpivotDF

def unacc(dataframe, unacc_col):

    w = Window().partitionBy('Province/State', 'Country/Region', 'Lat', 'Long').orderBy('Date').rowsBetween(-1, 0)

    def minus(lista):
        if len(lista) == 2:
            return lista[1] - lista[0]
        else:
            return lista[0]

    minus_udf = f.udf(minus, t.IntegerType())

    return dataframe.withColumn(unacc_col, minus_udf(f.collect_list(unacc_col).over(w)).cast(t.LongType()))

default_args = {
    'owner': 'Airflow',
    'email': ['your.email@domain.com'],
    'start_date': days_ago(1),
    'email_on_failure' : False
}

with DAG(
    dag_id = 'covid_etl',
    default_args = default_args,
    catchup=False,
    max_active_runs = 1,
    schedule_interval = '@daily',
    tags=['covid']
) as dag:

    # define function
    def raw_to_trusted():
        deaths = spark.read.csv(
            "/home/airflow/datalake/raw/covid19/time_series_covid19_deaths_global.csv",
            sep=',', inferSchema=True, header= True)

        recovered = spark.read.csv(
            "/home/airflow/datalake/raw/covid19/time_series_covid19_recovered_global.csv",
            sep=',', inferSchema=True, header= True)

        confirmed = spark.read.csv(
            "/home/airflow/datalake/raw/covid19/time_series_covid19_confirmed_global.csv",
            sep=',', inferSchema=True, header= True)

        unpivot_deaths = unpivot(
            dataframe= deaths,
            unpivot_col_name= 'quantidade_mortes')

        del deaths

        unpivot_recovered = unpivot(
            dataframe= recovered,
            unpivot_col_name= 'quantidade_recuperados')

        del recovered

        unpivot_confirmed = unpivot(
            dataframe= confirmed,
            unpivot_col_name= 'quantidade_confirmados')

        del confirmed

        unacc_deaths = unacc(
            dataframe= unpivot_deaths,
            unacc_col= 'quantidade_mortes')

        del unpivot_deaths

        unacc_recovered = unacc(
            dataframe= unpivot_recovered,
            unacc_col= 'quantidade_recuperados')

        del unpivot_recovered

        unacc_confirmed = unacc(
            dataframe= unpivot_confirmed,
            unacc_col= 'quantidade_confirmados')

        del unpivot_confirmed

        drcDF = unacc_deaths.na.fill("desconhecido")\
        .join(unacc_recovered.na.fill("desconhecido"),
            ['Province/State', 'Country/Region', 'Lat', 'Long', 'Date'],
            how= "outer")\
        .join(unacc_confirmed.na.fill("desconhecido"),
            ['Province/State', 'Country/Region', 'Lat', 'Long', 'Date'],
            how= "outer")

        del unacc_deaths, unacc_recovered, unacc_confirmed

        drcDF = drcDF.na.fill(value= 0, subset= ["quantidade_mortes", "quantidade_recuperados", "quantidade_confirmados"])

        drcDF = drcDF\
        .withColumn("ano", f.date_format(drcDF.Date, "yyyy").cast(t.IntegerType()))\
        .withColumn("mes", f.date_format(drcDF.Date, "MM").cast(t.IntegerType()))

        drcDF = drcDF.toDF(
            *['estado', 'pais', 'latitude', 'longitude', 'data',
            'quantidade_mortes', 'quantidade_recuperados', 'quantidade_confirmados',
            'ano', 'mes'])

        drcDF = drcDF.select('pais', 'estado', 'latitude', 'longitude', 'data',
            'quantidade_mortes', 'quantidade_recuperados', 'quantidade_confirmados',
            'ano', 'mes')

        fixing_type_col_list = [
            ('pais', t.StringType()),
            ('estado', t.StringType()),
            ('latitude', t.DoubleType()),
            ('longitude', t.DoubleType())]

        for fixing_type_col, col_type in fixing_type_col_list:
            drcDF = drcDF.withColumn(fixing_type_col, f.col(fixing_type_col).cast(col_type))

        drcDF.coalesce(1).write.parquet(
            path= "/home/airflow/datalake/trusted/",
            mode= 'overwrite',
            partitionBy= ["ano", "mes"])

        print(f"No de colunas: {len(drcDF.columns)}")
        print(f"No de linhas: {drcDF.count()}")
        print(f"Colunas: {drcDF.columns}")
        drcDF.printSchema()

        del drcDF

    raw_to_trusted_task = PythonOperator(
        task_id='raw_to_trusted',
        python_callable=raw_to_trusted
    )

    def trusted_to_refined():
        trusted = spark.read.parquet(
            "/home/airflow/datalake/trusted/",
            inferSchema=True, header= True)

        w = Window().partitionBy('estado', 'pais', 'latitude', 'longitude').orderBy('data').rowsBetween(-6, 0)

        trusted = trusted\
        .withColumn('media_movel_mortes', f.avg("quantidade_mortes").over(w).cast(t.DoubleType()))\
        .withColumn('media_movel_recuperados', f.avg("quantidade_recuperados").over(w).cast(t.DoubleType()))\
        .withColumn('media_movel_confirmados', f.avg("quantidade_confirmados").over(w).cast(t.DoubleType()))\
        .select("pais", "data", "media_movel_confirmados", "media_movel_mortes",
            "media_movel_recuperados", "ano")

        fixing_type_col_list = [
            ('pais', t.StringType()),
            ('data', t.TimestampType()),
            ('ano', t.IntegerType())]

        for fixing_type_col, col_type in fixing_type_col_list:
            trusted = trusted.withColumn(fixing_type_col, f.col(fixing_type_col).cast(col_type))

        trusted.coalesce(1).write.parquet(
            path= "/home/airflow/datalake/refined/",
            mode= 'overwrite',
            partitionBy= ["ano"])
    
        print(f"No de colunas: {len(trusted.columns)}")
        print(f"No de linhas: {trusted.count()}")
        print(f"Colunas: {trusted.columns}")
        trusted.printSchema()

    trusted_to_refined_task = PythonOperator(
        task_id='trusted_to_refined',
        python_callable=trusted_to_refined
    )

raw_to_trusted_task >> trusted_to_refined_task