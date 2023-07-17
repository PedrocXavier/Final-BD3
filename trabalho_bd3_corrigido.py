from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import *

yearDf = None

def import_data():
    for i in range(1, 13):
        path = f"files/yellow_tripdata_2022-{i:02}.parquet"
        monthDf = spark.read.parquet(path).withColumn("Month", lit(i))
    
        if yearDf == None:
            yearDf = monthDf
        else:
            yearDf.union(monthDf)

    return yearDf

def clean_data(df: DataFrame):
    pass

def ganhos_mes_milha(df_original: DataFrame):
    df_sum: DataFrame = df_original.where("Trip_distance > 0").groupBy("VendorId", "Month").sum("Total_amount", "Trip_distance")\
        .withColumnRenamed("sum(Total_amount)", "GainsPerVendor")\
        .withColumnRenamed("sum(Trip_distance)", "DistanceTraveled")

    df_sum = df_sum.withColumn(
        "AverageGains", col("GainsPerVendor")/col("DistanceTraveled"))
    
    df_sum.select("VendorId", "Month", "AverageGains").orderBy(
    'VendorId', 'Month', ascending=True).show()


def canceled_trips_month(df_sum: DataFrame, original_df: DataFrame):
    # GroupBy vazio para pegar media geral
    overall_avg = df_sum.groupBy().avg("AverageGains")

    # GroupBy para pegar media anual por VendorId
    df_sum = df_sum.groupBy("VendorId").avg(
        "AverageGains").withColumnRenamed("avg(AverageGains)", "AnualAvgPerVendor")

    # Adicionar coluna de media geral anual
    df_with_avg = df_sum.withColumn("TotalAvg", lit(
        overall_avg.first()[0])).select(['VendorId', 'AnualAvgPerVendor', 'TotalAvg'])

    # Join para manter apenas dados de Vendors que possuem media maior que a media geral
    df_with_avg = original_df.join(df_with_avg.where(
        col('AnualAvgPerVendor') > col('TotalAvg')), "VendorId")

    # Filtrar apenas as viagens canceladas e agrupar por mês
    df_canceled_trips: DataFrame = df_with_avg.filter((df_with_avg.tpep_pickup_datetime == df_with_avg.tpep_dropoff_datetime)
                                                      & (df_with_avg.Trip_distance == 0)
                                                      & (df_with_avg.PULocationID == df_with_avg.DOLocationID))\
        .groupBy('VendorId', 'Month').count().withColumnRenamed('count', 'CanceledTripsPerMonth')

    return df_canceled_trips




#MAIN

sc = SparkContext("local", "Trabalho em Grupo")
spark = SparkSession.builder.getOrCreate()