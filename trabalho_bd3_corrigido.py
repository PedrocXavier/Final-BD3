from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import *

def import_data():
    yearDf = None
    for i in range(1, 13):
        path = f"files/yellow_tripdata_2022-{i:02}.parquet"
        monthDf = spark.read.parquet(path).withColumn("Month", lit(i))
    
        if yearDf == None:
            yearDf = monthDf
        else:
            yearDf = yearDf.union(monthDf)

    return yearDf


def clean_data(df: DataFrame):
    # Filtrando viagens com método de pagamento inválido
    df = df.where("Payment_type > 0").where("Payment_type <= 6")

    # Atribuir 0 ao Total_ammount se o tipo de pagamento for igual a 3(no charge)
    df = df.withColumn("Total_amount", when(
        col("Payment_type") == 3, lit(0)).otherwise(col("Total_amount")))
    
    # Filtrando as corridas em que o ponto de partida for menor que o de chegada (Ponto de partida antes do de chegada)
    df = df.where("tpep_pickup_datetime <= tpep_dropoff_datetime")

    # Removendo viagens com custo menor que 0
    df = df.where("Total_amount >= 0.0")

    # Removendo viagens com distância menor que 0
    df = df.where("Trip_distance >= 0.0")

    return df

def earnings_vendor_month(df: DataFrame):

    # Filtra pela distância percorrida maior que 0, agrupa por mês e agência e faz a soma do Total_amount e da Trip_distance
    df_result = df.where("Trip_distance > 0")
    df_result = df_result.groupBy("VendorId", "Month")
    df_result = df_result.sum("Total_amount", "Trip_distance")\
        .withColumnRenamed("sum(Total_amount)", "EarningsVendor")\
        .withColumnRenamed("sum(Trip_distance)", "TotalDistance")
    
    # Adiciona a coluna de média entre EarningsVendor e TotalDistance
    df_result = df_result.withColumn(
        "AverageEarnings", col("EarningsVendor")/col("TotalDistance"))

    return df_result

def canceled_trips_month(df_earns: DataFrame, df: DataFrame):
    # Calcular média geral
    total_avg = df_earns.groupBy().avg("AverageEarnings")

    # Calcular média anual por VendorId
    df_earns = df_earns.groupBy("VendorId").avg(
        "AverageEarnings").withColumnRenamed("avg(AverageEarnings)", "AnualAvg")

    # Calcular média geral anual e adicionar coluna
    df_total_avg = df_earns.withColumn("TotalAvg", lit(
        total_avg.first()[0])).select(['VendorId', 'AnualAvg', 'TotalAvg'])

    # Excluindo vendors que possuem a média menor que a média geral
    df_total_avg = df.join(df_total_avg.where(
        col('AnualAvg') > col('TotalAvg')), "VendorId")

    # Remover viagens não canceladas e fazer agrupamento por mês
    df_canceled: DataFrame = df_total_avg.filter((df_total_avg.tpep_pickup_datetime == df_total_avg.tpep_dropoff_datetime)
                                                      & (df_total_avg.trip_distance == 0)
                                                      & (df_total_avg.PULocationID == df_total_avg.DOLocationID))\
        .groupBy('VendorId', 'Month').count().withColumnRenamed('count', 'CanceledTrips')

    return df_canceled


def top_ten_month(df: DataFrame):

    # Faz o agrupamento pelo VendorID e pelo Mês, somando as distâncias e o total ganho
    df_top_dist = df.groupBy("VendorId", "Month").sum("Total_amount", "Trip_distance")\
        .withColumnRenamed("sum(Total_amount)", "EarningsVendor")\
        .withColumnRenamed("sum(Trip_distance)", "TotalDistance")

    # Adicona a coluna AverageEarnings calculando a média entre EarningsVendor e TotalDistance, ordenando em ordem crescente
    # pelo mês, e decrescente pela média

    df_top_earn = df_top_dist.withColumn("AverageEarnings", col(
        "EarningsVendor")/col("TotalDistance")).orderBy(asc("Month"), desc("AverageEarnings"))

    return df_top_earn



#=================================== PROGRAMA PRINCIPAL ===================================

sc = SparkContext("local", "Trabalho em Grupo")
spark = SparkSession.builder.getOrCreate()

df = import_data()
df = clean_data(df)


# A: Ganho médio por milha por mês por VendorId

print("\nGanho médio por milha por mês:\n")

df_earns = earnings_vendor_month(df)

df_earns.select("VendorId", "Month", "AverageEarnings").orderBy(
    'VendorId', 'Month', ascending=True).show()


# B: Taxa de corridas canceladas por mês para os taxistas que possuem o ganho médio por milha superior a média geral
 
print("\nTaxa de corridas canceladas por mês:\n")

df_canceled_trips = canceled_trips_month(df_earns, df)

df_canceled_trips.show()


# C: Ganho médio dos top 10 taxistas que mais rodaram por mes no ano de 2022

print("\nGanho médio dos top 10 por mes:\n")

df_top_10_month = top_ten_month(df)

df_top_10_month.select("VendorId", "Month", "AverageEarnings").show()



#Reduzir 2% nos valores de viagens pagas com cartão de crédito e refazer as consultas anteriores

print("=============================================================================================\n")
print("\nConsultas considerando uma taxa de 2% no valor de viagens pagas com cartão de crédito:\n")
print("=============================================================================================\n")

df_with_tax = df.withColumn("Total_amount", when(col("Payment_type") == 1, round(col(
    "Total_amount") * 0.98, 2)).otherwise(col("Total_amount")))


# A: Ganho médio por milha por mês por VendorId

print("\nGanho médio por milha por mês:\n")

df_earns2 = earnings_vendor_month(df_with_tax)

df_earns2.select("VendorId", "Month", "AverageEarnings").orderBy(
    'VendorId', 'Month', ascending=True).show()


# B: Taxa de corridas canceladas por mês para os taxistas que possuem o ganho médio por milha superior a média geral

print("\nTaxa de corridas canceladas por mês:\n")

df_canceled_trips2 = canceled_trips_month(df_earns2, df_with_tax)

df_canceled_trips2.show()


# C: Ganho médio dos top 10 taxistas que mais rodaram por mes no ano de 2022

print("\nGanho médio dos top 10 por mes:\n")

df_top_10_month2 = top_ten_month(df_with_tax)

df_top_10_month2.select("VendorId", "Month", "AverageEarnings").show()
