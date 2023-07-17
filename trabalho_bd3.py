from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

sc = SparkContext("local", "Trabalho em Grupo")
spark = SparkSession.builder.getOrCreate()

df = []
yearDf = None

for i in range(1, 13):
    path = f"files/yellow_tripdata_2022-{i:02}.parquet"
    monthDf = spark.read.parquet(path)

    viewName = f"CorridaTaxi_{i:02}"

    monthDf.createOrReplaceTempView(viewName)

    df.append(monthDf)
    if yearDf == None:
        yearDf = monthDf
    else:
        yearDf.union(monthDf)

yearDf.createOrReplaceTempView("CorridaTaxi")

for month in range(1, len(df) + 1):
    viewName = f"CorridaTaxi_{month:02}"

    # Calcula o ganho médio por milha por agência.
    print(f"Ganho médio por milha no mês {month:02} por agência.")
    result = spark.sql(f"""
                       SELECT VendorID, (SUM(total_amount / trip_distance) / COUNT(1)) AS MEDIA 
                       FROM {viewName} 
                       WHERE payment_type != 6 AND trip_distance > 0
                       GROUP BY VendorID""")
    print(result)

# Calcula a média geral
media_geral = spark.sql("""
                        SELECT (SUM(total_amount / trip_distance) / COUNT(1)) AS media 
                        FROM CorridaTaxi
                        WHERE payment_type != 6 AND trip_distance > 0""").first().media
print("Média geral")
print(media_geral)

# Calcula a taxa de corrida canceladas por mês
for month in range(1, len(df) + 1):
    viewName = f"CorridaTaxi_{month:02}"

    # Calcula os vendors que estão acima da média
    result = spark.sql(f"""
            SELECT 
                main.VendorID,
                (SUM(main.total_amount / main.trip_distance) / COUNT(1)) AS MEDIA
            FROM {viewName} main
            WHERE payment_type <> 6 AND trip_distance > 0
            GROUP BY main.VendorID
            HAVING MEDIA >= {media_geral}""")

    vendors = result.collect()

    for vendor in vendors:
        print(f"A agência {vendor.VendorID} está acima da média anual.")

    # Para cada vendor calcula a taxa de corridas canceladas
    for vendor in vendors:
        voided_trips = spark.sql(f"""
            SELECT COUNT(1) as cnt FROM {viewName} WHERE VendorID = {vendor.VendorID} AND payment_type = 6
        """)
        finished_trips = spark.sql(f"""
            SELECT COUNT(1) as cnt FROM {viewName} WHERE VendorID = {vendor.VendorID} AND payment_type <> 6
        """)

        print(f"A taxa de corrida canceladas no mês {month:02} para a agência {vendor.VendorID} é {voided_trips.first().cnt / finished_trips.first().cnt}")


# Calcula o ganho médio para os top-10 agências por mês
for month in range(1, len(df) + 1):
    viewName = f"CorridaTaxi_{month:02}"

    # Carrega os vendors
    result = spark.sql(f"""
        SELECT VendorID, SUM(trip_distance) as total_distance FROM {viewName} GROUP BY VendorID ORDER BY total_distance DESC LIMIT 10
    """)

    vendors = result.collect()

    for vendor in vendors:
        print(f"A agência {vendor.VendorID} está no top 10 que mais rodaram no mês.")

    # Para cada vendor calcula o ganho médio
    for vendor in vendors:
        result = spark.sql(f"""
            SELECT VendorID, (SUM(total_amount) / COUNT(1)) AS MEDIA 
            FROM {viewName} 
            WHERE payment_type != 6 AND trip_distance > 0 AND VendorID = {vendor.VendorID}
            GROUP BY VendorID""")
        print(f"Ganho médio no mês {month:02} da agência {vendor.VendorID}.")
        print(result)

## =============== TAXA INCIDENTE DE 2% SOBRE CARTÃO DE CRÉDITO =================== ##

yearDf = None

for month in range(1, len(df) + 1):
    viewName = f"CorridaTaxi_{month:02}_tax"

    monthDf = df[month - 1]

    monthDf.withColumn("total_amount", when(monthDf.payment_type == 1, monthDf.total_amount * 1.02).otherwise(monthDf.total_amount))
    monthDf.createOrReplaceTempView(viewName)

    if yearDf == None:
        yearDf = monthDf
    else:
        yearDf.union(monthDf)

yearDf.createOrReplaceTempView("CorridaTaxi_tax")

for month in range(1, len(df) + 1):
    viewName = f"CorridaTaxi_{month:02}_tax"

    # Calcula o ganho médio por milha por agência.
    print(f"Ganho médio por milha no mês {month:02} por agência.")
    result = spark.sql(f"""
                       SELECT VendorID, (SUM(total_amount / trip_distance) / COUNT(1)) AS MEDIA 
                       FROM {viewName} 
                       WHERE payment_type != 6 AND trip_distance > 0
                       GROUP BY VendorID""")
    print(result)

# Calcula a média geral
media_geral = spark.sql("""
                        SELECT (SUM(total_amount / trip_distance) / COUNT(1)) AS media 
                        FROM CorridaTaxi_tax
                        WHERE payment_type != 6 AND trip_distance > 0""").first().media
print("Média geral")
print(media_geral)

# Calcula a taxa de corrida canceladas por mês
for month in range(1, len(df) + 1):
    viewName = f"CorridaTaxi_{month:02}_tax"

    # Calcula os vendors que estão acima da média
    result = spark.sql(f"""
            SELECT 
                main.VendorID,
                (SUM(main.total_amount / main.trip_distance) / COUNT(1)) AS MEDIA
            FROM {viewName} main
            WHERE payment_type <> 6 AND trip_distance > 0
            GROUP BY main.VendorID
            HAVING MEDIA >= {media_geral}""")

    vendors = result.collect()

    for vendor in vendors:
        print(f"A agência {vendor.VendorID} está acima da média anual.")

    # Para cada vendor calcula a taxa de corridas canceladas
    for vendor in vendors:
        voided_trips = spark.sql(f"""
            SELECT COUNT(1) as cnt FROM {viewName} WHERE VendorID = {vendor.VendorID} AND payment_type = 6
        """)
        finished_trips = spark.sql(f"""
            SELECT COUNT(1) as cnt FROM {viewName} WHERE VendorID = {vendor.VendorID} AND payment_type <> 6
        """)

        print(f"A taxa de corrida canceladas no mês {month:02} para a agência {vendor.VendorID} é {voided_trips.first().cnt / finished_trips.first().cnt}")


# Calcula o ganho médio para os top-10 agências por mês
for month in range(1, len(df) + 1):
    viewName = f"CorridaTaxi_{month:02}_tax"

    # Carrega os vendors
    result = spark.sql(f"""
        SELECT VendorID, SUM(trip_distance) as total_distance FROM {viewName} GROUP BY VendorID ORDER BY total_distance DESC LIMIT 10
    """)

    vendors = result.collect()

    for vendor in vendors:
        print(f"A agência {vendor.VendorID} está no top 10 que mais rodaram no mês.")

    # Para cada vendor calcula o ganho médio
    for vendor in vendors:
        result = spark.sql(f"""
            SELECT VendorID, (SUM(total_amount) / COUNT(1)) AS MEDIA 
            FROM {viewName} 
            WHERE payment_type != 6 AND trip_distance > 0 AND VendorID = {vendor.VendorID}
            GROUP BY VendorID""")
        print(f"Ganho médio no mês {month:02} da agência {vendor.VendorID}.")
        print(result)