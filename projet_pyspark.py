#%%
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, udf, expr
from pyspark.sql.types import FloatType, StructType, StructField, StringType
from pyspark.sql.dataframe import DataFrame
from FlightRadar24.api import FlightRadar24API


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#%%
def get_and_write_flights() -> DataFrame:
    """Get flights from FlightRadar24 and write them to the file"""

    def get_file_name() -> str:
        """Generate file name for the current date and time"""
        
        from datetime import datetime
        now = datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour
        minute = now.minute
        second = now.second
        milisecond = now.microsecond // 10_000
        return f"Flights/rawzone/tech_year={year}/tech_month={year}-{month}/tech_day={year}-{month}-{day}/flights{year}{month}{day}{hour}{minute}{second}{milisecond}.csv"


    fr_api = FlightRadar24API()
    flights = fr_api.get_flights()

    df = spark.createDataFrame(flights)
    df.coalesce(1).write.csv(get_file_name(), mode='overwrite', header=True, sep=';')
    return df

#%%

def clean_dataframe(df: DataFrame) -> DataFrame:
    """Clean dataframe"""

    df = df.filter(~df.destination_airport_iata.isin(["NaN", "N/A"]))
    df = df.filter(~df.origin_airport_iata.isin(["NaN", "N/A"]))
    
    return df

#%%

def add_distance_dataframe(df: DataFrame) -> DataFrame:
    """Add details to dataframe"""
    from math import sin, cos, sqrt, atan2, radians
    def distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Calculate distance between two points"""
        if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
            return -1
        # approximate radius of earth in km
        R = 6373.0

        lat1 = radians(lat1)
        lon1 = radians(lon1)
        lat2 = radians(lat2)
        lon2 = radians(lon2)

        dlon = lon2 - lon1
        dlat = lat2 - lat1

        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))

        distance = R * c
        return distance
    
    distance_udf = udf(distance, FloatType())

    df_airport = spark.read.csv("airports.csv", header=True, sep=',')
    df_airport = df_airport.drop("id", "ident", "type", "name", "elevation_ft", "iso_region", "municipality", "scheduled_service", "gps_code", "local_code", "home_link", "wikipedia_link", "keywords", "iso_country")

    df_airport = df_airport.filter(df_airport.iata_code.isNotNull() & df_airport.continent.isNotNull())

    df_airport = df_airport.withColumn("latitude_deg", df_airport["latitude_deg"].cast("float"))
    df_airport = df_airport.withColumn("longitude_deg", df_airport["longitude_deg"].cast("float"))
    
    df_airport_destination = df_airport.withColumnRenamed("iata_code", "destination_airport_iata")\
                                       .withColumnRenamed("latitude_deg", "destination_latitude_deg")\
                                       .withColumnRenamed("longitude_deg", "destination_longitude_deg")\
                                       .withColumnRenamed("continent", "destination_airport_continent")
    
    df_airport_origin = df_airport.withColumnRenamed("iata_code", "origin_airport_iata")\
                                  .withColumnRenamed("latitude_deg", "origin_latitude_deg")\
                                  .withColumnRenamed("longitude_deg", "origin_longitude_deg")\
                                  .withColumnRenamed("continent", "origin_airport_continent")

    df = df.join(broadcast(df_airport_destination), ["destination_airport_iata"], how='left')
    df = df.join(broadcast(df_airport_origin), ["origin_airport_iata"], how='left')

    df = df.withColumn("distance", distance_udf(df.origin_latitude_deg, df.origin_longitude_deg, df.destination_latitude_deg, df.destination_longitude_deg))
    
    return df


def add_aircrafts_dataframe(df: DataFrame) -> DataFrame:
    """Add aircrafts to dataframe"""
    df_aircrafts = spark.read.csv("planes.dat", header=False, sep=',')
    df_aircrafts = df_aircrafts.drop("_c1")
    df_aircrafts = df_aircrafts.withColumnRenamed("_c0", "aircraft_name")\
                               .withColumnRenamed("_c2", "aircraft_code")
    df_aircrafts = df_aircrafts.filter(df_aircrafts.aircraft_code.isNotNull())
    
    df = df.join(broadcast(df_aircrafts), df.aircraft_code == df_aircrafts.aircraft_code, how='left')
    return df

def add_airlines_dataframe(df: DataFrame) -> DataFrame:
    """Add airlines to dataframe"""
    df_airlines = spark.read.csv("airlines.dat", header=False, sep=',')
    df_airlines = df_airlines.select("_c1","_c3", "_c4")
    df_airlines = df_airlines.withColumnRenamed("_c1", "airline_name")\
                             .withColumnRenamed("_c3", "airline_iata")\
                             .withColumnRenamed("_c4", "airline_icao")
    
    df_airlines = df_airlines.filter(df_airlines.airline_iata.isNotNull() | df_airlines.airline_icao.isNotNull())

    df = df.join(broadcast(df_airlines), [(df.airline_icao == df_airlines.airline_icao) | (df.airline_iata == df_airlines.airline_iata)], how='left')
    df = df.drop("airline_icao").drop("airline_iata")
    
    return df


#%%

def get_active_flights(df: DataFrame) -> DataFrame:
    """Get active flights"""

    df = df.filter(df.on_ground == 0)
    return df

#%%

df = get_and_write_flights()

df = clean_dataframe(df)

df = add_distance_dataframe(df)
df = add_aircrafts_dataframe(df)
df = add_airlines_dataframe(df)

df_active = get_active_flights(df)

#%%

schema = StructType([
    StructField("continent_name", StringType(), True),
    StructField("continent", StringType(), True)
])

data = [("North America", "NA"), 
        ("South America", "SA"), 
        ("Europe", "EU"), 
        ("Asia", "AS"), 
        ("Africa", "AF"), 
        ("Australia", "OC")]
df_continent = broadcast(spark.createDataFrame(data, schema))

#%%

# Q1: La compagnie avec le + de vols en cours

df_active.createOrReplaceTempView("df_active")

df_q1 = spark.sql("""SELECT airline_name, COUNT(airline_name) AS nb_flights 
                                 FROM df_active 
                                 GROUP BY airline_name 
                                 ORDER BY nb_flights DESC 
                                 LIMIT 1""")

#%%

# Q2: Pour chaque continent, la compagnie avec le + de vols régionaux actifs

df_q2 = df_active.groupBy("origin_airport_continent", "destination_airport_continent", "airline_name")\
          .agg({"airline_name": "count"})\
          .orderBy("count(airline_name)", ascending=False)\
          .filter(df_active.origin_airport_continent == df_active.destination_airport_continent)

df_q2 = df_q2.withColumnRenamed("count(airline_name)", "number_of_flights")\
             .drop("origin_airport_continent")\
             .withColumnRenamed("destination_airport_continent", "continent")


df_q2.createOrReplaceTempView("df_q2")


df_q2 = spark.sql("""
    SELECT continent, airline_name, number_of_flights
    FROM (
        SELECT continent, airline_name, number_of_flights,
        ROW_NUMBER() OVER (PARTITION BY continent ORDER BY number_of_flights DESC) AS row_number
        FROM df_q2
    )
    WHERE row_number = 1
""")

df_q2 = df_q2.join(df_continent, df_q2.continent == df_continent.continent, how='left')             
df_q2 = df_q2.drop("continent")

#%%

# Q3: Le vol en cours avec le trajet le plus long

df_q3 = spark.sql("""
    SELECT * FROM df_active
    WHERE distance = (SELECT MAX(distance) FROM df_active)
""")

#%%

# Q4: Pour chaque continent, la longueur de vol moyenne 

df.createOrReplaceTempView("df")

df_q4 = spark.sql("""
    SELECT origin_airport_continent, AVG(distance) AS distance_mean
    FROM df
    WHERE distance > 0
    GROUP BY origin_airport_continent
""")

df_q4 = df_q4.join(df_continent, df_q4.origin_airport_continent == df_continent.continent, how='left')
df_q4 = df_q4.drop("origin_airport_continent", "continent")

#%%

# Q5: L'entreprise constructeur d'avions avec le plus de vols actifs

df_q5 = df_active.groupBy("aircraft_name")\
            .agg({"aircraft_name": "count"})\
            .orderBy("count(aircraft_name)", ascending=False)\
            .limit(1)

df_q5 = df_q5.withColumnRenamed("count(aircraft_name)", "number_of_flights")\
                .withColumnRenamed("aircraft_name", "aircraft")

#%%

# Q6: Pour chaque pays de compagnie aérienne, le top 3 des modèles d'avion en usage

df_q6 = df.groupBy("airline_name", "aircraft_name")\
          .agg({"airline_name": "count"})\
          .orderBy("count(airline_name)", ascending=False)\
          .filter(df.airline_name.isNotNull())\
          .filter(df.aircraft_name.isNotNull())

df_q6 = df_q6.withColumnRenamed("count(airline_name)", "number_of_flights")

df_q6.createOrReplaceTempView("df_q6")

df_q6 = spark.sql("""
    SELECT airline_name, aircraft_name, number_of_flights
    FROM (
        SELECT airline_name, aircraft_name, number_of_flights,
        ROW_NUMBER() OVER (PARTITION BY airline_name ORDER BY number_of_flights DESC) AS row_number
        FROM df_q6
    )
    WHERE row_number <= 3
""")

#%%

# Question Bonus: Quel aéroport a la plus grande différence entre le nombre de vol sortant et le nombre de vols entrants ?

df_qb_1 = spark.sql("""
    SELECT origin_airport_iata, COUNT(id) AS nb_departures
    FROM df
    GROUP BY origin_airport_iata
""")
                    
df_qb_2 = spark.sql("""
    SELECT destination_airport_iata, COUNT(id) AS nb_arrivals
    FROM df
    GROUP BY destination_airport_iata
""")

# Clean des valeurs nuls
df_qb_1 = df_qb_1.filter(df_qb_1.origin_airport_iata.isNotNull())
df_qb_2 = df_qb_2.filter(df_qb_2.destination_airport_iata.isNotNull())

df_qb = df_qb_1.join(df_qb_2, df_qb_1.origin_airport_iata == df_qb_2.destination_airport_iata)


df_qb = df_qb.withColumn("difference", expr("abs(nb_departures - nb_arrivals)"))
df_qb = df_qb.drop("origin_airport_iata", "nb_departures", "nb_arrivals")\
             .withColumnRenamed("destination_airport_iata", "airport_iata")

df_qb.createOrReplaceTempView("df_qb")


df_qb = spark.sql("""
    SELECT * FROM df_qb
    WHERE difference = (SELECT MAX(difference) FROM df_qb)
""")

#%%

# Clean des vues

spark.catalog.dropTempView("df")
spark.catalog.dropTempView("df_active")
spark.catalog.dropTempView("df_q2")
spark.catalog.dropTempView("df_q6")
spark.catalog.dropTempView("df_qb")

#%%
# Affichage Q1
df_q1.show()

#%%
# Affichage Q2
df_q2.show()

#%%
# Affichage Q3
df_q3.show()

#%%
# Affichage Q4
df_q4.show()

#%%
# Affichage Q5
df_q5.show()

#%%
# Affichage Q6
df_q6.show()

#%%
# Affichage QB
df_qb.show()