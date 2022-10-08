import pyspark
from pyspark.sql import SparkSession

spark_session = (
    SparkSession.builder.appName("Spark Postgres Read Example")
    .config("spark.driver.extraClassPath", "postgresql-42.5.0.jar")
    .getOrCreate()
)


film_actor_df = (
    spark_session.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/dvdrental")
    .option("dbtable", "film_actor")
    .option("user", "postgres")
    .option("password", "postgrespassword")
    .option("driver", "org.postgresql.Driver")
    .load()
    # .createOrReplaceTempView("spark_film_actor")
)

actor_df = (
    spark_session.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/dvdrental")
    .option("dbtable", "actor")
    .option("user", "postgres")
    .option("password", "postgrespassword")
    .option("driver", "org.postgresql.Driver")
    .load()
    # .createOrReplaceTempView("spark_actor")
)

film_df = (
    spark_session.read.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/dvdrental")
    .option("dbtable", "film")
    .option("user", "postgres")
    .option("password", "postgrespassword")
    .option("driver", "org.postgresql.Driver")
    .load()
    # .createOrReplaceTempView("spark_film")
)

film_film_actor_actor_df = film_df.join(film_actor_df, on="film_id", how="inner").join(
    actor_df,
    on="actor_id",
    how="inner",
)

# film_film_actor_actor_inner_df = film_film_actor_inner_df.join(
#     actor_df, on="actor_id", how="left_outer"
# )

print(
    film_film_actor_actor_df.groupBy(["first_name", "last_name"])
    .agg(
        {"actor_id": "count", "length": "sum"},
    )
    .limit(3)
    .show()
)
