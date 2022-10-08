from typing import Dict, List, Union
from pyspark.sql import Column, DataFrame, SparkSession


def get_spark_postgres_session() -> SparkSession:
    spark_session = (
        SparkSession.builder.appName("Spark Postgres Read Example")
        .config("spark.driver.extraClassPath", "postgresql-42.5.0.jar")
        .getOrCreate()
    )

    return spark_session


def extract_film_actor_table(spark_session: SparkSession) -> DataFrame:
    film_actor_df = (
        spark_session.read.format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/dvdrental")
        .option("dbtable", "film_actor")
        .option("user", "postgres")
        .option("password", "postgrespassword")
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    return film_actor_df


def extract_actor_df(spark_session: SparkSession) -> DataFrame:
    actor_df = (
        spark_session.read.format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/dvdrental")
        .option("dbtable", "actor")
        .option("user", "postgres")
        .option("password", "postgrespassword")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    return actor_df


def extract_film_table(spark_session: SparkSession) -> DataFrame:
    film_df = (
        spark_session.read.format("jdbc")
        .option("url", "jdbc:postgresql://localhost:5432/dvdrental")
        .option("dbtable", "film")
        .option("user", "postgres")
        .option("password", "postgrespassword")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    return film_df


def transform_join_tables(
    first_df: DataFrame,
    join_parameters: List[
        Union[Dict[str, str], Dict[str, DataFrame], Dict[str, Column]]
    ],
):
    """Joins from left to write, starting with first_df and the each df in the order found
        in join_parameters.

    join_parameters are structured by 3 k,v pairs for each df join:
        "df": DataFrame
        "on": column str or Column, list of column str or list of Columns,
            or join expression using Column
        "how": join type

    Parameters
    ----------
    first_df : DataFrame
    join_parameters : List[Dict[str, str]]
        _description_
    """

    joined_df = first_df
    for df in join_parameters:
        joined_df = joined_df.join(
            df["df"],
            df["on"],
            df["how"],
        )

    return joined_df


def transform_average_movie_length_by_actor(
    film_actor_joined_df: DataFrame,
) -> DataFrame:

    transformed_df = film_actor_joined_df.groupBy(
        ["actor_id", "last_name", "first_name"]
    ).agg({"length": "average"})

    return transformed_df


def transform_movie_count_by_actor(film_actor_joined_df: DataFrame) -> DataFrame:

    transformed_df = film_actor_joined_df.groupBy(
        ["actor_id", "last_name", "first_name"]
    ).agg({"actor_id": "count"})

    return transformed_df


def transform_total_screentime_by_actor(film_actor_joined_df: DataFrame) -> DataFrame:
    transformed_df = film_actor_joined_df.groupBy(
        ["actor_id", "last_name", "first_name"]
    ).agg({"length": "sum"})

    return transformed_df


if __name__ == "__main__":
    spark_session = get_spark_postgres_session()

    film_df = extract_film_table(spark_session)
    film_actor_df = extract_film_actor_table(spark_session)
    actor_df = extract_actor_df(spark_session)

    join_parameters = [
        {
            "df": film_actor_df,
            "on": "film_id",
            "how": "inner",
        },
        {
            "df": actor_df,
            "on": "actor_id",
            "how": "inner",
        },
    ]
    joined_df = transform_join_tables(film_df, join_parameters=join_parameters).sort(
        "last_name"
    )

    movie_count_by_actor = transform_movie_count_by_actor(joined_df).sort("last_name")
    avg_length_by_actor = transform_average_movie_length_by_actor(joined_df).sort(
        "last_name"
    )
    total_screentime_by_actor = transform_total_screentime_by_actor(joined_df).sort(
        "last_name"
    )

    join_parameters = [
        {
            "df": avg_length_by_actor.drop("last_name", "first_name"),
            "on": "actor_id",
            "how": "inner",
        },
        {
            "df": total_screentime_by_actor.drop("last_name", "first_name"),
            "on": "actor_id",
            "how": "inner",
        },
    ]
    final_df = transform_join_tables(movie_count_by_actor, join_parameters).drop(
        "actor_id"
    )

    print("--JOINED DF (FILM, FILM_ACTOR, ACTOR)--")
    joined_df.select("last_name", "first_name", "title").show()

    print("--MOVIE COUNT--")
    movie_count_by_actor.show()

    print("--AVERAGE MOVIE LENGTH--")
    avg_length_by_actor.show()

    print("--TOTAL SCREEN TIME--")
    total_screentime_by_actor.show()

    print("--FINAL TABLE--")
    final_df.show()
