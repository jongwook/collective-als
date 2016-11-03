package com.github.jongwook.cmf

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._

case class MovieLensRating(userId: Int, movieId: Int, rating: Float, timestamp: Int)
case class MovieLensMovie(movieId: Int, title: String, genres: String)
case class MovieLensTag(userId: Int, movieId: Int, tag: String, timestamp: Int)
case class MovieLensLink(movieId: Int, imdbId: Int, tmdbId: Int)

case class MovieLensData(ratings: Dataset[MovieLensRating], movies: Dataset[MovieLensMovie], tags: Dataset[MovieLensTag], links: Dataset[MovieLensLink])

object MovieLens {
  def load(path: String)(implicit spark: SparkSession) = {
    import spark.implicits._

    val linkSchema = StructType(Array(
      StructField("movieId", IntegerType),
      StructField("imdbId", IntegerType),
      StructField("tmdbId", IntegerType)
    ))
    val links = spark.read.option("header", "true").schema(linkSchema).csv(s"$path/links.csv").as[MovieLensLink]

    val movieSchema = StructType(Array(
      StructField("movieId", IntegerType),
      StructField("title", StringType),
      StructField("genres", StringType)
    ))
    val movies = spark.read.option("header", "true").schema(movieSchema).csv(s"$path/movies.csv").as[MovieLensMovie]

    val ratingSchema = StructType(Array(
      StructField("userId", IntegerType),
      StructField("movieId", IntegerType),
      StructField("rating", FloatType),
      StructField("timestamp", IntegerType)
    ))
    val ratings = spark.read.option("header", "true").schema(ratingSchema).csv(s"$path/ratings.csv").as[MovieLensRating]

    val tagSchema = StructType(Array(
      StructField("userId", IntegerType),
      StructField("movieId", IntegerType),
      StructField("tag", StringType),
      StructField("timestamp", IntegerType)
    ))
    val tags = spark.read.option("header", "true").schema(tagSchema).csv(s"$path/tags.csv").as[MovieLensTag]

    MovieLensData(ratings, movies, tags, links)
  }
}
