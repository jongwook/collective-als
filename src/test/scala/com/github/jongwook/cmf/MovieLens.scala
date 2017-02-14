package com.github.jongwook.cmf

import com.kakao.cuesheet.convert.Implicits
import com.kakao.mango.text.Resource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.csv.{CSVOptions, CSVRelation}
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

case class MovieLensRating(userId: Int, movieId: Int, rating: Float, timestamp: Int)
case class MovieLensMovie(movieId: Int, title: String, genres: String)
case class MovieLensTag(userId: Int, movieId: Int, tag: String, timestamp: Int)
case class MovieLensLink(movieId: Int, imdbId: Int, tmdbId: Int)

case class MovieLensData(ratings: Dataset[MovieLensRating], movies: Dataset[MovieLensMovie], tags: Dataset[MovieLensTag], links: Dataset[MovieLensLink])

object MovieLens extends Implicits {

  def parse(lines: RDD[String]): RDD[Array[String]] = {
    CSVRelation.univocityTokenizer(lines, null, CSVOptions())
  }

  def read[T: ClassTag : TypeTag](resource: String)(implicit spark: SparkSession, encoder: Encoder[T]): Dataset[T] = {
    val sc = spark.sparkContext
    val lines = sc.parallelize(Resource.lines(resource).toSeq.tail)
    val parsed = CSVRelation.univocityTokenizer(lines, null, CSVOptions())
    spark.createDataset(parsed.convertTo[T])
  }

  def load(resource: String)(implicit spark: SparkSession): MovieLensData = {
    import spark.implicits._

    val links = read[MovieLensLink](s"$resource/links.csv")
    val movies = read[MovieLensMovie](s"$resource/movies.csv")
    val ratings = read[MovieLensRating](s"$resource/ratings.csv")
    val tags = read[MovieLensTag](s"$resource/tags.csv")

    MovieLensData(ratings, movies, tags, links)
  }
}
