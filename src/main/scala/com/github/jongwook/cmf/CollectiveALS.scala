package com.github.jongwook.cmf

import java.{util => ju}

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.github.jongwook.cmf.CollectiveALS.Rating
import com.github.jongwook.cmf.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, FloatType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.{Dependency, Partitioner, ShuffleDependency, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Sorting
import scala.util.hashing.byteswap64


class CollectiveALS(entities: String*) extends Serializable {

  var rank: Int = 10

  val numBlocks = new Array[Int](entities.size)
  ju.Arrays.fill(numBlocks, 2000)

  def numUserBlocks: Int = numBlocks(0)
  def numItemBlocks: Int = numBlocks(1)

  var implicitPrefs: Boolean = false
  var alpha: Double = 1.0

  private val cols: Array[String] = {
    entities.size match {
      case 0 => Array("user", "item")
      case 1 => throw new IllegalArgumentException("There should be at least 2 entities")
      case _ => entities.toArray
    }
  }

  def userCol: String = cols(0)
  def itemCol: String = cols(1)
  def entityCol(index: Int): String = cols(index)

  var ratingCol: String = "rating"
  var predictionCol: String = "prediction"
  var maxIter: Int = 10
  var regParam: Double = 0.1
  var nonnegative: Boolean = false
  var checkpointInterval: Int = 10
  var seed: Long = this.getClass.getName.hashCode.toLong
  var intermediateStorageLevel: String = "MEMORY_AND_DISK"
  var finalStorageLevel: String = "MEMORY_AND_DISK"

  def setRank(value: Int): this.type = { rank = value; this }

  def setNumUserBlocks(value: Int): this.type = { numBlocks(0) = value; this }
  def setNumItemBlocks(value: Int): this.type = { numBlocks(1) = value; this }
  def setNumBlocks(index: Int, value: Int): this.type = { numBlocks(index) = value; this }

  def setImplicitPrefs(value: Boolean): this.type = { implicitPrefs = value; this }
  def setAlpha(value: Double): this.type = { alpha = value; this }

  def setUserCol(value: String): this.type = { cols(0) = value; this }
  def setItemCol(value: String): this.type = { cols(1) = value; this }
  def setEntityCol(index: Int, value: String): this.type = { cols(index) = value; this }

  def setRatingCol(value: String): this.type = { ratingCol = value; this }
  def setPredictionCol(value: String): this.type = { predictionCol = value; this }
  def setMaxIter(value: Int): this.type = { maxIter = value; this }
  def setRegParam(value: Double): this.type = { regParam = value; this }
  def setNonnegative(value: Boolean): this.type = { nonnegative = value; this }
  def setCheckpointInterval(value: Int): this.type = { checkpointInterval = value; this }
  def setSeed(value: Long): this.type = { seed = value; this }
  def setIntermediateStorageLevel(value: String): this.type = { intermediateStorageLevel = value; this }
  def setFinalStorageLevel(value: String): this.type = { finalStorageLevel = value; this }

  def setNumBlocks(value: Int): this.type = { ju.Arrays.fill(numBlocks, value); this }

  val checkedCast = udf { (n: Double) =>
    if (n > Int.MaxValue || n < Int.MinValue) {
      throw new IllegalArgumentException(s"ALS only supports values in Integer range for columns " +
        s"$userCol and $itemCol. Value $n was out of Integer range.")
    } else {
      n.toInt
    }
  }

  def fit(dataset: Dataset[_]): CollectiveALSModel = fit((cols(0), cols(1)) -> dataset)

  def fit(datasets: ((String, String), Dataset[_])*): CollectiveALSModel = {
    datasets.foreach {
      case ((leftCol, rightCol), dataset) => transformSchema(dataset.schema, leftCol, rightCol)
    }

    val session = datasets.head._2.sparkSession
    import session.implicits._

    val r = if (ratingCol != "") col(ratingCol).cast(FloatType) else lit(1.0f)
    val data = datasets.map {
      case ((leftEntity, rightEntity), dataset) =>
        val Seq(left, right) = Seq(leftEntity, rightEntity).map(cols.indexOf)

        if (left == -1) throw new IllegalArgumentException(s"Unknown entity: $leftEntity")
        if (right == -1) throw new IllegalArgumentException(s"Unknown entity: $rightEntity")

        val ratings = dataset
          .select(checkedCast(col(leftEntity).cast(DoubleType)), checkedCast(col(rightEntity).cast(DoubleType)), r)
          .rdd.map { row =>
            Rating(row.getInt(0), row.getInt(1), row.getFloat(2))
          }

        (left, right) -> ratings
    }

    val factors: Seq[RDD[(Int, Array[Float])]] = CollectiveALS.train(data, entities, rank = rank,
      numBlocks = numBlocks, maxIter = maxIter, regParam = regParam, implicitPrefs = implicitPrefs,
      alpha = alpha, nonnegative = nonnegative,
      intermediateRDDStorageLevel = StorageLevel.fromString(intermediateStorageLevel),
      finalRDDStorageLevel = StorageLevel.fromString(finalStorageLevel),
      checkpointInterval = checkpointInterval, seed = seed)

    val dataFrames = factors.map(_.toDF("id", "features"))
    val model = new CollectiveALSModel(rank, dataFrames: _*)
    //instrLog.logSuccess(model)
    model.setEntityCols(cols)
    model.setPredictionCol(predictionCol)
  }

  def transformSchema(schema: StructType, leftCol: String, rightCol: String): StructType = {
    SchemaUtils.checkNumericType(schema, leftCol)
    SchemaUtils.checkNumericType(schema, rightCol)
    // rating will be cast to Float
    SchemaUtils.checkNumericType(schema, ratingCol)
    SchemaUtils.appendColumn(schema, predictionCol, FloatType)
  }

}

object CollectiveALS {

  val logger = LoggerFactory.getLogger(this.getClass.getName.stripSuffix("$"))

  case class Rating[@specialized(Int, Long) ID](left: ID, right: ID, rating: Float)

  /** Trait for least squares solvers applied to the normal equation. */
  trait LeastSquaresNESolver extends Serializable {
    /** Solves a least squares problem with regularization (possibly with other constraints). */
    def solve(ne: NormalEquation, lambda: Double): Array[Float]
  }

  /** Cholesky solver for least square problems. */
  class CholeskySolver extends LeastSquaresNESolver {

    /**
      * Solves a least squares problem with L2 regularization:
      *
      *   min norm(A x - b)^2^ + lambda * norm(x)^2^
      *
      * @param ne a [[NormalEquation]] instance that contains AtA, Atb, and n (number of instances)
      * @param lambda regularization constant
      * @return the solution x
      */
    override def solve(ne: NormalEquation, lambda: Double): Array[Float] = {
      val k = ne.k
      // Add scaled lambda to the diagonals of AtA.
      var i = 0
      var j = 2
      while (i < ne.triK) {
        ne.ata(i) += lambda
        i += j
        j += 1
      }
      CholeskyDecomposition.solve(ne.ata, ne.atb)
      val x = new Array[Float](k)
      i = 0
      while (i < k) {
        x(i) = ne.atb(i).toFloat
        i += 1
      }
      ne.reset()
      x
    }
  }

  /** NNLS solver. */
  class NNLSSolver extends LeastSquaresNESolver {
    private var rank: Int = -1
    private var workspace: NNLS.Workspace = _
    private var ata: Array[Double] = _
    private var initialized: Boolean = false

    private def initialize(rank: Int): Unit = {
      if (!initialized) {
        this.rank = rank
        workspace = NNLS.createWorkspace(rank)
        ata = new Array[Double](rank * rank)
        initialized = true
      } else {
        require(this.rank == rank)
      }
    }

    /**
      * Solves a nonnegative least squares problem with L2 regularization:
      *
      *   min_x_  norm(A x - b)^2^ + lambda * n * norm(x)^2^
      *   subject to x >= 0
      */
    override def solve(ne: NormalEquation, lambda: Double): Array[Float] = {
      val rank = ne.k
      initialize(rank)
      fillAtA(ne.ata, lambda)
      val x = NNLS.solve(ata, ne.atb, workspace)
      ne.reset()
      x.map(x => x.toFloat)
    }

    /**
      * Given a triangular matrix in the order of fillXtX above, compute the full symmetric square
      * matrix that it represents, storing it into destMatrix.
      */
    private def fillAtA(triAtA: Array[Double], lambda: Double) {
      var i = 0
      var pos = 0
      var a = 0.0
      while (i < rank) {
        var j = 0
        while (j <= i) {
          a = triAtA(pos)
          ata(i * rank + j) = a
          ata(j * rank + i) = a
          pos += 1
          j += 1
        }
        ata(i * rank + i) += lambda
        i += 1
      }
    }
  }

  /**
    * Representing a normal equation to solve the following weighted least squares problem:
    *
    * minimize \sum,,i,, c,,i,, (a,,i,,^T^ x - b,,i,,)^2^ + lambda * x^T^ x.
    *
    * Its normal equation is given by
    *
    * \sum,,i,, c,,i,, (a,,i,, a,,i,,^T^ x - b,,i,, a,,i,,) + lambda * x = 0.
    */
  class NormalEquation(val k: Int) extends Serializable {

    /** Number of entries in the upper triangular part of a k-by-k matrix. */
    val triK = k * (k + 1) / 2
    /** A^T^ * A */
    val ata = new Array[Double](triK)
    /** A^T^ * b */
    val atb = new Array[Double](k)

    private val da = new Array[Double](k)
    private val upper = "U"

    private def copyToDouble(a: Array[Float]): Unit = {
      var i = 0
      while (i < k) {
        da(i) = a(i)
        i += 1
      }
    }

    /** Adds an observation. */
    def add(a: Array[Float], b: Double, c: Double = 1.0): this.type = {
      require(c >= 0.0)
      require(a.length == k)
      copyToDouble(a)
      blas.dspr(upper, k, c, da, 1, ata)
      if (b != 0.0) {
        blas.daxpy(k, c * b, da, 1, atb, 1)
      }
      this
    }

    /** Merges another normal equation object. */
    def merge(other: NormalEquation): this.type = {
      require(other.k == k)
      blas.daxpy(ata.length, 1.0, other.ata, 1, ata, 1)
      blas.daxpy(atb.length, 1.0, other.atb, 1, atb, 1)
      this
    }

    /** Resets everything to zero, which should be called after each solve. */
    def reset(): Unit = {
      ju.Arrays.fill(ata, 0.0)
      ju.Arrays.fill(atb, 0.0)
    }
  }

  /**
    * :: DeveloperApi ::
    * Implementation of the ALS algorithm.
    */
  @DeveloperApi
  def train[ID: ClassTag]( // scalastyle:ignore
    data: Seq[((Int, Int), RDD[Rating[ID]])],
    entities: Seq[String],
    rank: Int = 10,
    numBlocks: Seq[Int] = Seq(10),
    maxIter: Int = 10,
    regParam: Double = 1.0,
    implicitPrefs: Boolean = false,
    alpha: Double = 1.0,
    nonnegative: Boolean = false,
    intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    finalRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK,
    checkpointInterval: Int = 10,
    seed: Long = 0L)(
    implicit ord: Ordering[ID]): Seq[RDD[(ID, Array[Float])]] = {

    require(intermediateRDDStorageLevel != StorageLevel.NONE,
      "ALS is not designed to run without persisting intermediate RDDs.")

    val sc = data.head._2.sparkContext

    val numDataFrames = data.size
    val numEntities = entities.size

    /** partitioner for each factor, length: numEntities */
    val partitioners = (numBlocks.size match {
      case 1 => List.fill(numEntities)(numBlocks.head)
      case n if n == numEntities => numBlocks
      case _ => throw new IllegalArgumentException("numBlocks should have the same size as the number of entities")
    }).map {
      blocks => new ALSPartitioner(blocks)
    }

    /** length: numEntities */
    val indexEncoders = partitioners.map{ part => new LocalIndexEncoder(part.numPartitions) }

    val solver = if (nonnegative) new NNLSSolver else new CholeskySolver

    /** length: numDataFrames */
    val entityInOutBlocks = data.zipWithIndex.flatMap {
      case (((left, right), ratings), index) =>
        val blockRatings = partitionRatings(ratings, partitioners(left), partitioners(right)).persist(intermediateRDDStorageLevel)

        val leftEntity = entities(left)
        val rightEntity = entities(right)

        val leftPartitioner = partitioners(left)
        val rightPartitioner = partitioners(right)

        val (leftInBlocks, leftOutBlocks) = makeBlocks(leftEntity, blockRatings, leftPartitioner, rightPartitioner, intermediateRDDStorageLevel)
        leftInBlocks.count()
        leftOutBlocks.count()

        val swappedBlockRatings = blockRatings.map {
          case ((leftBlockId, rightBlockId), RatingBlock(leftIds, rightIds, localRatings)) =>
            ((rightBlockId, leftBlockId), RatingBlock(rightIds, leftIds, localRatings))
        }

        val (rightInBlocks, rightOutBlocks) = makeBlocks(rightEntity, swappedBlockRatings, rightPartitioner, leftPartitioner, intermediateRDDStorageLevel)

        rightInBlocks.count()
        rightOutBlocks.count()

        Seq((left, right) -> (leftInBlocks, leftOutBlocks), (right, left) -> (rightInBlocks, rightOutBlocks))
    }

    val seedGen = new XORShiftRandom(seed)

    // map: source entity number -> sequence of (dst entity, srcInBlocks, srcOutBlocks)
    val groupedInOutBlocks: Map[Int, Seq[(Int, RDD[(Int, InBlock[ID])], RDD[(Int, OutBlock)])]] = entityInOutBlocks.groupBy {
      case ((src, dst), _) => src
    }.mapValues {
      _.map {
        case ((src, dst), (inBlocks, outBlocks)) => (dst, inBlocks, outBlocks)
      }
    }

    // map: destination entity number -> sequence of (src entity, srcInBlocks, srcOutBlocks)
    val reverseGroupedInOutBlocks: Map[Int, Seq[(Int, RDD[(Int, InBlock[ID])], RDD[(Int, OutBlock)])]] = entityInOutBlocks.groupBy {
      case ((src, dst), _) => dst
    }.mapValues {
      _.map {
        case ((src, dst), (inBlocks, outBlocks)) => (src, inBlocks, outBlocks)
      }
    }

    val entitySets = groupedInOutBlocks.mapValues { seq =>
      sc.union(seq.map {
        case (dst, inBlocks, outBlocks) =>
          inBlocks.flatMap {
            case (srcBlockId, inBlock) =>
              inBlock.srcIds
          }
      }).distinct()
    }

    // map: entity number -> RDD[ entity id -> latent factors ]
    val entityFactors: mutable.HashMap[Int, RDD[(ID, Factor)]] = mutable.HashMap((0 until numEntities).map { entity =>
      entity -> initialize(entitySets(entity), rank, seedGen.nextLong())
    }: _*)

    for (iter <- 0 until maxIter) {
      for (entity <- 0 until numEntities) {
        val encoder = indexEncoders(entity)
        val srcOutBlockSeq = reverseGroupedInOutBlocks(entity).map {
          case (src, inBlocks, outBlocks) => (src, outBlocks)
        }.sortBy(_._1)
        val dstInBlockSeq = groupedInOutBlocks(entity).map {
          case (dst, inBlocks, outBlocks) => (dst, inBlocks)
        }.sortBy(_._1)
        assert(srcOutBlockSeq.map(_._1) == dstInBlockSeq.map(_._1))
        val srcFactorSeq = reverseGroupedInOutBlocks(entity).map { case (src, inBlock, _) => (entityFactors(src), inBlock) }
        val newFactors = computeFactors(srcFactorSeq, srcOutBlockSeq, dstInBlockSeq, rank, regParam, encoder, implicitPrefs, alpha, solver)
        newFactors.localCheckpoint()
        newFactors.count()
        entityFactors.update(entity, newFactors)
      }
    }

    val entityIdAndFactors = (0 until numEntities).map { entity =>
      entityFactors(entity)
          .setName(s"${entities(entity)}Factors")
          .persist(finalRDDStorageLevel)
    }
//
//    if (finalRDDStorageLevel != StorageLevel.NONE) {
//      entityIdAndFactors.foreach(_.count())
//      entityFactors.values.foreach(_.unpersist())
//      entityInOutBlocks.foreach {
//        case (_, (inBlock, outBlock)) =>
//          inBlock.unpersist()
//          outBlock.unpersist()
//      }
//    }

    entityIdAndFactors

//
//    if (implicitPrefs) {
//      for (iter <- 1 to maxIter) {
//        userFactors.setName(s"userFactors-$iter").persist(intermediateRDDStorageLevel)
//        val previousItemFactors = itemFactors
//        itemFactors = computeFactors(userFactors, userOutBlocks, itemInBlocks, rank, regParam,
//          userLocalIndexEncoder, implicitPrefs, alpha, solver)
//        previousItemFactors.unpersist()
//        itemFactors.setName(s"itemFactors-$iter").persist(intermediateRDDStorageLevel)
//        // TODO: Generalize PeriodicGraphCheckpointer and use it here.
//        val deps = itemFactors.dependencies
//        val previousUserFactors = userFactors
//        userFactors = computeFactors(itemFactors, itemOutBlocks, userInBlocks, rank, regParam,
//          itemLocalIndexEncoder, implicitPrefs, alpha, solver)
//        previousUserFactors.unpersist()
//      }
//    } else {
//      for (iter <- 0 until maxIter) {
//        itemFactors = computeFactors(userFactors, userOutBlocks, itemInBlocks, rank, regParam,
//          userLocalIndexEncoder, solver = solver)
//        userFactors = computeFactors(itemFactors, itemOutBlocks, userInBlocks, rank, regParam,
//          itemLocalIndexEncoder, solver = solver)
//      }
//    }
  }

  /**
    * Factor block that stores factors (Array[Float]) for each entity as a record in RDD
    */
  private type Factor = Array[Float]

  /**
    * Factor block that stores factors (Array[Float]) in an Array.
    */
  private type FactorBlock = Array[Array[Float]]

  /**
    * Out-link block that stores, for each dst (item/user) block, which src (user/item) factors to
    * send. For example, outLinkBlock(0) contains the local indices (not the original src IDs) of the
    * src factors in this block to send to dst block 0.
    */
  private type OutBlock = Array[Array[Int]]

  /**
    * In-link block for computing src (user/item) factors. This includes the original src IDs
    * of the elements within this block as well as encoded dst (item/user) indices and corresponding
    * ratings. The dst indices are in the form of (blockId, localIndex), which are not the original
    * dst IDs. To compute src factors, we expect receiving dst factors that match the dst indices.
    * For example, if we have an in-link record
    *
    * {srcId: 0, dstBlockId: 2, dstLocalIndex: 3, rating: 5.0},
    *
    * and assume that the dst factors are stored as dstFactors: Map[Int, Array[Array[Float]]], which
    * is a blockId to dst factors map, the corresponding dst factor of the record is dstFactor(2)(3).
    *
    * We use a CSC-like (compressed sparse column) format to store the in-link information. So we can
    * compute src factors one after another using only one normal equation instance.
    *
    * @param srcIds src ids (ordered)
    * @param dstPtrs dst pointers. Elements in range [dstPtrs(i), dstPtrs(i+1)) of dst indices and
    *                ratings are associated with srcIds(i).
    * @param dstEncodedIndices encoded dst indices
    * @param ratings ratings
    * @see [[LocalIndexEncoder]]
    */
  case class InBlock[@specialized(Int, Long) ID: ClassTag](
    srcIds: Array[ID],
    dstPtrs: Array[Int],
    dstEncodedIndices: Array[Int],
    ratings: Array[Float]) {
    /** Size of the block. */
    def size: Int = ratings.length
    require(dstEncodedIndices.length == size)
    require(dstPtrs.length == srcIds.length + 1)
  }

  /**
    * Initializes factors randomly given the in-link blocks.
    *
    * @param ids entity ids
    * @param rank rank
    * @return initialized factor blocks
    */
  private def initialize[ID](
    ids: RDD[ID],
    rank: Int,
    seed: Long): RDD[(ID, Factor)] = {
    // Choose a unit vector uniformly at random from the unit sphere, but from the
    // "first quadrant" where all elements are nonnegative. This can be done by choosing
    // elements distributed as Normal(0,1) and taking the absolute value, and then normalizing.
    // This appears to create factorizations that have a slightly better reconstruction
    // (<1%) compared picking elements uniformly at random in [0,1].
    ids.map { id =>
      val random = new XORShiftRandom(byteswap64(seed))
      val factor = Array.fill(rank)(random.nextGaussian().toFloat)
      val nrm = blas.snrm2(rank, factor, 1)
      blas.sscal(rank, 1.0f / nrm, factor, 1)
      (id, factor)
    }
  }

  private def makeFactorBlocks[ID](factors: RDD[(ID, Factor)], inBlocks: RDD[(Int, InBlock[ID])]): RDD[(Int, FactorBlock)] = {
    val numPartitions = inBlocks.getNumPartitions
    factors.map {
      case (id, factor) => ((id.asInstanceOf[Number].longValue() % numPartitions).toInt, (id, factor))
    }.groupByKey().mapValues {
      factors => factors.toMap
    }.join(inBlocks).mapValues[FactorBlock] {
      case (factorMap, InBlock(srcIds, _, _, _)) => srcIds.map(factorMap.apply)
    }
  }

  private def explodeFactorBlocks[ID](blocks: RDD[(Int, FactorBlock)], inBlocks: RDD[(Int, InBlock[ID])]): RDD[(ID, Factor)] = {
    blocks.join(inBlocks).flatMap {
      case (blockId, (factorBlock, InBlock(srcIds, _, _, _))) => srcIds.zip(factorBlock)
    }
  }

  /**
    * A rating block that contains src IDs, dst IDs, and ratings, stored in primitive arrays.
    */
  case class RatingBlock[@specialized(Int, Long) ID: ClassTag](
    srcIds: Array[ID],
    dstIds: Array[ID],
    ratings: Array[Float]) {
    /** Size of the block. */
    def size: Int = srcIds.length
    require(dstIds.length == srcIds.length)
    require(ratings.length == srcIds.length)
  }

  /**
    * Builder for [[RatingBlock]]. [[scala.collection.mutable.ArrayBuilder]] is used to avoid boxing/unboxing.
    */
  class RatingBlockBuilder[@specialized(Int, Long) ID: ClassTag]
    extends Serializable {

    private val srcIds = mutable.ArrayBuilder.make[ID]
    private val dstIds = mutable.ArrayBuilder.make[ID]
    private val ratings = mutable.ArrayBuilder.make[Float]
    var size = 0

    /** Adds a rating. */
    def add(r: Rating[ID]): this.type = {
      size += 1
      srcIds += r.left
      dstIds += r.right
      ratings += r.rating
      this
    }

    /** Merges another [[RatingBlockBuilder]]. */
    def merge(other: RatingBlock[ID]): this.type = {
      size += other.srcIds.length
      srcIds ++= other.srcIds
      dstIds ++= other.dstIds
      ratings ++= other.ratings
      this
    }

    /** Builds a [[RatingBlock]]. */
    def build(): RatingBlock[ID] = {
      RatingBlock[ID](srcIds.result(), dstIds.result(), ratings.result())
    }
  }

  /**
    * Partitions raw ratings into blocks.
    *
    * @param ratings raw ratings
    * @param srcPart partitioner for src IDs
    * @param dstPart partitioner for dst IDs
    * @return an RDD of rating blocks in the form of ((srcBlockId, dstBlockId), ratingBlock)
    */
  private def partitionRatings[ID: ClassTag](
    ratings: RDD[Rating[ID]],
    srcPart: Partitioner,
    dstPart: Partitioner): RDD[((Int, Int), RatingBlock[ID])] = {

    /* The implementation produces the same result as the following but generates less objects.

    ratings.map { r =>
      ((srcPart.getPartition(r.user), dstPart.getPartition(r.item)), r)
    }.aggregateByKey(new RatingBlockBuilder)(
        seqOp = (b, r) => b.add(r),
        combOp = (b0, b1) => b0.merge(b1.build()))
      .mapValues(_.build())
    */

    val numPartitions = srcPart.numPartitions * dstPart.numPartitions
    ratings.mapPartitions { iter =>
      val builders = Array.fill(numPartitions)(new RatingBlockBuilder[ID])
      iter.flatMap { r =>
        val srcBlockId = srcPart.getPartition(r.left)
        val dstBlockId = dstPart.getPartition(r.right)
        val idx = srcBlockId + srcPart.numPartitions * dstBlockId
        val builder = builders(idx)
        builder.add(r)
        if (builder.size >= 2048) { // 2048 * (3 * 4) = 24k
          builders(idx) = new RatingBlockBuilder
          Iterator.single(((srcBlockId, dstBlockId), builder.build()))
        } else {
          Iterator.empty
        }
      } ++ {
        builders.view.zipWithIndex.filter(_._1.size > 0).map { case (block, idx) =>
          val srcBlockId = idx % srcPart.numPartitions
          val dstBlockId = idx / srcPart.numPartitions
          ((srcBlockId, dstBlockId), block.build())
        }
      }
    }.groupByKey().mapValues { blocks =>
      val builder = new RatingBlockBuilder[ID]
      blocks.foreach(builder.merge)
      builder.build()
    }.setName("ratingBlocks")
  }

  /**
    * Builder for uncompressed in-blocks of (srcId, dstEncodedIndex, rating) tuples.
    *
    * @param encoder encoder for dst indices
    */
  class UncompressedInBlockBuilder[@specialized(Int, Long) ID: ClassTag](
    encoder: LocalIndexEncoder)(
    implicit ord: Ordering[ID]) {

    private val srcIds = mutable.ArrayBuilder.make[ID]
    private val dstEncodedIndices = mutable.ArrayBuilder.make[Int]
    private val ratings = mutable.ArrayBuilder.make[Float]

    /**
      * Adds a dst block of (srcId, dstLocalIndex, rating) tuples.
      *
      * @param dstBlockId dst block ID
      * @param srcIds original src IDs
      * @param dstLocalIndices dst local indices
      * @param ratings ratings
      */
    def add(
      dstBlockId: Int,
      srcIds: Array[ID],
      dstLocalIndices: Array[Int],
      ratings: Array[Float]): this.type = {
      val sz = srcIds.length
      require(dstLocalIndices.length == sz)
      require(ratings.length == sz)
      this.srcIds ++= srcIds
      this.ratings ++= ratings
      var j = 0
      while (j < sz) {
        this.dstEncodedIndices += encoder.encode(dstBlockId, dstLocalIndices(j))
        j += 1
      }
      this
    }

    /** Builds a [[UncompressedInBlock]]. */
    def build(): UncompressedInBlock[ID] = {
      new UncompressedInBlock(srcIds.result(), dstEncodedIndices.result(), ratings.result())
    }
  }

  /**
    * A block of (srcId, dstEncodedIndex, rating) tuples stored in primitive arrays.
    */
  class UncompressedInBlock[@specialized(Int, Long) ID: ClassTag](
    val srcIds: Array[ID],
    val dstEncodedIndices: Array[Int],
    val ratings: Array[Float])(
    implicit ord: Ordering[ID]) {

    /** Size the of block. */
    def length: Int = srcIds.length

    /**
      * Compresses the block into an [[InBlock]]. The algorithm is the same as converting a
      * sparse matrix from coordinate list (COO) format into compressed sparse column (CSC) format.
      * Sorting is done using Spark's built-in Timsort to avoid generating too many objects.
      */
    def compress(): InBlock[ID] = {
      val sz = length
      assert(sz > 0, "Empty in-link block should not exist.")
      sort()
      val uniqueSrcIdsBuilder = mutable.ArrayBuilder.make[ID]
      val dstCountsBuilder = mutable.ArrayBuilder.make[Int]
      var preSrcId = srcIds(0)
      uniqueSrcIdsBuilder += preSrcId
      var curCount = 1
      var i = 1
      var j = 0
      while (i < sz) {
        val srcId = srcIds(i)
        if (srcId != preSrcId) {
          uniqueSrcIdsBuilder += srcId
          dstCountsBuilder += curCount
          preSrcId = srcId
          j += 1
          curCount = 0
        }
        curCount += 1
        i += 1
      }
      dstCountsBuilder += curCount
      val uniqueSrcIds = uniqueSrcIdsBuilder.result()
      val numUniqueSrdIds = uniqueSrcIds.length
      val dstCounts = dstCountsBuilder.result()
      val dstPtrs = new Array[Int](numUniqueSrdIds + 1)
      var sum = 0
      i = 0
      while (i < numUniqueSrdIds) {
        sum += dstCounts(i)
        i += 1
        dstPtrs(i) = sum
      }
      InBlock(uniqueSrcIds, dstPtrs, dstEncodedIndices, ratings)
    }

    private def sort(): Unit = {
      val sz = length
      // Since there might be interleaved log messages, we insert a unique id for easy pairing.
      val sortId = Utils.random.nextInt()
      logger.debug(s"Start sorting an uncompressed in-block of size $sz. (sortId = $sortId)")
      val start = System.nanoTime()
      val sorter = new Sorter(new UncompressedInBlockSort[ID])
      sorter.sort(this, 0, length, Ordering[KeyWrapper[ID]])
      val duration = (System.nanoTime() - start) / 1e9
      logger.debug(s"Sorting took $duration seconds. (sortId = $sortId)")
    }
  }

  /**
    * A wrapper that holds a primitive key.
    *
    * @see [[UncompressedInBlockSort]]
    */
  private class KeyWrapper[@specialized(Int, Long) ID: ClassTag](
    implicit ord: Ordering[ID]) extends Ordered[KeyWrapper[ID]] {

    var key: ID = _

    override def compare(that: KeyWrapper[ID]): Int = {
      ord.compare(key, that.key)
    }

    def setKey(key: ID): this.type = {
      this.key = key
      this
    }
  }

  /**
    * [[SortDataFormat]] of [[UncompressedInBlock]] used by [[Sorter]].
    */
  private class UncompressedInBlockSort[@specialized(Int, Long) ID: ClassTag](
    implicit ord: Ordering[ID])
    extends SortDataFormat[KeyWrapper[ID], UncompressedInBlock[ID]] {

    override def newKey(): KeyWrapper[ID] = new KeyWrapper()

    override def getKey(
      data: UncompressedInBlock[ID],
      pos: Int,
      reuse: KeyWrapper[ID]): KeyWrapper[ID] = {
      if (reuse == null) {
        new KeyWrapper().setKey(data.srcIds(pos))
      } else {
        reuse.setKey(data.srcIds(pos))
      }
    }

    override def getKey(
      data: UncompressedInBlock[ID],
      pos: Int): KeyWrapper[ID] = {
      getKey(data, pos, null)
    }

    private def swapElements[@specialized(Int, Float) T](
      data: Array[T],
      pos0: Int,
      pos1: Int): Unit = {
      val tmp = data(pos0)
      data(pos0) = data(pos1)
      data(pos1) = tmp
    }

    override def swap(data: UncompressedInBlock[ID], pos0: Int, pos1: Int): Unit = {
      swapElements(data.srcIds, pos0, pos1)
      swapElements(data.dstEncodedIndices, pos0, pos1)
      swapElements(data.ratings, pos0, pos1)
    }

    override def copyRange(
      src: UncompressedInBlock[ID],
      srcPos: Int,
      dst: UncompressedInBlock[ID],
      dstPos: Int,
      length: Int): Unit = {
      System.arraycopy(src.srcIds, srcPos, dst.srcIds, dstPos, length)
      System.arraycopy(src.dstEncodedIndices, srcPos, dst.dstEncodedIndices, dstPos, length)
      System.arraycopy(src.ratings, srcPos, dst.ratings, dstPos, length)
    }

    override def allocate(length: Int): UncompressedInBlock[ID] = {
      new UncompressedInBlock(
        new Array[ID](length), new Array[Int](length), new Array[Float](length))
    }

    override def copyElement(
      src: UncompressedInBlock[ID],
      srcPos: Int,
      dst: UncompressedInBlock[ID],
      dstPos: Int): Unit = {
      dst.srcIds(dstPos) = src.srcIds(srcPos)
      dst.dstEncodedIndices(dstPos) = src.dstEncodedIndices(srcPos)
      dst.ratings(dstPos) = src.ratings(srcPos)
    }
  }

  /**
    * Creates in-blocks and out-blocks from rating blocks.
    *
    * @param prefix prefix for in/out-block names
    * @param ratingBlocks rating blocks
    * @param srcPart partitioner for src IDs
    * @param dstPart partitioner for dst IDs
    * @return (in-blocks, out-blocks)
    */
  private def makeBlocks[ID: ClassTag](
    prefix: String,
    ratingBlocks: RDD[((Int, Int), RatingBlock[ID])],
    srcPart: Partitioner,
    dstPart: Partitioner,
    storageLevel: StorageLevel)(
    implicit srcOrd: Ordering[ID]): (RDD[(Int, InBlock[ID])], RDD[(Int, OutBlock)]) = {
    val inBlocks = ratingBlocks.map {
      case ((srcBlockId, dstBlockId), RatingBlock(srcIds, dstIds, ratings)) =>
        // The implementation is a faster version of
        // val dstIdToLocalIndex = dstIds.toSet.toSeq.sorted.zipWithIndex.toMap
        val start = System.nanoTime()
        val dstIdSet = new OpenHashSet[ID](1 << 20)
        dstIds.foreach(dstIdSet.add)
        val sortedDstIds = new Array[ID](dstIdSet.size)
        var i = 0
        var pos = dstIdSet.nextPos(0)
        while (pos != -1) {
          sortedDstIds(i) = dstIdSet.getValue(pos)
          pos = dstIdSet.nextPos(pos + 1)
          i += 1
        }
        assert(i == dstIdSet.size)
        Sorting.quickSort(sortedDstIds)
        val dstIdToLocalIndex = new mutable.OpenHashMap[ID, Int](sortedDstIds.length)
        i = 0
        while (i < sortedDstIds.length) {
          dstIdToLocalIndex.update(sortedDstIds(i), i)
          i += 1
        }
        logger.debug(
          "Converting to local indices took " + (System.nanoTime() - start) / 1e9 + " seconds.")
        val dstLocalIndices = dstIds.map(dstIdToLocalIndex.apply)
        (srcBlockId, (dstBlockId, srcIds, dstLocalIndices, ratings))
    }.groupByKey(new ALSPartitioner(srcPart.numPartitions))
      .mapValues { iter =>
        val builder =
          new UncompressedInBlockBuilder[ID](new LocalIndexEncoder(dstPart.numPartitions))
        iter.foreach { case (dstBlockId, srcIds, dstLocalIndices, ratings) =>
          builder.add(dstBlockId, srcIds, dstLocalIndices, ratings)
        }
        builder.build().compress()
      }.setName(prefix + "InBlocks")
      .persist(storageLevel)
    val outBlocks = inBlocks.mapValues { case InBlock(srcIds, dstPtrs, dstEncodedIndices, _) =>
      val encoder = new LocalIndexEncoder(dstPart.numPartitions)
      val activeIds = Array.fill(dstPart.numPartitions)(mutable.ArrayBuilder.make[Int])
      var i = 0
      val seen = new Array[Boolean](dstPart.numPartitions)
      while (i < srcIds.length) {
        var j = dstPtrs(i)
        ju.Arrays.fill(seen, false)
        while (j < dstPtrs(i + 1)) {
          val dstBlockId = encoder.blockId(dstEncodedIndices(j))
          if (!seen(dstBlockId)) {
            activeIds(dstBlockId) += i // add the local index in this out-block
            seen(dstBlockId) = true
          }
          j += 1
        }
        i += 1
      }
      activeIds.map { x =>
        x.result()
      }
    }.setName(prefix + "OutBlocks")
      .persist(storageLevel)
    (inBlocks, outBlocks)
  }

  /**
    * Compute dst factors by constructing and solving least square problems.
    *
    * @param srcFactorSeq src factors
    * @param srcOutBlockSeq src out-blocks
    * @param dstInBlockSeq dst in-blocks
    * @param rank rank
    * @param regParam regularization constant
    * @param srcEncoder encoder for src local indices
    * @param implicitPrefs whether to use implicit preference
    * @param alpha the alpha constant in the implicit preference formulation
    * @param solver solver for least squares problems
    * @return dst factors
    */
  private def computeFactors[ID: ClassTag](
    srcFactorSeq: Seq[(RDD[(ID, Factor)], RDD[(Int, InBlock[ID])])],
    srcOutBlockSeq: Seq[(Int, RDD[(Int, OutBlock)])],
    dstInBlockSeq: Seq[(Int, RDD[(Int, InBlock[ID])])],
    rank: Int,
    regParam: Double,
    srcEncoder: LocalIndexEncoder,
    implicitPrefs: Boolean = false,
    alpha: Double = 1.0,
    solver: LeastSquaresNESolver): RDD[(ID, Factor)] = {

    import org.apache.spark.SparkContext._

    (srcOutBlockSeq zip dstInBlockSeq zip srcFactorSeq).map {
      case (((src, srcOutBlocks), (dst, dstInBlocks)), (srcFactorRDD, srcInBlocks)) =>
        val numSrcBlocks = srcFactorRDD.partitions.length

//        val a = srcFactorRDD.keys.collect()
//        println(s"srcFactorRDD contains 67: ${a.contains(67)}, size ${a.size}")
//        val b = srcInBlocks.values.flatMap(_.srcIds).collect()
//        println(s"srcInBlocks contains 67: ${b.contains(67)}, size ${b.size}")
//        val c = dstInBlocks.values.flatMap(_.srcIds).collect()
//        println(s"dstInBlocks contains 67: ${c.contains(67)}, size ${c.size}")

        val srcFactorBlock = makeFactorBlocks(srcFactorRDD, srcInBlocks)

        val YtY = if (implicitPrefs) Some(computeYtY(srcFactorBlock, rank)) else None

        val srcOut = srcOutBlocks.join(srcFactorBlock).flatMap {
          case (srcBlockId, (srcOutBlock, srcFactors)) =>
            srcOutBlock.view.zipWithIndex.map { case (activeIndices, dstBlockId) =>
              (dstBlockId, (srcBlockId, activeIndices.map(idx => srcFactors(idx))))
            }
        }
        val merged = srcOut.groupByKey(new ALSPartitioner(dstInBlocks.partitions.length))
        dstInBlocks.join(merged).flatMap {
          case (_, (InBlock(dstIds, srcPtrs, srcEncodedIndices, ratings), srcFactors)) =>
            val sortedSrcFactors = new Array[FactorBlock](numSrcBlocks)
            srcFactors.foreach { case (srcBlockId, factors) =>
              sortedSrcFactors(srcBlockId) = factors
            }
            //val dstFactors = new Array[Array[Float]](dstIds.length)
            var j = 0
            val equations = new ArrayBuffer[(ID, (Int, NormalEquation))]()
            while (j < dstIds.length) {
              val ls = new NormalEquation(rank)
              if (implicitPrefs) {
                ls.merge(YtY.get)
              }
              var i = srcPtrs(j)
              var numExplicits = 0
              while (i < srcPtrs(j + 1)) {
                val encoded = srcEncodedIndices(i)
                val blockId = srcEncoder.blockId(encoded)
                val localIndex = srcEncoder.localIndex(encoded)
                val srcFactor = sortedSrcFactors(blockId)(localIndex)
                val rating = ratings(i)
                if (implicitPrefs) {
                  // Extension to the original paper to handle b < 0. confidence is a function of |b|
                  // instead so that it is never negative. c1 is confidence - 1.0.
                  val c1 = alpha * math.abs(rating)
                  // For rating <= 0, the corresponding preference is 0. So the term below is only added
                  // for rating > 0. Because YtY is already added, we need to adjust the scaling here.
                  if (rating > 0) {
                    numExplicits += 1
                    ls.add(srcFactor, (c1 + 1.0) / c1, c1)
                  }
                } else {
                  ls.add(srcFactor, rating)
                  numExplicits += 1
                }
                i += 1
              }
              // Weight lambda by the number of explicit ratings based on the ALS-WR paper.
              //dstFactors(j) = solver.solve(ls, numExplicits * regParam)
              equations.append((dstIds(j), (numExplicits, ls)))
              j += 1
            }
            equations
        }
    }.reduce[RDD[(ID, (Int, NormalEquation))]] { (left, right) =>
      left.fullOuterJoin(right).mapValues {
        case (Some((leftExplicits, leftEquation)), Some((rightExplicits, rightEquation))) =>
          (leftExplicits + rightExplicits, leftEquation.merge(rightEquation))
        case (Some((leftExplicits, leftEquation)), None) =>
          (leftExplicits, leftEquation)
        case (None, Some((rightExplicits, rightEquation))) =>
          (rightExplicits, rightEquation)
        case (None, None) =>
          throw new RuntimeException("fullOuterJoin should not produce (None, None)")
      }
    }.mapValues {
      case (numExplicits, equation) =>
        solver.solve(equation, numExplicits * regParam)
    }
  }

  /**
    * Computes the Gramian matrix of user or item factors, which is only used in implicit preference.
    * Caching of the input factors is handled in [[CollectiveALS#train]].
    */
  private def computeYtY(factorBlocks: RDD[(Int, FactorBlock)], rank: Int): NormalEquation = {
    factorBlocks.values.aggregate(new NormalEquation(rank))(
      seqOp = (ne, factors) => {
        factors.foreach(ne.add(_, 0.0))
        ne
      },
      combOp = (ne1, ne2) => ne1.merge(ne2))
  }

  /**
    * Encoder for storing (blockId, localIndex) into a single integer.
    *
    * We use the leading bits (including the sign bit) to store the block id and the rest to store
    * the local index. This is based on the assumption that users/items are approximately evenly
    * partitioned. With this assumption, we should be able to encode two billion distinct values.
    *
    * @param numBlocks number of blocks
    */
  class LocalIndexEncoder(numBlocks: Int) extends Serializable {

    require(numBlocks > 0, s"numBlocks must be positive but found $numBlocks.")

    private[this] final val numLocalIndexBits =
      math.min(java.lang.Integer.numberOfLeadingZeros(numBlocks - 1), 31)
    private[this] final val localIndexMask = (1 << numLocalIndexBits) - 1

    /** Encodes a (blockId, localIndex) into a single integer. */
    def encode(blockId: Int, localIndex: Int): Int = {
      require(blockId < numBlocks)
      require((localIndex & ~localIndexMask) == 0)
      (blockId << numLocalIndexBits) | localIndex
    }

    /** Gets the block id from an encoded index. */
    @inline
    def blockId(encoded: Int): Int = {
      encoded >>> numLocalIndexBits
    }

    /** Gets the local index from an encoded index. */
    @inline
    def localIndex(encoded: Int): Int = {
      encoded & localIndexMask
    }
  }

  /**
    * Partitioner used by ALS. We require that getPartition is a projection. That is, for any key k,
    * we have getPartition(getPartition(k)) = getPartition(k). Since the default HashPartitioner
    * satisfies this requirement, we simply use a type alias here.
    */
  type ALSPartitioner = org.apache.spark.HashPartitioner

  /**
    * Private function to clean up all of the shuffles files from the dependencies and their parents.
    */
  def cleanShuffleDependencies[T](
    sc: SparkContext,
    deps: Seq[Dependency[_]],
    blocking: Boolean = false): Unit = {
    // If there is no reference tracking we skip clean up.
    val field = sc.getClass.getDeclaredField("_cleaner")
    field.setAccessible(true)
    field.get(sc).asInstanceOf[Option[{ def doCleanupShuffle(id: Int, blocking: Boolean) }]].foreach { cleaner =>
      /**
        * Clean the shuffles & all of its parents.
        */
      def cleanEagerly(dep: Dependency[_]): Unit = {
        if (dep.isInstanceOf[ShuffleDependency[_, _, _]]) {
          val shuffleId = dep.asInstanceOf[ShuffleDependency[_, _, _]].shuffleId
          cleaner.doCleanupShuffle(shuffleId, blocking)
        }
        val rdd = dep.rdd
        val rddDeps = rdd.dependencies
        if (rdd.getStorageLevel == StorageLevel.NONE && rddDeps != null) {
          rddDeps.foreach(cleanEagerly)
        }
      }
      deps.foreach(cleanEagerly)
    }
  }
}