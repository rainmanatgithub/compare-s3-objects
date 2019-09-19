package org.skygate.falcon

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

  // Note: Use snake-case (e.g., aws-principal) instead of camel case (e.g., awsPrincipal) on command line
  val bucket = opt[String](required = true)
  val awsPrincipal = opt[String](required = true)
  val awsCredential = opt[String](required = true)
  val publicKey = opt[String](required = true)
  val privateKey = opt[String](required = true)
  val materialSet = opt[String](required = true)
  val materialSerial = opt[Long](default = Some(1))
  val region = opt[String](default = Some("us-east-1"))

  val baselineManifestKey = opt[String](required = true)
  val manifestKey = opt[String](required = true)

  val diffsToShow = opt[Int](default = Some(3))

  verify()
}

object S3Differ extends LazyLogging {

  implicit class Helpers(rows: Dataset[Row]) {
    def flatten(): Dataset[Row] = {
      val cols = flattenSchema(rows.schema)
      val aliases = cols.zipWithIndex.map(tuple => tuple._1.alias(tuple._1.toString.split("\\.").last + "_" + tuple._2))
      rows.select(aliases: _*)
    }
  }

  def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName))
      }
    })
  }

  def convertToRows(lines: Seq[String], spark: SparkSession) : Dataset[Row] = {
    import spark.implicits._

    val step = 1000
    val segments = lines.sliding(step, step).toSeq
    val frames = segments.map(segment => {
      val frame = spark.read.json(segment.toDS)
      frame
    })
    logger.info(s"${segments.length} segments => ${frames.length} frames")

    val rows = frames.reduce(_ union _)
    rows
  }

  def readRows(reader: S3Reader, bucket: String, manifestKey: String,
    spark: SparkSession) : Dataset[Row] = {

    val manifest = reader.readObject(bucket, manifestKey)
    val lines = reader.readObjects(bucket, manifest)
    val rows = convertToRows(lines, spark)
    logger.info(s"rows.count = ${rows.count} from ${manifestKey}")

    rows
  }

  def readRows(conf: Conf, spark: SparkSession, fromBaseline: Boolean = true) : Dataset[Row] = {
    val encryptionMaterials = S3Reader.buildRsaMaterials(conf.publicKey(), conf.privateKey(),
      conf.materialSet(), conf.materialSerial().toString)
    val reader = new S3Reader(conf.awsPrincipal(), conf.awsCredential(),
      encryptionMaterials, conf.region())

    val manifestKey = if (fromBaseline) conf.baselineManifestKey() else conf.manifestKey()
    readRows(reader, conf.bucket(), manifestKey, spark)
  }

  def process(conf: Conf, spark: SparkSession) : Dataset[Row] = {
    val encryptionMaterials = S3Reader.buildRsaMaterials(conf.publicKey(), conf.privateKey(),
      conf.materialSet(), conf.materialSerial().toString)
    val reader = new S3Reader(conf.awsPrincipal(), conf.awsCredential(),
      encryptionMaterials, conf.region())

    val baselineRows = readRows(reader, conf.bucket(), conf.baselineManifestKey(), spark)
    val rows = readRows(reader, conf.bucket(), conf.manifestKey(), spark)

    rows.except(baselineRows)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]")
      .appName("S3DifferApp")
      .getOrCreate()

    val conf = new Conf(args)
    val diff = process(conf, spark)
    logger.info(s"diff.count = ${diff.count}")
    diff.flatten.show(conf.diffsToShow())
  }

}
