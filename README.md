# compare-s3-objects
compare two sets of s3 objects specified in manifest files

## Notes

* Process a large data set on an EMR master node

spark-shell --master yarn --deploy-mode client \
    --driver-memory 30G \
    --executor-memory 30G --executor-cores 12 --num-executors 30 \
    --conf spark.driver.maxResultSize=20g

sc.getConf.getAll.sorted.foreach(println)

rows.rdd.partitions.size
rows.rdd.getNumPartitions


* Sample commands

=== Interactive ===

```
spark-shell \
    --master yarn \
    --deploy-mode client \
    --driver-memory 30G \
    --executor-memory 30G --executor-cores 12 --num-executors 30 \
    --conf spark.driver.maxResultSize=20g \
    --driver-java-options='-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop' \
    --jars s3://path-to-jar/S3Differ-1.0.jar
```

```
// Setup
val args = Seq[String](
  "--bucket",
  "placeholder",
  "--aws-principal",
  "placeholder",
  "--aws-credential",
  "placeholder",
  "--public-key",
  "placeholder",
  "--private-key",
  "placeholder",
  "--material-set",
  "placeholder",
  "--material-serial",
  "1",
  "--region",
  "us-east-1",
  "--baseline-manifest-key",
  "placeholder",
  "--manifest-key",
  "placeholder",
  "--diffs-to-show",
  "3"
)

import org.skygate.falcon._
import org.skygate.falcon.S3Differ._
val conf = new Conf(args)
```

```
// Inspect content of a manifest or data object
val encryptionMaterials = S3Reader.buildRsaMaterials(conf.publicKey(), conf.privateKey(),
  conf.materialSet(), conf.materialSerial().toString)
val reader = new S3Reader(conf.awsPrincipal(), conf.awsCredential(),
  encryptionMaterials, conf.region())
val x = reader.readObject("bucket", "manifestKey")
x.foreach(println)
val y = reader.readGzippedObject("bucket", "gzippedObjKey")
y.take(1).foreach(println)
```

```
// Render rows
val rows = S3Differ.readRows(conf, spark, false).cache
rows.printSchema

val data = rows.flatten
data.cache.count

// Render data from a temp view
data.createOrReplaceTempView("data_table")
spark.sql("select * from data_table limit 3").show

// Find matching row(s) based on a qualifer
val matchedRows = rows.select(rows.col("*")).filter("visitorRecord.qualifier = 200000057996741")
matchedRows.flatten.show(3)
```

```
// Group rows by multiple columns
val aggregatedData = data.groupBy($"eventSource_15", $"adUserId_21", $"adBrowserId_20").count.where($"count" > 1).sort($"count".desc)
aggregatedData.cache.show(20, 200)
aggregatedData.select(sum("count")).show

val summary = data.groupBy($"eventSource_15", $"adUserId_21", $"adBrowserId_20").count.describe()
summary.show(20, 200)
```

```
// Dedupe a given set of events in S3
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window

// Partition rows by multiple columns, and dedupe rows per partition based on a tie-breaker
val partitionKey = Seq($"eventSource_15", $"adUserId_21", $"adBrowserId_20")
val tierBreaker = $"qualifier_18"
val partitionedWindow = Window.partitionBy(partitionKey: _*).orderBy(tierBreaker.desc)
val dedupedData = data.withColumn("rownum", row_number.over(partitionedWindow)).where($"rownum" === 1).drop("rownum").cache
dedupedData.groupBy($"eventSource_15", $"adUserId_21", $"adBrowserId_20").count.where($"count" > 1).count

// Identify rows that used to have duplicates
val dedupedRows = dedupedData.join(
    aggregatedData,
    Seq("eventSource_15", "adUserId_21","adBrowserId_20"),
    "left"
).cache
dedupedRows.count
dedupedRows.where("adUserId_21 = '0101d99c99c2f5d331bb1926b4331976e98f7adaf44fbac5390e34ef88b9b154de92'").show
data.where("adUserId_21 = '0101d99c99c2f5d331bb1926b4331976e98f7adaf44fbac5390e34ef88b9b154de92'").orderBy(tierBreaker.desc).show
```

```
// Find out differences between two sets of events specified by manfifest
val diff = S3Differ.process(conf, spark)
diff.show(3)
```

```
// Simulate the case with diffs
val baselineRows = S3Differ.readRows(conf, spark)
baselineRows.cache.count
val rows = baselineRows.sample(false, 0.9999, 1).limit(19690)
rows.cache.count

val diff1 = baselineRows.except(rows)
diff1.cache.count

val updatedRows = diff1.selectExpr("""
  named_struct(
    'eventRecord', eventRecord,
    'recordType', recordType,
    'visitorRecord', named_struct(
      'domain', upper(visitorRecord.domain),
      'eventIdentity', visitorRecord.eventIdentity,
      'eventSource', visitorRecord.eventSource,
      'ignoreIdentityUpdate', false,
      'payload', visitorRecord.payload,
      'qualifier', visitorRecord.qualifier,
      'timestamp', visitorRecord.timestamp + 88,
      'userIdentity', named_struct(
        'adBrowserId', visitorRecord.userIdentity.adBrowserId,
        'adUserId', visitorRecord.userIdentity.adUserId,
        'ksoDeviceId', visitorRecord.userIdentity.ksoDeviceId,
        'sisDeviceId', visitorRecord.userIdentity.sisDeviceId
      )
    )
  ) as bx_struct
""").select($"bx_struct.*")
val newRows = rows.union(updatedRows)

baselineRows.except(newRows).flatten.select($"qualifier_18".alias("qualifier"), $"domain_9", $"timestamp_19").sort($"qualifier").show(20, false)
newRows.except(rows).flatten.select($"qualifier_18".alias("qualifier"), $"domain_9", $"timestamp_19").sort($"timestamp_19").show(20, false)
```

=== Non-Interactive ===
```
spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 30G \
    --executor-memory 30G --executor-cores 12 --num-executors 30 \
    --conf spark.driver.maxResultSize=20g \
    --class org.skygate.falcon.S3Differ \
    s3://path-to-jar/S3Differ-1.0.jar \
    --bucket "placeholder" \
    --aws-principal "placeholder" \
    --aws-credential "placeholder" \
    --public-key "placeholder" \
    --private-key "placeholder" \
    --material-set "placeholder" \
    --material-serial 1 \
    --region "us-east-1" \
    --baseline-manifest-key "placeholder" \
    --manifest-key "placeholder"
    --diffs-to-show 3 > diff.log 2>&1

grep 'INFO S3Differ' diff.log
```

=== Interactive for small data set ===
```
[Zeppelin]

Step 1: Remote into master node and download the JAR file

$ sudo mkdir /var/bx && sudo chmod 755 /var/bx && cd /var/bx
$ sudo aws s3 cp s3://path-to-jar/S3Differ-1.0.jar .

Step 2: Follow the instructions below to load the JAR via the Zeppelin GUI

https://zeppelin.apache.org/docs/latest/usage/interpreter/dependency_management.html

Step 3: Run adhoc queries from GUI

import org.skygate.falcon._
val conf = new Conf(args)

// Render rows
val rows = S3Differ.readRows(conf, spark, false)
rows.show(3)
z.show(rows.flatten.limit(3))

// Render diff rows
val baselineRows = S3Differ.readRows(conf, spark)
val diff = rows.except(baselineRows)
diff.count
z.show(diff.flatten.limit(3))
```

=== Find out the size of objects per prefix (similar to du for POSIX) ===

```
spark-shell \
    --master yarn \
    --deploy-mode client \
    --driver-memory 30G \
    --executor-memory 30G --executor-cores 4 --num-executors 120 \
    --conf spark.driver.maxResultSize=20g \
    --driver-java-options='-Dspark.yarn.app.container.log.dir=/mnt/var/log/hadoop' \
    --jars s3://bx.fenix.gamma/jars/S3Inventory-1.0.jar


val srcBucketName = "???"
val scrBucketKey = "???"
val destBucketName = "???"
val destPrefix = "output/temp"

import org.skygate.falcon.inventory.rrs._;

val s3Client = new CachedS3ClientFactory().get();
val inventoryManifestRetriever = new InventoryManifestRetriever(s3Client, srcBucketName, scrBucketKey);
val manifest = inventoryManifestRetriever.getInventoryManifest();
val fileSchema = manifest.getFileSchema();


import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaSparkContext;

val javaSC = new JavaSparkContext(sc);
val clientFactory = javaSC.broadcast(new CachedS3ClientFactory());
val locatorRDD = javaSC.parallelize(manifest.getLocators()).repartition(960);
val linesRDD = locatorRDD.map(new InventoryReportLineRetriever(clientFactory, manifest));
val pojoRDD = linesRDD.flatMap(new InventoryReportMapper(manifest));


import spark.implicits._
import org.apache.spark.sql.{Encoder, Encoders, types}

val encoder = Encoders.bean(classOf[InventoryReportLine])
val schema = encoder.schema
schema.printTreeString
val rawDataset = spark.createDataset(pojoRDD)(encoder)

val dataset = rawDataset.withColumn("size", $"size".cast(types.IntegerType)).withColumn("levels", split($"key", "\\/")).select($"levels".getItem(0).as("level1"), $"levels".getItem(1).as("level2"), $"size", size($"levels").alias("level_depth")).cache
dataset.printSchema

import org.apache.spark.sql.functions._


val level1Agg = dataset.groupBy("level1").agg(count(lit(1)).alias("num_of_objs"), sum("size").alias("total_size")).orderBy($"total_size".desc).cache
level1Agg.withColumn("num_of_objs", format_number($"num_of_objs", 0)).withColumn("total_size", format_number($"total_size", 0)).show(50, 100)


val level2Agg = dataset.filter($"level_depth" > lit(1)).groupBy("level1", "level2").agg(count(lit(1)).alias("num_of_objs"), sum("size").alias("total_size")).orderBy($"total_size".desc).cache
level2Agg.withColumn("num_of_objs", format_number($"num_of_objs", 0)).withColumn("total_size", format_number($"total_size", 0)).show(50, 100)
level2Agg.write.format("parquet").save("/bx/Level2Agg.parquet")


val zeppelinData = spark.read.load("/bx/Level2Agg.parquet")
val zeppelinView = zeppelinData.select(concat($"level1", lit("/"), $"level2"), $"total_size")

import scala.util.Try
val sizes = zeppelinData.select($"total_size").take(1000).map(r => r.get(0)).toSeq
val sizesInLong = sizes.map(s => Try(s.asInstanceOf[Number].longValue).getOrElse(0L).asInstanceOf[Long]).sorted(Ordering[Long].reverse).zipWithIndex
val histogram = sc.parallelize(sizesInLong).toDF("size", "index").select($"index" + 1 as "index", $"size").orderBy($"size".desc)
z.show(histogram)



val tags = Set(
  "retail-attributed-events",
  "preprocessed-pda-ranking-events",
  "preprocessed-pda-click-events",
  "pda-preprocessed-view-events",
  "obsidian-impression-ces-hourly",
  "fenix-click-ces-hourly",
  "al-poppin-preprocessed-imp-events",
  "al-poppin-preprocessed-click-events"
)
import org.apache.spark.broadcast.Broadcast
def udf_check(tags: Broadcast[scala.collection.immutable.Set[String]]) = {
  udf {(s: String) => tags.value.exists(s.contains(_))}
}
val checked_result = zeppelinData.withColumn("tag_check", udf_check(sc.broadcast(tags))($"level2"))
val matchedTags = checked_result.filter($"tag_check" === true).orderBy($"level1", $"level2", $"total_size".desc)
matchedTags.withColumn("num_of_objs", format_number($"num_of_objs", 0)).withColumn("total_size", format_number($"total_size", 0)).show(50, 100)
```

