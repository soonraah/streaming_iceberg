package example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rand, randn, row_number, udf, when}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.IntegerType

import java.sql.Timestamp
import scala.concurrent.duration._
import scala.util.Random

object UpdateRunner {
  def main(args: Array[String]): Unit = {
    // Create SparkSession instance
    val spark = SparkSession
      .builder()
      .appName("StreamingIcebergExperiment")
      .master("local[2]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.my_catalog.type", "hadoop")
      .config("spark.sql.catalog.my_catalog.warehouse", "data/warehouse")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // Drop table
    spark
      .sql(
        """drop table if exists
          |    my_catalog.my_db.device_temperature
          |""".stripMargin
      )

    // Create table
    spark
      .sql(
        """create table
          |    my_catalog.my_db.device_temperature
          |(
          |    device_id int,
          |    operation string,
          |    temperature double,
          |    ts timestamp
          |)
          |using iceberg
          |tblproperties (
          |    'format-version' = '2',
          |    'write.delete.mode' = 'merge-on-read',
          |    'write.update.mode' = 'merge-on-read',
          |    'write.merge.mode' = 'merge-on-read'
          |)
          |""".stripMargin
      )

    val random = new Random()

    val addOutOfOrderness = udf {
      (timestamp: Timestamp) =>
        // add time randomly in [-5, +5) sec
        val ms = timestamp.getTime + random.nextInt(10000) - 5000
        new Timestamp(ms)
    }

    // Prepare input data
    val dfInput = spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()
      .select(
        // Generate device_id in 0~127 randomly
        (rand() * 128).cast(IntegerType).as("device_id"),
        // 90% of operation is upsert and 10% is delete
        when(rand() > 0.1, "upsert").otherwise("delete").as("operation"),
        // Generate temperature randomly based on standard normal distribution
        (randn() * 5.0 + 20.0).as("temperature"),
        // timestamp is out of order
        addOutOfOrderness($"timestamp").as("ts")
      )

    // Update table for each mini batch
    dfInput
      .writeStream
      .trigger(Trigger.ProcessingTime(30.seconds))
      .foreachBatch {
        (dfBatch: DataFrame, batchId: Long) =>
          println(s"Processing batchId=$batchId")

          // Eliminate duplicated device_id
          val dfDedup = dfBatch
            .withColumn(
              "row_num",
              row_number()
                .over(Window.partitionBy($"device_id").orderBy($"ts".desc))
            )
            .where($"row_num" === 1)
            .drop($"row_num")
          // createOrReplaceTempView() doesn't work
          dfDedup.createOrReplaceGlobalTempView("input")

          // Insert, update and delete records by 'merge into'
          spark
            .sql(
              """merge into
                |    my_catalog.my_db.device_temperature
                |using(
                |    select
                |        *
                |    from
                |        global_temp.input
                |) as input
                |on
                |    device_temperature.device_id = input.device_id
                |    and device_temperature.ts < input.ts
                |when matched and input.operation = 'upsert' then update set *
                |when matched and input.operation = 'delete' then delete
                |when not matched then insert *
                |""".stripMargin
            )

          ()
      }
      .start
      .awaitTermination()

    spark.stop()
  }
}
