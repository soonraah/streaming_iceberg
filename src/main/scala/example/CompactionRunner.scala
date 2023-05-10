package example

import org.apache.spark.sql.SparkSession

object CompactionRunner {
  def main(args: Array[String]): Unit = {
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

    spark
      .sql(
        """call my_catalog.system.rewrite_data_files(
           |    table => 'my_db.device_temperature',
           |    strategy => 'binpack'
           |)
          |""".stripMargin
      )
      .show(100, truncate = false)

    spark.stop()
  }
}
