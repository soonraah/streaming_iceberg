package example

import org.apache.spark.sql.SparkSession

object SelectRunner {
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
        """select
          |    *
          |from
          |    my_catalog.my_db.device_temperature
          |order by
          |    device_id
          |""".stripMargin
      )
      .show(128, truncate = false)

    spark.stop()
  }
}
