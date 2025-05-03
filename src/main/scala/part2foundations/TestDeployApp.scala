package part2foundations

import org.apache.spark.sql.{SaveMode, SparkSession}

object TestDeployApp {

  // TestDeployApp inputFile outputFile
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Need input file and output file")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("Test Deploy App")
      //.config("spark.master", "local[2]") // uncomment this if you want to run locally in IntelliJ
      // method 1
      .config("spark.executor.memory", "1g")
      .getOrCreate()

    import spark.implicits._

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json(args(0))

    val goodComediesDF = moviesDF.select(
      $"Title",
      $"IMDB_Rating".as("Rating"),
      $"Release_Date".as("Release")
    )
      .where(($"Major_Genre" === "Comedy") and ($"IMDB_Rating" > 6.5))
      .orderBy($"Rating".desc_nulls_last)

    // method 2
    // spark.conf.set("spark.executor.memory", "1g") // This will FAIL - you cannot change value during the runtime for static spark configuration like executor memory
    // spark.conf.set("spark.sql.shuffle.partitions", "200") // This WORKS - dynamic spark configurations can be changed at runtime

    /*
      method 3: pass configs as command line arguments:

        spark-submit ... --conf spark.executor.memory=1g

      You can also use dedicated command line arguments for certain configurations:
        --master = spark.master
        --executor-memory = spark.executor.memory
        --driver-memory = spark.driver.memory

        and many more.
    */
    goodComediesDF.show()

    goodComediesDF.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save(args(1))

  }
}
