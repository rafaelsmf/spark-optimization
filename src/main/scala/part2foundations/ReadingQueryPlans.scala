package part2foundations

import org.apache.spark.sql.SparkSession

object ReadingQueryPlans {
  ///////////////////////////////////////////////////////////////////// Boilerplate
  // you don't need this code in the Spark shell
  // this code is needed if you want to run it locally in IntelliJ

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Reading Query Plans")
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    ///////////////////////////////////////////////////////////////////// Boilerplate

    // plan 1 - a simple transformation
    val simpleNumbers = spark.range(1, 1000000)
    val times5 = simpleNumbers.selectExpr("id * 5 as id")
    times5.explain() // this is how you show a query plan
    /*
      == Physical Plan ==
      *(1) Project [(id#0L * 5) AS id#2L]
      +- *(1) Range (1, 1000000, step=1, splits=6)
     */

    // plan 2 - a shuffle
    val moreNumbers = spark.range(1, 1000000, 2)
    val split7 = moreNumbers.repartition(7)

    split7.explain()
    /*
      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=14]
         +- Range (1, 1000000, step=2, splits=1)
     */

    // plan 3 - shuffle + transformation
    split7.selectExpr("id * 5 as id").explain()
    /*
      == Physical Plan ==
      AdaptiveSparkPlan isFinalPlan=false
      +- Project [(id#4L * 5) AS id#8L]
         +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=25]
            +- Range (1, 1000000, step=2, splits=1)
     */

    // plan 4 - a more complex job with a join
    val ds1 = spark.range(1, 10000000)
    val ds2 = spark.range(1, 20000000, 2)
    val ds3 = ds1.repartition(7)
    val ds4 = ds2.repartition(9)
    val ds5 = ds3.selectExpr("id * 3 as id")
    val joined = ds5.join(ds4, "id")
    val sum = joined.selectExpr("sum(id)")
    sum.explain()
    /*

    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- HashAggregate(keys=[], functions=[sum(id#18L)])
       +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=72]
          +- HashAggregate(keys=[], functions=[partial_sum(id#18L)])
             +- Project [id#18L]
                +- SortMergeJoin [id#18L], [id#12L], Inner
                   :- Sort [id#18L ASC NULLS FIRST], false, 0
                   :  +- Exchange hashpartitioning(id#18L, 200), ENSURE_REQUIREMENTS, [plan_id=64]
                   :     +- Project [(id#10L * 3) AS id#18L]
                   :        +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=54]
                   :           +- Range (1, 10000000, step=1, splits=1)
                   +- Sort [id#12L ASC NULLS FIRST], false, 0
                      +- Exchange hashpartitioning(id#12L, 200), ENSURE_REQUIREMENTS, [plan_id=65]
                         +- Exchange RoundRobinPartitioning(9), REPARTITION_BY_NUM, [plan_id=57]
                            +- Range (1, 20000000, step=2, splits=1)
     */

    /**
     * Exercises - read the Query Plans and try to understand the code that generated them.
     */

    // exercise 1
    /*
      == Physical Plan ==
      *(1) Project [firstName#153, lastName#155, (cast(salary#159 as double) / 1.1) AS salary_EUR#168]
      +- FileScan csv [firstName#153,lastName#155,salary#159] Batched: false, Format: CSV, Location: InMemoryFileIndex(1 paths)[file:src/main/resources/data/employees_headers], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<firstName:string,lastName:string,salary:string>
     */
    val employeesDF = spark.read.option("header", true).csv("src/main/resources/data/employees_headers")
    val empEur = employeesDF.selectExpr("firstName", "lastName", "salary / 1.1 as salary_EUR")

    // exercise 2
    /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- HashAggregate(keys=[dept#47], functions=[avg(salary#64)])
       +- Exchange hashpartitioning(dept#47, 200), ENSURE_REQUIREMENTS, [plan_id=106]
          +- HashAggregate(keys=[dept#47], functions=[partial_avg(salary#64)])
             +- Project [dept#47, cast(salary#50 as int) AS salary#64]
                +- FileScan csv [dept#47,salary#50] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:src/main/resources/data/employees_headers], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<dept:string,salary:string>
       */
    val avgSals = employeesDF
      .selectExpr("dept", "cast(salary as int) as salary")
      .groupBy("dept")
      .avg("salary")

    // exercise 3
    /*
    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- Project [id#0L]
       +- SortMergeJoin [id#0L], [id#2L], Inner
          :- Sort [id#0L ASC NULLS FIRST], false, 0
          :  +- Exchange hashpartitioning(id#0L, 200), ENSURE_REQUIREMENTS, [plan_id=17]
          :     +- Range (1, 10000000, step=3, splits=6)
          +- Sort [id#2L ASC NULLS FIRST], false, 0
             +- Exchange hashpartitioning(id#2L, 200), ENSURE_REQUIREMENTS, [plan_id=18]
                +- Range (1, 10000000, step=5, splits=6)
     */
    val d1 = spark.range(1, 10000000, 3)
    val d2 = spark.range(1, 10000000, 5)
    val j1 = d1.join(d2, "id")
  }
}
