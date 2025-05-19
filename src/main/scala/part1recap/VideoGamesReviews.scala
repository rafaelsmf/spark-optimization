package part1recap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object VideoGamesReviews {

  val spark = SparkSession.builder()
    .appName("Amazon Video Games Reviews")
    .master("local[2]")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
    Source: https://amazon-reviews-2023.github.io/

    Instructions:

    1. Inside src/main/resources/data, create the following directory structure: amazon_reviews/video_games/

    2. Download the following files and place them in the video_games folder:
       - https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Video_Games.jsonl.gz
       - https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/meta_categories/meta_Video_Games.jsonl.gz

    3. Extract the contents of each .gz file in the same folder.
  */

  /*
    | Field             | Type   | Explanation                                                                                                                                           |
    |-------------------|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
    | `rating`          | float  | Rating of the product (from 1.0 to 5.0).                                                                                                               |
    | `title`           | str    | Title of the user review.                                                                                                                             |
    | `text`            | str    | Text body of the user review.                                                                                                                         |
    | `images`          | list   | Images that users post after they have received the product. Each image has different sizes, represented by `small_image_url`, `medium_image_url`, and `large_image_url`. |
    | `asin`            | str    | ID of the product.                                                                                                                                    |
    | `parent_asin`     | str    | Parent ID of the product. Products with different colors, styles, sizes usually belong to the same parent ID. The previous "asin" in Amazon datasets is actually the parent ID. Use this to find product meta. |
    | `user_id`         | str    | ID of the reviewer.                                                                                                                                   |
    | `timestamp`       | int    | Time of the review (Unix time).                                                                                                                       |
    | `verified_purchase` | bool | User purchase verification.                                                                                                                           |
    | `helpful_vote`    | int    | Helpful votes of the review.                                                                                                                          |
   */

  val reviewsSchema = StructType(Seq(
    StructField("rating", FloatType, nullable = true),
    StructField("title", StringType, nullable = true),
    StructField("text", StringType, nullable = true),
    StructField("images", ArrayType(MapType(StringType, StringType)), nullable = true),
    StructField("asin", StringType, nullable = true),
    StructField("parent_asin", StringType, nullable = true),
    StructField("user_id", StringType, nullable = true),
    StructField("timestamp", LongType, nullable = true),
    StructField("verified_purchase", BooleanType, nullable = true),
    StructField("helpful_vote", IntegerType, nullable = true)
  ))

  val reviewsDF = spark.read
    .schema(reviewsSchema)
    .json("src/main/resources/data/amazon_reviews/video_games/Video_Games.jsonl")

  /*
    Video Games Metadata
    |----------------------------------------|
    | Field           | Type   | Explanation |
    |-----------------|--------|-------------|
    | `main_category` | str    | Main category (i.e., domain) of the product. |
    | `title`         | str    | Name of the product. |
    | `average_rating`| float  | Rating of the product shown on the product page. |
    | `rating_number` | int    | Number of ratings in the product. |
    | `features`      | list   | Bullet-point format features of the product. |
    | `description`   | list   | Description of the product. |
    | `price`         | float  | Price in US dollars (at time of crawling). |
    | `images`        | list   | Images of the product. Each image has different sizes (`thumb`, `large`, `hi_res`). The `variant` field shows the position of image. |
    | `videos`        | list   | Videos of the product including title and URL. |
    | `store`         | str    | Store name of the product. |
    | `categories`    | list   | Hierarchical categories of the product. |
    | `details`       | dict   | Product details, including materials, brand, sizes, etc. |
    | `parent_asin`   | str    | Parent ID of the product. |
    | `bought_together`| list  | Recommended bundles from the websites. |
   */

  val productSchema = StructType(Seq(
    StructField("main_category", StringType, nullable = false),
    StructField("title", StringType, nullable = false),
    StructField("average_rating", FloatType, nullable = true),
    StructField("rating_number", IntegerType, nullable = true),
    StructField("features", ArrayType(StringType), nullable = false),
    StructField("description", ArrayType(StringType), nullable = false),
    StructField("price", FloatType, nullable = false),
    StructField("images", ArrayType(StructType(Seq(
      StructField("thumb", StringType, nullable = true),
      StructField("large", StringType, nullable = true),
      StructField("variant", StringType, nullable = true),
      StructField("hi_res", StringType, nullable = true),
    ))), nullable = true),
    StructField("videos", ArrayType(StructType(Seq(
      StructField("title", StringType, nullable = true),
      StructField("URL", StringType, nullable = true),
    ))), nullable = true),
    StructField("store", StringType, nullable = false),
    StructField("categories", ArrayType(StringType), nullable = false),
    StructField("details", MapType(StringType, StringType), nullable = true),
    StructField("parent_asin", StringType, nullable = false),
    StructField("bought_together", ArrayType(StringType), nullable = true),
  ))

  val productsDF = spark.read
    .schema(productSchema)
    .json("src/main/resources/data/amazon_reviews/video_games/metadata_Video_Games.jsonl")

  /**
   * Exercises
   *
   * 1. What are the top 10 product categories with the highest proportion of low ratings (less than or equal to 2) among verified purchases?
   * 2. Who are the most active users in each category? Do they tend to give positive reviews (ratings greater than or equal to 4) or negative reviews (ratings less than or equal to 2)?
   * 3. What is the impact of image-based reviews on product evaluation metrics, such as average rating and number of helpful votes?
   * 4. What is the frequency of the following keywords — "the", "and", "this", "that", "with", "for", "you", "your", "they", "their" — in highly rated reviews (ratings ≥ 4) versus poorly rated reviews (ratings ≤ 2)?
   */

  import spark.implicits._

  // 1.
  val cachedProductsDF = productsDF.cache()
  val lowRatingsVerifiedDF = reviewsDF
    .filter($"verified_purchase" === true && $"rating" <= 2.0)
    .join(cachedProductsDF, "parent_asin")
    .withColumn("category", explode($"categories"))

  val totalVerifiedDF = reviewsDF
    .filter($"verified_purchase" === true)
    .join(cachedProductsDF, "parent_asin")
    .withColumn("category", explode($"categories"))

  lowRatingsVerifiedDF
    .groupBy("category")
    .agg(count("parent_asin").as("low_count"))
    .join(
      totalVerifiedDF.groupBy("category")
        .agg(count("parent_asin").as("total_count")),
      "category"
    )
    .withColumn("low_rating_ratio", $"low_count" / $"total_count")
    .orderBy(desc("low_rating_ratio"))
    .limit(10)
    .collect()
    .foreach { row =>
      val category = row.getString(0)
      val ratio = "%.2f".format(row.getDouble(3))
      println(s"- $category: $ratio ratio")
    }

  // 2.
  reviewsDF.join(cachedProductsDF, "parent_asin")
    .withColumn("category", explode($"categories"))
    .groupBy("category", "user_id")
    .agg(
      count("parent_asin").as("review_count"),
      avg("rating").as("avg_rating")
    )
    .withColumn("sentiment",
      when($"avg_rating" >= 4.0, "positive")
        .when($"avg_rating" <= 2.0, "negative")
        .otherwise("neutral")
    )
    .orderBy($"category", desc("review_count"))
    .show()

  // 3.
  reviewsDF
    .withColumn("has_image", size($"images") > 0)
    .groupBy("has_image")
    .agg(
      avg("rating").as("avg_rating"),
      avg("helpful_vote").as("avg_helpful_votes"),
      count("*").as("total_reviews")
    )
    .show()

  // 4.
  val stopWords = Set("the", "and", "this", "that", "with", "for", "you", "your", "they", "their")

  val tokenizedDF = reviewsDF
    .select("rating", "text")
    .withColumn("tokens", split(lower(regexp_replace($"text", "[^a-zA-Z\\s]", "")), "\\s+"))

  val explodedDF = tokenizedDF
    .withColumn("token", explode($"tokens"))
    .filter(length($"token") > 3)
    .filter(!$"token".isin(stopWords.toSeq: _*))

  val posWordsDF = explodedDF
    .filter("rating >= 4.0")
    .groupBy("token")
    .count()
    .orderBy(desc("count"))
    .limit(10)
    .collect()
    .foreach { row =>
      println(s"- ${row.getString(0)}: ${row.getLong(1)} occurrences")
    }

  val negWordsDF = explodedDF
    .filter("rating <= 2.0")
    .groupBy("token")
    .count()
    .orderBy(desc("count"))
    .limit(10)
    .collect()
    .foreach { row =>
      println(s"- ${row.getString(0)}: ${row.getLong(1)} occurrences")
    }

  def main(args: Array[String]): Unit = {

  }
}
