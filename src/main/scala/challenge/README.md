# Challenge

## 🎯 Objective

You are joining a **Data Engineering team** at a company that is building a **search and analytics engine for web content**, based on public crawls from the [Common Crawl](https://commoncrawl.org). Your first task is to process and analyze the **April 2025 crawl** to extract meaningful insights and prepare clean, optimized datasets for downstream teams (Data Science, Search Ranking, and Product).

You will use **Apache Spark (RDDs)** on an **AWS EMR cluster**, focusing on performance, scalability, and reliability.

---

## 📂 Dataset

Use the **WARC metadata table** from:

```
s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2025-18/subset=warc/
```

Each record describes a webpage fetched during the crawl, including:

- URL
- Timestamp
- MIME type
- HTTP status
- Page size
- Languages
- HTML title
- Outgoing links
- Domain info

You’ll work only with **metadata** (not raw HTML) for this challenge.

---

## 🚧 Problem Statement

Your goal is to build an efficient Spark pipeline (using **RDD APIs**) that allows:

1. **Domain filtering and profiling**
2. **Skew-aware joins**
3. **Iterative aggregation and transformation**
4. **Broadcast optimization**
5. **Query tuning and resilience**

These outputs will feed downstream systems that power a **web intelligence dashboard**.

---

## 🧩 Tasks

### 1. 🕵️ Filter and Profile Pages from Target Domains

**Problem**: Product teams want visibility into `.com`, `.br`, and `.org` websites.

**Implement**:
- Read the metadata files using Spark.
- Extract and parse:
  - `url`, `domain`, `crawl_timestamp`, `content_mime_type`, `content_languages`, `page_len`, `title`, `outlink_count`
- Filter records where:
  - Domain ends in `.com`, `.br`, or `.org`
  - HTTP status = 200
  - MIME type is `text/html`

**Expected Output**: RDD of filtered metadata entries for valid HTML pages.

---

### 2. 📊 Identify Top Domains by Volume and Content Type

**Problem**: You need to identify high-volume domains and their content diversity.

**Implement**:
- Aggregate counts of pages **per domain**
- For each domain, compute:
  - Total pages
  - Distinct content languages
  - Average page size
- Use RDD `map`, `reduceByKey`, `groupByKey`, and `flatMap`

**Bonus**:
- Use `mapPartitions` for iterator-to-iterator optimization
- Apply checkpointing to persist intermediate domain stats

---

### 3. 🧬 Join with Domain Categories (Skewed Join)

**Problem**: Product owners have a CSV mapping domain → business category (e.g., e-commerce, news, gov). Join this with crawled data.

**Implement**:
- Simulate a **skewed dataset** by injecting dummy rows with a common domain key (e.g., `wikipedia.org`)
- Implement a **salted join strategy** using Spark RDDs to avoid skew
- Alternatively, use a **broadcast join** if the dimension table is small

**Expected Output**: Enriched metadata with domain category.

---

### 4. 🔁 Iterative Aggregation with cogroup

**Problem**: You must analyze domains both by:
- \# of pages
- \# of unique outlinks

**Implement**:
- Build two separate RDDs:
  1. (domain, page count)
  2. (domain, unique outlink count)
- Perform a **`cogroup`** to combine metrics and compute:
  - Influence Score = `pages * log(outlinks + 1)`

---

### 5. 🧪 Column Pruning + Parquet Export

**Problem**: Downstream teams need a compact version of the dataset for analysis.

**Implement**:
- Prune unused columns before writing
- Save to S3 as partitioned Parquet:
  - Partition by domain suffix: `.com`, `.br`, `.org`
  - Compress with Snappy

**Expected Output**:
- `/output/domain_suffix=com/.../*.parquet`

---

### 6. 🚀 Performance Tuning

**Problem**: Your job must scale to 100GB+ inputs. Optimize.

**Implement & Explain**:
- Repartitioning strategy
- Shuffle tuning (`spark.sql.shuffle.partitions`)
- Serialization tweaks (e.g., Kryo)
- Memory and executor config in EMR

---
