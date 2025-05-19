# Review: Spark Optimization with Scala

## Chapter 1 - Welcome

### Lesson 1
I've added a PowerShell script to users from Windows build images more easily. Here's a quick step to follow:
- **PowerShell Execution Policy:**  
  To execute the PowerShell script:
  1. Check the current execution policy:
     ```powershell
     Get-ExecutionPolicy  
     ```  
  2. If the result is `Restricted`, run the following command **as Administrator**:
     ```powershell
     Set-ExecutionPolicy RemoteSigned  
     ```  
  3. Then, execute the build script:
     ```powershell
     .\build-images.ps1  
     ```  

- **Correction at 11:19:**  
  The correct command to access the Spark master container is:
  ```bash
  docker exec -it spark-cluster-spark-master-1 bash  

- **Running Spark Locally on Windows:**
Please, refer to these resource if you don't have **WinUtils** in your computer.

[WinUtils Binaries](https://kontext.tech/article/825/hadoop-331-winutils)

## Chapter 2 - Spark Performance Foundations

### IntelliJ IDEA Configuration for Java 17+
To run Spark locally with Kryo serializer, add these VM options in IntelliJ:

```
--add-opens java.base/java.nio=ALL-UNNAMED 
--add-opens java.base/sun.nio.ch=ALL-UNNAMED  
--add-opens java.base/java.lang=ALL-UNNAMED  
--add-opens java.base/java.lang.reflect=ALL-UNNAMED  
--add-opens java.base/java.util=ALL-UNNAMED  
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED
```

### Lesson 1

- **Remove Apache Mesos Support:**
  Spark 4.0 no longer supports Apache Mesos as a cluster manager [Spark 4.0 Migration Guide](https://spark.apache.org/docs/4.0.0-preview2/core-migration-guide.html#ddl-statements).

### Lesson 2

- **Correction at 00:38:**
  Fix the tag name from `2.1-job-anatomy` to `2.2-job-anatomy`.

- **Correction at 01:22:**
  The correct command is:
```bash
  docker exec -it spark-cluster-spark-master-1 bash 
```

### Lesson 3

- **Correction at 03:07:**
  The correct command is:
```bash
  docker exec -it spark-cluster-spark-master-1 bash 
```

- **Note on AQE (Adaptive Query Execution):**
  The `explain` output may differ due to AQE optimizations (e.g., exchange steps).

### Lesson 4

- **Tip:**
  Use `spark.sparkContext.setJobDescription()` for better job tracking.
  E.g.:
```bash

spark.sparkContext.setJobDescription("Example 1")
val df = spark.read
  .option("inferSchema", "true")
  .json(path)
df.show()
```
Now, see in Spark UI the job description.

### Lesson 5

- **RDD to DF Conversion:**
  The physical plan now splits the conversion into a separate job, distinct from subsequent operations (e.g., at **04:23**).

### Lesson 6

- **Correction at 18:32:**  
  Remove Apache Mesos from the list of supported cluster managers.

- **Correction at 26:16:**
  Fix the `spark.executor.memory` configuration:

```bash
  --conf spark.executor.memory=1g
```