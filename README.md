# 📊 Hadoop Web Log Analytics: MapReduce & Distributed Cache

<div align="center">

![Java](https://img.shields.io/badge/Java-11+-ED8B00?style=for-the-badge&logo=java&logoColor=white)
![Apache Hadoop](https://img.shields.io/badge/Apache_Hadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)
![MapReduce](https://img.shields.io/badge/MapReduce-Distributed-4E9A06?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

*A high-performance MapReduce application designed to process large-scale web server logs, categorize traffic via Distributed Cache, and compute aggregated performance and error metrics.*

</div>

---

##  Table of Contents

- [Project Overview](#project-overview)
- [System Architecture](#system-architecture)
- [Project Structure](#project-structure)
- [Data Specifications](#data-specifications)
- [Execution Guide](#execution-guide)
- [Technical Implementation](#technical-implementation)
- [Combiner Optimization Analysis](#combiner-optimization-analysis)
- [Sample Output](#sample-output)
- [Author & License](#author--license)
---

## 📌 Project Overview

This project implements a distributed data processing pipeline using **Apache Hadoop MapReduce**. It is designed to ingest massive volumes of web server access logs (1GB+), classify HTTP requests into predefined categories using a **Distributed Cache**, and calculate critical business metrics including total request volume, average response latency, and HTTP error rates.

To demonstrate the performance impact of the MapReduce Combiner, this repository contains **two complete, isolated implementations**:
1.  **With Combiner:** Includes a local aggregation step at the Mapper level to reduce network shuffle traffic.
2.  **Without Combiner:** Processes data directly from Mappers to Reducers for baseline comparison.

**Key Engineering Highlights:**
*   **Distributed Processing:** Leverages Hadoop's parallel processing capabilities to handle large datasets efficiently.
*   **Distributed Cache:** Broadcasts a lightweight URL-to-Category mapping file to all worker nodes, eliminating redundant HDFS reads.
*   **Combiner Optimization:** Implements a local aggregation step at the Mapper level to drastically reduce network shuffle traffic.

---

## 🏗 System Architecture

```text
[ HDFS Input Logs ] --> [ Mapper ] --> [ Combiner (Local Aggregation) ] 
                                              |
                                              v
                                      [ Network Shuffle ]
                                              |
                                              v
                                      [ Reducer (Final Aggregation) ] --> [ HDFS Output ]
                                              ^
                                              |
[ HDFS Cache File ] -----------------> [ Distributed Cache ]
```

---

## 📁 Project Structure

The project is organized into two distinct directories to isolate the two execution variants. Each directory contains its own Java source files, pre-compiled JAR, input cache file, and generated output.

```text
hadoop-web-log-analytics/
├── README.md                          # Project documentation
├── log.png                            # MapReduce execution flow diagram
│
├── with_combiner/                     # Implementation WITH Combiner optimization
│   ├── CacheDriver.java               # Job configuration & entry point
│   ├── CacheMapper.java               # Log parsing & category mapping
│   ├── CacheCombiner.java             # Local reducer for shuffle optimization
│   ├── CacheReducer.java              # Final aggregation & metric calculation
│   ├── Task3.jar                      # Pre-compiled executable JAR
│   ├── url_categories.txt             # Distributed cache lookup file
│   └── task3_output_with_combiner     # Generated output results
│
└── without_combiner/                  # Implementation WITHOUT Combiner
    ├── CacheDriver.java               # Job configuration & entry point
    ├── CacheMapper.java               # Log parsing & category mapping
    ├── CacheReducer.java              # Final aggregation & metric calculation
    ├── Task3.jar                      # Pre-compiled executable JAR
    ├── url_categories.txt             # Distributed cache lookup file
    └── task3_output_without_combiner  # Generated output results
```

---

## 📊 Data Specifications

### 1. Input Data Format (Web Logs)
The input consists of CSV-formatted web server logs (no header row).

| Column | Data Type | Description |
| :--- | :--- | :--- |
| `requestId` | `String` | Unique identifier for the HTTP request. |
| `urlPath` | `String` | The requested URL path (e.g., `/product/123`). |
| `responseTimeMs`| `Integer` | Server response time in milliseconds. |
| `statusCode` | `Integer` | HTTP response status code (e.g., 200, 404, 500). |

### 2. Cache File Format (URL Categories)
A lightweight mapping file distributed to all Mappers. Uses prefix-matching logic.

```text
/image,StaticImage
/product,ProductPage
/login,AuthPage
```

### 3. Output Data Format
Tab-separated values generated by the Reducer.

```text
<Category>	Requests: <count>	AvgResponseTime: <avg_ms>	Errors: <error_count>
```
*(Note: `error_count` includes all HTTP status codes `>= 400`)*

---

## 🚀 Execution Guide

### Prerequisites
*   Java 11+
*   Apache Hadoop 2.x or 3.x (running in pseudo-distributed or fully distributed mode).
*   *Note: The project includes pre-compiled JARs (`Task3.jar`), so Maven is not required to run the jobs.*

### Step 1: Prepare HDFS
Upload your web log input file and the cache file to HDFS:
```bash
# Create directories
hdfs dfs -mkdir -p /user/hadoop/input /user/hadoop/cache

# Upload the cache file (use the one from either folder)
hdfs dfs -put with_combiner/url_categories.txt /user/hadoop/cache/

# Upload your web logs
hdfs dfs -put <your_web_logs_file.txt> /user/hadoop/input/
```

### Step 2: Run the Job (WITH Combiner)
Execute the pre-compiled JAR from the `with_combiner` directory:
```bash
hadoop jar with_combiner/Task3.jar CacheDriver \
    /user/hadoop/input \
    /user/hadoop/output_with_combiner \
    /user/hadoop/cache/url_categories.txt
```

### Step 3: Run the Job (WITHOUT Combiner)
Execute the pre-compiled JAR from the `without_combiner` directory:
```bash
hadoop jar without_combiner/Task3.jar CacheDriver \
    /user/hadoop/input \
    /user/hadoop/output_without_combiner \
    /user/hadoop/cache/url_categories.txt
```

### Step 4: Retrieve and Compare Results
```bash
# View output from the Combiner version
hdfs dfs -cat /user/hadoop/output_with_combiner/part-r-00000

# View output from the baseline version
hdfs dfs -cat /user/hadoop/output_without_combiner/part-r-00000
```

---

## 🛠️ Technical Implementation

### Distributed Cache Strategy
Instead of relying on Hadoop's deprecated `DistributedCache` API, this implementation utilizes a modern approach. The cache file path is passed via the Job configuration. Inside the `Mapper.setup()` method, the file is read directly from HDFS using the `FileSystem` API and loaded into an in-memory `HashMap` for O(1) prefix lookups.

### Data Flow & Serialization
*   **Mapper:** Parses the CSV log, performs prefix matching against the cache map, and emits `<Category, "1|responseTime|errorFlag">`.
*   **Combiner:** Parses the pipe-delimited string, performs local summation of counts, response times, and errors, and emits `<Category, "partialCount|partialSumTime|partialErrors">`.
*   **Reducer:** Aggregates the partial sums from all Mappers/Combiners, calculates the final average response time (using floor division), and formats the output.

---

## ⚡ Combiner Optimization Analysis

The `CacheCombiner` implements the exact same aggregation logic as the `CacheReducer`. Because the aggregation operations (summation and counting) are both **associative** and **commutative**, the mathematical result remains identical whether the Combiner is used or not.

**Why use the Combiner then?**
*   **Network I/O Reduction:** By aggregating data locally on the DataNode before the Shuffle phase, the volume of data transferred across the network to the Reducers is drastically reduced.
*   **Reducer Load Balancing:** It prevents Reducers from being bottlenecked by massive intermediate datasets, leading to faster overall job completion times.

*(Compare `with_combiner/task3_output_with_combiner` and `without_combiner/task3_output_without_combiner` to verify identical results).*

---

## 📝 Sample Output

Both execution variants produce the same final metrics (example):

```text
Admin           Requests: 1110      AvgResponseTime: 1516   Errors: 34
Auth            Requests: 9198      AvgResponseTime: 1105   Errors: 922
FilterPage      Requests: 469150    AvgResponseTime: 1268   Errors: 3604
OTHER           Requests: 7042788   AvgResponseTime: 992    Errors: 231952
ProductPage     Requests: 460416    AvgResponseTime: 1251   Errors: 25482
SearchPage      Requests: 14246     AvgResponseTime: 1176   Errors: 7230
StaticImage     Requests: 11396816  AvgResponseTime: 1558   Errors: 8218
```

---

## 👤 Author & License

**Developed by:** `Youssef Tarek `

**Context:** `Completed as part of the Big Data Analytics Assignment.`

This project is licensed under the **MIT License**. See the `LICENSE` file for details.
