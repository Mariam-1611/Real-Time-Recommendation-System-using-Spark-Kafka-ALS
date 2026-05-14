# Real-Time Recommendation System using Spark, Kafka & ALS

A scalable **Big Data Analytics** project that combines **Machine Learning**, **Real-Time Streaming**, and **Interactive Visualization** to build a real-time movie recommendation system using the **MovieLens 1M Dataset**.

This project integrates:

* Apache Spark MLlib (ALS Collaborative Filtering)
* Apache Kafka Streaming
* Spark Structured Streaming
* Streamlit Dashboard
* Real-Time Trending Analytics & Alerts

---

## 📌 Project Overview

This system simulates a production-style recommendation engine similar to those used by Netflix, Spotify, and Amazon.

The project focuses on:

* 🎯 Personalized movie recommendations
* 📈 Real-time trending movie detection
* ⚡ Streaming analytics with low latency
* 🚨 Alert generation for rating spikes and trending items
* 📊 Interactive real-time dashboard visualization

The system processes over **1 million movie ratings** from the MovieLens dataset and achieves an average recommendation latency of **1.5 seconds**.

---

## 🏗️ System Architecture

```text
[ MovieLens 1M Dataset ]
          |
          v
[ Spark ALS Training ]
          |
          v
[ Saved ALS Model ] <------ [ Kafka Producer ]
          |                           |
          v                           v
[ Spark Structured Streaming Consumer ]
    |            |             |
    v            v             v
[ Analytics ] [ Alerts ] [ Recommendations ]
                    |
                    v
           [ Streamlit Dashboard ]
```

---

# Technologies Used

| Technology                 | Purpose                    |
| -------------------------- | -------------------------- |
| PySpark MLlib              | ALS Recommendation Model   |
| Apache Kafka               | Real-time Event Streaming  |
| Spark Structured Streaming | Stream Processing          |
| Streamlit                  | Dashboard UI               |
| Plotly                     | Interactive Visualizations |
| Python                     | Core Development           |
| MovieLens 1M               | Dataset                    |

---

## 📂 Dataset

### MovieLens 1M Dataset

* **1,000,209 ratings**
* **6,040 users**
* **3,952 movies**

Dataset format:

```text
userId::movieId::rating::timestamp
```

---

## 🤖 Machine Learning Component

### ALS Collaborative Filtering

The recommendation engine uses **Alternating Least Squares (ALS)** from Spark MLlib.

### Model Configuration

| Parameter         | Value |
| ----------------- | ----- |
| rank              | 20    |
| maxIter           | 10    |
| regParam          | 0.1   |
| coldStartStrategy | drop  |
| nonnegative       | True  |

### Evaluation Result

| Metric        | Value    |
| ------------- | -------- |
| RMSE          | 0.8715   |
| Training Time | 13.3 sec |

✅ RMSE is well below the required threshold.

---

## ⚡ Streaming Pipeline

### Kafka Streaming

The producer simulates live user interactions:

```json
{
  "user_id": 4270,
  "item_id": 50,
  "rating": 4.8,
  "timestamp": "2026-05-12T17:02:00"
}
```

### Kafka Configuration

| Property      | Value         |
| ------------- | ------------- |
| Topic         | movie-ratings |
| Partitions    | 2             |
| Kafka Version | 3.7.0         |

---

## 📊 Real-Time Analytics

The system computes:

* Average Rating
* Interaction Count
* Trending Score

### Custom Trending Score

The project introduces a custom metric:

\text{Trending Score} = \text{avg_rating} \times \log(\text{interaction_count}+1)

This balances:

* Quality of ratings
* Number of interactions

---

## 🚨 Alert System

The system generates real-time alerts for:

| Alert Type       | Condition             |
| ---------------- | --------------------- |
| High Rating Item | avg_rating > 4.5      |
| Trending Item    | trending_score > 8    |
| Rating Spike     | interaction_count > 7 |

Example:

```text
ALERT: Movie 208 avg_rating=5.0
ALERT: Movie 1 trending_score=10.36
```

---

## 📈 Dashboard Features

The Streamlit dashboard updates every 2 seconds and includes:

* KPI Metrics
* Trending Movies
* Top Active Users
* Alerts Table
* Personalized Recommendations

### Dashboard Preview Features

* 📊 Plotly Charts
* 🔥 Live Trending Analytics
* ⚠️ Alert Monitoring
* 🎯 Recommendation Visualization

---

## ⏱️ Performance Results

| Metric          | Result               |
| --------------- | -------------------- |
| Average Latency | 1527 ms              |
| Minimum Latency | 813 ms               |
| Maximum Latency | 5699 ms (cold start) |

✅ Bonus latency target achieved (< 5000ms)

---

## 📁 Project Structure

```text
mini_project3/
│
├── train_als.py
├── kafka_producer.py
├── stream_consumer.py
├── dashboard.py
├── latency_benchmark.py
├── requirements.txt
├── README.md
└── als_model/
```

---

# 🚀 How to Run

## 1️⃣ Start Zookeeper

```bash
~/kafka/bin/zookeeper-server-start.sh ~/kafka/config/zookeeper.properties
```

---

## 2️⃣ Start Kafka Broker

```bash
~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties
```

---

## 3️⃣ Create Kafka Topic

```bash
~/kafka/bin/kafka-topics.sh \
--create \
--topic movie-ratings \
--bootstrap-server localhost:9092 \
--partitions 2 \
--replication-factor 1
```

---

## 4️⃣ Train ALS Model

```bash
spark-submit train_als.py
```

---

## 5️⃣ Start Streaming Consumer

```bash
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
stream_consumer.py
```

---

## 6️⃣ Run Kafka Producer

```bash
python3 kafka_producer.py
```

---

## 7️⃣ Launch Dashboard

```bash
streamlit run dashboard.py --server.port 8501
```

---

## 🧠 Key Learning Outcomes

* Distributed Machine Learning using Spark
* Real-time stream processing with Kafka
* Stateful streaming analytics
* Watermark handling for late data
* Recommendation systems with ALS
* Dashboard visualization with Streamlit

---

## 🌟 Features Beyond Requirements

✅ Custom Trending Score
✅ Watermark-based late event handling
✅ Real-time Streamlit dashboard
✅ Low-latency recommendation serving
✅ Parallel Kafka partition strategy

---

## 👩‍💻 Authors

* **Mariam Mohamed Goda**
* **Yasmin Osama**

**Zewail City of Science and Technology**
Department of Data Science & Artificial Intelligence
May 2026

---

## 📜 License

This project is for educational and academic purposes.
