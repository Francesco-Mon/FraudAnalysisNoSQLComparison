# NoSQL Fraud Detection Benchmark: MongoDB vs Neo4j

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![MongoDB](https://img.shields.io/badge/MongoDB-4.4%2B-green)
![Neo4j](https://img.shields.io/badge/Neo4j-4.x-blue)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

## 📌 Overview
This project performs a comparative analysis between a **Document-Oriented Database (MongoDB)** and a **Graph Database (Neo4j)** within the context of a **Fraud Detection System**.

The goal is to benchmark performance across datasets of increasing size (25%, 50%, 75%, 100% of ~20k transactions), measuring execution times for complex queries involving aggregations and multi-entity relationships (Clients, Shops, Transactions).

## 🚀 Key Features
*   **Synthetic Data Generation**: Uses `Faker` to generate realistic datasets (Clients, Shops, Transactions) with injected fraud patterns.
*   **ETL Pipeline**: Python scripts to import CSV data into MongoDB collections and Neo4j graph nodes/relationships.
*   **Statistical Analysis**: Calculates **Average Execution Time**, **Standard Deviation**, and **95% Confidence Intervals** over 30+ iterations to ensure reliable benchmarks.
*   **Data Visualization**: Generates histograms comparing "Cold Start" vs. "Average" performance.

## 📊 The Queries
The benchmark focuses on 4 specific queries designed to stress different database capabilities:

1.  **Simple Lookup**: Count customers from a specific country (Italy).
2.  **Aggregation**: Calculate total count and sum of fraudulent transactions.
3.  **Join/Lookup**: Find fraudulent transactions performed specifically by Italian customers (requires joining Transaction → Client).
4.  **Complex Relationship**: Identify the shop with the highest number of fraudulent transactions from Italian customers (requires joining Transaction → Client → Shop).

## 🛠️ Tech Stack
*   **Language**: Python
*   **Databases**: MongoDB, Neo4j
*   **Libraries**:
    *   `pandas` (Data manipulation)
    *   `pymongo`, `neo4j`, `py2neo` (Database drivers)
    *   `scipy`, `numpy` (Statistical analysis)
    *   `matplotlib` (Visualization)
    *   `faker` (Data generation)

## 📂 Project Structure
The source code is located in the `src/` folder:

*   `src/genera_dataset.py`: Generates the CSV datasets.
*   `src/importToMongoDB.py`: Loads CSVs into MongoDB.
*   `src/importToNeo4j.py`: Loads CSVs into Neo4j Graph.
*   `src/queryMongoDB.py`: Executes benchmark queries on MongoDB.
*   `src/queryNeo4j.py`: Executes benchmark queries on Neo4j.
*   `src/startIstogrammi.py`: Visualizes the results.

## ⚙️ Setup & Installation

1.  **Clone the repository**
    ```bash
    git clone https://github.com/Francesco-Mon/FraudAnalysisNoSQLComparison.git
    cd FraudAnalysisNoSQLComparison
    ```

2.  **Install dependencies**
    ```bash
    pip install pandas pymongo neo4j py2neo scipy numpy matplotlib faker
    ```

3.  **Database Configuration**
    *   Ensure **MongoDB** is running on `localhost:27017`.
    *   Ensure **Neo4j** is running on `localhost:7687`.
    *   *Note: Check `src/queryNeo4j.py` and `src/importToNeo4j.py` to update your Neo4j password if it differs from the default.*

## 🏃‍♂️ How to Run

Run the scripts in the following order from the root directory:

1.  **Generate Data**
    ```bash
    python src/genera_dataset.py
    ```
2.  **Import Data**
    ```bash
    python src/importToMongoDB.py
    python src/importToNeo4j.py
    ```
3.  **Run Benchmarks**
    ```bash
    python src/queryMongoDB.py
    python src/queryNeo4j.py
    ```
4.  **Visualize Results**
    ```bash
    python src/startIstogrammi.py
    ```

## 🎓 Academic Context
This project was developed as part of the Bachelor's Degree in Computer Science.

---
<p align="center">
  Developed by <a href="https://github.com/Francesco-Mon">Francesco Montecucco</a> - University of Messina
</p>
