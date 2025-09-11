# Amazon Music Reviews Analysis Project

![(https://upload.wikimedia.org/wikipedia/commons/a/a9/Amazon_logo.svg)](https://cdn.mos.cms.futurecdn.net/Ahm7PJ4fbUzqdz7B7ANnzb-970-80.jpg.webp)





This project is an implementation for the Database Design course at Sharif University of Technology. It involves setting up a data pipeline to ingest, process, and analyze the Amazon Music Reviews dataset.

The primary technology used is **OpenSearch**, a powerful open-source search and analytics suite. The project includes scripts for data preprocessing, bulk ingestion, querying, and performance benchmarking. The entire environment is containerized using Docker for easy setup and reproducibility.

## Project Structure

    .
    ├── docker-compose.yml      # Defines OpenSearch and Dashboards services
    ├── mappings/               # OpenSearch index mappings and settings
    │   └── amazon-music.json
    ├── pipelines/              # OpenSearch ingest pipelines
    │   └── compute_fields.json
    ├── queries/                # JSON definitions for the required queries
    │   ├── q1.json
    │   └── ...
    ├── src/                    # Python scripts for the data pipeline and analysis
    │   ├── parse_stream.py     # Parses and cleans the raw dataset
    │   ├── bulk_ingest.py      # Bulk-ingests data into OpenSearch
    │   ├── run_queries.py      # Executes predefined queries
    │   └── bench.py            # Runs performance benchmarks
    └── README.md               # This file

## How to Run

Follow these steps to set up the environment, ingest the data, and run the queries.

### Prerequisites

* **Docker and Docker Compose:** Ensure they are installed and running on your system.
* **Python 3:** Required to run the processing and analysis scripts. Install dependencies with `pip install httpx kafka-python`.
* **Dataset:** Download the [Amazon Music Reviews dataset](http://snap.stanford.edu/data/web-Amazon.html) and place the `Music.txt.gz` file in the root of the project directory.
* **A command-line tool** like `curl` or a REST client (e.g., Postman) to interact with the OpenSearch API.

### Step 1: Start the Services

Start the OpenSearch and OpenSearch Dashboards containers in the background.

```bash
docker-compose up -d
```

Wait a few moments for the services to initialize. You can verify that OpenSearch is running by sending a request to its API:

```bash
curl http://localhost:9200
```

You should receive a JSON response with cluster information.

### Step 2: Create the OpenSearch Index and Pipeline
First, create the OpenSearch index with the custom mappings and analyzers defined in `mappings/amazon-music.json`
```bash
# Delete the index if it already exists (optional)
curl -X DELETE "http://localhost:9200/amazon-music-reviews"

# Create the index
curl -X PUT "http://localhost:9200/amazon-music-reviews" \
     -H "Content-Type: application/json" \
     --data-binary "@mappings/amazon-music.json"
```

Next, create the ingest pipeline that will be used to compute the `helpfulness_ratio` on the fly.
```bash
curl -X PUT "http://localhost:9200/_ingest/pipeline/compute_fields" \
     -H "Content-Type: application/json" \
     --data-binary "@pipelines/compute_fields.json"
```
### Step 3: Preprocess the Dataset
Run the `parse_stream.py` script to clean the raw dataset and convert it into a newline-delimited JSON (`.ndjson`) file. This script handles malformed lines, normalizes text, and prepares the data for ingestion.

```bash
python src/parse_stream.py --input Music.txt.gz > reviews.ndjson
```
This will create a `reviews.ndjson` file in your project directory.
### Step 4: Ingest Data into OpenSearch
Use the `bulk_ingest.py` script to efficiently load the `reviews.ndjson` file into your OpenSearch index. This script uses the bulk API and the `compute_fields` pipeline.

```bash
python src/bulk_ingest.py --index amazon-music-reviews --jsonl reviews.ndjson --pipeline compute_fields
```
After the script finishes, you can verify that the data has been ingested:
```bash
# Check the total document count
curl "http://localhost:9200/amazon-music-reviews/_count"

# See an example document
curl "http://localhost:9200/amazon-music-reviews/_search?size=1"
```

### Step 5: Run Queries
Execute the predefined queries against your data using the `run_queries.py` script.
```bash
# Run all queries
python src/run_queries.py --index amazon-music-reviews

# Run a specific query (e.g., q3)
python src/run_queries.py --index amazon-music-reviews --q q3
```

### Run Performance Benchmarks
Use the `bench.py` script to measure the performance of a specific query under different concurrency levels.
```bash
# Benchmark the query in 'queries/q1.json' with 4, 8, and 16 concurrent clients
python src/bench.py --query-file queries/q1.json --concurrency 4 8 16
```

The script will output detailed performance metrics, including throughput (QPS) and latency percentiles.

## About Project
[![Amazon Logo](https://upload.wikimedia.org/wikipedia/commons/a/a9/Amazon_logo.svg)](https://screenrec.com/share/xBIvDCXOZ8)


## Authors
- Ali Najar (401102701)
- Sadegh Mohammadian (401109477)
- Mazdak Teymourian (401101495)
- Maryam Shiran (400109446)
