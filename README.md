# DataBase-Project

# Amazon Music Reviews Analysis Project

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

