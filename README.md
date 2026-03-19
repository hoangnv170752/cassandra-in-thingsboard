# Cassandra in ThingsBoard API

A lightweight FastAPI application designed to provide a direct read-only REST API interface to query data from a ThingsBoard **Cassandra** database (`iotcore` keyspace).

ThingsBoard natively stores massive amounts of time-series data (telemetry), entity attributes, and logs in Cassandra. This project helps developers and engineers easily query that time-series data without writing raw CQL.

## Features

- **Health Check**: Verify connectivity to the Cassandra cluster.
- **Database Inspection**: List keyspaces and tables.
- **Raw Queries**: Execute safe raw CQL queries (only `SELECT` and `DESCRIBE`).
- **Entity Specific Endpoints (iotcore)**:
  - `GET /entity/{entity_id}/logs`: Fetch logs (`cs_tb_log`) for an entity.
  - `GET /entity/{entity_id}/keys`: List all telemetry keys recorded for an entity.
  - `GET /entity/{entity_id}/timeseries/latest`: Fetch the latest known telemetry values.
  - `GET /entity/{entity_id}/partitions`: List all available time partitions for a specific telemetry key.
  - `GET /entity/{entity_id}/timeseries`: Fetch time-series data for a single partition.
  - `GET /entity/{entity_id}/timeseries/range`: **(Recommended)** Automatically resolve partitions and fetch telemetry data across any given start and end epoch timestamp.

## Requirements

- Python 3.10+
- A running instance of Cassandra (configured by ThingsBoard)

## Installation

1. **Clone the repository and enter the directory**:
   ```bash
   git clone <your-repo>
   cd cassandra-in-thingsboard
   ```

2. **Create a virtual environment and install dependencies**:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Environment Setup**:
   Copy the example environment file:
   ```bash
   cp .env.example .env
   ```
   Open `.env` and configure your Cassandra connection:
   ```env
   CASSANDRA_IP=127.0.0.1
   CASSANDRA_PORT=9042
   ```

## Usage

1. **Start the FastAPI server**:
   ```bash
   python main.py
   ```
   Alternatively, run with native Uvicorn:
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8000 --reload
   ```

2. **View Interactive API Documentation**:
   Open your browser and navigate to:
   [http://localhost:8000/docs](http://localhost:8000/docs)
   From there, you can interact with all endpoints directly through the Swagger UI.

## Examples using `curl`

**Check health:**
```bash
curl http://localhost:8000/health
```

**Fetch the latest telemetry values for a Device:**
```bash
# Example entity_id: 11223344-5566-7788-99aa-bbccddeeff00
curl "http://localhost:8000/entity/11223344-5566-7788-99aa-bbccddeeff00/timeseries/latest?entity_type=DEVICE"
```

**Fetch a time-range of 'temperature' telemetry data (auto-partitioning):**
```bash
# start_ts and end_ts are in Epoch milliseconds
curl "http://localhost:8000/entity/11223344-5566-7788-99aa-bbccddeeff00/timeseries/range?entity_type=DEVICE&key=temperature&start_ts=1700000000000&end_ts=1730000000000"
```

## Structure

- `main.py`: FastAPI application routing, request validation, and endpoint logic.
- `db.py`: Cassandra driver cluster connection, pooling, and session singleton management.
- `requirements.txt`: Python package dependencies.
