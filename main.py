"""
FastAPI application – Cassandra query interface.

Endpoints
---------
GET  /health          – check Cassandra connectivity
GET  /keyspaces       – list all keyspaces
GET  /tables          – list tables in a keyspace  (?keyspace=system)
POST /query           – run a raw CQL SELECT query

Entity endpoints (iotcore keyspace)
------------------------------------
GET  /entity/{entity_id}/logs                – cs_tb_log
GET  /entity/{entity_id}/timeseries/latest   – ts_kv_latest_cf
GET  /entity/{entity_id}/keys                – ts_kv_latest_cf (key list)
GET  /entity/{entity_id}/partitions          – ts_kv_partitions_cf
GET  /entity/{entity_id}/timeseries          – ts_kv_cf
"""

import os
from contextlib import asynccontextmanager
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from db import get_session

load_dotenv()

# ---------------------------------------------------------------------------
# Lifespan – connect once on startup, close on shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Warm-up: establish the session before serving requests
    try:
        get_session()
        print(
            f"✅ Connected to Cassandra at "
            f"{os.getenv('CASSANDRA_IP')}:{os.getenv('CASSANDRA_PORT')}"
        )
    except Exception as exc:
        print(f"❌ Could not connect to Cassandra: {exc}")
    yield
    # Shutdown: nothing extra needed (driver handles cleanup)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Cassandra Query API",
    description="Simple FastAPI interface to inspect and query a Cassandra cluster.",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    cql: str
    """A CQL SELECT statement to execute."""

    keyspace: str | None = None
    """Optional keyspace to set before executing the query."""


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    row_count: int


# ---------------------------------------------------------------------------
# Routes – generic
# ---------------------------------------------------------------------------

@app.get("/health", summary="Cassandra health check")
def health_check():
    """Verify that the API can reach the Cassandra cluster."""
    try:
        session = get_session()
        result = session.execute("SELECT release_version FROM system.local")
        row = result.one()
        return {
            "status": "ok",
            "cassandra_version": row.release_version if row else "unknown",
            "host": os.getenv("CASSANDRA_IP"),
            "port": os.getenv("CASSANDRA_PORT"),
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Cassandra unreachable: {exc}")


@app.get("/keyspaces", summary="List all keyspaces")
def list_keyspaces():
    """Return all keyspace names visible to this cluster."""
    try:
        session = get_session()
        rows = session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces"
        )
        return {"keyspaces": [r.keyspace_name for r in rows]}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/tables", summary="List tables in a keyspace")
def list_tables(keyspace: str = Query(..., description="Keyspace name, e.g. system")):
    """Return all table names within the given keyspace."""
    try:
        session = get_session()
        rows = session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s",
            (keyspace,),
        )
        tables = [r.table_name for r in rows]
        if not tables:
            return {"keyspace": keyspace, "tables": [], "note": "Keyspace not found or empty"}
        return {"keyspace": keyspace, "tables": tables}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/query", response_model=QueryResponse, summary="Execute a CQL query")
def execute_query(request: QueryRequest):
    """
    Execute an arbitrary CQL **SELECT** statement.

    Optionally supply a `keyspace` to switch context before running the query.
    For safety only SELECT / DESCRIBE statements are accepted.
    """
    cql_lower = request.cql.strip().lower()
    if not (cql_lower.startswith("select") or cql_lower.startswith("describe")):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT / DESCRIBE queries are permitted.",
        )

    try:
        session = get_session()

        if request.keyspace:
            session.set_keyspace(request.keyspace)

        result = session.execute(request.cql)

        columns: list[str] = result.column_names or []
        rows: list[list[Any]] = [
            [str(v) if v is not None else None for v in row] for row in result
        ]

        return QueryResponse(columns=columns, rows=rows, row_count=len(rows))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Routes – entity_id (iotcore keyspace)
# ---------------------------------------------------------------------------

def _rows_to_dicts(result) -> list[dict]:
    """Convert a Cassandra ResultSet to a list of plain dicts."""
    cols = result.column_names or []
    return [
        {col: (str(v) if v is not None else None) for col, v in zip(cols, row)}
        for row in result
    ]


@app.get(
    "/entity/{entity_id}/logs",
    summary="[cs_tb_log] Get logs by entity_id",
    tags=["Entity"],
)
def get_entity_logs(
    entity_id: str,
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of rows to return"),
):
    """
    Returns records from the **cs_tb_log** table in the `iotcore` keyspace
    by `entity_id` (timeuuid format).

    - **entity_id**: UUID of the entity (timeuuid format)
    - **limit**: Record limit (default 100, max 1000)
    """
    try:
        session = get_session()
        rows = session.execute(
            f"SELECT entity_id, time, content, file, function, line "
            f"FROM iotcore.cs_tb_log WHERE entity_id = {entity_id} LIMIT {limit}"
        )
        data = _rows_to_dicts(rows)
        return {"entity_id": entity_id, "table": "cs_tb_log", "count": len(data), "rows": data}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/entity/{entity_id}/timeseries/latest",
    summary="[ts_kv_latest_cf] Get latest values by entity_id",
    tags=["Entity"],
)
def get_entity_latest(
    entity_id: str,
    entity_type: str = Query(..., description="Entity type, e.g. DEVICE"),
    key: str | None = Query(None, description="Filter by a specific key (optional)"),
):
    """
    Returns the latest records from **ts_kv_latest_cf** by `entity_id` + `entity_type`.

    - **entity_type**: E.g. `DEVICE`, `ASSET`, `CUSTOMER`, …
    - **key**: If provided, only returns the specific key (clustering column)
    """
    try:
        session = get_session()
        if key:
            cql = (
                f"SELECT entity_id, entity_type, key, ts, bool_v, dbl_v, str_v, long_v, json_v "
                f"FROM iotcore.ts_kv_latest_cf "
                f"WHERE entity_id = {entity_id} AND entity_type = '{entity_type}' AND key = '{key}'"
            )
        else:
            cql = (
                f"SELECT entity_id, entity_type, key, ts, bool_v, dbl_v, str_v, long_v, json_v "
                f"FROM iotcore.ts_kv_latest_cf "
                f"WHERE entity_id = {entity_id} AND entity_type = '{entity_type}'"
            )
        rows = session.execute(cql)
        data = _rows_to_dicts(rows)
        return {"entity_id": entity_id, "entity_type": entity_type, "table": "ts_kv_latest_cf", "count": len(data), "rows": data}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/entity/{entity_id}/keys",
    summary="[ts_kv_latest_cf] Get list of keys for an entity",
    tags=["Entity"],
)
def get_entity_keys(
    entity_id: str,
    entity_type: str = Query(..., description="Entity type, e.g. DEVICE"),
):
    """
    Returns a list of **keys** (telemetry keys) and their latest timestamps
    from the `ts_kv_latest_cf` table.
    """
    try:
        session = get_session()
        rows = session.execute(
            f"SELECT key, ts FROM iotcore.ts_kv_latest_cf "
            f"WHERE entity_id = {entity_id} AND entity_type = '{entity_type}'"
        )
        data = _rows_to_dicts(rows)
        return {"entity_id": entity_id, "entity_type": entity_type, "table": "ts_kv_latest_cf", "count": len(data), "keys": data}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/entity/{entity_id}/partitions",
    summary="[ts_kv_partitions_cf] Get partitions by entity_id",
    tags=["Entity"],
)
def get_entity_partitions(
    entity_id: str,
    entity_type: str = Query(..., description="Entity type, e.g. DEVICE"),
    key: str = Query(..., description="Telemetry key, e.g. temperature"),
):
    """
    Returns the **partitions** (time intervals) from `ts_kv_partitions_cf`
    for a specific `entity_id` + `entity_type` + `key`.
    """
    try:
        session = get_session()
        rows = session.execute(
            f"SELECT entity_id, entity_type, key, partition "
            f"FROM iotcore.ts_kv_partitions_cf "
            f"WHERE entity_id = {entity_id} AND entity_type = '{entity_type}' AND key = '{key}'"
        )
        data = _rows_to_dicts(rows)
        return {"entity_id": entity_id, "entity_type": entity_type, "key": key, "table": "ts_kv_partitions_cf", "count": len(data), "rows": data}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/entity/{entity_id}/timeseries",
    summary="[ts_kv_cf] Get time-series data by entity_id",
    tags=["Entity"],
)
def get_entity_timeseries(
    entity_id: str,
    entity_type: str = Query(..., description="Entity type, e.g. DEVICE"),
    key: str = Query(..., description="Telemetry key, e.g. temperature"),
    partition: int = Query(..., description="Partition (epoch ms), from /partitions endpoint"),
    limit: int = Query(100, ge=1, le=10000, description="Max number of data points"),
):
    """
    Returns time-series data from the **ts_kv_cf** table using the full partition key:
    `entity_id` + `entity_type` + `key` + `partition`.

    > **Tip**: Call `/entity/{entity_id}/partitions` first to get a list of valid partitions.
    """
    try:
        session = get_session()
        rows = session.execute(
            f"SELECT entity_id, entity_type, key, partition, ts, bool_v, dbl_v, str_v, long_v, json_v "
            f"FROM iotcore.ts_kv_cf "
            f"WHERE entity_id = {entity_id} AND entity_type = '{entity_type}' "
            f"AND key = '{key}' AND partition = {partition} "
            f"LIMIT {limit}"
        )
        data = _rows_to_dicts(rows)
        return {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "key": key,
            "partition": partition,
            "table": "ts_kv_cf",
            "count": len(data),
            "rows": data,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/entity/{entity_id}/timeseries/range",
    summary="[ts_kv_cf] Get time-series by time range (auto-resolves partitions)",
    tags=["Entity"],
)
def get_entity_timeseries_range(
    entity_id: str,
    entity_type: str = Query(..., description="Entity type, e.g. DEVICE"),
    key: str = Query(..., description="Telemetry key, e.g. temperature"),
    start_ts: int = Query(..., description="Start timestamp (epoch milliseconds)"),
    end_ts: int = Query(..., description="End timestamp (epoch milliseconds)"),
    limit: int = Query(1000, ge=1, le=50000, description="Max number of data points (default 1000)"),
):
    """
    Fetches time-series data from **ts_kv_cf** for an entity's `key`
    within the time range `[start_ts, end_ts]` — **without needing to know the partition**.

    The endpoint will automatically:
    1. Look up `ts_kv_partitions_cf` to find all partitions falling within the time range.
    2. Query each partition, filtering by `ts BETWEEN start_ts AND end_ts`.
    3. Merge and return the results sorted by `ts` ascending.

    **Example querying 1 month of data** (epoch ms):
    ```
    start_ts = 1706745600000   # 2024-02-01 00:00:00 UTC
    end_ts   = 1709251199000   # 2024-02-29 23:59:59 UTC
    ```

    > **Note**: ThingsBoard partitions by month — there is 1 partition per month.
    """
    if start_ts >= end_ts:
        raise HTTPException(status_code=400, detail="start_ts must be less than end_ts")

    try:
        session = get_session()

        # Step 1: Get all partitions for this entity+key
        partition_rows = session.execute(
            f"SELECT partition FROM iotcore.ts_kv_partitions_cf "
            f"WHERE entity_id = {entity_id} AND entity_type = '{entity_type}' AND key = '{key}'"
        )
        all_partitions = [r.partition for r in partition_rows]

        if not all_partitions:
            return {
                "entity_id": entity_id,
                "entity_type": entity_type,
                "key": key,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "partitions_queried": [],
                "count": 0,
                "rows": [],
            }

        # Step 2: Filter partitions that fall inside [start_ts, end_ts]
        # ThingsBoard partitions are usually start of month (epoch ms)
        # Partition is valid if: partition <= end_ts AND (partition_next > start_ts)
        # Simpler approach: take partition <= end_ts that is closest to start_ts
        relevant_partitions = [p for p in all_partitions if p <= end_ts]
        # Filter out very old partitions: keep partitions where
        # (partition + ~31 days) >= start_ts
        MS_PER_MONTH = 31 * 24 * 3600 * 1000
        relevant_partitions = [p for p in relevant_partitions if p + MS_PER_MONTH >= start_ts]

        if not relevant_partitions:
            return {
                "entity_id": entity_id,
                "entity_type": entity_type,
                "key": key,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "partitions_queried": [],
                "count": 0,
                "rows": [],
            }

        # Step 3: Query each partition, filtering by ts
        all_rows: list[Any] = []
        for partition in sorted(relevant_partitions):
            rows = session.execute(
                f"SELECT entity_id, entity_type, key, partition, ts, "
                f"bool_v, dbl_v, str_v, long_v, json_v "
                f"FROM iotcore.ts_kv_cf "
                f"WHERE entity_id = {entity_id} AND entity_type = '{entity_type}' "
                f"AND key = '{key}' AND partition = {partition} "
                f"AND ts >= {start_ts} AND ts <= {end_ts} "
                f"LIMIT {limit}"
            )
            all_rows.extend(_rows_to_dicts(rows))
            if len(all_rows) >= limit:
                break

        # Sort ascending by ts and apply global limit
        all_rows.sort(key=lambda r: int(r.get("ts", 0) or 0))
        all_rows_sliced = all_rows[:limit]

        return {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "key": key,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "partitions_queried": sorted(relevant_partitions),
            "table": "ts_kv_cf",
            "count": len(all_rows_sliced),
            "rows": all_rows_sliced,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Entry-point (python main.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
