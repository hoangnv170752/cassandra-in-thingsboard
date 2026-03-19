"""
Cassandra connection helper.
Reads CASSANDRA_IP and CASSANDRA_PORT from .env (or environment).
"""

import os
from dotenv import load_dotenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy

load_dotenv()

CASSANDRA_IP = os.getenv("CASSANDRA_IP", "127.0.0.1")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))


def get_cluster() -> Cluster:
    """Return a Cassandra Cluster instance (unauthenticated by default)."""
    return Cluster(
        contact_points=[CASSANDRA_IP],
        port=CASSANDRA_PORT,
        load_balancing_policy=DCAwareRoundRobinPolicy(),
        protocol_version=4,
    )


# Module-level singleton session (lazy-initialised)
_session = None


def get_session():
    """Return a reusable Cassandra session, creating it on first call."""
    global _session
    if _session is None or _session.is_shutdown:
        cluster = get_cluster()
        _session = cluster.connect()          # connect without a keyspace
    return _session
