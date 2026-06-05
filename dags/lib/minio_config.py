"""Central MinIO/S3 credential resolution for DAGs.

Credentials are read from the environment at runtime — there are deliberately
**no hardcoded secret defaults** (the access/secret keys must be injected via
the Airflow environment, e.g. infra/airflow/airflow.env, or a secrets backend).
The endpoint is not a secret, so it keeps a sensible local default.

This removes the ``minioadmin123`` literals that previously lived in DAG code.
"""

import os
from typing import Any


def get_minio_settings() -> dict[str, Any]:
    """Return MinIO connection settings as a kwargs dict.

    Returns
    -------
    dict
        ``{minio_endpoint, minio_access_key, minio_secret_key}`` suitable for
        the ``extract_*_to_minio`` helpers and the boto3 client builder.

    Raises
    ------
    RuntimeError
        If ``MINIO_ACCESS_KEY`` or ``MINIO_SECRET_KEY`` is not set, so a
        misconfigured deployment fails fast instead of silently using a
        well-known default credential.
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        raise RuntimeError(
            "MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set in the environment "
            "(no hardcoded defaults). Provide them via the Airflow environment "
            "file (infra/airflow/airflow.env) or a secrets backend."
        )

    return {
        "minio_endpoint": endpoint,
        "minio_access_key": access_key,
        "minio_secret_key": secret_key,
    }


def make_s3_client(settings: dict[str, Any] | None = None):
    """Build a boto3 S3 client for MinIO from *settings* (or the environment)."""
    import boto3

    cfg = settings or get_minio_settings()
    return boto3.client(
        "s3",
        endpoint_url=cfg["minio_endpoint"],
        aws_access_key_id=cfg["minio_access_key"],
        aws_secret_access_key=cfg["minio_secret_key"],
        region_name="us-east-1",
    )
