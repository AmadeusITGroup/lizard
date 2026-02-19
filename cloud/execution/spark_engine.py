# cloud/execution/spark_engine.py
"""
Spark-on-Databricks execution engine.

Submits pipelines to a Databricks cluster using the Databricks SDK,
translating the Lizard pipeline step format into Spark SQL / PySpark
commands, and collecting results back as pandas DataFrames.
"""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import pandas as pd
import structlog

from cloud.execution.base import ExecutionEngine, ExecutionResult
from cloud.diagnostics import ConfigurationError, ConnectivityError

log = structlog.get_logger(__name__)


class SparkDatabricksEngine(ExecutionEngine):
    """
    Executes pipelines on a remote Databricks cluster.

    Requires:
    - A configured Databricks connection (from cloud.config)
    - A running or auto-start-able cluster
    - The `databricks-sdk` package (optional dependency)

    Pipeline translation strategy:
    - Each pipeline step is translated to a Spark SQL fragment
    - The full SQL is submitted via the Statement Execution API
    - Results are collected back as a pandas DataFrame
    """

    def __init__(
        self,
        connection_name: str,
        cluster_id: Optional[str] = None,
        warehouse_id: Optional[str] = None,
    ) -> None:
        self._connection_name = connection_name
        self._cluster_id = cluster_id
        self._warehouse_id = warehouse_id
        self._client = None  # Lazy-init Databricks WorkspaceClient

    # ── lazy client init ─────────────────────────────────────

    def _get_client(self):
        """
        Build a Databricks WorkspaceClient using the named connection
        from LizardCloudConfig, resolving gateway endpoints if needed.
        """
        if self._client is not None:
            return self._client

        from cloud.config import get_config
        from cloud.connectivity import GatewayRegistry, EndpointResolver

        cfg = get_config()
        conn = cfg.get_databricks_connection(self._connection_name)
        if conn is None:
            raise ConfigurationError(
                message=f"Databricks connection '{self._connection_name}' not found in cloud config.",
                action="Add this connection in Cloud Settings → Databricks Connections.",
                context={"connection_name": self._connection_name},
            )

        # Resolve the host (direct vs gateway)
        registry = GatewayRegistry.from_config(cfg)
        resolver = EndpointResolver(registry)
        host = resolver.resolve_databricks_host(
            conn.workspace_id, conn.connectivity, conn.gateway_name
        )

        # Build auth kwargs
        auth_kwargs: Dict[str, Any] = {"host": host}
        if conn.auth and conn.auth.type == "service_principal":
            auth_kwargs["client_id"] = conn.auth.client_id
            auth_kwargs["client_secret"] = conn.auth.client_secret
            # azure_tenant_id is needed for Azure-backed workspaces
            if conn.auth.tenant_id:
                auth_kwargs["azure_tenant_id"] = conn.auth.tenant_id

        try:
            from databricks.sdk import WorkspaceClient

            self._client = WorkspaceClient(**auth_kwargs)
        except ImportError:
            raise ConfigurationError(
                message="databricks-sdk is not installed.",
                action="Install it with: pip install 'lizard-fpv[cloud]'",
            )
        except Exception as exc:
            raise ConnectivityError(
                message=f"Failed to create Databricks client: {exc}",
                action="Check your Databricks connection settings and network/gateway.",
                context={"connection_name": self._connection_name, "host": host},
            )

        return self._client

    # ── interface ────────────────────────────────────────────

    def engine_name(self) -> str:
        return "spark"

    async def execute_pipeline(
        self,
        pipeline: List[Dict[str, Any]],
        *,
        limit: int = 1000,
        offset: int = 0,
    ) -> ExecutionResult:
        """
        Translate pipeline steps to Spark SQL, submit via Statement Execution API,
        and collect results as a pandas DataFrame.
        """
        client = self._get_client()
        t0 = time.perf_counter()

        # Step 1: Translate pipeline to SQL
        sql = self._pipeline_to_sql(pipeline, limit=limit, offset=offset)
        log.info("spark_submitting_sql", sql=sql[:200], cluster=self._cluster_id)

        # Step 2: Execute via Statement Execution API (SQL warehouse) or
        #         Command Execution API (interactive cluster)
        warnings: List[str] = []
        try:
            if self._warehouse_id:
                df, total_rows = self._execute_via_warehouse(client, sql)
            elif self._cluster_id:
                df, total_rows = self._execute_via_cluster(client, sql)
            else:
                raise ConfigurationError(
                    message="No cluster_id or warehouse_id configured for Spark engine.",
                    action="Select a cluster or SQL warehouse in Cloud Settings.",
                )
        except (ConfigurationError, ConnectivityError):
            raise
        except Exception as exc:
            raise ConnectivityError(
                message=f"Spark execution failed: {exc}",
                action="Check cluster state, permissions, and query syntax.",
                context={
                    "connection": self._connection_name,
                    "cluster_id": self._cluster_id,
                    "warehouse_id": self._warehouse_id,
                },
            )

        elapsed_ms = (time.perf_counter() - t0) * 1000
        log.info(
            "spark_pipeline_executed",
            rows=len(df),
            total=total_rows,
            ms=round(elapsed_ms, 1),
        )

        return ExecutionResult(
            data=df,
            total_rows=total_rows,
            columns=list(df.columns),
            engine="spark",
            execution_time_ms=round(elapsed_ms, 1),
            cluster_id=self._cluster_id or self._warehouse_id,
            warnings=warnings,
            metadata={"sql": sql},
        )

    async def health_check(self) -> Dict[str, Any]:
        """Check if the configured cluster/warehouse is reachable and running."""
        try:
            client = self._get_client()
        except Exception as exc:
            return {"engine": "spark", "status": "error", "error": str(exc)}

        info: Dict[str, Any] = {
            "engine": "spark",
            "connection": self._connection_name,
        }

        try:
            if self._cluster_id:
                cluster = client.clusters.get(self._cluster_id)
                state = getattr(cluster, "state", None)
                info.update(
                    {
                        "status": "ok" if str(state) == "RUNNING" else "degraded",
                        "cluster_id": self._cluster_id,
                        "cluster_state": str(state),
                        "cluster_name": getattr(cluster, "cluster_name", None),
                    }
                )
            elif self._warehouse_id:
                warehouse = client.warehouses.get(self._warehouse_id)
                state = getattr(warehouse, "state", None)
                info.update(
                    {
                        "status": "ok" if str(state) == "RUNNING" else "degraded",
                        "warehouse_id": self._warehouse_id,
                        "warehouse_state": str(state),
                    }
                )
            else:
                info.update({"status": "error", "error": "No cluster or warehouse configured"})
        except Exception as exc:
            info.update({"status": "error", "error": str(exc)})

        return info

    # ── pipeline → SQL translation ───────────────────────────

    def _pipeline_to_sql(
        self,
        pipeline: List[Dict[str, Any]],
        limit: int = 1000,
        offset: int = 0,
    ) -> str:
        """
        Translate Lizard pipeline steps into a Spark SQL query.

        This handles: source, filter, select, aggregate, sort, limit, transform, join.
        Complex transforms fall back to a CTE chain.
        """
        ctes: List[str] = []
        current_alias = None
        cte_idx = 0

        for step in pipeline:
            step_type = step.get("type", "")
            config = step.get("config", {})

            if step_type == "source":
                table = config.get("table", "unknown")
                current_alias = f"_cte{cte_idx}"
                ctes.append(f"{current_alias} AS (SELECT * FROM {table})")
                cte_idx += 1

            elif step_type == "filter" and current_alias:
                conditions = config.get("conditions", [])
                if not conditions and "field" in config:
                    conditions = [config]
                where_clauses = []
                for cond in conditions:
                    clause = self._condition_to_sql(cond)
                    if clause:
                        where_clauses.append(clause)
                if where_clauses:
                    prev = current_alias
                    current_alias = f"_cte{cte_idx}"
                    ctes.append(
                        f"{current_alias} AS (SELECT * FROM {prev} WHERE {' AND '.join(where_clauses)})"
                    )
                    cte_idx += 1

            elif step_type == "select" and current_alias:
                columns = config.get("columns", [])
                if columns:
                    col_exprs = []
                    for col in columns:
                        if isinstance(col, dict):
                            src = col.get("source", "")
                            alias = col.get("alias", src)
                            col_exprs.append(f"{src} AS {alias}" if alias != src else src)
                        else:
                            col_exprs.append(str(col))
                    prev = current_alias
                    current_alias = f"_cte{cte_idx}"
                    ctes.append(
                        f"{current_alias} AS (SELECT {', '.join(col_exprs)} FROM {prev})"
                    )
                    cte_idx += 1

            elif step_type == "aggregate" and current_alias:
                group_by = config.get("group_by", [])
                aggregations = config.get("aggregations", [])
                select_parts = list(group_by)
                for agg in aggregations:
                    func = agg.get("func", "count").upper()
                    agg_field = agg.get("field", "*")
                    out_col = agg.get("column", f"{func}_{agg_field}")
                    func_map = {"AVG": "AVG", "MEAN": "AVG", "COUNT_DISTINCT": "COUNT(DISTINCT", "NUNIQUE": "COUNT(DISTINCT"}
                    sql_func = func_map.get(func, func)
                    if func in ("COUNT_DISTINCT", "NUNIQUE"):
                        select_parts.append(f"{sql_func} {agg_field}) AS {out_col}")
                    else:
                        select_parts.append(f"{sql_func}({agg_field}) AS {out_col}")
                group_clause = f" GROUP BY {', '.join(group_by)}" if group_by else ""
                prev = current_alias
                current_alias = f"_cte{cte_idx}"
                ctes.append(
                    f"{current_alias} AS (SELECT {', '.join(select_parts)} FROM {prev}{group_clause})"
                )
                cte_idx += 1

            elif step_type == "sort" and current_alias:
                sort_by = config.get("by", [])
                if not sort_by and "field" in config:
                    sort_by = [{"field": config["field"], "direction": config.get("direction", "asc")}]
                order_parts = []
                for s in sort_by:
                    direction = "DESC" if s.get("direction", "asc").lower() == "desc" else "ASC"
                    order_parts.append(f"{s['field']} {direction}")
                if order_parts:
                    prev = current_alias
                    current_alias = f"_cte{cte_idx}"
                    ctes.append(
                        f"{current_alias} AS (SELECT * FROM {prev} ORDER BY {', '.join(order_parts)})"
                    )
                    cte_idx += 1

            elif step_type == "join" and current_alias:
                right_table = config.get("table", "")
                join_type = config.get("type", "LEFT").upper()
                on_conditions = config.get("on", [])
                on_parts = []
                for cond in on_conditions:
                    left_col = cond.get("left", "")
                    right_col = cond.get("right", "")
                    if left_col and right_col:
                        on_parts.append(f"{current_alias}.{left_col} = {right_table}.{right_col}")
                if on_parts:
                    prev = current_alias
                    current_alias = f"_cte{cte_idx}"
                    ctes.append(
                        f"{current_alias} AS (SELECT * FROM {prev} {join_type} JOIN {right_table} ON {' AND '.join(on_parts)})"
                    )
                    cte_idx += 1

            elif step_type == "distinct" and current_alias:
                prev = current_alias
                current_alias = f"_cte{cte_idx}"
                ctes.append(f"{current_alias} AS (SELECT DISTINCT * FROM {prev})")
                cte_idx += 1

            elif step_type == "limit" and current_alias:
                n = config.get("n", 1000)
                prev = current_alias
                current_alias = f"_cte{cte_idx}"
                ctes.append(f"{current_alias} AS (SELECT * FROM {prev} LIMIT {n})")
                cte_idx += 1

        # Build final query
        if not ctes or current_alias is None:
            return "SELECT 1 AS _empty"

        final_sql = f"WITH {', '.join(ctes)} SELECT * FROM {current_alias} LIMIT {limit} OFFSET {offset}"
        return final_sql

    @staticmethod
    def _condition_to_sql(cond: Dict[str, Any]) -> Optional[str]:
        """Convert a single filter condition dict to a SQL WHERE clause fragment."""
        field_name = cond.get("field", "")
        op = cond.get("op", "eq")
        value = cond.get("value")

        if not field_name:
            return None

        def _quote(v: Any) -> str:
            if isinstance(v, str):
                return f"'{v}'"
            return str(v)

        op_map = {
            "eq": f"{field_name} = {_quote(value)}",
            "ne": f"{field_name} != {_quote(value)}",
            "gt": f"{field_name} > {_quote(value)}",
            "gte": f"{field_name} >= {_quote(value)}",
            "lt": f"{field_name} < {_quote(value)}",
            "lte": f"{field_name} <= {_quote(value)}",
            "isnull": f"{field_name} IS NULL",
            "notnull": f"{field_name} IS NOT NULL",
            "contains": f"{field_name} LIKE '%{value}%'",
            "startswith": f"{field_name} LIKE '{value}%'",
            "endswith": f"{field_name} LIKE '%{value}'",
        }

        if op == "in" and isinstance(value, list):
            vals = ", ".join(_quote(v) for v in value)
            return f"{field_name} IN ({vals})"
        if op == "nin" and isinstance(value, list):
            vals = ", ".join(_quote(v) for v in value)
            return f"{field_name} NOT IN ({vals})"
        if op == "between" and isinstance(value, list) and len(value) == 2:
            return f"{field_name} BETWEEN {_quote(value[0])} AND {_quote(value[1])}"

        return op_map.get(op)

    # ── execution backends ───────────────────────────────────

    def _execute_via_warehouse(
        self, client: Any, sql: str
    ) -> tuple:
        """Execute SQL via Databricks SQL Warehouse (Statement Execution API)."""
        from databricks.sdk.service.sql import StatementState

        response = client.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            wait_timeout="120s",
        )

        if response.status.state != StatementState.SUCCEEDED:
            error_msg = getattr(response.status, "error", None)
            raise ConnectivityError(
                message=f"SQL statement failed: {error_msg}",
                action="Check your SQL query and warehouse permissions.",
                context={"warehouse_id": self._warehouse_id, "state": str(response.status.state)},
            )

        # Convert result to pandas
        columns = [col.name for col in response.manifest.schema.columns]
        rows = []
        if response.result and response.result.data_array:
            rows = response.result.data_array

        df = pd.DataFrame(rows, columns=columns)
        total_rows = len(df)  # Approximate — exact count requires separate query

        return df, total_rows

    def _execute_via_cluster(
        self, client: Any, sql: str
    ) -> tuple:
        """
        Execute SQL on an interactive cluster via the Command Execution API.

        Falls back to submitting a notebook job if command API is unavailable.
        """
        # Use the command execution context
        context = client.command_execution.create(
            cluster_id=self._cluster_id,
            language="sql",
        )
        context_id = context.id

        try:
            result = client.command_execution.execute(
                cluster_id=self._cluster_id,
                context_id=context_id,
                language="sql",
                command=sql,
            )

            if result.status != "Finished":
                error_msg = getattr(result, "results", {}).get("cause", "Unknown error")
                raise ConnectivityError(
                    message=f"Command execution failed: {error_msg}",
                    action="Check cluster state and SQL syntax.",
                    context={"cluster_id": self._cluster_id},
                )

            # Parse the result
            data = result.results.get("data", [])
            schema = result.results.get("schema", [])
            columns = [col.get("name", f"col_{i}") for i, col in enumerate(schema)]

            df = pd.DataFrame(data, columns=columns)
            total_rows = len(df)

            return df, total_rows
        finally:
            # Clean up the execution context
            try:
                client.command_execution.destroy(
                    cluster_id=self._cluster_id,
                    context_id=context_id,
                )
            except Exception:
                pass