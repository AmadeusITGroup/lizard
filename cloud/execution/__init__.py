# cloud/execution/__init__.py
"""
Lizard Execution Engine Package.

Provides the ExecutionEngine abstraction and concrete implementations:
  - LocalPandasEngine: wraps existing PipelineExecutor (pandas, in-process)
  - SparkDatabricksEngine: submits pipelines to a Databricks cluster
"""