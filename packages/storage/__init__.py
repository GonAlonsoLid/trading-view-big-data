"""
Storage backends for data persistence.

Available backends:
- parquet_writer: Local Parquet writer with partitioning

Future:
- hdfs_writer: HDFS writer for distributed storage
- hive_writer: Hive metastore integration
"""

from packages.storage.parquet_writer import ParquetPartitionedWriter

__all__ = ["ParquetPartitionedWriter"]

