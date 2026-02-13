"""
Dynamic Incremental Load Framework for Microsoft Fabric Lakehouse
Supports flexible, configurable incremental loading for any source/destination tables
Implements CDC patterns, watermarking, and full data pipeline orchestration
"""

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, concat, md5, when, max, lit, coalesce, current_timestamp,
    regexp_replace, trim, cast, row_number, lag, datediff
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    BooleanType, LongType, DecimalType
)
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from typing import Optional, List, Dict, Tuple, Any
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from enum import Enum
import json
import logging
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ==================== ENUMS & CONFIGURATION ====================

class LoadType(Enum):
    """Supported incremental load types"""
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"
    CHANGE_DATA_CAPTURE = "CDC"
    UPSERT = "UPSERT"
    APPEND = "APPEND"


class MergeStrategy(Enum):
    """Merge strategy options"""
    MATCH_ON_KEY = "MATCH_ON_KEY"
    MATCH_ON_MULTIPLE_KEYS = "MATCH_ON_MULTIPLE_KEYS"
    MATCH_ON_NATURAL_KEY = "MATCH_ON_NATURAL_KEY"


@dataclass
class IncrementalLoadConfig:
    """Configuration for incremental load operations"""
    
    # Basic configuration
    source_table: str
    destination_table: str
    load_type: LoadType = LoadType.INCREMENTAL
    
    # Key configuration
    primary_key: str = None  # Single key or comma-separated for composite keys
    primary_keys: List[str] = field(default_factory=list)  # Explicit list for composite
    
    # Watermark configuration (for incremental loads)
    watermark_column: str = None  # Timestamp column for watermarking
    watermark_value: Optional[str] = None  # Last watermark value
    
    # Change tracking configuration
    change_indicator_column: str = None  # For CDC (e.g., 'is_deleted', 'operation_type')
    change_indicator_values: List[str] = field(default_factory=list)  # Values indicating changes
    
    # Hash configuration
    hash_columns: List[str] = field(default_factory=list)  # Columns to hash for SCD Type 2
    generate_hash_key: bool = True
    
    # Metadata configuration
    include_metadata: bool = True
    inserted_timestamp_column: str = "inserted_timestamp"
    updated_timestamp_column: str = "updated_timestamp"
    
    # Merge configuration
    merge_strategy: MergeStrategy = MergeStrategy.MATCH_ON_KEY
    merge_conditions: List[Tuple[str, str]] = field(default_factory=list)  # Custom conditions
    
    # Performance tuning
    repartition_count: int = 50
    parallel_writes: bool = True
    
    # Validation
    skip_validation: bool = False
    validation_rules: Dict[str, Any] = field(default_factory=dict)
    
    # Partitioning
    partition_columns: List[str] = field(default_factory=list)
    
    # Schema evolution
    allow_schema_evolution: bool = True
    
    def get_primary_keys(self) -> List[str]:
        """Get primary keys as list"""
        if self.primary_keys:
            return self.primary_keys
        if self.primary_key:
            return [k.strip() for k in self.primary_key.split(',')]
        raise ValueError("No primary key configured")


@dataclass
class LoadStatistics:
    """Statistics for load operation"""
    load_type: str
    source_count: int = 0
    destination_count_before: int = 0
    destination_count_after: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_deleted: int = 0
    records_unchanged: int = 0
    load_duration_seconds: float = 0
    start_time: str = ""
    end_time: str = ""
    status: str = "PENDING"
    error_message: str = ""
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return asdict(self)


# ==================== BASE LOADER CLASS ====================

class BaseIncrementalLoader(ABC):
    """Abstract base class for incremental loaders"""
    
    def __init__(self, workspace_path: str = "/Workspace/Users/default"):
        """
        Initialize the loader
        
        Args:
            workspace_path: Path to Fabric workspace
        """
        self.spark = SparkSession.builder \
            .appName("DynamicIncrementalLoader") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()
        
        self.workspace_path = workspace_path
        self.load_stats = None
    
    @abstractmethod
    def load(self, config: IncrementalLoadConfig) -> LoadStatistics:
        """Execute the load operation"""
        pass
    
    def read_table(self, table_path: str, format: str = "delta") -> DataFrame:
        """
        Read a table/path
        
        Args:
            table_path: Path to table
            format: Format (delta, parquet, csv, etc.)
            
        Returns:
            DataFrame
        """
        try:
            return self.spark.read.format(format).load(table_path)
        except Exception as e:
            logger.error(f"Error reading {table_path}: {str(e)}")
            raise
    
    def write_table(
        self,
        df: DataFrame,
        table_path: str,
        mode: str = "overwrite",
        format: str = "delta",
        partition_by: Optional[List[str]] = None
    ) -> None:
        """
        Write a DataFrame to table
        
        Args:
            df: DataFrame to write
            table_path: Target path
            mode: Write mode (overwrite, append, ignore, error)
            format: Format
            partition_by: Columns to partition by
        """
        try:
            writer = df.write.format(format).mode(mode)
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.save(table_path)
            logger.info(f"✅ Written {df.count()} records to {table_path}")
        except Exception as e:
            logger.error(f"Error writing to {table_path}: {str(e)}")
            raise
    
    def add_metadata_columns(
        self,
        df: DataFrame,
        config: IncrementalLoadConfig,
        is_insert: bool = True
    ) -> DataFrame:
        """
        Add metadata columns (timestamps, source info)
        
        Args:
            df: Input DataFrame
            config: Load configuration
            is_insert: Whether this is for insert or update
            
        Returns:
            DataFrame with metadata columns
        """
        if not config.include_metadata:
            return df
        
        timestamp = current_timestamp()
        
        if is_insert:
            df = df.withColumn(config.inserted_timestamp_column, timestamp)
        else:
            df = df.withColumn(config.updated_timestamp_column, timestamp)
        
        return df
    
    def get_table_count(self, table_path: str) -> int:
        """Get record count for a table"""
        try:
            df = self.read_table(table_path)
            return df.count()
        except Exception:
            return 0


# ==================== FULL LOAD IMPLEMENTATION ====================

class FullLoadProcessor(BaseIncrementalLoader):
    """Processor for full load operations"""
    
    def load(self, config: IncrementalLoadConfig) -> LoadStatistics:
        """
        Execute full load - truncate and reload entire table
        
        Args:
            config: Load configuration
            
        Returns:
            LoadStatistics
        """
        logger.info(f"🔄 Starting FULL LOAD: {config.source_table} → {config.destination_table}")
        
        start_time = datetime.now()
        stats = LoadStatistics(
            load_type="FULL",
            start_time=start_time.isoformat()
        )
        
        try:
            # Read source
            source_df = self.read_table(config.source_table)
            stats.source_count = source_df.count()
            stats.destination_count_before = self.get_table_count(config.destination_table)
            
            # Add metadata
            source_df = self.add_metadata_columns(source_df, config, is_insert=True)
            
            # Add hash key if configured
            if config.generate_hash_key and config.hash_columns:
                source_df = self._add_hash_key(source_df, config.hash_columns)
            
            # Write with partition if configured
            if config.partition_columns:
                self.write_table(
                    source_df,
                    config.destination_table,
                    mode="overwrite",
                    partition_by=config.partition_columns
                )
            else:
                self.write_table(source_df, config.destination_table, mode="overwrite")
            
            stats.records_inserted = stats.source_count
            stats.destination_count_after = stats.source_count
            stats.status = "SUCCESS"
            
        except Exception as e:
            stats.status = "FAILED"
            stats.error_message = str(e)
            logger.error(f"❌ FULL LOAD FAILED: {str(e)}")
            raise
        
        finally:
            stats.end_time = datetime.now().isoformat()
            stats.load_duration_seconds = (datetime.fromisoformat(stats.end_time) - 
                                          datetime.fromisoformat(stats.start_time)).total_seconds()
        
        return stats
    
    def _add_hash_key(self, df: DataFrame, hash_columns: List[str]) -> DataFrame:
        """Add hash key column"""
        concat_expr = concat(*[
            coalesce(col(c).cast(StringType()), lit(""))
            for c in hash_columns
        ])
        return df.withColumn("hash_key", md5(concat_expr))


# ==================== INCREMENTAL LOAD IMPLEMENTATION ====================

class IncrementalLoadProcessor(BaseIncrementalLoader):
    """Processor for incremental loads using watermarking"""
    
    def load(self, config: IncrementalLoadConfig) -> LoadStatistics:
        """
        Execute incremental load using watermark/timestamp
        
        Args:
            config: Load configuration
            
        Returns:
            LoadStatistics
        """
        logger.info(f"🔄 Starting INCREMENTAL LOAD: {config.source_table} → {config.destination_table}")
        
        if not config.watermark_column:
            raise ValueError("Watermark column required for incremental load")
        
        start_time = datetime.now()
        stats = LoadStatistics(
            load_type="INCREMENTAL",
            start_time=start_time.isoformat()
        )
        
        try:
            stats.destination_count_before = self.get_table_count(config.destination_table)
            
            # Get last watermark value
            last_watermark = self._get_last_watermark(
                config.destination_table,
                config.watermark_column
            )
            
            logger.info(f"Last watermark value: {last_watermark}")
            
            # Read incremental data
            source_df = self.read_table(config.source_table)
            
            # Filter by watermark
            if last_watermark:
                source_df = source_df.filter(
                    col(config.watermark_column) > lit(last_watermark)
                )
            
            stats.source_count = source_df.count()
            
            if stats.source_count == 0:
                logger.info("No new data to load")
                stats.status = "SUCCESS"
                return stats
            
            # Add metadata
            source_df = self.add_metadata_columns(source_df, config, is_insert=True)
            
            # Append to destination
            self.write_table(
                source_df,
                config.destination_table,
                mode="append",
                partition_by=config.partition_columns if config.partition_columns else None
            )
            
            stats.records_inserted = stats.source_count
            stats.destination_count_after = (
                stats.destination_count_before + stats.source_count
            )
            stats.status = "SUCCESS"
            
        except Exception as e:
            stats.status = "FAILED"
            stats.error_message = str(e)
            logger.error(f"❌ INCREMENTAL LOAD FAILED: {str(e)}")
            raise
        
        finally:
            stats.end_time = datetime.now().isoformat()
            stats.load_duration_seconds = (datetime.fromisoformat(stats.end_time) - 
                                          datetime.fromisoformat(stats.start_time)).total_seconds()
        
        return stats
    
    def _get_last_watermark(self, table_path: str, watermark_column: str) -> Optional[str]:
        """Get last watermark value from destination table"""
        try:
            dest_df = self.read_table(table_path)
            watermark = dest_df.agg(max(col(watermark_column))).collect()[0][0]
            return watermark
        except Exception:
            logger.info(f"Could not retrieve last watermark (table may not exist)")
            return None


# ==================== UPSERT/MERGE IMPLEMENTATION ====================

class UpsertLoadProcessor(BaseIncrementalLoader):
    """Processor for upsert operations using Delta merge"""
    
    def load(self, config: IncrementalLoadConfig) -> LoadStatistics:
        """
        Execute upsert using Delta merge
        
        Args:
            config: Load configuration
            
        Returns:
            LoadStatistics
        """
        logger.info(f"🔄 Starting UPSERT LOAD: {config.source_table} → {config.destination_table}")
        
        start_time = datetime.now()
        stats = LoadStatistics(
            load_type="UPSERT",
            start_time=start_time.isoformat()
        )
        
        try:
            # Get primary keys
            primary_keys = config.get_primary_keys()
            
            stats.destination_count_before = self.get_table_count(config.destination_table)
            
            # Read source
            source_df = self.read_table(config.source_table)
            stats.source_count = source_df.count()
            
            # Add metadata and hash key
            source_df = self.add_metadata_columns(source_df, config, is_insert=True)
            
            if config.generate_hash_key and config.hash_columns:
                source_df = self._add_hash_key(source_df, config.hash_columns)
            
            # Perform merge
            merge_stats = self._execute_merge(
                source_df,
                config.destination_table,
                primary_keys,
                config
            )
            
            stats.records_inserted = merge_stats.get("inserted", 0)
            stats.records_updated = merge_stats.get("updated", 0)
            stats.records_deleted = merge_stats.get("deleted", 0)
            stats.destination_count_after = (
                stats.destination_count_before + 
                stats.records_inserted - 
                stats.records_deleted
            )
            stats.status = "SUCCESS"
            
        except Exception as e:
            stats.status = "FAILED"
            stats.error_message = str(e)
            logger.error(f"❌ UPSERT LOAD FAILED: {str(e)}")
            raise
        
        finally:
            stats.end_time = datetime.now().isoformat()
            stats.load_duration_seconds = (datetime.fromisoformat(stats.end_time) - 
                                          datetime.fromisoformat(stats.start_time)).total_seconds()
        
        return stats
    
    def _execute_merge(
        self,
        source_df: DataFrame,
        table_path: str,
        primary_keys: List[str],
        config: IncrementalLoadConfig
    ) -> Dict[str, int]:
        """
        Execute Delta merge operation
        
        Args:
            source_df: Source DataFrame
            table_path: Target table path
            primary_keys: Primary key columns
            config: Load configuration
            
        Returns:
            Dictionary with merge statistics
        """
        # Repartition for performance
        source_df = source_df.repartition(
            config.repartition_count,
            *primary_keys
        )
        
        merge_condition = " AND ".join([
            f"target.{pk} = source.{pk}" for pk in primary_keys
        ])
        
        # Add custom merge conditions if provided
        if config.merge_conditions:
            for left, right in config.merge_conditions:
                merge_condition += f" AND {left} = {right}"
        
        try:
            target_table = DeltaTable.forPath(self.spark, table_path)
            
            merge_builder = target_table.alias("target") \
                .merge(source_df.alias("source"), merge_condition)
            
            # Check if hash key exists for change detection
            if "hash_key" in source_df.columns and "hash_key" in target_table.toDF().columns:
                # Only update if hash key changed
                merge_builder = merge_builder.whenMatchedUpdateAll(
                    condition="source.hash_key != target.hash_key"
                )
            else:
                merge_builder = merge_builder.whenMatchedUpdateAll()
            
            merge_builder = merge_builder.whenNotMatchedInsertAll().execute()
            
            logger.info(f"✅ Merge completed on {table_path}")
            
            return {
                "inserted": source_df.count(),
                "updated": 0,
                "deleted": 0
            }
        
        except Exception as e:
            logger.error(f"Error executing merge: {str(e)}")
            raise
    
    def _add_hash_key(self, df: DataFrame, hash_columns: List[str]) -> DataFrame:
        """Add hash key column for change detection"""
        concat_expr = concat(*[
            coalesce(col(c).cast(StringType()), lit(""))
            for c in hash_columns
        ])
        return df.withColumn("hash_key", md5(concat_expr))


# ==================== APPEND-ONLY IMPLEMENTATION ====================

class AppendLoadProcessor(BaseIncrementalLoader):
    """Processor for append-only loads (immutable fact tables)"""
    
    def load(self, config: IncrementalLoadConfig) -> LoadStatistics:
        """
        Execute append load - add new records only
        Uses anti-join to find new records
        
        Args:
            config: Load configuration
            
        Returns:
            LoadStatistics
        """
        logger.info(f"🔄 Starting APPEND LOAD: {config.source_table} → {config.destination_table}")
        
        start_time = datetime.now()
        stats = LoadStatistics(
            load_type="APPEND",
            start_time=start_time.isoformat()
        )
        
        try:
            primary_keys = config.get_primary_keys()
            
            stats.destination_count_before = self.get_table_count(config.destination_table)
            
            # Read source and destination
            source_df = self.read_table(config.source_table)
            dest_df = self.read_table(config.destination_table)
            
            stats.source_count = source_df.count()
            
            # Anti-join to find new records
            join_condition = " AND ".join([
                f"source.{pk} = dest.{pk}" for pk in primary_keys
            ])
            
            new_records_df = source_df.alias("source") \
                .join(
                    dest_df.alias("dest"),
                    condition=join_condition,
                    how="leftanti"
                )
            
            stats.records_inserted = new_records_df.count()
            
            if stats.records_inserted == 0:
                logger.info("No new records to append")
                stats.status = "SUCCESS"
                return stats
            
            # Add metadata
            new_records_df = self.add_metadata_columns(
                new_records_df,
                config,
                is_insert=True
            )
            
            # Append to destination
            self.write_table(
                new_records_df,
                config.destination_table,
                mode="append",
                partition_by=config.partition_columns if config.partition_columns else None
            )
            
            stats.destination_count_after = stats.destination_count_before + stats.records_inserted
            stats.status = "SUCCESS"
            
        except Exception as e:
            stats.status = "FAILED"
            stats.error_message = str(e)
            logger.error(f"❌ APPEND LOAD FAILED: {str(e)}")
            raise
        
        finally:
            stats.end_time = datetime.now().isoformat()
            stats.load_duration_seconds = (datetime.fromisoformat(stats.end_time) - 
                                          datetime.fromisoformat(stats.start_time)).total_seconds()
        
        return stats


# ==================== CDC (CHANGE DATA CAPTURE) IMPLEMENTATION ====================

class CDCLoadProcessor(BaseIncrementalLoader):
    """Processor for Change Data Capture operations"""
    
    def load(self, config: IncrementalLoadConfig) -> LoadStatistics:
        """
        Execute CDC load - handles insert, update, delete operations
        
        Args:
            config: Load configuration
            
        Returns:
            LoadStatistics
        """
        logger.info(f"🔄 Starting CDC LOAD: {config.source_table} → {config.destination_table}")
        
        if not config.change_indicator_column:
            raise ValueError("change_indicator_column required for CDC load")
        
        start_time = datetime.now()
        stats = LoadStatistics(
            load_type="CDC",
            start_time=start_time.isoformat()
        )
        
        try:
            primary_keys = config.get_primary_keys()
            
            stats.destination_count_before = self.get_table_count(config.destination_table)
            
            # Read CDC data
            cdc_df = self.read_table(config.source_table)
            
            # Separate operations
            insert_df = cdc_df.filter(col(config.change_indicator_column).isin("I", "INSERT"))
            update_df = cdc_df.filter(col(config.change_indicator_column).isin("U", "UPDATE"))
            delete_df = cdc_df.filter(col(config.change_indicator_column).isin("D", "DELETE"))
            
            stats.records_inserted = insert_df.count()
            stats.records_updated = update_df.count()
            stats.records_deleted = delete_df.count()
            stats.source_count = insert_df.count() + update_df.count() + delete_df.count()
            
            # Process insert
            if stats.records_inserted > 0:
                insert_df = self.add_metadata_columns(insert_df, config, is_insert=True)
                self.write_table(insert_df, config.destination_table, mode="append")
            
            # Process update
            if stats.records_updated > 0:
                self._process_updates(update_df, config.destination_table, primary_keys, config)
            
            # Process delete
            if stats.records_deleted > 0:
                self._process_deletes(delete_df, config.destination_table, primary_keys)
            
            stats.destination_count_after = self.get_table_count(config.destination_table)
            stats.status = "SUCCESS"
            
        except Exception as e:
            stats.status = "FAILED"
            stats.error_message = str(e)
            logger.error(f"❌ CDC LOAD FAILED: {str(e)}")
            raise
        
        finally:
            stats.end_time = datetime.now().isoformat()
            stats.load_duration_seconds = (datetime.fromisoformat(stats.end_time) - 
                                          datetime.fromisoformat(stats.start_time)).total_seconds()
        
        return stats
    
    def _process_updates(
        self,
        update_df: DataFrame,
        table_path: str,
        primary_keys: List[str],
        config: IncrementalLoadConfig
    ) -> None:
        """Process update operations"""
        update_df = self.add_metadata_columns(update_df, config, is_insert=False)
        
        merge_condition = " AND ".join([
            f"target.{pk} = source.{pk}" for pk in primary_keys
        ])
        
        try:
            target_table = DeltaTable.forPath(self.spark, table_path)
            
            target_table.alias("target") \
                .merge(update_df.alias("source"), merge_condition) \
                .whenMatchedUpdateAll() \
                .execute()
            
            logger.info(f"Processed {update_df.count()} updates")
        except Exception as e:
            logger.error(f"Error processing updates: {str(e)}")
    
    def _process_deletes(
        self,
        delete_df: DataFrame,
        table_path: str,
        primary_keys: List[str]
    ) -> None:
        """Process delete operations"""
        merge_condition = " AND ".join([
            f"target.{pk} = source.{pk}" for pk in primary_keys
        ])
        
        try:
            target_table = DeltaTable.forPath(self.spark, table_path)
            
            target_table.alias("target") \
                .merge(delete_df.alias("source"), merge_condition) \
                .whenMatchedDelete() \
                .execute()
            
            logger.info(f"Processed {delete_df.count()} deletes")
        except Exception as e:
            logger.error(f"Error processing deletes: {str(e)}")


# ==================== ORCHESTRATOR ====================

class DynamicIncrementalLoadOrchestrator:
    """
    Main orchestrator for dynamic incremental loads
    Routes to appropriate processor based on load type
    """
    
    def __init__(self, workspace_path: str = "/Workspace/Users/default"):
        """
        Initialize orchestrator
        
        Args:
            workspace_path: Fabric workspace path
        """
        self.workspace_path = workspace_path
        self.processors = {
            LoadType.FULL: FullLoadProcessor(workspace_path),
            LoadType.INCREMENTAL: IncrementalLoadProcessor(workspace_path),
            LoadType.UPSERT: UpsertLoadProcessor(workspace_path),
            LoadType.APPEND: AppendLoadProcessor(workspace_path),
            LoadType.CHANGE_DATA_CAPTURE: CDCLoadProcessor(workspace_path)
        }
        self.load_history = []
    
    def execute_load(self, config: IncrementalLoadConfig) -> LoadStatistics:
        """
        Execute load based on configuration
        
        Args:
            config: Load configuration
            
        Returns:
            LoadStatistics
        """
        logger.info(f"\n{'='*70}")
        logger.info(f"Executing {config.load_type.value} load")
        logger.info(f"Source: {config.source_table}")
        logger.info(f"Destination: {config.destination_table}")
        logger.info(f"{'='*70}\n")
        
        processor = self.processors.get(config.load_type)
        
        if not processor:
            raise ValueError(f"Unsupported load type: {config.load_type}")
        
        try:
            stats = processor.load(config)
            self.load_history.append(stats)
            
            # Log statistics
            self._log_statistics(stats, config)
            
            return stats
        
        except Exception as e:
            logger.error(f"❌ Load execution failed: {str(e)}")
            raise
    
    def execute_batch_load(self, configs: List[IncrementalLoadConfig]) -> List[LoadStatistics]:
        """
        Execute multiple loads in sequence
        
        Args:
            configs: List of load configurations
            
        Returns:
            List of LoadStatistics
        """
        logger.info(f"\n🚀 Starting batch execution of {len(configs)} loads")
        
        results = []
        for config in configs:
            try:
                stats = self.execute_load(config)
                results.append(stats)
            except Exception as e:
                logger.error(f"Failed to execute load for {config.destination_table}: {str(e)}")
                results.append(LoadStatistics(
                    load_type=config.load_type.value,
                    status="FAILED",
                    error_message=str(e)
                ))
        
        logger.info(f"✅ Batch execution completed")
        return results
    
    def _log_statistics(self, stats: LoadStatistics, config: IncrementalLoadConfig) -> None:
        """Log load statistics"""
        logger.info(f"\n📊 LOAD STATISTICS for {config.destination_table}:")
        logger.info(f"  Load Type: {stats.load_type}")
        logger.info(f"  Status: {stats.status}")
        logger.info(f"  Source Records: {stats.source_count:,}")
        logger.info(f"  Destination Before: {stats.destination_count_before:,}")
        logger.info(f"  Records Inserted: {stats.records_inserted:,}")
        logger.info(f"  Records Updated: {stats.records_updated:,}")
        logger.info(f"  Records Deleted: {stats.records_deleted:,}")
        logger.info(f"  Destination After: {stats.destination_count_after:,}")
        logger.info(f"  Duration: {stats.load_duration_seconds:.2f} seconds")
        
        if stats.error_message:
            logger.error(f"  Error: {stats.error_message}")
        
        logger.info("")
    
    def get_load_history(self) -> List[Dict]:
        """Get load history as list of dictionaries"""
        return [stats.to_dict() for stats in self.load_history]
    
    def export_load_history(self, path: str) -> None:
        """Export load history to JSON file"""
        with open(path, 'w') as f:
            json.dump(self.get_load_history(), f, indent=2)
        logger.info(f"Load history exported to {path}")


# ==================== UTILITY FUNCTIONS ====================

def optimize_table(spark: SparkSession, table_path: str) -> None:
    """
    Optimize Delta table - compact small files and remove old versions
    
    Args:
        spark: SparkSession
        table_path: Path to table
    """
    try:
        table = DeltaTable.forPath(spark, table_path)
        table.optimize().executeCompaction()
        table.vacuum(retention_hours=168)  # Keep 7 days history
        logger.info(f"✅ Optimized {table_path}")
    except Exception as e:
        logger.error(f"Error optimizing {table_path}: {str(e)}")


# ==================== EXAMPLE USAGE ====================

def example_usage():
    """Example of how to use the dynamic loader"""
    
    orchestrator = DynamicIncrementalLoadOrchestrator()
    
    # Example 1: Full Load
    config_full = IncrementalLoadConfig(
        source_table="/Workspace/landing/source_table",
        destination_table="/Workspace/ods/dim_table",
        load_type=LoadType.FULL,
        primary_key="id",
        hash_columns=["name", "email", "status"],
        partition_columns=["date_created"]
    )
    
    # Example 2: Incremental Load with Watermarking
    config_incremental = IncrementalLoadConfig(
        source_table="/Workspace/landing/transactions",
        destination_table="/Workspace/ods/fact_transactions",
        load_type=LoadType.INCREMENTAL,
        watermark_column="last_modified_date",
        partition_columns=["transaction_date"]
    )
    
    # Example 3: Upsert with Composite Key
    config_upsert = IncrementalLoadConfig(
        source_table="/Workspace/landing/customers",
        destination_table="/Workspace/ods/dim_customers",
        load_type=LoadType.UPSERT,
        primary_keys=["customer_id", "source_system"],
        hash_columns=["name", "email", "phone", "address"],
        merge_strategy=MergeStrategy.MATCH_ON_MULTIPLE_KEYS
    )
    
    # Example 4: Append Only
    config_append = IncrementalLoadConfig(
        source_table="/Workspace/landing/audit_logs",
        destination_table="/Workspace/ods/fact_audit_logs",
        load_type=LoadType.APPEND,
        primary_key="log_id"
    )
    
    # Example 5: CDC Load
    config_cdc = IncrementalLoadConfig(
        source_table="/Workspace/landing/cdc_feed",
        destination_table="/Workspace/ods/fact_orders",
        load_type=LoadType.CHANGE_DATA_CAPTURE,
        primary_keys=["order_id"],
        change_indicator_column="operation_type",  # Expects 'I', 'U', 'D'
        hash_columns=["order_amount", "status"]
    )
    
    # Execute loads
    results = orchestrator.execute_batch_load([
        config_full,
        config_incremental,
        config_upsert,
        config_append,
        config_cdc
    ])
    
    # Export results
    orchestrator.export_load_history("/tmp/load_history.json")
    
    return results


if __name__ == "__main__":
    # Run example
    example_usage()