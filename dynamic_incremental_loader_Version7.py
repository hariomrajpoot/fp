"""
================================================================================
Dynamic Incremental Load Framework for Microsoft Fabric Lakehouse
================================================================================

PURPOSE:
  This framework provides a flexible, configurable solution for incremental 
  loading of data from various sources to a Microsoft Fabric Lakehouse.
  
FEATURES:
  - Supports multiple load types: FULL, INCREMENTAL, UPSERT, APPEND, CDC
  - Database-driven configuration using load_config_table
  - JSON-based column mapping and schema definition
  - Hash-based change detection (SCD Type 2)
  - Watermark-based incremental loads
  - Metadata tracking (inserted_timestamp, updated_timestamp)
  - Delta Lake optimization and management

ARCHITECTURE:
  1. Configuration Layer: Load configurations from Fabric config table
  2. Schema Layer: Load column mappings and target schema from JSON files
  3. Processing Layer: Different processors for different load types
  4. Orchestration Layer: Routes loads to appropriate processors

FLOW:
  Config Table → Column Mappings JSON → Target Schema JSON → Processor → Destination

================================================================================
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

# ================================================================================
# LOGGING CONFIGURATION
# ================================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ================================================================================
# GLOBAL CONFIGURATION VARIABLES
# ================================================================================

# Load Configuration Table Path
# This table contains all load configurations and points to JSON files
# for column mappings and schema definitions
load_config_table = "abfss://65f4ba7b-bd4f-4c55-b034-fb84ade8c986@onelake.dfs.fabric.microsoft.com/830dfb51-967c-4fcc-8eb8-02b913d93f0e/Tables/Config/DataProcessingConfig"

logger.info(f"Configuration Table Path: {load_config_table}")


# ================================================================================
# ENUMS & DATA CLASSES
# ================================================================================

class LoadType(Enum):
    """
    Enum for supported incremental load types.
    
    FULL: Load entire table from scratch (truncate and reload)
    INCREMENTAL: Load only new data using watermark (append-only)
    UPSERT: Merge data using primary keys (insert/update/delete)
    APPEND: Append only new records based on anti-join
    CDC: Change Data Capture (handle I/U/D operations)
    """
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"
    CHANGE_DATA_CAPTURE = "CDC"
    UPSERT = "UPSERT"
    APPEND = "APPEND"


class MergeStrategy(Enum):
    """
    Enum for merge strategy options used in UPSERT loads.
    
    MATCH_ON_KEY: Match on single primary key
    MATCH_ON_MULTIPLE_KEYS: Match on composite primary keys
    MATCH_ON_NATURAL_KEY: Match on natural business keys
    """
    MATCH_ON_KEY = "MATCH_ON_KEY"
    MATCH_ON_MULTIPLE_KEYS = "MATCH_ON_MULTIPLE_KEYS"
    MATCH_ON_NATURAL_KEY = "MATCH_ON_NATURAL_KEY"


@dataclass
class ColumnMapping:
    """
    Represents a column mapping between source and destination tables.
    
    Attributes:
        source_column: Name of column in source table
        destination_column: Name of column in destination table
        data_type: Optional Spark data type for casting (e.g., 'bigint', 'string')
        nullable: Whether column can contain null values (default: True)
        transformation: Optional SQL transformation to apply to column
                       Example: "regexp_replace(phone_number, '[^0-9]', '')"
    
    Example:
        ColumnMapping(
            source_column="cust_id",
            destination_column="customer_id",
            data_type="bigint",
            nullable=False,
            transformation=None
        )
    """
    source_column: str
    destination_column: str
    data_type: Optional[str] = None
    nullable: bool = True
    transformation: Optional[str] = None


@dataclass
class IncrementalLoadConfig:
    """
    Configuration object for incremental load operations.
    
    This object holds all configuration details loaded from the config table
    and JSON files. It's passed to processors to execute the load.
    
    Attributes:
        BASIC CONFIGURATION:
            source_table: Path to source data (delta table, parquet, etc.)
            destination_table: Path to destination delta table
            load_type: Type of load (FULL, INCREMENTAL, UPSERT, APPEND, CDC)
        
        KEY CONFIGURATION:
            primary_key: Single primary key or comma-separated list
            primary_keys: List of primary key columns (for composite keys)
        
        COLUMN CONFIGURATION:
            column_mappings: List of ColumnMapping objects
                            Defines which columns to read and how to transform them
        
        WATERMARK CONFIGURATION (for INCREMENTAL loads):
            watermark_column: Column to track last loaded timestamp/value
            watermark_value: Last watermark value (used to filter new data)
        
        CDC CONFIGURATION (for CDC loads):
            change_indicator_column: Column indicating operation type (I/U/D)
            change_indicator_values: Values representing I/U/D operations
        
        HASH CONFIGURATION (for change detection):
            hash_columns: Columns to include in hash calculation
            generate_hash_key: Whether to generate hash key (default: True)
        
        METADATA CONFIGURATION:
            include_metadata: Whether to add timestamp columns (default: True)
            inserted_timestamp_column: Column name for insert timestamp
            updated_timestamp_column: Column name for update timestamp
        
        MERGE CONFIGURATION (for UPSERT):
            merge_strategy: Strategy for matching rows during merge
            merge_conditions: Additional custom merge conditions
        
        PERFORMANCE TUNING:
            repartition_count: Number of partitions for performance (default: 50)
            parallel_writes: Enable parallel write operations
        
        VALIDATION:
            skip_validation: Skip validation rules (default: False)
            validation_rules: Dictionary of custom validation rules
        
        PARTITIONING:
            partition_columns: Columns to partition destination table by
        
        SCHEMA EVOLUTION:
            allow_schema_evolution: Allow new columns in source (default: True)
        
        TARGET SCHEMA:
            target_table_structure_file_path: Path to JSON file with target schema
            target_table_structure: Parsed StructType from JSON file
        
        IDENTIFIERS:
            load_id: Unique identifier for this load configuration
            load_name: Human-readable name for this load
    """
    
    # BASIC CONFIGURATION
    source_table: str
    destination_table: str
    load_type: LoadType = LoadType.INCREMENTAL
    
    # KEY CONFIGURATION
    primary_key: str = None  # Single key or comma-separated
    primary_keys: List[str] = field(default_factory=list)  # Explicit list
    
    # COLUMN CONFIGURATION
    column_mappings: List[ColumnMapping] = field(default_factory=list)
    
    # WATERMARK CONFIGURATION (for INCREMENTAL loads)
    watermark_column: str = None
    watermark_value: Optional[str] = None
    
    # CDC CONFIGURATION (for CDC loads)
    change_indicator_column: str = None
    change_indicator_values: List[str] = field(default_factory=list)
    
    # HASH CONFIGURATION (for change detection)
    hash_columns: List[str] = field(default_factory=list)
    generate_hash_key: bool = True
    
    # METADATA CONFIGURATION
    include_metadata: bool = True
    inserted_timestamp_column: str = "inserted_timestamp"
    updated_timestamp_column: str = "updated_timestamp"
    
    # MERGE CONFIGURATION (for UPSERT)
    merge_strategy: MergeStrategy = MergeStrategy.MATCH_ON_KEY
    merge_conditions: List[Tuple[str, str]] = field(default_factory=list)
    
    # PERFORMANCE TUNING
    repartition_count: int = 50
    parallel_writes: bool = True
    
    # VALIDATION
    skip_validation: bool = False
    validation_rules: Dict[str, Any] = field(default_factory=dict)
    
    # PARTITIONING
    partition_columns: List[str] = field(default_factory=list)
    
    # SCHEMA EVOLUTION
    allow_schema_evolution: bool = True
    
    # TARGET TABLE STRUCTURE
    target_table_structure_file_path: Optional[str] = None
    target_table_structure: Optional[StructType] = None
    
    # IDENTIFIERS
    load_id: Optional[str] = None
    load_name: Optional[str] = None
    
    def get_primary_keys(self) -> List[str]:
        """
        Get primary keys as list.
        
        Handles both formats:
        - primary_keys: List (e.g., ["customer_id", "order_date"])
        - primary_key: String with comma-separated values (e.g., "customer_id,order_date")
        
        Returns:
            List of primary key column names
            
        Raises:
            ValueError: If no primary key is configured
        """
        if self.primary_keys:
            return self.primary_keys
        if self.primary_key:
            return [k.strip() for k in self.primary_key.split(',')]
        raise ValueError("No primary key configured")


@dataclass
class LoadStatistics:
    """
    Statistics collected during load operation.
    
    Tracks metrics about the load execution including record counts,
    duration, status, and any errors encountered.
    
    Attributes:
        load_type: Type of load executed
        source_count: Number of records read from source
        destination_count_before: Record count in destination before load
        destination_count_after: Record count in destination after load
        records_inserted: Number of new records inserted
        records_updated: Number of existing records updated
        records_deleted: Number of records deleted
        records_unchanged: Number of records that didn't change
        load_duration_seconds: Total execution time in seconds
        start_time: ISO format timestamp when load started
        end_time: ISO format timestamp when load ended
        status: Load status (PENDING, SUCCESS, FAILED)
        error_message: Error message if load failed
        load_id: Identifier of the load configuration
        load_name: Name of the load configuration
    """
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
    load_id: str = ""
    load_name: str = ""
    
    def to_dict(self) -> Dict:
        """Convert statistics to dictionary for logging/export"""
        return asdict(self)


# ================================================================================
# SCHEMA STRUCTURE LOADER
# ================================================================================

class SchemaStructureLoader:
    """
    Loads and parses schema structure from JSON files.
    
    This class reads JSON files containing StructField definitions and converts
    them into Spark StructType objects that can be applied to DataFrames.
    
    JSON File Format:
        {
            "fields": [
                {
                    "name": "customer_id",
                    "type": "bigint",
                    "nullable": false
                },
                {
                    "name": "customer_name",
                    "type": "string",
                    "nullable": false
                },
                ...
            ]
        }
    
    Supported Data Types:
        - String types: string, varchar, char, text
        - Integer types: bigint, long, int, integer
        - Decimal types: decimal, double, float
        - Boolean: boolean, bool
        - Timestamp: timestamp, datetime, timestamp_ntz
    """
    
    @staticmethod
    def load_schema_from_json(file_path: str) -> StructType:
        """
        Load StructType schema from JSON file.
        
        This method reads a JSON file from Lakehouse that contains field
        definitions and converts it into a Spark StructType object.
        
        Args:
            file_path: Path to JSON file
                      Example: "/Workspace/config/schemas/customer_target.json"
                      or: "abfss://container@storage.dfs.core.windows.net/path/file.json"
            
        Returns:
            StructType: Spark StructType representing the schema
            
        Raises:
            FileNotFoundError: If JSON file doesn't exist
            ValueError: If JSON format is invalid
            
        Example:
            schema = SchemaStructureLoader.load_schema_from_json(
                "/Workspace/config/schemas/customer_target.json"
            )
        """
        try:
            logger.info(f"Loading schema structure from: {file_path}")
            
            # ✅ OPEN AND READ JSON FILE from Lakehouse
            # This uses standard Python file I/O which works with Fabric paths
            with open(file_path, 'r') as f:
                schema_dict = json.load(f)
            
            # Parse the JSON dictionary into StructType
            struct_type = SchemaStructureLoader._parse_schema_dict(schema_dict)
            
            logger.info(f"✅ Schema structure loaded successfully from {file_path}")
            return struct_type
        
        except FileNotFoundError:
            logger.error(f"Schema file not found: {file_path}")
            raise ValueError(f"Schema file not found: {file_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in {file_path}: {str(e)}")
            raise ValueError(f"Invalid JSON format: {str(e)}")
        except Exception as e:
            logger.error(f"Error loading schema: {str(e)}")
            raise
    
    @staticmethod
    def _parse_schema_dict(schema_dict: Dict[str, Any]) -> StructType:
        """
        Parse schema dictionary to StructType.
        
        Converts a dictionary (parsed from JSON) into a Spark StructType
        by processing each field definition.
        
        Args:
            schema_dict: Dictionary with 'fields' key containing field definitions
            
        Returns:
            StructType: Spark StructType object
            
        Raises:
            ValueError: If schema format is invalid
        """
        try:
            fields = []
            
            # Handle both direct field list and nested structure
            field_list = schema_dict.get('fields', []) if isinstance(schema_dict, dict) else schema_dict
            
            if not field_list:
                raise ValueError("No 'fields' found in schema definition")
            
            # Process each field definition
            for field_def in field_list:
                field = SchemaStructureLoader._create_struct_field(field_def)
                fields.append(field)
            
            # Create and return StructType
            return StructType(fields)
        
        except Exception as e:
            logger.error(f"Error parsing schema dictionary: {str(e)}")
            raise
    
    @staticmethod
    def _create_struct_field(field_def: Dict[str, Any]) -> StructField:
        """
        Create StructField from field definition.
        
        Converts a single field definition from JSON into a Spark StructField.
        
        Args:
            field_def: Dictionary containing field definition
                      Must have 'name' and 'type' keys
                      Optional 'nullable' key (default: True)
            
        Returns:
            StructField: Spark StructField object
            
        Raises:
            ValueError: If field definition is incomplete
        """
        try:
            field_name = field_def.get('name')
            field_type = field_def.get('type')
            nullable = field_def.get('nullable', True)
            
            # Validate required fields
            if not field_name or not field_type:
                raise ValueError("Field definition must have 'name' and 'type'")
            
            # Parse data type string to Spark type
            data_type = SchemaStructureLoader._parse_data_type(field_type)
            
            logger.info(f"Creating StructField: {field_name} ({field_type})")
            
            # Create and return StructField
            return StructField(field_name, data_type, nullable=nullable)
        
        except Exception as e:
            logger.error(f"Error creating StructField: {str(e)}")
            raise
    
    @staticmethod
    def _parse_data_type(type_str: str) -> Any:
        """
        Parse data type string to Spark data type.
        
        Converts string representations of data types (e.g., 'bigint', 'string')
        into Spark data type objects.
        
        Supported types and their mappings:
            String types:
                'string' → StringType()
                'varchar' → StringType()
                'char' → StringType()
                'text' → StringType()
            
            BigInt types:
                'bigint' → LongType()
                'long' → LongType()
                'int64' → LongType()
            
            Integer types:
                'int' → LongType()
                'integer' → LongType()
                'int32' → LongType()
            
            Decimal types:
                'decimal' → DecimalType(38, 18)
                'double' → DecimalType(38, 18)
                'float' → DecimalType(38, 18)
            
            Boolean types:
                'boolean' → BooleanType()
                'bool' → BooleanType()
            
            Timestamp types:
                'timestamp' → TimestampType()
                'datetime' → TimestampType()
                'timestamp_ntz' → TimestampType()
        
        Args:
            type_str: Data type string (case-insensitive)
            
        Returns:
            Spark data type object
        """
        type_str = type_str.lower().strip()
        
        # Mapping of string types to Spark data types
        type_mapping = {
            # String types
            'string': StringType(),
            'varchar': StringType(),
            'char': StringType(),
            'text': StringType(),
            
            # BigInt types
            'bigint': LongType(),
            'long': LongType(),
            'int64': LongType(),
            
            # Int types
            'int': LongType(),
            'integer': LongType(),
            'int32': LongType(),
            
            # Decimal/Float types
            'decimal': DecimalType(38, 18),
            'double': DecimalType(38, 18),
            'float': DecimalType(38, 18),
            
            # Boolean type
            'boolean': BooleanType(),
            'bool': BooleanType(),
            
            # Timestamp types
            'timestamp': TimestampType(),
            'datetime': TimestampType(),
            'timestamp_ntz': TimestampType(),
        }
        
        if type_str in type_mapping:
            return type_mapping[type_str]
        
        # Default to StringType if type not recognized
        logger.warning(f"Unknown data type: {type_str}, defaulting to StringType")
        return StringType()


# ================================================================================
# DATABASE CONFIGURATION LOADER
# ================================================================================

class DatabaseConfigurationLoader:
    """
    Loads configuration from database tables and JSON files.
    
    This class reads the load_config_table and retrieves configuration for
    specific loads, including column mappings and target schemas from JSON files.
    
    FLOW:
        1. Read load_config_table
        2. Filter by load_id or load_name
        3. Extract file paths (column_mapping_file_path, target_table_structure_file_path)
        4. Load JSON files
        5. Create IncrementalLoadConfig object
    """
    
    @staticmethod
    def load_all_configs(
        spark: SparkSession,
        config_table_path: str
    ) -> List[IncrementalLoadConfig]:
        """
        Load all active configurations from database.
        
        Reads all active (is_active = true) configurations from the config table
        and returns them as IncrementalLoadConfig objects.
        
        Args:
            spark: SparkSession instance
            config_table_path: Path to load_config_table
            
        Returns:
            List of IncrementalLoadConfig objects for all active loads
            
        Example:
            configs = DatabaseConfigurationLoader.load_all_configs(
                spark,
                "/Workspace/metadata/load_config_table"
            )
        """
        try:
            logger.info(f"Loading all configurations from {config_table_path}")
            
            # ✅ STEP 1: READ THE CONFIG TABLE from Lakehouse
            config_df = spark.read.format("delta").load(config_table_path)
            
            # ✅ STEP 2: FILTER FOR ACTIVE CONFIGURATIONS
            # Select only rows where is_active = true or is_active is null (default active)
            active_configs = config_df.filter(
                (col("is_active") == True) | (col("is_active").isNull())
            ).collect()
            
            logger.info(f"Found {len(active_configs)} active configurations")
            
            # ✅ STEP 3: CONVERT EACH ROW TO CONFIG OBJECT
            configs = []
            for row in active_configs:
                # This row contains one load configuration
                config = DatabaseConfigurationLoader._row_to_config(row, spark)
                configs.append(config)
            
            return configs
        
        except Exception as e:
            logger.error(f"Error loading configurations: {str(e)}")
            raise
    
    @staticmethod
    def load_config_by_load_id(
        spark: SparkSession,
        config_table_path: str,
        load_id: str
    ) -> IncrementalLoadConfig:
        """
        Load specific configuration by load_id.
        
        Retrieves a single configuration from the config table using the
        unique load_id identifier.
        
        Args:
            spark: SparkSession instance
            config_table_path: Path to load_config_table
            load_id: Unique load identifier (e.g., "LOAD_001")
            
        Returns:
            IncrementalLoadConfig object for the specified load_id
            
        Raises:
            ValueError: If load_id not found in config table
            
        Example:
            config = DatabaseConfigurationLoader.load_config_by_load_id(
                spark,
                "/Workspace/metadata/load_config_table",
                "LOAD_001"
            )
        """
        try:
            logger.info(f"Loading configuration for load_id: {load_id}")
            
            # ✅ STEP 1: READ THE CONFIG TABLE
            config_df = spark.read.format("delta").load(config_table_path)
            
            # ✅ STEP 2: FILTER BY LOAD_ID
            # Filter to find the row with matching load_id
            result = config_df.filter(col("load_id") == lit(load_id)).collect()
            
            # Check if configuration found
            if not result:
                raise ValueError(f"Configuration not found for load_id: {load_id}")
            
            # ✅ STEP 3: CONVERT ROW TO CONFIG OBJECT
            config = DatabaseConfigurationLoader._row_to_config(result[0], spark)
            
            logger.info(f"✅ Configuration loaded for load_id: {load_id}")
            return config
        
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            raise
    
    @staticmethod
    def load_config_by_load_name(
        spark: SparkSession,
        config_table_path: str,
        load_name: str
    ) -> IncrementalLoadConfig:
        """
        Load specific configuration by load_name.
        
        Retrieves a single configuration from the config table using the
        human-readable load_name identifier.
        
        Args:
            spark: SparkSession instance
            config_table_path: Path to load_config_table
            load_name: Load name identifier (e.g., "customer_incremental_load")
            
        Returns:
            IncrementalLoadConfig object for the specified load_name
            
        Raises:
            ValueError: If load_name not found in config table
            
        Example:
            config = DatabaseConfigurationLoader.load_config_by_load_name(
                spark,
                "/Workspace/metadata/load_config_table",
                "customer_incremental_load"
            )
        """
        try:
            logger.info(f"Loading configuration for load_name: {load_name}")
            
            # ✅ STEP 1: READ THE CONFIG TABLE
            config_df = spark.read.format("delta").load(config_table_path)
            
            # ✅ STEP 2: FILTER BY LOAD_NAME
            result = config_df.filter(col("load_name") == lit(load_name)).collect()
            
            # Check if configuration found
            if not result:
                raise ValueError(f"Configuration not found for load_name: {load_name}")
            
            # ✅ STEP 3: CONVERT ROW TO CONFIG OBJECT
            config = DatabaseConfigurationLoader._row_to_config(result[0], spark)
            
            logger.info(f"✅ Configuration loaded for load_name: {load_name}")
            return config
        
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            raise
    
    @staticmethod
    def _row_to_config(
        row: Any,
        spark: SparkSession
    ) -> IncrementalLoadConfig:
        """
        Convert database row to IncrementalLoadConfig object.
        
        This is the core conversion method that:
        1. Extracts all configuration values from the database row
        2. Parses comma-separated strings into lists
        3. Loads column mappings from JSON file
        4. Loads target schema from JSON file
        5. Creates and returns IncrementalLoadConfig object
        
        FLOW:
            Database Row (from config table)
                ↓
            Extract columns from row
                ↓
            Parse comma-separated values into lists
                ↓
            Load column_mappings.json file
                ↓
            Load target_schema.json file
                ↓
            Create IncrementalLoadConfig object
                ↓
            Return to caller
        
        Args:
            row: Single database row from config table
            spark: SparkSession instance
            
        Returns:
            IncrementalLoadConfig object with all configuration loaded
        """
        try:
            logger.info(f"Converting database row to config object")
            
            # ================================================================================
            # PARSE PRIMARY KEYS
            # ================================================================================
            # Primary keys can be stored as:
            # 1. Comma-separated string: "customer_id,order_date"
            # 2. List: ["customer_id", "order_date"]
            # This code handles both formats
            
            primary_keys = []
            if hasattr(row, 'primary_keys') and row.primary_keys:
                if isinstance(row.primary_keys, str):
                    # Split by comma and trim whitespace
                    primary_keys = [k.strip() for k in row.primary_keys.split(',')]
                elif isinstance(row.primary_keys, list):
                    # Already a list
                    primary_keys = row.primary_keys
            
            logger.info(f"Primary keys: {primary_keys}")
            
            # ================================================================================
            # PARSE HASH COLUMNS
            # ================================================================================
            # Columns used for generating hash key for change detection (SCD Type 2)
            # Example: "name,email,phone" → ["name", "email", "phone"]
            
            hash_columns = []
            if hasattr(row, 'hash_columns') and row.hash_columns:
                if isinstance(row.hash_columns, str):
                    hash_columns = [c.strip() for c in row.hash_columns.split(',')]
                elif isinstance(row.hash_columns, list):
                    hash_columns = row.hash_columns
            
            logger.info(f"Hash columns: {hash_columns}")
            
            # ================================================================================
            # PARSE PARTITION COLUMNS
            # ================================================================================
            # Columns to partition the destination table by
            # Example: "load_date,year" → ["load_date", "year"]
            
            partition_columns = []
            if hasattr(row, 'partition_columns') and row.partition_columns:
                if isinstance(row.partition_columns, str):
                    partition_columns = [c.strip() for c in row.partition_columns.split(',')]
                elif isinstance(row.partition_columns, list):
                    partition_columns = row.partition_columns
            
            logger.info(f"Partition columns: {partition_columns}")
            
            # ================================================================================
            # PARSE CHANGE INDICATOR VALUES
            # ================================================================================
            # For CDC loads - values indicating different operation types
            # Example: "I,U,D" → ["I", "U", "D"]
            
            change_indicator_values = []
            if hasattr(row, 'change_indicator_values') and row.change_indicator_values:
                if isinstance(row.change_indicator_values, str):
                    change_indicator_values = [v.strip() for v in row.change_indicator_values.split(',')]
                elif isinstance(row.change_indicator_values, list):
                    change_indicator_values = row.change_indicator_values
            
            logger.info(f"Change indicator values: {change_indicator_values}")
            
            # ================================================================================
            # LOAD COLUMN MAPPINGS FROM JSON FILE
            # ================================================================================
            # Reads the column mapping JSON file specified in the config table
            # This file defines which columns to read from source and how to transform them
            
            column_mappings = []
            column_mapping_file_path = row.column_mapping_file_path if hasattr(row, 'column_mapping_file_path') else None
            
            if column_mapping_file_path:
                try:
                    logger.info(f"Loading column mappings from: {column_mapping_file_path}")
                    column_mappings = DatabaseConfigurationLoader._load_column_mappings_from_json(
                        column_mapping_file_path
                    )
                    logger.info(f"✅ Column mappings loaded: {len(column_mappings)} mappings")
                except Exception as e:
                    logger.warning(f"Could not load column mappings from JSON: {str(e)}")
            
            # ================================================================================
            # LOAD TARGET TABLE STRUCTURE FROM JSON FILE
            # ================================================================================
            # Reads the target schema JSON file specified in the config table
            # This file defines the final structure of columns in destination table
            
            target_table_structure = None
            target_table_structure_file_path = row.target_table_structure_file_path if hasattr(row, 'target_table_structure_file_path') else None
            
            if target_table_structure_file_path:
                try:
                    logger.info(f"Loading target schema from: {target_table_structure_file_path}")
                    target_table_structure = SchemaStructureLoader.load_schema_from_json(
                        target_table_structure_file_path
                    )
                    logger.info(f"✅ Target schema loaded: {len(target_table_structure.fields)} fields")
                except Exception as e:
                    logger.warning(f"Could not load target table structure: {str(e)}")
            
            # ================================================================================
            # CREATE INCREMENTAL LOAD CONFIG OBJECT
            # ================================================================================
            # Combine all configuration values into a single config object
            # This object is passed to processors for execution
            
            config = IncrementalLoadConfig(
                # Basic identification
                load_id=row.load_id if hasattr(row, 'load_id') else None,
                load_name=row.load_name if hasattr(row, 'load_name') else None,
                
                # Table paths
                source_table=row.source_table,
                destination_table=row.destination_table,
                
                # Load type
                load_type=LoadType(row.load_type),
                
                # Keys and mappings
                primary_keys=primary_keys,
                primary_key=row.primary_keys if hasattr(row, 'primary_keys') else None,
                column_mappings=column_mappings,
                
                # Watermark configuration (for INCREMENTAL loads)
                watermark_column=row.watermark_column if hasattr(row, 'watermark_column') else None,
                watermark_value=row.watermark_value if hasattr(row, 'watermark_value') else None,
                
                # CDC configuration (for CDC loads)
                change_indicator_column=row.change_indicator_column if hasattr(row, 'change_indicator_column') else None,
                change_indicator_values=change_indicator_values,
                
                # Hash configuration
                hash_columns=hash_columns,
                generate_hash_key=row.generate_hash_key if hasattr(row, 'generate_hash_key') else True,
                
                # Partitioning
                partition_columns=partition_columns,
                
                # Merge strategy
                merge_strategy=MergeStrategy(row.merge_strategy) if hasattr(row, 'merge_strategy') and row.merge_strategy else MergeStrategy.MATCH_ON_KEY,
                
                # Metadata configuration
                include_metadata=row.include_metadata if hasattr(row, 'include_metadata') else True,
                inserted_timestamp_column=row.inserted_timestamp_column if hasattr(row, 'inserted_timestamp_column') else "inserted_timestamp",
                updated_timestamp_column=row.updated_timestamp_column if hasattr(row, 'updated_timestamp_column') else "updated_timestamp",
                
                # Performance tuning
                repartition_count=row.repartition_count if hasattr(row, 'repartition_count') else 50,
                
                # Validation
                skip_validation=row.skip_validation if hasattr(row, 'skip_validation') else False,
                allow_schema_evolution=row.allow_schema_evolution if hasattr(row, 'allow_schema_evolution') else True,
                
                # Target schema loaded from JSON
                column_mappings=column_mappings,
                target_table_structure_file_path=target_table_structure_file_path,
                target_table_structure=target_table_structure
            )
            
            logger.info(f"✅ Configuration object created: {config.load_name or config.load_id}")
            return config
        
        except Exception as e:
            logger.error(f"Error converting row to config: {str(e)}")
            raise
    
    @staticmethod
    def _load_column_mappings_from_json(file_path: str) -> List[ColumnMapping]:
        """
        Load column mappings from JSON file.
        
        Reads a JSON file containing column mapping definitions and converts
        them into ColumnMapping objects.
        
        JSON Format:
            {
                "mappings": [
                    {
                        "source_column": "cust_id",
                        "destination_column": "customer_id",
                        "data_type": "bigint",
                        "nullable": false,
                        "transformation": null
                    },
                    ...
                ]
            }
        
        Args:
            file_path: Path to JSON file in Lakehouse
            
        Returns:
            List of ColumnMapping objects
            
        Raises:
            FileNotFoundError: If JSON file doesn't exist
            ValueError: If JSON format is invalid
        """
        try:
            logger.info(f"Loading column mappings from JSON: {file_path}")
            
            # ✅ OPEN AND READ JSON FILE
            with open(file_path, 'r') as f:
                mappings_data = json.load(f)
            
            column_mappings = []
            
            # Handle both direct mapping list and nested structure
            mappings_list = mappings_data.get('mappings', []) if isinstance(mappings_data, dict) else mappings_data
            
            if not mappings_list:
                logger.warning(f"No 'mappings' found in {file_path}")
                return []
            
            # ✅ PROCESS EACH MAPPING
            for mapping_def in mappings_list:
                mapping = ColumnMapping(
                    source_column=mapping_def.get('source_column'),
                    destination_column=mapping_def.get('destination_column'),
                    data_type=mapping_def.get('data_type'),
                    nullable=mapping_def.get('nullable', True),
                    transformation=mapping_def.get('transformation')
                )
                column_mappings.append(mapping)
            
            logger.info(f"✅ Loaded {len(column_mappings)} column mappings from JSON")
            return column_mappings
        
        except FileNotFoundError:
            logger.error(f"Column mapping file not found: {file_path}")
            raise ValueError(f"Column mapping file not found: {file_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in {file_path}: {str(e)}")
            raise ValueError(f"Invalid JSON format: {str(e)}")
        except Exception as e:
            logger.error(f"Error loading column mappings from JSON: {str(e)}")
            raise


# ================================================================================
# BASE LOADER CLASS
# ================================================================================

class BaseIncrementalLoader(ABC):
    """
    Abstract base class for incremental loaders.
    
    This is the parent class for all load processors. It provides common
    functionality used by all load types:
    - Reading tables from Lakehouse
    - Applying column mappings
    - Writing tables to Lakehouse
    - Adding metadata columns
    - Getting table record counts
    
    Subclasses:
    - FullLoadProcessor: FULL loads (truncate and reload)
    - IncrementalLoadProcessor: INCREMENTAL loads (watermark-based)
    - UpsertLoadProcessor: UPSERT loads (merge operations)
    - AppendLoadProcessor: APPEND loads (anti-join detection)
    - CDCLoadProcessor: CDC loads (change data capture)
    """
    
    def __init__(self, workspace_path: str = "/Workspace/Users/default"):
        """
        Initialize the base loader.
        
        Creates a SparkSession with optimized configurations for Lakehouse operations.
        
        Args:
            workspace_path: Path to Fabric workspace (default: "/Workspace/Users/default")
        """
        # ✅ CREATE SPARK SESSION with optimized configurations
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
        """
        Abstract method to execute the load operation.
        
        Each subclass must implement this method with their specific load logic.
        """
        pass
    
    def read_table(self, table_path: str, format: str = "delta") -> DataFrame:
        """
        Read a table from Lakehouse.
        
        This method reads an entire table/dataset from the specified path into memory.
        
        IMPORTANT:
            - ALL columns from the table are read
            - This happens here: spark.read.format(format).load(table_path)
            - Column filtering happens later in apply_column_mappings()
        
        Args:
            table_path: Path to table (e.g., "/Workspace/landing/customers")
            format: Format type (default: "delta")
                   Supported: delta, parquet, csv, json, etc.
            
        Returns:
            DataFrame: Spark DataFrame containing all data from the table
            
        Example:
            df = loader.read_table("/Workspace/landing/customers")
            # df now contains ALL columns from the customers table
        """
        try:
            logger.info(f"📖 Reading table: {table_path}")
            
            # ✅ READ ENTIRE TABLE (ALL COLUMNS)
            df = self.spark.read.format(format).load(table_path)
            
            # Log the columns read
            logger.info(f"✅ Table read successfully")
            logger.info(f"   Total columns: {len(df.columns)}")
            logger.info(f"   Columns: {df.columns}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading {table_path}: {str(e)}")
            raise
    
    def apply_column_mappings(
        self,
        df: DataFrame,
        column_mappings: List[ColumnMapping],
        target_structure: Optional[StructType] = None
    ) -> DataFrame:
        """
        Apply column mappings to source DataFrame and enforce target schema.
        
        This is a critical method that:
        1. Transforms columns (rename, cast types, apply expressions)
        2. Filters to only mapped columns (drops unmapped columns)
        3. Applies target schema structure
        4. Reorders columns to match target schema
        
        FLOW:
            Source DataFrame (all columns)
                ↓
            Apply transformations for each mapped column
                ↓
            Select only destination columns (drop unmapped)
                ↓
            Apply target schema structure (if provided)
                ↓
            Return transformed DataFrame
        
        Args:
            df: Source DataFrame with all columns from source table
            column_mappings: List of ColumnMapping objects defining transformations
            target_structure: Optional StructType defining final schema
            
        Returns:
            DataFrame: Transformed with only mapped columns in target schema order
            
        Example:
            # Start with 20 columns from source
            df = loader.read_table(source_table)  # 20 columns
            
            # Define 5 column mappings
            mappings = [
                ColumnMapping("id", "customer_id", data_type="bigint"),
                ColumnMapping("name", "customer_name", data_type="string"),
                # ... 3 more mappings
            ]
            
            # Apply mappings
            df = loader.apply_column_mappings(df, mappings)  # 5 columns
        """
        # If no mappings or target structure, return original
        if not column_mappings and not target_structure:
            logger.info("No column mappings or target structure provided, returning original DataFrame")
            return df
        
        logger.info(f"🔄 Applying {len(column_mappings)} column mappings")
        
        try:
            # ================================================================================
            # STEP 1: APPLY COLUMN TRANSFORMATIONS
            # ================================================================================
            # For each mapping, perform:
            # - Rename column or apply transformation
            # - Cast to target data type
            
            for mapping in column_mappings:
                # Check if source column exists in DataFrame
                if mapping.source_column not in df.columns:
                    logger.warning(f"Source column '{mapping.source_column}' not found in DataFrame")
                    continue
                
                # Apply transformation if specified
                if mapping.transformation:
                    logger.info(f"  Applying transformation to {mapping.source_column}: {mapping.transformation}")
                    # Execute SQL expression on the column
                    df = df.withColumn(
                        mapping.destination_column,
                        F.expr(mapping.transformation)
                    )
                else:
                    # Simple rename: create new column with mapped name
                    if mapping.source_column != mapping.destination_column:
                        df = df.withColumn(
                            mapping.destination_column,
                            col(mapping.source_column)
                        )
                
                # Apply data type conversion if specified
                if mapping.data_type:
                    logger.info(f"  Converting {mapping.destination_column} to {mapping.data_type}")
                    df = df.withColumn(
                        mapping.destination_column,
                        col(mapping.destination_column).cast(mapping.data_type)
                    )
            
            # ================================================================================
            # STEP 2: SELECT ONLY MAPPED DESTINATION COLUMNS
            # ================================================================================
            # This is where unmapped columns are dropped
            # Example: If source has 20 columns but only 5 are mapped,
            #          only those 5 are selected, other 15 are dropped
            
            destination_columns = [m.destination_column for m in column_mappings]
            df = df.select(destination_columns)
            
            logger.info(f"✅ Selected {len(destination_columns)} columns")
            logger.info(f"   Columns: {destination_columns}")
            
            # ================================================================================
            # STEP 3: APPLY TARGET TABLE STRUCTURE
            # ================================================================================
            # If target schema is provided, enforce it:
            # - Add missing columns with null values
            # - Reorder columns to match target structure
            # - Cast all columns to target data types
            
            if target_structure:
                logger.info(f"🔄 Applying target table structure with {len(target_structure.fields)} fields")
                
                target_columns = [field.name for field in target_structure.fields]
                
                # Check which columns exist and which are missing
                existing_columns = [col_name for col_name in target_columns if col_name in df.columns]
                missing_columns = [col_name for col_name in target_columns if col_name not in df.columns]
                
                if missing_columns:
                    logger.warning(f"Missing columns in source data: {missing_columns}")
                    # Add missing columns with null values
                    # These will be filled during merge or insert operations
                    for missing_col in missing_columns:
                        df = df.withColumn(missing_col, lit(None))
                
                # Reorder columns to match target structure and cast to correct types
                # This ensures column order and data types exactly match target schema
                df = df.select([
                    col(field.name).cast(field.dataType) 
                    for field in target_structure.fields
                ])
                
                logger.info(f"✅ Target table structure applied successfully")
                logger.info(f"   Final columns: {[field.name for field in target_structure.fields]}")
            
            logger.info(f"✅ Column mappings applied successfully")
            return df
        
        except Exception as e:
            logger.error(f"Error applying column mappings: {str(e)}")
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
        Write a DataFrame to Lakehouse table.
        
        Writes the processed DataFrame to the destination table in Lakehouse.
        Supports different write modes and partitioning.
        
        Args:
            df: DataFrame to write
            table_path: Destination table path (e.g., "/Workspace/ods/dim_customers")
            mode: Write mode (default: "overwrite")
                 - "overwrite": Replace entire table
                 - "append": Add new rows to existing table
                 - "ignore": Skip if table exists
                 - "error": Raise error if table exists
            format: File format (default: "delta")
                   Recommended to keep as "delta" for Lakehouse
            partition_by: List of columns to partition table by (optional)
                         Example: ["load_date", "year"]
            
        Example:
            loader.write_table(
                df,
                "/Workspace/ods/dim_customers",
                mode="append",
                partition_by=["load_date"]
            )
        """
        try:
            logger.info(f"✍️  Writing table: {table_path}")
            logger.info(f"   Write mode: {mode}")
            logger.info(f"   Columns: {len(df.columns)}")
            logger.info(f"   Records: {df.count():,}")
            
            # ✅ PREPARE WRITE OPERATION
            writer = df.write.format(format).mode(mode)
            
            # Add partitioning if specified
            if partition_by:
                logger.info(f"   Partitioning by: {partition_by}")
                writer = writer.partitionBy(*partition_by)
            
            # ✅ EXECUTE WRITE
            writer.save(table_path)
            
            logger.info(f"✅ Successfully written {df.count():,} records to {table_path}")
            
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
        Add metadata columns (timestamps) to DataFrame.
        
        Adds timestamp columns to track when records were inserted or updated.
        These are commonly used for auditing and tracking data lineage.
        
        Args:
            df: Input DataFrame
            config: Load configuration containing metadata column names
            is_insert: Whether this is an insert (True) or update (False) operation
                      - insert: Adds inserted_timestamp
                      - update: Adds updated_timestamp
            
        Returns:
            DataFrame with added metadata columns
            
        Example:
            df = loader.add_metadata_columns(df, config, is_insert=True)
            # df now has an 'inserted_timestamp' column with current time
        """
        if not config.include_metadata:
            logger.info("Metadata inclusion disabled in config")
            return df
        
        # ✅ GET CURRENT TIMESTAMP
        timestamp = current_timestamp()
        
        # ✅ ADD APPROPRIATE TIMESTAMP COLUMN
        if is_insert:
            logger.info(f"Adding insert timestamp column: {config.inserted_timestamp_column}")
            df = df.withColumn(config.inserted_timestamp_column, timestamp)
        else:
            logger.info(f"Adding update timestamp column: {config.updated_timestamp_column}")
            df = df.withColumn(config.updated_timestamp_column, timestamp)
        
        return df
    
    def get_table_count(self, table_path: str) -> int:
        """
        Get record count for a table.
        
        Returns the number of rows in a table. Used for tracking statistics
        before and after load operations.
        
        Args:
            table_path: Path to table
            
        Returns:
            Number of records (rows) in the table, or 0 if table doesn't exist
        """
        try:
            df = self.read_table(table_path)
            count = df.count()
            logger.info(f"Table record count: {count:,}")
            return count
        except Exception:
            logger.info(f"Table not found or empty: {table_path}")
            return 0


# ================================================================================
# LOAD TYPE PROCESSORS
# ================================================================================

class FullLoadProcessor(BaseIncrementalLoader):
    """
    Processor for FULL load operations.
    
    FULL loads truncate the entire destination table and reload all data
    from the source. This is useful for:
    - Initial data loads
    - Complete data refreshes
    - Rebuilding dimension tables
    
    PROCESS:
    1. Read all data from source table
    2. Apply column mappings and transformations
    3. Add metadata columns (timestamps)
    4. Generate hash keys if configured
    5. Overwrite destination table (truncate and reload)
    6. Track statistics
    """
    
    def load(self, config: IncrementalLoadConfig) -> LoadStatistics:
        """
        Execute FULL load - truncate and reload entire table.
        
        Args:
            config: Load configuration
            
        Returns:
            LoadStatistics with operation metrics
        """
        logger.info(f"\n{'='*70}")
        logger.info(f"🔄 Starting FULL LOAD: {config.source_table} → {config.destination_table}")
        logger.info(f"{'='*70}")
        
        start_time = datetime.now()
        stats = LoadStatistics(
            load_type="FULL",
            load_id=config.load_id or "",
            load_name=config.load_name or "",
            start_time=start_time.isoformat()
        )
        
        try:
            # ✅ STEP 1: READ SOURCE TABLE (ALL COLUMNS)
            logger.info("STEP 1: Reading source table")
            source_df = self.read_table(config.source_table)
            
            # ✅ STEP 2: APPLY COLUMN MAPPINGS (SELECT ONLY MAPPED COLUMNS)
            logger.info("STEP 2: Applying column mappings")
            source_df = self.apply_column_mappings(
                source_df,
                config.column_mappings,
                config.target_table_structure
            )
            
            # Record source count
            stats.source_count = source_df.count()
            stats.destination_count_before = self.get_table_count(config.destination_table)
            
            logger.info(f"Source records: {stats.source_count:,}")
            logger.info(f"Destination records (before): {stats.destination_count_before:,}")
            
            # ✅ STEP 3: ADD METADATA COLUMNS
            logger.info("STEP 3: Adding metadata columns")
            source_df = self.add_metadata_columns(source_df, config, is_insert=True)
            
            # ✅ STEP 4: GENERATE HASH KEY IF CONFIGURED
            if config.generate_hash_key and config.hash_columns:
                logger.info("STEP 4: Generating hash key")
                source_df = self._add_hash_key(source_df, config.hash_columns)
            
            # ✅ STEP 5: WRITE TO DESTINATION (OVERWRITE MODE = TRUNCATE + RELOAD)
            logger.info("STEP 5: Writing to destination table")
            if config.partition_columns:
                logger.info(f"Partitioning by: {config.partition_columns}")
                self.write_table(
                    source_df,
                    config.destination_table,
                    mode="overwrite",
                    partition_by=config.partition_columns
                )
            else:
                self.write_table(source_df, config.destination_table, mode="overwrite")
            
            # ✅ UPDATE STATISTICS
            stats.records_inserted = stats.source_count
            stats.destination_count_after = stats.source_count
            stats.status = "SUCCESS"
            
            logger.info(f"✅ FULL LOAD COMPLETED SUCCESSFULLY")
            
        except Exception as e:
            stats.status = "FAILED"
            stats.error_message = str(e)
            logger.error(f"❌ FULL LOAD FAILED: {str(e)}")
            raise
        
        finally:
            # ✅ CALCULATE DURATION
            stats.end_time = datetime.now().isoformat()
            stats.load_duration_seconds = (datetime.fromisoformat(stats.end_time) - 
                                          datetime.fromisoformat(stats.start_time)).total_seconds()
        
        return stats
    
    def _add_hash_key(self, df: DataFrame, hash_columns: List[str]) -> DataFrame:
        """
        Add hash key column for change detection.
        
        Generates an MD5 hash of specified columns concatenated together.
        Used for detecting if row data has changed (SCD Type 2).
        
        Args:
            df: Input DataFrame
            hash_columns: Columns to include in hash calculation
            
        Returns:
            DataFrame with added hash_key column
        """
        logger.info(f"Generating hash key from columns: {hash_columns}")
        
        # Concatenate all hash columns (handling nulls)
        concat_expr = concat(*[
            coalesce(col(c).cast(StringType()), lit(""))
            for c in hash_columns
        ])
        
        # Generate MD5 hash of concatenated string
        return df.withColumn("hash_key", md5(concat_expr))


class IncrementalLoadProcessor(BaseIncrementalLoader):
    """
    Processor for INCREMENTAL load operations.
    
    INCREMENTAL loads use a watermark (timestamp or sequence) to load only
    new or modified data. Useful for:
    - Daily incremental loads
    - Event streaming
    - Near real-time data pipelines
    
    PROCESS:
    1. Get last watermark value from destination
    2. Read source and filter for records after watermark
    3. Apply column mappings
    4. Add metadata columns
    5. Append to destination table
    6. Track statistics
    """
    
    def load(self, config: IncrementalLoadConfig) -> LoadStatistics:
        """
        Execute INCREMENTAL load using watermark/timestamp.
        
        Loads only new data since the last watermark value.
        
        Args:
            config: Load configuration
            
        Returns:
            LoadStatistics with operation metrics
        """
        logger.info(f"\n{'='*70}")
        logger.info(f"🔄 Starting INCREMENTAL LOAD: {config.source_table} → {config.destination_table}")
        logger.info(f"{'='*70}")
        
        if not config.watermark_column:
            raise ValueError("Watermark column required for incremental load")
        
        start_time = datetime.now()
        stats = LoadStatistics(
            load_type="INCREMENTAL",
            load_id=config.load_id or "",
            load_name=config.load_name or "",
            start_time=start_time.isoformat()
        )
        
        try:
            stats.destination_count_before = self.get_table_count(config.destination_table)
            
            # ✅ STEP 1: GET LAST WATERMARK VALUE
            logger.info("STEP 1: Getting last watermark value")
            last_watermark = self._get_last_watermark(
                config.destination_table,
                config.watermark_column
            )
            
            logger.info(f"Last watermark value: {last_watermark}")
            
            # ✅ STEP 2: READ SOURCE AND FILTER BY WATERMARK
            logger.info("STEP 2: Reading and filtering source by watermark")
            source_df = self.read_table(config.source_table)
            
            # ✅ APPLY COLUMN MAPPINGS
            source_df = self.apply_column_mappings(
                source_df,
                config.column_mappings,
                config.target_table_structure
            )
            
            # Filter to only records after last watermark
            if last_watermark:
                logger.info(f"Filtering records where {config.watermark_column} > {last_watermark}")
                source_df = source_df.filter(
                    col(config.watermark_column) > lit(last_watermark)
                )
            
            stats.source_count = source_df.count()
            logger.info(f"Records to load: {stats.source_count:,}")
            
            # If no new data, return success
            if stats.source_count == 0:
                logger.info("No new data to load")
                stats.status = "SUCCESS"
                stats.destination_count_after = stats.destination_count_before
                return stats
            
            # ✅ STEP 3: ADD METADATA COLUMNS
            logger.info("STEP 3: Adding metadata columns")
            source_df = self.add_metadata_columns(source_df, config, is_insert=True)
            
            # ✅ STEP 4: APPEND TO DESTINATION
            logger.info("STEP 4: Appending to destination table")
            self.write_table(
                source_df,
                config.destination_table,
                mode="append",
                partition_by=config.partition_columns if config.partition_columns else None
            )
            
            # ✅ UPDATE STATISTICS
            stats.records_inserted = stats.source_count
            stats.destination_count_after = (
                stats.destination_count_before + stats.source_count
            )
            stats.status = "SUCCESS"
            
            logger.info(f"✅ INCREMENTAL LOAD COMPLETED SUCCESSFULLY")
            
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
        """
        Get last watermark value from destination table.
        
        Retrieves the maximum value of the watermark column to know
        where to start filtering new data from.
        
        Args:
            table_path: Destination table path
            watermark_column: Column to get max value from
            
        Returns:
            Maximum watermark value or None if table doesn't exist
        """
        try:
            logger.info(f"Getting max value of {watermark_column} from destination")
            dest_df = self.read_table(table_path)
            watermark = dest_df.agg(max(col(watermark_column))).collect()[0][0]
            logger.info(f"Last watermark: {watermark}")