#!/usr/bin/env python
# coding: utf-8

# ## generic-ingestion 
# 
# null

# In[6]:


FLOW_TYPE       = None
INPUT_DIR       = None
ARCHIVE_DIR     = None
FAILED_DIR      = None
TARGET_SCHEMA   = None
TARGET_TABLE    = None
TABLES_PATH     = None


# In[1]:


from pyspark.sql import functions as F
from pyspark.sql.types import *



# In[4]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %run schema


# In[1]:


PARSERS = {
    "inbound": parse_lineage_inbound,
    "outbound": parse_lineage_outbound,
    "chiller_image": parse_chiller_image,
    "standardcost": parse_standardcost
}

if FLOW_TYPE not in PARSERS:
    raise Exception(f"Unsupported FLOW_TYPE: {FLOW_TYPE}")

parse_fn = PARSERS[FLOW_TYPE]


# In[6]:


def ensure_dirs(*paths):
    for p in paths:
        try:
            mssparkutils.fs.mkdirs(p)
        except Exception:
            pass


def list_files_recursive(path):
    files, stack = [], [path]
    while stack:
        curr = stack.pop()
        for e in mssparkutils.fs.ls(curr):
            if e.isDir:
                stack.append(e.path)
            else:
                files.append(e)
    return files


# In[ ]:


# -------------------------------
# DQ rules
# -------------------------------

# Check 1: Ensure DataFrame is not empty
# Fails if no records are present
def dq_not_empty(df):
    if df.rdd.isEmpty():
        raise Exception("DataFrame is empty")


# Check 2: Ensure DataFrame has columns (schema exists)
# Fails if parser returns no columns
def dq_has_columns(df):
    if not df.columns or len(df.columns) == 0:
        raise Exception("No columns present")


# Check 3: Detect fully NULL rows
# Fails if any row has ALL columns as NULL
def dq_no_fully_null_rows(df):
    null_expr = " AND ".join([f"{c} IS NULL" for c in df.columns])
    if df.filter(null_expr).limit(1).count() > 0:
        raise Exception("Fully NULL row detected")


# Check 4: Detect full-row duplicate records
# Fails if duplicate rows exist across all columns
def dq_no_full_row_duplicates(df):
    if df.count() != df.distinct().count():
        raise Exception("Duplicate rows detected")


# Check 5: Detect corrupt records created by Spark parser
# Fails if _corrupt_record column contains non-null values
def dq_no_corrupt_records(df):
    if "_corrupt_record" in df.columns:
        if df.filter(F.col("_corrupt_record").isNotNull()).limit(1).count() > 0:
            raise Exception("Corrupt records detected")


# Utility: Normalize empty strings to NULL
# Converts "" or whitespace-only values to NULL for consistent DQ checks
def dq_normalize_empty_strings(df):
    for c in df.columns:
        df = df.withColumn(
            c,
            F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c))
        )
    return df


# -------------------------------
# Generic Pre-DQ Runner (with DQ logging)
# -------------------------------
# Executes all DQ rules sequentially
# Logs PASS/FAIL per rule
# Stops ingestion immediately on first failure
def run_generic_pre_dq(df, file_name, dq_log):

    # Ordered list of DQ checks to execute
    dq_steps = [
        ("NOT_EMPTY", dq_not_empty),               # Data availability check
        ("HAS_COLUMNS", dq_has_columns),           # Schema existence check
        ("NO_CORRUPT", dq_no_corrupt_records),     # Parser corruption check
        ("NO_FULLY_NULL_ROWS", dq_no_fully_null_rows),  # Blank row check
        ("NO_DUPLICATES", dq_no_full_row_duplicates)    # Duplicate row check
    ]

    # Execute each DQ rule
    for rule_name, rule_fn in dq_steps:
        try:
            rule_fn(df)   # Run DQ check
            dq_log.append((file_name, rule_name, "PASS", None))  # Log success
        except Exception as e:
            dq_log.append((file_name, rule_name, "FAIL", str(e)[:300]))  # Log failure
            raise Exception(f"DQ_{rule_name}_FAILED")  # Stop ingestion


# In[7]:


# ==========================================================
# Ensure archive / failed directories exist
# ==========================================================
ensure_dirs(ARCHIVE_DIR, FAILED_DIR)

ALLOWED_EXTENSIONS = ["txt", "csv", "xlsx"]

# ==========================================================
# List input files
# ==========================================================
files = [
    f for f in list_files_recursive(INPUT_DIR)
    if f.name.lower().split(".")[-1] in ALLOWED_EXTENSIONS
]

log = []
dq_log = []

# ==========================================================
# Target Delta path
# ==========================================================
TARGET_TABLE_PATH = f"{TABLES_PATH}/{TARGET_SCHEMA}/{TARGET_TABLE}"

# ==========================================================
# Ingestion loop
# ==========================================================
for f in files:
    try:
        df_out = parse_fn([f.path])

        df_out = dq_normalize_empty_strings(df_out)

        run_generic_pre_dq(df_out, f.name, dq_log)

        (
            df_out.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(TARGET_TABLE_PATH)
        )

        row_count = df_out.count()

        log.append((f.name, "SUCCESS", str(row_count), "DQ_PASSED"))

        # mssparkutils.fs.mv(
        #     f.path,
        #     f"{ARCHIVE_DIR}/{f.name}",
        #     overwrite=True
        # )

    except Exception as e:
        log.append((f.name, "FAILED", "0", str(e)[:300]))

        mssparkutils.fs.mv(
            f.path,
            f"{FAILED_DIR}/{f.name}",
            overwrite=True
        )


# In[8]:


LOG_SCHEMA = "Log"
LOG_TABLE  = "Ingestion"

LOG_TABLE_PATH = f"{TABLES_PATH}/{LOG_SCHEMA}/{LOG_TABLE}"

log_df = spark.createDataFrame(
    log,
    StructType([
        StructField("file", StringType(), True),
        StructField("status", StringType(), True),
        StructField("rows", StringType(), True),
        StructField("dq_status", StringType(), True)
    ])
)

log_df = (
    log_df
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("target_table", F.lit(f"{TARGET_SCHEMA}.{TARGET_TABLE}"))
)

(
    log_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(LOG_TABLE_PATH)
)


# In[ ]:


DQ_LOG_SCHEMA = "Log"
DQ_LOG_TABLE  = "dq_check"

DQ_LOG_TABLE_PATH = f"{TABLES_PATH}/{DQ_LOG_SCHEMA}/{DQ_LOG_TABLE}"

dq_log_df = spark.createDataFrame(
    dq_log,
    StructType([
        StructField("file", StringType(), True),
        StructField("dq_rule", StringType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True)
    ])
)

dq_log_df = (
    dq_log_df
    .withColumn("dq_ts", F.current_timestamp())
    .withColumn("target_table", F.lit(f"{TARGET_SCHEMA}.{TARGET_TABLE}"))
)

(
    dq_log_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(DQ_LOG_TABLE_PATH)
)

dq_log_df.show(truncate=False)

