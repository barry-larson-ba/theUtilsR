# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Oracle with theUtilsR
# MAGIC
# MAGIC How to configure a connection, what `read_oracle()` does at each stage, and
# MAGIC the type handling that is easy to get wrong.
# MAGIC
# MAGIC **Most of this notebook runs without Oracle.** The sections that need a real
# MAGIC connection are marked **[NEEDS ORACLE]**; everything else runs against an
# MAGIC in-process DuckDB standing in for a DBI-connected database, which exercises
# MAGIC the same code path.

# COMMAND ----------

find_package_root <- function(start = getwd(), max_up = 6L) {
  path <- normalizePath(start, winslash = "/", mustWork = FALSE)

  for (i in seq_len(max_up)) {
    if (file.exists(file.path(path, "DESCRIPTION"))) return(path)

    parent <- dirname(path)

    if (identical(parent, path)) break

    path <- parent
  }

  NULL
}

root <- find_package_root()

if (!is.null(root) && requireNamespace("devtools", quietly = TRUE)) {
  devtools::load_all(root, quiet = TRUE)
} else {
  library(theUtilsR)
}

library(dplyr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Two ways in, one function
# MAGIC
# MAGIC The Python package is Spark-only: it takes a `SparkSession` and reads over
# MAGIC JDBC. This port takes a **connection object** and dispatches on it, because R
# MAGIC users reach for a DBI connection at least as often as for Spark.
# MAGIC
# MAGIC | `con` is | `read_oracle_raw()` does | what you must configure |
# MAGIC |---|---|---|
# MAGIC | `sparklyr` connection | `spark_read_jdbc()`, query wrapped as `(query) tmp` | `jdbc_url`, `username`, `password` |
# MAGIC | `DBIConnection` | `tbl(con, sql(query))` | nothing more — the connection *is* the credential |
# MAGIC
# MAGIC Either way you get back something lazy, so further dplyr verbs still run on
# MAGIC the server.
# MAGIC
# MAGIC `sparklyr` is a `Suggests` dependency, not an `Imports`: the package installs
# MAGIC and its tests run without it. That mirrors the Python package's rule that
# MAGIC `import theUtils` must never require pyspark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration
# MAGIC
# MAGIC Credentials live in the notebook, not in source control. They are **session**
# MAGIC state, held in an environment inside the package, so everything sharing an R
# MAGIC session shares one connection identity.
# MAGIC
# MAGIC The Python original goes to some trouble here: it deliberately leaves four
# MAGIC names undefined at module level, because Python resolves a function's globals
# MAGIC in the module where the function was defined, so assigning them in the
# MAGIC notebook does not work. R has no such trap — a package function's environment
# MAGIC is the package namespace, and that namespace is sealed — so `configure()` is
# MAGIC the only way in, rather than a convenience wrapper over one.

# COMMAND ----------

# [NEEDS ORACLE] the DBI path
#
# configure(
#   con = DBI::dbConnect(
#     odbc::odbc(),
#     Driver   = "Oracle 21 ODBC driver",
#     DBQ      = "//host:1521/service",
#     UID      = dbutils.secrets.get("scope", "oracle-user"),
#     PWD      = dbutils.secrets.get("scope", "oracle-password")
#   )
# )

# COMMAND ----------

# [NEEDS ORACLE] the Spark path
#
# The Oracle JDBC driver has to be attached to the cluster; the package does not
# ship it.
#
# configure(
#   con      = sparklyr::spark_connect(method = "databricks"),
#   jdbc_url = "jdbc:oracle:thin:@//host:1521/service",
#   username = dbutils.secrets.get("scope", "oracle-user"),
#   password = dbutils.secrets.get("scope", "oracle-password")
# )

# COMMAND ----------

# MAGIC %md
# MAGIC `configure()` leaves alone anything it is not given, so you can update
# MAGIC settings piecemeal. `is_configured()` is the guard worth calling before a
# MAGIC long-running read — otherwise the error surfaces only once a query is issued.

# COMMAND ----------

reset_configuration()

configure(jdbc_url = "jdbc:oracle:thin:@//host:1521/svc", username = "reporting")
configure(password = "not-a-real-password")

str(get_oracle_options())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A stand-in database
# MAGIC
# MAGIC DuckDB, in process, so the rest of this notebook runs anywhere. It is a DBI
# MAGIC connection, so it goes down exactly the same branch a real Oracle ODBC
# MAGIC connection would.

# COMMAND ----------

reset_configuration()

con <- DBI::dbConnect(duckdb::duckdb())

DBI::dbWriteTable(con, "members", data.frame(
  member_id  = c(1001, 1002, 1003),
  first_name = c("Rosa", "Ida", "Wei"),
  balance    = c(120.5, 0, 33.25)
), overwrite = TRUE)

configure(con = con)

is_configured()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `read_oracle_raw()` — the query, untouched
# MAGIC
# MAGIC The statement becomes a subquery, which has two consequences worth
# MAGIC remembering:
# MAGIC
# MAGIC * **no trailing semicolon** — it would break the wrapping
# MAGIC * **Oracle row-limit syntax**, `FETCH FIRST n ROWS ONLY` or `ROWNUM`, not
# MAGIC   `LIMIT`
# MAGIC
# MAGIC Any valid `SELECT` works, so you never need to name a table.

# COMMAND ----------

raw <- read_oracle_raw("SELECT member_id, first_name, balance FROM members")

class(raw)

# COMMAND ----------

# MAGIC %md
# MAGIC It is lazy. Verbs compose onto the query and run on the server; nothing
# MAGIC crosses the wire until `collect()`.

# COMMAND ----------

filtered <- raw |> filter(balance > 0) |> select(member_id, balance)

dbplyr::remote_query(filtered)

# COMMAND ----------

collect(filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `read_oracle()` — the entry point
# MAGIC
# MAGIC Stages, in this order:
# MAGIC
# MAGIC 1. `read_oracle_raw()` — execute the query
# MAGIC 2. `convert_integer_decimals()` — scale-0 decimals to `BIGINT`
# MAGIC 3. `apply_schema()` — explicit per-column casts
# MAGIC 4. collection — `source`, `tibble`, `data.frame` or `data.table`
# MAGIC
# MAGIC **Step 2 precedes step 3** so that a caller-supplied `schema` always wins over
# MAGIC the automatic decimal handling. Preserve that ordering if you modify it.

# COMMAND ----------

read_oracle("SELECT * FROM members", return_type = "tibble")

# COMMAND ----------

# MAGIC %md
# MAGIC ### `return_type` is validated before the query runs
# MAGIC
# MAGIC A typo here is a programming error, and finding it after a multi-minute
# MAGIC Oracle round trip helps nobody. Keep new argument validation up there with it.

# COMMAND ----------

tryCatch(
  read_oracle("SELECT * FROM members", return_type = "polars"),
  error = function(e) conditionMessage(e)
)

# COMMAND ----------

RETURN_TYPES

# COMMAND ----------

# MAGIC %md
# MAGIC `"source"` is the default and leaves the result on the server. The other three
# MAGIC pull the **entire** result into driver memory, so filter or aggregate first.

# COMMAND ----------

list(
  source     = class(read_oracle("SELECT * FROM members")),
  tibble     = class(read_oracle("SELECT * FROM members", return_type = "tibble")),
  data.frame = class(read_oracle("SELECT * FROM members", return_type = "data.frame"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `apply_schema()` — casting columns
# MAGIC
# MAGIC A named character vector of column to **canonical type**. Canonical rather
# MAGIC than backend-native so the same schema definition survives a move from Oracle
# MAGIC to Spark — the only spelling that actually differs is the string type, which
# MAGIC Spark calls `STRING` and everyone else calls `VARCHAR`.
# MAGIC
# MAGIC It casts columns that already exist. It does not add or rename them, so the
# MAGIC schema has to describe the query you actually ran.

# COMMAND ----------

apply_schema(
  tibble::tibble(pract_id = "101", month = "0", flag = 1),
  c(pract_id = "integer", month = "integer", flag = "logical")
)

# COMMAND ----------

# MAGIC %md
# MAGIC A missing column is caught up front, so the message names what is missing and
# MAGIC what is available. Letting the engine hit it instead produces an
# MAGIC `UNRESOLVED_COLUMN` buried in a Java stack trace, which hides the one useful
# MAGIC line.

# COMMAND ----------

tryCatch(
  apply_schema(tibble::tibble(a = 1), c(nope = "integer")),
  error = function(e) conditionMessage(e)
)

# COMMAND ----------

# MAGIC %md
# MAGIC `SCHEMAS` is a placeholder for schemas worth defining once and sharing across
# MAGIC notebooks. It is **not** consulted by `read_oracle()` — pass the value
# MAGIC through yourself: `read_oracle(query, schema = SCHEMAS$member)`.

# COMMAND ----------

str(SCHEMAS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `convert_integer_decimals()` — the Oracle NUMBER problem
# MAGIC
# MAGIC Oracle numeric columns arrive over JDBC as decimals, which is faithful but
# MAGIC awkward downstream. Which decimal you get depends on how the column was
# MAGIC declared, and the distinction matters:
# MAGIC
# MAGIC | Oracle declaration | arrives as | touched by the default? |
# MAGIC |---|---|---|
# MAGIC | `NUMBER(10, 2)` | `DecimalType(10, 2)` | no — a real fixed-point value |
# MAGIC | `NUMBER(38)` | `DecimalType(38, 0)` | **yes** — scale explicitly 0 |
# MAGIC | `NUMBER` | `DecimalType(38, 10)` | no — scale not declared at all |
# MAGIC
# MAGIC That last row is the one that surprises people. A bare `NUMBER` does not
# MAGIC report a scale, and Spark's Oracle dialect represents it as
# MAGIC `DecimalType(38, 10)` — so an ID column declared as plain `NUMBER` shows up
# MAGIC with ten decimal places it never uses.
# MAGIC
# MAGIC Pass `decimal_scales = c(0, 10)` to catch those too. That is opt-in because
# MAGIC scale 10 is ambiguous: it covers both integer IDs and genuinely fractional
# MAGIC values, and the cast truncates toward zero, **silently**. Only widen it for
# MAGIC columns you know hold whole numbers.

# COMMAND ----------

DEFAULT_DECIMAL_SCALES

# COMMAND ----------

# MAGIC %md
# MAGIC ### This is a Spark-path concern only
# MAGIC
# MAGIC On a DBI connection there is no Decimal intermediary — the driver maps Oracle
# MAGIC `NUMBER` to an R numeric on the way out — so there is nothing to recast and
# MAGIC the function returns its input unchanged. `read_oracle()` still calls it
# MAGIC unconditionally, so the pipeline reads the same either way.

# COMMAND ----------

identical(
  collect(convert_integer_decimals(read_oracle_raw("SELECT * FROM members"))),
  collect(read_oracle_raw("SELECT * FROM members"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Sources — never name a table in a notebook
# MAGIC
# MAGIC `read_source()` is the entry point for pipeline code. `R/sources.R` is the
# MAGIC only place that maps a logical name to a physical one; do not put a table name
# MAGIC in a transform or a notebook cell, add a logical name there instead.

# COMMAND ----------

ORACLE_TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC Point the resolver somewhere else once, and every caller follows. Here at a
# MAGIC catalog table, using the DuckDB connection as the stand-in.

# COMMAND ----------

DBI::dbWriteTable(con, "catalog_members", data.frame(
  pract_id = c(101, 102), month = c(0, 1)
), overwrite = TRUE)

previous <- use_resolver(
  catalog_reader(con, tables = c(base_data_hist = "catalog_members"))
)

read_source(BASE_DATA_HIST, where = "month == 0", return_type = "tibble")

# COMMAND ----------

use_resolver(previous)

# For a test, `frame_reader()` needs no connection at all.
previous <- use_resolver(frame_reader(list(
  base_data_hist = tibble::tibble(pract_id = c(1, 2), month = c(0, 1))
)))

read_source(BASE_DATA_HIST, where = "month == 0")

# COMMAND ----------

use_resolver(previous)

DBI::dbDisconnect(con, shutdown = TRUE)
reset_configuration()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Things to keep in mind
# MAGIC
# MAGIC * Credentials are **session** state. Everything sharing an R session shares
# MAGIC   one connection identity.
# MAGIC * `read_oracle()` returns something lazy by default. Filter and aggregate
# MAGIC   before collecting, not after.
# MAGIC * `arrange()` once, at the end, immediately before `collect()`. An `ORDER BY`
# MAGIC   in a subquery is something most engines are free to discard.
# MAGIC * Never commit a credential. `.gitignore` already covers `tnsnames.ora`,
# MAGIC   `wallet/`, `.env` and the usual key extensions, but the real defence is
# MAGIC   reading them from a secret scope, as the cells above do.
