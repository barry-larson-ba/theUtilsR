# theUtilsR

Helpers for pulling Oracle tables into Spark or a DBI connection, plus the
reusable credentialing-report transforms.

The R port of the Python [`theUtils`](../theUtils) package. Same business rules,
same test expectations, different shape where R can do better — see
[One implementation, not three](#one-implementation-not-three).

```r
# install once on the cluster, or devtools::load_all() a Git-folder checkout
library(theUtilsR)
library(dplyr)

configure(con = DBI::dbConnect(odbc::odbc(), dsn = "ORCL"))

report <- read_source(BASE_DATA_HIST, where = "month = 0") |>
  is_active_credentialing_row() |>
  classify_facility_status() |>
  exclude_faccodes() |>
  facility_crosstab(index = TELEMED_IDENTITY_COLUMNS) |>
  select_report_columns() |>
  arrange(pract_id) |>
  collect()
```

---

## What is in here

| File | What it holds |
|---|---|
| `R/oracle_utils.R` | connection configuration, `read_oracle()` and its stages |
| `R/sources.R` | logical table names, and the swappable resolver that maps them |
| `R/report_transforms.R` | the eleven business-rule transforms, and every constant they are defined against |
| `R/crosstab.R` | the facility crosstab — the `proc transpose` step |
| `R/facility_codes.R` | the facility code/name registry |
| `R/report_layout.R` | column order and display headers, as data |
| `R/pipeline.R` | `run_stages()` — runs a report expressed as a named stage list |

Plus `tests/testthat/` (the suite), `notebooks/` (six Databricks R notebooks),
and `man/` (roxygen-generated; do not edit by hand).

Working on the package itself? Read [`DEVELOPMENT.md`](DEVELOPMENT.md) first — it
holds the constraints that are load-bearing but not obvious from the source: which
R functions survive translation to SQL, where the local/remote dispatch is
unavoidable, and the two stage orderings that change the report silently if moved.

---

## One implementation, not three

The Python package writes every business rule **three times** — pandas, Polars,
PySpark — and runs one shared table of test cases against all three so they
cannot drift apart. That is a real cost paid for a real reason: those three
libraries share no common verb set.

R does not have that problem. dplyr verbs run unchanged on a local tibble and on
a remote table, because dbplyr translates them into SQL when the input is
remote. So each rule is written **once**, and drift is impossible rather than
merely tested for.

What survives from the Python design is the *shape* of the API. There, the
pandas variant took a data frame and the Polars/Spark variants returned a bare
expression to splice into `filter()` or `with_columns()`. Both forms are
genuinely useful, so both exist here — and the data-taking form is implemented
in terms of the expression form, so the rule still has exactly one definition:

```r
is_active_credentialing_expr()      # a quosure, for filter(df, !!expr)
is_active_credentialing_row(data)   # filter(data, !!expr) -- same rule
```

| Python | R |
|---|---|
| `classify_facility_status_pandas(df)` | `classify_facility_status(data)` |
| `classify_facility_status_polars()` → `Expr` | `facility_status_expr()` → quosure |
| `classify_facility_status_spark()` → `Column` | *(the same quosure)* |

### Where the abstraction leaks

Two functions dispatch on whether the input is a lazy table. Both are marked in
the source, and both branches are covered by the same tests.

- **`first_per_group()` / `dedupe_first()`** — locally, `arrange()` fixes row
  order and a later `row_number()` sees it. On a lazy table `arrange()` becomes
  an `ORDER BY` in a subquery, which most engines are free to ignore; the window
  frame's ordering has to be set with `dbplyr::window_order()` instead.
- **`facility_crosstab()`** — `tidyr::pivot_wider()` has no lazy-table method,
  so the remote branch writes the classic SQL pivot by hand, one conditional
  aggregate per facility: `MAX(CASE WHEN faccode = 'OAK' THEN stat END)`.

A third thing, not a dispatch but a deliberate omission: **the stages do not
sort a remote result.** Ordering an intermediate remote result is meaningless —
the `ORDER BY` lands in a subquery the engine may discard. Sort once, at the
end, immediately before `collect()`.

---

## Reading Oracle

One `read_oracle()`, dispatching on the connection it is given. This is the one
real design change from the Python package, which takes a `SparkSession` and is
Spark-only.

| `con` is | how it reads | what you configure |
|---|---|---|
| a `sparklyr` connection | `spark_read_jdbc()`, query wrapped as `(query) tmp` | `jdbc_url`, `username`, `password` |
| a `DBIConnection` | `tbl(con, sql(query))` | nothing more — the connection *is* the credential |

```r
# DBI
configure(con = DBI::dbConnect(odbc::odbc(), dsn = "ORCL", uid = user, pwd = pw))

# sparklyr — the Oracle JDBC driver must be attached to the cluster
configure(
  con      = sparklyr::spark_connect(method = "databricks"),
  jdbc_url = "jdbc:oracle:thin:@//host:1521/service",
  username = user,
  password = pw
)
```

`sparklyr` is a `Suggests` dependency, not an `Imports`: the package installs
and its tests run without it. That mirrors the Python package's rule that
`import theUtils` must never require pyspark.

Credentials live in the notebook, not in source control, and they are **session**
state — everything sharing an R session shares one connection identity.

### The pipeline `read_oracle()` composes

1. `read_oracle_raw()` — execute the query
2. `convert_integer_decimals()` — scale-0 decimals to `BIGINT` (a no-op off the
   Spark path)
3. `apply_schema()` — explicit per-column casts
4. collection — `"source"` (default, stays remote), `"tibble"`, `"data.frame"`
   or `"data.table"`

Step 2 precedes step 3 so a caller-supplied `schema` always wins over the
automatic decimal handling.

Because the statement becomes a subquery it must **not** end in a semicolon, and
row limits use Oracle syntax (`FETCH FIRST n ROWS ONLY` or `ROWNUM`), not
`LIMIT`.

### The Oracle `NUMBER` problem

| Oracle declaration | arrives as | recast by the default? |
|---|---|---|
| `NUMBER(10, 2)` | `DecimalType(10, 2)` | no — a real fixed-point value |
| `NUMBER(38)` | `DecimalType(38, 0)` | **yes** — scale explicitly 0 |
| `NUMBER` | `DecimalType(38, 10)` | no — scale not declared at all |

The last row surprises people: a bare `NUMBER` reports no scale, and Spark's
Oracle dialect represents it as `DecimalType(38, 10)`, so an ID column declared
as plain `NUMBER` arrives with ten decimal places it never uses. Pass
`decimal_scales = c(0, 10)` to catch those — opt-in, because scale 10 covers
both integer IDs and genuinely fractional values and the cast truncates toward
zero, silently.

---

## Sources

Pipeline stages take frames; none of them should know a schema name, a
connection, or which system is authoritative this quarter. `R/sources.R` is the
one place that does.

```r
read_source(BASE_DATA_HIST, where = "month = 0")
```

A resolver is just a function `(name, columns, where, return_type) -> table`, so
it can be swapped:

```r
use_resolver(catalog_reader(sc))                       # Unity Catalog
use_resolver(frame_reader(list(base_data_hist = df)))  # a test, no connection
```

`use_resolver()` returns the previous resolver so a temporary swap can be undone.

**Do not put a table name in a transform or a notebook cell.** Add a logical name
to `R/sources.R` instead.

---

## Reports

There is deliberately **no orchestrating `build()` function**. The stages are
small frame-to-frame functions meant to be composed and inspected one at a time;
the composition is a notebook-level decision.

`run_stages()` does not change that — it is the *applier*, and it knows nothing
about any particular report. Handing it a named list of stages buys what a `|>`
chain cannot give you: the report reads as a table of contents, a failing stage
is named in the error rather than surfacing as an anonymous dplyr backtrace,
`through =` runs a prefix for debugging, and the row count after each stage is
recorded so "which stage dropped them?" has an answer.

```r
report <- base |> run_stages(telemed_stages(facilities), count = TRUE)

stage_trace()
#>   stage           rows  delta
#>   <input>            8     NA
#>   population         7     -1
#>   reportable         6     -1
#>   drop_admin         5     -1
#>   transpose          2     -3
```

Counting a lazy table costs a query per stage, so it is off by default on remote
input; `dplyr::compute()` the read once first and it becomes free. See
`notebooks/weekly_telemed_job.r`.

Both formulations of the telemed report — the chain in
`notebooks/weekly_telemed_pipeline.r` and the stage list in
`notebooks/weekly_telemed_job.r` — are pinned by
`tests/testthat/test-weekly_telemed.R`, which runs each on a local tibble *and*
on a real SQL engine and asserts the whole output frame against one expectation.
They cannot diverge from each other or from the package.

Two ordering constraints in the telemed pipeline that are easy to break:

- The **decimal fix before the explicit schema** rule in `read_oracle()`.
- `dominant_telemed_program()` and `home_facility_rows()` run **before**
  `exclude_faccodes()`. In the SAS original the exclusion applied only at the
  transpose, so an administrative faccode still counted toward a practitioner's
  program total. Swapping these changes the answer silently.

---

## Testing

```r
devtools::test()     # or: testthat::test_local(".")
devtools::check()    # 0 errors, 0 warnings, 0 notes
```

Every rule test runs **twice**: once on a tibble, once on the same rows in
DuckDB as a lazy table. DuckDB stands in for Spark and Oracle — it is the SQL
engine that happens to be installable as an R package, and it exercises the
translation layer, which is where a portability bug actually lives.

DuckDB is a `Suggests` dependency. A run without it exercises the local paths and
reports the rest as skips; that is the intended behaviour, not a degraded run.

### Translation-sensitive functions

Not every R function has a SQL translation. These are the ones this package
depends on, and the ones to re-check when adding a backend:

`toupper`, `substr`, `grepl`, `case_when`, `coalesce`, `if_else`, `is.na`,
`stringr::str_replace_all`, `stringr::str_trim`.

Notably **`gsub()` is not translated** by several backends (DuckDB among them),
which is why the program-text cleanup uses stringr.

---

## Installing

```r
# from a Git-folder checkout, picking up edits without a reinstall
devtools::load_all("/Workspace/Repos/you/theUtilsR")

# or build and install onto the cluster
devtools::install(".")
```

`notebooks/loading_in_databricks.r` covers this properly: finding the repo root,
`load_all()` versus installing, dependencies, persistence across cluster
restarts, and the failure modes. Run it first on a new cluster.

Unlike the Python port there is no `sys.path` puzzle to solve — R has no
equivalent of an automatic repo-root entry, so the package is always loaded
deliberately, and the notebooks say which way they are doing it.

---

## Notebooks

| Notebook | What it covers |
|---|---|
| `weekly_telemed_pipeline.r` | the report, one stage per cell. Runs with no Oracle connection |
| `weekly_telemed_report.r` | the whole SAS macro, one cell per SAS step — titles, count, workbook and email included |
| `weekly_telemed_job.r` | the runnable job: stage list, row-count trace, checks, workbook, exit payload |
| `api_reference.r` | every exported function, with the reasoning behind it |
| `oracle_utils_demo.r` | connections, `read_oracle()`, type handling. Uses DuckDB as a stand-in |
| `loading_in_databricks.r` | getting the package loaded from a Git folder — start here on a new cluster |

All six are in Databricks source format and run top to bottom on any cluster.
