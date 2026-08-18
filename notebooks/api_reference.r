# Databricks notebook source
# MAGIC %md
# MAGIC # theUtilsR — API reference
# MAGIC
# MAGIC A tour of every exported function, with the reasoning that is not obvious from
# MAGIC the signature. Runs top to bottom on any cluster: nothing here needs Oracle.
# MAGIC
# MAGIC If you are coming from the Python `theUtils`, start at section 1 — the shape
# MAGIC of the API is the main thing that changed.

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
# MAGIC ## 1. Why there is one implementation, not three
# MAGIC
# MAGIC The Python package writes every business rule three times — once for pandas,
# MAGIC once for Polars, once for PySpark — and runs one shared table of test cases
# MAGIC against all three so they cannot drift apart. That is a real cost paid for a
# MAGIC real reason: those three libraries share no common verb set.
# MAGIC
# MAGIC R does not have that problem. dplyr verbs run unchanged on a local frame and
# MAGIC on a remote table, because dbplyr translates them into SQL when the input is
# MAGIC remote. So each rule is written **once**, and drift is impossible rather than
# MAGIC merely tested for.
# MAGIC
# MAGIC | Python | R |
# MAGIC |---|---|
# MAGIC | `classify_facility_status_pandas(df)` | `classify_facility_status(data)` |
# MAGIC | `classify_facility_status_polars()` → `Expr` | `facility_status_expr()` → quosure |
# MAGIC | `classify_facility_status_spark()` → `Column` | *(same quosure)* |
# MAGIC
# MAGIC The asymmetry the Python package has — data-taking on one backend, expression-
# MAGIC returning on the others — survives here as a **pair**, because both forms are
# MAGIC genuinely useful. The data-taking form is implemented in terms of the
# MAGIC expression form, so the rule still has exactly one definition:
# MAGIC
# MAGIC ```r
# MAGIC is_active_credentialing_row <- function(data) {
# MAGIC   dplyr::filter(data, !!is_active_credentialing_expr())
# MAGIC }
# MAGIC ```

# COMMAND ----------

# The two forms, same answer.
rows <- tibble::tibble(
  primary_fac_flag = c("Y", "N"),
  current_status   = c("CURRENT", "CURRENT"),
  status_category  = c("ACTIVE", "TELEMEDICINE AFFILIATE")
)

list(
  applied  = classify_facility_status(rows)$stat,
  spliced  = mutate(rows, my_name = !!facility_status_expr())$my_name
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Where the abstraction leaks
# MAGIC
# MAGIC Two functions dispatch on whether the input is a lazy table. Both are marked
# MAGIC in the source, and both branches are covered by the same tests.
# MAGIC
# MAGIC **`first_per_group()` / `dedupe_first()`** — locally, `arrange()` fixes row
# MAGIC order and a later `row_number()` sees it. On a lazy table `arrange()` becomes
# MAGIC an `ORDER BY` in a subquery, which most engines are free to ignore; the window
# MAGIC frame's ordering has to be set with `dbplyr::window_order()` instead.
# MAGIC
# MAGIC **`facility_crosstab()`** — `tidyr::pivot_wider()` has no lazy-table method,
# MAGIC so the remote branch writes the classic SQL pivot by hand, one conditional
# MAGIC aggregate per facility: `MAX(CASE WHEN faccode = 'OAK' THEN stat END)`.
# MAGIC
# MAGIC A third thing worth knowing, which is not a dispatch but a deliberate
# MAGIC omission: **the stages do not sort a remote result.** See section 7.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ranking — `first_per_group()` and `dedupe_first()`
# MAGIC
# MAGIC `desc` takes either one value for every ordering column or one per column, so
# MAGIC a ranking can mix directions. "Highest count, then earliest name" is the
# MAGIC common case: it makes a tie-break deterministic, which is what stops a
# MAGIC week-over-week diff of the report from flapping.

# COMMAND ----------

counts <- tibble::tibble(
  pract_id  = c(1, 1, 2, 2),
  program   = c("Zebra", "Alpha", "TeleICU", "TelePsych"),
  fac_count = c(2, 2, 3, 1)
)

first_per_group(counts, "pract_id", c("fac_count", "program"), desc = c(TRUE, FALSE))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Missing values sort last, in both directions
# MAGIC
# MAGIC That is not any engine's default, and no two engines agree on it — R puts
# MAGIC `NA` last regardless of direction, most SQL dialects put `NULL` last only when
# MAGIC descending, and some put it first. Without an explicit rule, a missing value
# MAGIC in an ordering column would pick a different winner depending on where the
# MAGIC report ran.
# MAGIC
# MAGIC `first_per_group()` asks for it explicitly, by materialising an `IS NULL` flag
# MAGIC as a real column and sorting on that first. (`window_order()` accepts only
# MAGIC bare column names or `desc(col)`, so the flag cannot be an inline expression —
# MAGIC that is why the column exists.)

# COMMAND ----------

with_nulls <- tibble::tibble(
  pract_id = c(1, 1),
  program  = c("has-value", "is-null"),
  n        = c(1, NA)
)

list(
  descending = first_per_group(with_nulls, "pract_id", "n", desc = TRUE)$program,
  ascending  = first_per_group(with_nulls, "pract_id", "n", desc = FALSE)$program
)

# COMMAND ----------

# MAGIC %md
# MAGIC `dedupe_first()` is `first_per_group()` with an ascending default — the SAS
# MAGIC `proc sort; by X; if first.X;` idiom. Its `order_by` defaults to the group
# MAGIC keys, which is deterministic on those and **arbitrary on every other column**.
# MAGIC Name the columns you actually want to rank on whenever more than one row per
# MAGIC group can survive.

# COMMAND ----------

dedupe_first(
  tibble::tibble(
    pract_id = c(1, 1), faccode = c("OAK", "OAK"),
    rank = c(2, 1), note = c("loser", "winner")
  ),
  by = c("pract_id", "faccode"),
  order_by = "rank"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The rules
# MAGIC
# MAGIC Every business-rule constant lives at the top of `R/report_transforms.R`.
# MAGIC Change a rule there, never inside a transform body — that is the point of the
# MAGIC whole refactor.

# COMMAND ----------

str(list(
  credentialed_categories = CREDENTIALED_CATEGORIES,
  provisional_prefix      = PROVISIONAL_PREFIX,
  telemed_substring       = TELEMED_SUBSTRING,
  telemed_category        = TELEMED_CATEGORY,
  status_labels           = STATUS_LABELS
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Two telemedicine constants, deliberately
# MAGIC
# MAGIC `TELEMED_SUBSTRING` (`"TELEMED"`) is what the classifier looks for *inside* a
# MAGIC status category when deciding a facility's status. `TELEMED_CATEGORY`
# MAGIC (`"TELEMEDICINE AFFILIATE"`) is the exact, full category that defines the
# MAGIC report population. The second is stricter than the first, on purpose.
# MAGIC
# MAGIC `"REGIONAL TELEMED UNIT"` is the row that shows the difference: it classifies
# MAGIC as `T`, but it does not put anyone in the population.

# COMMAND ----------

edge <- tibble::tibble(
  pract_id         = 1,
  primary_fac_flag = "N",
  current_status   = "CURRENT",
  status_category  = "REGIONAL TELEMED UNIT",
  primary_record   = "N",
  credentialed     = "Y",
  month            = 0L
)

list(
  classifies_as = classify_facility_status(edge)$stat,
  in_population = nrow(is_current_telemed_row(edge))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Missing values in the classifier
# MAGIC
# MAGIC An `NA` in any tested column makes its condition `NA`, which `case_when()`
# MAGIC treats as "not matched" — so the row falls through to `STATUS_INACTIVE`. That
# MAGIC is the same answer pandas gives (where the comparisons coerce `NaN` to
# MAGIC `FALSE`) and the same answer SQL gives.

# COMMAND ----------

classify_facility_status(tibble::tibble(
  primary_fac_flag = NA_character_,
  current_status   = NA_character_,
  status_category  = NA_character_
))$stat

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The registries
# MAGIC
# MAGIC `FACILITY_CODES` is **order-sensitive** — it drives report column layout, so
# MAGIC do not sort it on the assumption that it is arbitrary. `EXCLUDED_FACCODES` is
# MAGIC not; membership testing is the only thing it is ever used for.
# MAGIC
# MAGIC Both are checked for internal drift at package load. A code added without a
# MAGIC name, a duplicated code, or a code that is both reportable and excluded stops
# MAGIC the package from loading rather than producing a wrong report six weeks later.

# COMMAND ----------

list(
  n_reportable = length(FACILITY_CODES),
  first_three  = FACILITY_CODES[1:3],
  excluded     = EXCLUDED_FACCODES,
  oakland      = FACILITY_NAMES[["OAK"]]
)

# COMMAND ----------

# The drift checks, called directly. .onLoad() runs these; they are exported so
# you can re-run them after editing a registry interactively.
tryCatch(
  validate_facility_registry(codes = c("ANT", "FRE"), names_ = c(ANT = "Antioch")),
  error = function(e) conditionMessage(e)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The crosstab
# MAGIC
# MAGIC Two things it does that a bare pivot does not:
# MAGIC
# MAGIC * every code in the registry becomes a column, in registry order, whether or
# MAGIC   not any row mentioned it
# MAGIC * gaps are filled with a blank, not `NA` — blank is a *meaningful* status
# MAGIC   here, and `NA` would render as the string "NA" in most writers

# COMMAND ----------

one_row <- tibble::tibble(pract_id = 1, faccode = "OAK", stat = "T")

crosstab <- facility_crosstab(one_row, index = "pract_id")

list(
  columns   = colnames(crosstab),
  oak       = crosstab$OAK,
  # SAC appeared in no row and is still a column, holding the fill value
  sac       = crosstab$SAC,
  any_na    = anyNA(crosstab)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `exclude_faccodes()` and the missing-faccode case
# MAGIC
# MAGIC `!(faccode %in% excluded)` is `NA` for a missing faccode, and both
# MAGIC `dplyr::filter()` and a SQL `WHERE` drop those rows — whereas pandas'
# MAGIC `~Series.isin()` returns `True` for `NaN` and keeps them. The rule names the
# MAGIC case explicitly rather than inheriting whichever answer the engine happens to
# MAGIC give, so the R port answers what the Python one answers.

# COMMAND ----------

exclude_faccodes(tibble::tibble(faccode = c("OAK", "REG", NA)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Sorting, and when it means anything
# MAGIC
# MAGIC The stages sort a local frame and deliberately **leave a lazy one alone**.
# MAGIC Ordering an intermediate remote result is not meaningful: the `ORDER BY` lands
# MAGIC in a subquery, which most engines are free to discard, and dbplyr warns about
# MAGIC exactly that.
# MAGIC
# MAGIC On a remote backend, `arrange()` once at the end of the pipeline, immediately
# MAGIC before `collect()`. That is the only position where a SQL `ORDER BY` survives.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Sources — logical names, swappable resolvers
# MAGIC
# MAGIC Pipeline stages take frames; none of them should know a schema name, a
# MAGIC connection, or which system is authoritative this quarter. `R/sources.R` is
# MAGIC the one place that does.

# COMMAND ----------

list(
  names   = TABLE_NAMES,
  oracle  = ORACLE_TABLES,
  catalog = CATALOG_TABLES,
  select  = build_select("rpt.msow_base_data_hist", "pract_id", "month = 0")
)

# COMMAND ----------

# MAGIC %md
# MAGIC A resolver is just a function
# MAGIC `(name, columns, where, return_type) -> table`. `frame_reader()` is the
# MAGIC substitute a test wants: hand it a named list of frames and every
# MAGIC `read_source()` call resolves against them, with no connection of any kind.
# MAGIC
# MAGIC `use_resolver()` returns the previous resolver so you can put it back.

# COMMAND ----------

previous <- use_resolver(frame_reader(list(
  base_data_hist = tibble::tibble(pract_id = c(1, 2), month = c(0, 1))
)))

result <- read_source(BASE_DATA_HIST, columns = "pract_id", where = "month == 0")

use_resolver(previous)

result

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Reading Oracle
# MAGIC
# MAGIC See `oracle_utils_demo.r` for the full treatment. The short version: one
# MAGIC `read_oracle()` that dispatches on the connection it is given.
# MAGIC
# MAGIC | `con` | how it reads | credentials |
# MAGIC |---|---|---|
# MAGIC | `spark_connection` | `spark_read_jdbc()`, query wrapped as `(query) tmp` | `configure(jdbc_url =, username =, password =)` |
# MAGIC | `DBIConnection` | `tbl(con, sql(query))` | already in the connection |
# MAGIC
# MAGIC That is the one real design change from the Python package, which takes a
# MAGIC `SparkSession` and is Spark-only.

# COMMAND ----------

RETURN_TYPES

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Report layout
# MAGIC
# MAGIC Column order and headers, held as data. The SAS originals carried this in two
# MAGIC places at once — a `label=` on every `proc sql` select item and a
# MAGIC `define <col> / display '...'` in `proc report` — which is why the two
# MAGIC drifted.

# COMMAND ----------

tibble::tibble(
  column = ordered_columns(),
  header = unname(headers()[ordered_columns()])
) |> print(n = 30)
