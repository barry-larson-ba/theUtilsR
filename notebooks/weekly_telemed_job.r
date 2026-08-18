# Databricks notebook source
# MAGIC %md
# MAGIC # Weekly Telemedicine Affiliates — the job
# MAGIC
# MAGIC The one you schedule. Reads the snapshot, runs the report, writes the
# MAGIC workbook, exits with a status payload a Job task can branch on.
# MAGIC
# MAGIC The other two telemed notebooks explain the report. This one runs it.
# MAGIC
# MAGIC | | organised by | reads Oracle | writes a file | for |
# MAGIC |---|---|---|---|---|
# MAGIC | `weekly_telemed_pipeline.r` | package stage | no | no | learning the stages |
# MAGIC | `weekly_telemed_report.r` | SAS step | optional | yes | tracing a SAS line to its replacement |
# MAGIC | **this one** | **not at all — see below** | **yes, by default** | **yes** | **Monday morning** |
# MAGIC
# MAGIC ### The report is a list, not a chain
# MAGIC
# MAGIC Every other formulation of this pipeline is a `|>` chain. This one names the
# MAGIC stages and puts them in a list, and `run_stages()` applies them in order.
# MAGIC That is not a stylistic preference — it buys four things a chain cannot:
# MAGIC
# MAGIC * the whole report is legible in one cell, as a table of contents
# MAGIC * a failing stage is **named** in the error, instead of surfacing as a dplyr
# MAGIC   backtrace that does not say which of five near-identical joins broke
# MAGIC * `run_stages(..., through = "classify")` runs a prefix, which is what you
# MAGIC   actually want at 7am when the numbers look wrong
# MAGIC * the row count after each stage is recorded, so "which stage dropped them?"
# MAGIC   has an answer instead of a bisect
# MAGIC
# MAGIC The stage list lives here rather than in the package on purpose. `R/` holds
# MAGIC the rules; the *composition* is a notebook-level decision, editable without a
# MAGIC package release. `run_stages()` itself is in the package because it knows
# MAGIC nothing about telemedicine — it is the applier, not the report.
# MAGIC
# MAGIC Correctness is not this notebook's job to argue: `tests/testthat/test-weekly_telemed.R`
# MAGIC runs this exact stage list against the golden expectation, alongside the
# MAGIC chain form, on both a local frame and a real SQL engine.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

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

suppressPackageStartupMessages(library(dplyr))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Talking to Databricks without requiring it
# MAGIC
# MAGIC `dbutils` only exists on a cluster. These shims resolve it by name at call
# MAGIC time and fall back to a default, so this notebook runs unchanged in RStudio
# MAGIC — which is the only reason the parameter cell below is testable at all.

# COMMAND ----------

dbutils_call <- function(name, ..., .default = NULL) {
  f <- tryCatch(get(name, envir = globalenv()), error = function(e) NULL)

  if (!is.function(f)) return(.default)

  tryCatch(f(...), error = function(e) .default)
}

on_databricks <- function() is.function(
  tryCatch(get("dbutils.widgets.get", envir = globalenv()), error = function(e) NULL)
)

param <- function(name, default) {
  value <- dbutils_call("dbutils.widgets.get", name, .default = NULL)

  if (is.null(value) || !nzchar(value)) default else value
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parameters
# MAGIC
# MAGIC Job task parameters, with defaults that make an interactive run safe:
# MAGIC `dry_run` defaults to **yes**, so nothing is written until you ask.

# COMMAND ----------

invisible(dbutils_call("dbutils.widgets.text", "snapshot_month", "0", "Snapshot month (0 = current)"))
invisible(dbutils_call("dbutils.widgets.text", "output_dir", "/dbfs/FileStore/reports", "Output directory"))
invisible(dbutils_call("dbutils.widgets.dropdown", "source", "oracle", list("oracle", "synthetic"), "Source"))
invisible(dbutils_call("dbutils.widgets.dropdown", "dry_run", "yes", list("yes", "no"), "Dry run"))

snapshot_month <- as.integer(param("snapshot_month", "0"))
output_dir     <- param("output_dir", tempdir())
source_kind    <- param("source", if (on_databricks()) "oracle" else "synthetic")
dry_run        <- identical(param("dry_run", "yes"), "yes")
run_date       <- Sys.Date()

str(list(
  snapshot_month = snapshot_month,
  output_dir     = output_dir,
  source         = source_kind,
  dry_run        = dry_run,
  run_date       = format(run_date)
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The source
# MAGIC
# MAGIC `source = "oracle"` reads the real tables; `"synthetic"` swaps in a resolver
# MAGIC backed by fixture frames, so the whole notebook runs with no connection.
# MAGIC Every cell below this one is identical either way — that is the point of
# MAGIC `use_resolver()`.
# MAGIC
# MAGIC Note the `compute()`. The read is materialised once so the per-stage row
# MAGIC counts cost nothing; without it, counting after each stage would re-run the
# MAGIC whole lineage and issue one Oracle round trip per stage.
# MAGIC
# MAGIC The month filter is a plain `filter()` rather than `read_source(where = )`.
# MAGIC That is deliberate and it is not a loss of pushdown: `read_source()` hands
# MAGIC back a lazy table, so dbplyr folds the predicate into the generated SQL and
# MAGIC Oracle still does the work. It also sidesteps a wart in the `where` argument
# MAGIC — `oracle_reader()` pastes that string into SQL, where `month = 0` is a
# MAGIC comparison, while the other resolvers parse it as R, where it is not. A
# MAGIC `filter()` means the same thing on every resolver.

# COMMAND ----------

synthetic_snapshot <- function() {
  list(
    base_data_hist = tibble::tribble(
      ~pract_id, ~last_name, ~first_name, ~middle_initial, ~degree, ~faccode, ~section_name, ~expertise, ~primary_fac_flag, ~primary_record, ~credentialed, ~current_status, ~status_category, ~primary_dea, ~primary_license, ~month,
      101L, "Alvarez", "Rosa", "M", "MD", "SFO", "Cardiology", NA, "Y", "Y", "Y", "CURRENT", "ACTIVE", "Y", "Y", 0L,
      101L, "Alvarez", "Rosa", "M", "MD", "OAK", "Cardiology", "Tele-Stroke", "N", "N", "Y", "CURRENT", "TELEMEDICINE AFFILIATE", "Y", "Y", 0L,
      101L, "Alvarez", "Rosa", "M", "MD", "SAC", "Cardiology", NA, "N", "N", "Y", "CURRENT", "COURTESY", "Y", "Y", 0L,
      101L, "Alvarez", "Rosa", "M", "MD", "REG", "Cardiology", "Tele-Stroke", "N", "N", "Y", "CURRENT", "TELEMEDICINE AFFILIATE", "Y", "Y", 0L,
      102L, "Brennan", "Ida", "R", "DO", "OAK", "Neurology", "TelePsych", "N", "N", "Y", "CURRENT", "TELEMEDICINE AFFILIATE", "Y", "Y", 0L,
      103L, "Chen", "Wei", "L", "MD", "SAC", "Neurology", NA, "Y", "Y", "Y", "CURRENT", "ACTIVE", "Y", "Y", 0L,
      103L, "Chen", "Wei", "L", "MD", "SFO", "Neurology", "TeleICU", "N", "N", "Y", "CURRENT", "TELEMEDICINE AFFILIATE", "Y", "Y", 0L,
      103L, "Chen", "Wei", "L", "MD", "VAL", "Neurology", NA, "N", "N", "N", "EXPIRED", "ACTIVE", "Y", "Y", 0L
    ),
    practitioner_facilities = tibble::tribble(
      ~pract_id, ~faccode, ~section2,
      101L,      "SFO",    "Echo",
      101L,      "OAK",    NA,
      103L,      "SAC",    "EEG"
    )
  )
}

if (identical(source_kind, "synthetic")) {
  use_resolver(frame_reader(synthetic_snapshot()))
  message("source: synthetic fixtures, no connection")
} else {
  # [NEEDS ORACLE] uncomment and point at your secret scope
  #
  # configure(con = DBI::dbConnect(
  #   odbc::odbc(),
  #   Driver = "Oracle 21 ODBC driver",
  #   DBQ    = "//host:1521/service",
  #   UID    = dbutils_call("dbutils.secrets.get", "scope", "oracle-user"),
  #   PWD    = dbutils_call("dbutils.secrets.get", "scope", "oracle-password")
  # ))
  stopifnot(is_configured())
  message("source: Oracle")
}

base_data <- read_source(BASE_DATA_HIST) |>
  filter(month == snapshot_month)

facilities <- read_source(
  PRACTITIONER_FACILITIES,
  columns = c("pract_id", "faccode", "section2")
)

if (inherits(base_data, "tbl_lazy")) {
  base_data <- compute(base_data)
  facilities <- compute(facilities)
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The report
# MAGIC
# MAGIC Eleven stages. Read top to bottom, this is the entire weekly telemed report.
# MAGIC
# MAGIC The two constraints that are easy to break, both visible here as *position*
# MAGIC in the list rather than as prose:
# MAGIC
# MAGIC * `one_per_site` runs before `classify` — the crosstab needs at most one row
# MAGIC   per practitioner and facility.
# MAGIC * `attach_home` and `attach_program` run before `drop_admin`. In the SAS
# MAGIC   original the exclusion applied only at the transpose, so an administrative
# MAGIC   faccode still counted toward a practitioner's program total. Moving
# MAGIC   `drop_admin` up changes the answer, silently.

# COMMAND ----------

SITE_KEY <- c("pract_id", "faccode")

telemed_stages <- function(facilities) list(
  population     = \(d) restrict_to_practitioners(d, telemed_population(d)),
  reportable     = is_active_credentialing_row,
  privileges     = \(d) d |>
                     rename(section1 = "section_name") |>
                     left_join(facilities, by = SITE_KEY),
  one_per_site   = \(d) dedupe_first(d, by = SITE_KEY),
  classify       = classify_facility_status,
  attach_home    = \(d) left_join(d, home_facility_rows(d), by = "pract_id"),
  attach_program = \(d) left_join(d, dominant_telemed_program(d), by = "pract_id"),
  clean_program  = clean_program_text,
  drop_admin     = exclude_faccodes,
  transpose      = \(d) facility_crosstab(d, index = TELEMED_IDENTITY_COLUMNS),
  layout         = select_report_columns
)

names(telemed_stages(facilities))

# COMMAND ----------

report <- base_data |>
  run_stages(telemed_stages(facilities), count = TRUE) |>
  arrange(pract_id) |>
  collect()

report

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Where the rows went
# MAGIC
# MAGIC The trace, free from having named the stages. `delta` is the row change each
# MAGIC stage caused; `transpose` going from many rows to one per practitioner is the
# MAGIC big one, and everything before it should be explicable.
# MAGIC
# MAGIC `stage_trace()` with no argument reads the last run in the session — the
# MAGIC attribute survives dplyr verbs but not `collect()`, and every remote report
# MAGIC ends with a `collect()`.

# COMMAND ----------

stage_trace()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Debugging a stage
# MAGIC
# MAGIC When a number looks wrong, run a prefix and look at what came out. No
# MAGIC commenting-out, no re-running the cells above.

# COMMAND ----------

base_data |>
  run_stages(telemed_stages(facilities), through = "classify", quiet = TRUE) |>
  count(stat) |>
  collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Checks
# MAGIC
# MAGIC The `%step_check` equivalent. These are assertions, not diagnostics: if one
# MAGIC fails the job stops here rather than emailing a wrong workbook.

# COMMAND ----------

checks <- list(
  "report is 28 columns" =
    identical(ncol(report), length(ordered_columns())),
  "columns are in report order" =
    identical(colnames(report), ordered_columns()),
  "one row per practitioner" =
    !anyDuplicated(report$pract_id),
  "no administrative faccode became a column" =
    !any(EXCLUDED_FACCODES %in% colnames(report)),
  "every cell holds a known status" =
    all(unlist(report[FACILITY_CODES]) %in% names(STATUS_LABELS)),
  "every practitioner has a home facility" =
    !anyNA(report$home_facility),
  "report is not empty" =
    nrow(report) > 0L
)

failed <- names(checks)[!vapply(checks, isTRUE, logical(1))]

if (length(failed)) {
  stop("Report checks failed:\n  - ", paste(failed, collapse = "\n  - "))
}

message("all ", length(checks), " checks passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. The workbook
# MAGIC
# MAGIC The SAS `ods excel` / `proc report` step. Column headers come from
# MAGIC `headers()`, the same mapping the pipeline projects with — the SAS original
# MAGIC carried them in two places and they drifted.
# MAGIC
# MAGIC `dry_run = yes` stops before writing. That is the default, so an interactive
# MAGIC run does not quietly overwrite the file a job produced.

# COMMAND ----------

titled <- report
names(titled) <- unname(headers()[colnames(report)])

output_path <- file.path(
  output_dir,
  sprintf("weekly_telemed_%s.xlsx", format(run_date, "%Y%m%d"))
)

if (dry_run) {
  message("dry run: would write ", nrow(titled), " rows to ", output_path)
  written <- NA_character_
} else {
  rlang::check_installed("writexl", "to write the workbook.")

  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  writexl::write_xlsx(
    stats::setNames(list(titled), "Telemedicine Affiliates"),
    path = output_path
  )

  message("wrote ", output_path)
  written <- output_path
}

head(titled, 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Exit
# MAGIC
# MAGIC The payload a downstream Job task reads with
# MAGIC `dbutils.notebook.run(...)`, or that shows up in the run output. Off a
# MAGIC cluster this just prints.
# MAGIC
# MAGIC `&telemeds_cnt.` in the SAS was the affiliate count spliced into the email
# MAGIC subject; it is `practitioners` here.

# COMMAND ----------

status <- list(
  status         = "ok",
  run_date       = format(run_date),
  snapshot_month = snapshot_month,
  source         = source_kind,
  practitioners  = nrow(report),
  facilities     = length(FACILITY_CODES),
  dry_run        = dry_run,
  output         = written
)

payload <- if (requireNamespace("jsonlite", quietly = TRUE)) {
  jsonlite::toJSON(status, auto_unbox = TRUE)
} else {
  paste(names(status), unlist(lapply(status, format)), sep = "=", collapse = "; ")
}

cat(payload, "\n")

invisible(dbutils_call("dbutils.notebook.exit", as.character(payload)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes for whoever maintains this
# MAGIC
# MAGIC * **Adding a stage** is a line in `telemed_stages()`. Add the matching line
# MAGIC   to the stage list in `tests/testthat/test-weekly_telemed.R` — that test is
# MAGIC   what stops this notebook drifting from the package.
# MAGIC * **Do not move `drop_admin` up the list.** See §4.
# MAGIC * **`count = TRUE` is only cheap because of the `compute()`** in §3. Drop the
# MAGIC   `compute()` and every stage re-runs the Oracle read to produce its count.
# MAGIC * **The email is not here.** `weekly_telemed_report.r` §17 covers the
# MAGIC   `%emlist` / `%send_email` translation; wire it to this notebook's `status`
# MAGIC   payload rather than re-deriving the counts.
