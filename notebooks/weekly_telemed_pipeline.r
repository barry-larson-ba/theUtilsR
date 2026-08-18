# Databricks notebook source
# MAGIC %md
# MAGIC # Weekly Telemedicine Affiliates — the pipeline
# MAGIC
# MAGIC The R port of `rpt_weekly_telemed.sas`, one stage per cell.
# MAGIC
# MAGIC The deliverable is the **transposed dataset**: one row per practitioner, one
# MAGIC column per facility, holding `P` / `C` / `T` / blank. That is the SAS
# MAGIC `transposed` dataset, the thing `proc report` was handed.
# MAGIC
# MAGIC **This notebook needs no Oracle connection.** It builds a small synthetic
# MAGIC snapshot and runs on that, so you can execute it top to bottom on any cluster.
# MAGIC The cell that reads the real tables is marked **[NEEDS ORACLE]** and is
# MAGIC commented out — swap it in and everything below is unchanged.
# MAGIC
# MAGIC ### Scope
# MAGIC
# MAGIC | In | Out (deferred) |
# MAGIC |---|---|
# MAGIC | population, joins, classification, transpose | the formatted `.xlsx` (`ods excel` / `proc report`) |
# MAGIC | column order and headers, as data | `%send_email` and the `%emlist` distribution lists |
# MAGIC | | `%job_monitor` / `%step_check` — use Databricks Job task alerts |
# MAGIC
# MAGIC ### One pipeline, not three
# MAGIC
# MAGIC The Python package implements every stage three times — pandas, Polars and
# MAGIC PySpark — because those three libraries share no common verb set. R does not
# MAGIC have that problem. The same dplyr code below runs on a local tibble and on a
# MAGIC Spark or Oracle table, because dbplyr turns the verbs into SQL when the input
# MAGIC is remote.
# MAGIC
# MAGIC So there is **one** stage list, and it is the one the test suite runs
# MAGIC (`tests/testthat/test-weekly_telemed.R`). If you change a stage's signature or
# MAGIC output columns, that test is what catches this notebook drifting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Load the package
# MAGIC
# MAGIC Unlike the Python port, there is no `sys.path` puzzle to solve here — R has no
# MAGIC equivalent of an automatic repo-root entry, so the package is always loaded
# MAGIC deliberately. Two ways, depending on what you are doing:
# MAGIC
# MAGIC * **working in a Git folder** — `devtools::load_all()` on the checkout, which
# MAGIC   picks up edits without a reinstall. That is what the cell below does.
# MAGIC * **running a job** — install the built package onto the cluster and
# MAGIC   `library(theUtilsR)`. No path handling at all.
# MAGIC
# MAGIC The helper walks up from the working directory looking for the `DESCRIPTION`
# MAGIC file, so it does not hardcode a workspace path and works whether this notebook
# MAGIC sits in `notebooks/` or gets moved.

# COMMAND ----------

find_package_root <- function(start = getwd(), max_up = 6L) {
  path <- normalizePath(start, winslash = "/", mustWork = FALSE)

  for (i in seq_len(max_up)) {
    if (file.exists(file.path(path, "DESCRIPTION"))) {
      return(path)
    }

    parent <- dirname(path)

    if (identical(parent, path)) break

    path <- parent
  }

  NULL
}

root <- find_package_root()

if (!is.null(root) && requireNamespace("devtools", quietly = TRUE)) {
  devtools::load_all(root, quiet = TRUE)
  message("loaded theUtilsR from ", root)
} else {
  library(theUtilsR)
}

library(dplyr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The snapshot
# MAGIC
# MAGIC Three practitioners, chosen so every branch of the pipeline is exercised:
# MAGIC
# MAGIC * **101** — primary at SFO, telemedicine at OAK *and* at REG, courtesy at SAC.
# MAGIC   One practitioner covering `P`, `C` and `T`, plus an administrative faccode.
# MAGIC * **102** — a current telemedicine affiliate with no live credential at a
# MAGIC   primary facility. Must not appear in the output at all.
# MAGIC * **103** — primary at SAC, telemedicine at SFO, plus a row the
# MAGIC   active-credentialing filter drops.

# COMMAND ----------

base_data <- tibble::tribble(
  ~pract_id, ~last_name, ~first_name, ~middle_initial, ~degree, ~faccode, ~section_name, ~expertise, ~primary_fac_flag, ~primary_record, ~credentialed, ~current_status, ~status_category, ~primary_dea, ~primary_license, ~month,
  101L, "Alvarez", "Rosa", "M", "MD", "SFO", "Cardiology", NA, "Y", "Y", "Y", "CURRENT", "ACTIVE", "Y", "Y", 0L,
  101L, "Alvarez", "Rosa", "M", "MD", "OAK", "Cardiology", "Tele-Stroke", "N", "N", "Y", "CURRENT", "TELEMEDICINE AFFILIATE", "Y", "Y", 0L,
  101L, "Alvarez", "Rosa", "M", "MD", "SAC", "Cardiology", NA, "N", "N", "Y", "CURRENT", "COURTESY", "Y", "Y", 0L,
  101L, "Alvarez", "Rosa", "M", "MD", "REG", "Cardiology", "Tele-Stroke", "N", "N", "Y", "CURRENT", "TELEMEDICINE AFFILIATE", "Y", "Y", 0L,
  102L, "Brennan", "Ida", "R", "DO", "OAK", "Neurology", "TelePsych", "N", "N", "Y", "CURRENT", "TELEMEDICINE AFFILIATE", "Y", "Y", 0L,
  103L, "Chen", "Wei", "L", "MD", "SAC", "Neurology", NA, "Y", "Y", "Y", "CURRENT", "ACTIVE", "Y", "Y", 0L,
  103L, "Chen", "Wei", "L", "MD", "SFO", "Neurology", "TeleICU", "N", "N", "Y", "CURRENT", "TELEMEDICINE AFFILIATE", "Y", "Y", 0L,
  103L, "Chen", "Wei", "L", "MD", "VAL", "Neurology", NA, "N", "N", "N", "EXPIRED", "ACTIVE", "Y", "Y", 0L
)

facilities <- tibble::tribble(
  ~pract_id, ~faccode, ~section2,
  101L,      "SFO",    "Echo",
  101L,      "OAK",    NA,
  103L,      "SAC",    "EEG"
)

base_data

# COMMAND ----------

# MAGIC %md
# MAGIC ### [NEEDS ORACLE] the real tables
# MAGIC
# MAGIC Uncomment this to run against the live snapshot. Nothing below changes: every
# MAGIC stage takes a frame, and a remote table is a frame as far as dplyr is
# MAGIC concerned.
# MAGIC
# MAGIC Note that `read_source()` is the only thing here that knows a schema name.
# MAGIC Do not put a table name in a stage or a notebook cell — add a logical name to
# MAGIC `R/sources.R` instead.

# COMMAND ----------

# configure(
#   con = DBI::dbConnect(odbc::odbc(), dsn = "ORCL", uid = user, pwd = password)
# )
#
# # ...or, on a cluster with the Oracle JDBC driver attached:
# #
# # configure(
# #   con      = sparklyr::spark_connect(method = "databricks"),
# #   jdbc_url = jdbc_url,
# #   username = username,
# #   password = password
# # )
#
# base_data  <- read_source(BASE_DATA_HIST, where = "month = 0")
# facilities <- read_source(
#   PRACTITIONER_FACILITIES,
#   columns = c("pract_id", "faccode", "section2")
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The population
# MAGIC
# MAGIC A practitioner is in scope when the snapshot holds **both** a current
# MAGIC telemedicine affiliation (at any facility) and a live credential at their
# MAGIC primary facility. That pairing is what the SAS original expressed as a
# MAGIC self-join.
# MAGIC
# MAGIC 102 has the first and not the second, so it drops out here — before any of
# MAGIC the expensive work.

# COMMAND ----------

population <- telemed_population(base_data)

population

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Restrict, filter, join
# MAGIC
# MAGIC `restrict_to_practitioners()` is a semi-join rather than an inner join:
# MAGIC `population` is distinct on `pract_id`, but a semi-join says so to the planner
# MAGIC and cannot duplicate rows even if it somehow is not.
# MAGIC
# MAGIC `is_active_credentialing_row()` is the "reportable row" rule — DEA and licence
# MAGIC on file (or missing, treated as OK), credentialed or still an applicant, and
# MAGIC the current month. It drops 103's expired VAL row.

# COMMAND ----------

rows <- base_data |>
  restrict_to_practitioners(population) |>
  is_active_credentialing_row() |>
  rename(section1 = "section_name") |>
  left_join(facilities, by = c("pract_id", "faccode"))

rows |> select(pract_id, faccode, section1, section2, status_category)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. De-duplicate, then classify
# MAGIC
# MAGIC `dedupe_first()` is the SAS `proc sort; by X; if first.X;` idiom. It takes an
# MAGIC `order_by` because rows on a remote table have no inherent order — "first
# MAGIC after sort" is not a thing a database will guarantee you unless you name the
# MAGIC ranking. It defaults to the group keys, which is deterministic on those and
# MAGIC arbitrary on everything else.
# MAGIC
# MAGIC The crosstab needs at most one row per (practitioner, facility), so this has
# MAGIC to happen before the transpose.

# COMMAND ----------

rows <- rows |>
  dedupe_first(by = c("pract_id", "faccode")) |>
  classify_facility_status()

rows |> select(pract_id, faccode, primary_fac_flag, current_status, status_category, stat)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Home facility and dominant program
# MAGIC
# MAGIC **Ordering constraint — the easiest thing in this notebook to break.**
# MAGIC
# MAGIC These two stages run *before* `exclude_faccodes()`. In the SAS original the
# MAGIC exclusion applied only at the transpose, so an administrative faccode still
# MAGIC counted toward a practitioner's program total. 101's `Tele-Stroke` appears at
# MAGIC OAK and at REG; excluding REG first would change the count, and with it the
# MAGIC winner, silently.
# MAGIC
# MAGIC `dominant_telemed_program()` breaks ties on the program name. The SAS original
# MAGIC ranked on the facility count alone and left ties to whatever order the sort
# MAGIC happened to produce, which made a week-over-week diff of the report flap.

# COMMAND ----------

home <- home_facility_rows(rows)
programs <- dominant_telemed_program(rows)

home

# COMMAND ----------

programs

# COMMAND ----------

rows <- rows |>
  left_join(home, by = "pract_id") |>
  left_join(programs, by = "pract_id") |>
  clean_program_text()

rows |> select(pract_id, home_facility, home_section1, home_section2, telemed_program)

# COMMAND ----------

# MAGIC %md
# MAGIC `clean_program_text()` is the SAS `compbl(compress(x, , 'kads'))` pair: drop
# MAGIC everything that is not a letter, digit or space, then collapse runs of blanks.
# MAGIC `Tele-Stroke` becomes `TeleStroke`.
# MAGIC
# MAGIC The trailing trim has no SAS counterpart — SAS character variables are
# MAGIC blank-padded to their declared width, so trailing blanks were never visible
# MAGIC there. Trimming keeps a value from sorting differently than it displays.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Exclude, then transpose
# MAGIC
# MAGIC Now — and only now — the administrative codes come out, and the crosstab
# MAGIC pivots `faccode` into columns.
# MAGIC
# MAGIC Two things the crosstab does that a bare pivot does not:
# MAGIC
# MAGIC * **every** code in `FACILITY_CODES` becomes a column, in registry order,
# MAGIC   whether or not any row mentioned it. A pivot emits only the values it saw,
# MAGIC   so a facility with no qualifying practitioners this week would silently drop
# MAGIC   a column — and `proc report` would then fail on the missing variable rather
# MAGIC   than print an empty one.
# MAGIC * gaps are filled with a blank, not `NA`. Blank is a *meaningful* status here
# MAGIC   (see `STATUS_LABELS`), and `NA` would render as the string "NA".

# COMMAND ----------

wide <- rows |>
  exclude_faccodes() |>
  facility_crosstab(index = TELEMED_IDENTITY_COLUMNS)

report <- select_report_columns(wide)

report

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Ordering, and collecting
# MAGIC
# MAGIC The stages deliberately do **not** sort a remote result. An `ORDER BY` in a
# MAGIC subquery is something most engines are free to discard, and dbplyr warns when
# MAGIC you ask for one — so sorting an intermediate result is at best wasted work and
# MAGIC at worst misleading.
# MAGIC
# MAGIC Sort once, at the end, immediately before collecting. On a local frame this is
# MAGIC redundant (the stages already sorted); on a remote one it is the only place
# MAGIC the ordering survives.

# COMMAND ----------

report <- report |>
  arrange(pract_id) |>
  collect()

report

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Headers
# MAGIC
# MAGIC Column order and display headers live in `R/report_layout.R`, as data. The SAS
# MAGIC originals carried this in two places at once — a `label=` on every `proc sql`
# MAGIC select item and a `define <col> / display '...'` in `proc report` — which is
# MAGIC why the two drifted.
# MAGIC
# MAGIC A facility's header is its own code, generated rather than listed, so a code
# MAGIC added to the registry cannot be forgotten here.

# COMMAND ----------

tibble::tibble(
  column = ordered_columns(),
  header = unname(headers()[ordered_columns()])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is deliberately missing
# MAGIC
# MAGIC There is **no orchestrating `build()` function** in the package. The stages are
# MAGIC small frame-to-frame functions meant to be composed and inspected one at a
# MAGIC time, which is what this notebook does; the composition lives here, and it is
# MAGIC pinned by `tests/testthat/test-weekly_telemed.R`, which runs this exact
# MAGIC sequence on a local tibble *and* on a real SQL engine and asserts the whole
# MAGIC output frame.
# MAGIC
# MAGIC The formatted workbook, the distribution list and the job monitoring are all
# MAGIC still out of scope — see the table at the top.
