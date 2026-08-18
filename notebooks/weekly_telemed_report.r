# Databricks notebook source
# MAGIC %md
# MAGIC # Weekly Telemedicine Affiliates — the whole SAS macro
# MAGIC
# MAGIC `rpt_weekly_telemed.sas`, step for step, including the four things
# MAGIC `weekly_telemed_pipeline.r` deliberately leaves out: the titles, the
# MAGIC affiliate count, the workbook, and the email.
# MAGIC
# MAGIC ### How this differs from `weekly_telemed_pipeline.r`
# MAGIC
# MAGIC | | `weekly_telemed_pipeline.r` | this notebook |
# MAGIC |---|---|---|
# MAGIC | organised by | package stage | SAS step |
# MAGIC | covers | population → transposed frame | the whole macro |
# MAGIC | shows the SAS | no | yes, above each cell |
# MAGIC | writes a file | no | yes, guarded |
# MAGIC
# MAGIC Neither notebook is the source of truth for the business rules — `R/` is, and
# MAGIC `tests/testthat/test-weekly_telemed.R` is what keeps both of them honest. This
# MAGIC one calls the same exported stages in the same order; it just cuts the
# MAGIC pipeline at the SAS dataset boundaries instead of the package's, so a
# MAGIC `list_practs3` in the old code has something to point at.
# MAGIC
# MAGIC **No Oracle connection needed.** It builds a synthetic snapshot and runs on
# MAGIC that. The cell that reads the real tables is marked **[NEEDS ORACLE]** and is
# MAGIC commented out.
# MAGIC
# MAGIC ### The SAS steps, and where each one lands
# MAGIC
# MAGIC | SAS | here |
# MAGIC |---|---|
# MAGIC | `%job_monitor`, `%step_check` | §1, a local helper — see the note there |
# MAGIC | `telemed_list` | §4 `telemed_population()` |
# MAGIC | `list_practs` + `%dttm_to_sasdates` | §5 |
# MAGIC | `list_practs2` | §6 |
# MAGIC | `list_practs3` | §7 `dedupe_first()` |
# MAGIC | `home_facility` | §8 `home_facility_rows()` |
# MAGIC | `telemed_programs`, `telemed_programs2` | §9 `dominant_telemed_program()` |
# MAGIC | `list_practs4` | §10 |
# MAGIC | `telemed_participant` | §11 — dead in the SAS, reproduced anyway |
# MAGIC | `to_transpose`, `proc transpose` | §12 `facility_crosstab()` |
# MAGIC | final `proc sort` | §13 |
# MAGIC | `title`, `title2` | §14 |
# MAGIC | `&telemeds_cnt.` | §15 |
# MAGIC | `ods excel` / `proc report` | §16 |
# MAGIC | `%emlist`, `%send_email` | §17 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Load the package
# MAGIC
# MAGIC The same cell as the other notebooks, and it has to stay inlined: it is what
# MAGIC makes the package available, so it cannot come from the package.

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
# MAGIC ## 1. `%job_monitor` and `%step_check`
# MAGIC
# MAGIC ```sas
# MAGIC %job_monitor(job_event=START);
# MAGIC %step_check(step_nm=Inital queries, em_msg=Err found during initial queries.);
# MAGIC ```
# MAGIC
# MAGIC `%step_check` tested `&syserr.` after each block and aborted the job with a
# MAGIC message. R does not need the first half — an error in a cell already stops
# MAGIC the notebook, and Databricks already reports which cell — so what is left is
# MAGIC the *assertion*: the step ran, and its output is not empty.
# MAGIC
# MAGIC That half is worth keeping, and is arguably the more useful one. A SAS step
# MAGIC that silently produced zero rows still set `&syserr.=0`, so the original
# MAGIC checks would not have caught it; this one does.
# MAGIC
# MAGIC For real scheduling, `%job_monitor`'s job belongs to a Databricks Job task
# MAGIC and its alerts, not to notebook code.

# COMMAND ----------

# Row count that works on a local frame and on a remote table alike. nrow() on
# a lazy table is NA -- the count is a question only the engine can answer.
n_rows <- function(data) {
  if (inherits(data, "tbl_lazy")) {
    as.integer(dplyr::pull(dplyr::collect(dplyr::summarise(data, n = dplyr::n())), "n"))
  } else {
    nrow(data)
  }
}

step_check <- function(step_nm, data = NULL, em_msg = NULL, min_rows = 1L) {
  if (is.null(data)) {
    message("*** step ok: ", step_nm)
    return(invisible(TRUE))
  }

  n <- n_rows(data)

  if (n < min_rows) {
    stop(
      if (is.null(em_msg)) {
        paste0(
          "Err found during ", step_nm, ": ", n, " row(s), expected at least ",
          min_rows, "."
        )
      } else {
        em_msg
      },
      call. = FALSE
    )
  }

  message("*** step ok: ", step_nm, " (", n, " rows)")
  invisible(TRUE)
}

step_check("job start")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The `%let` block
# MAGIC
# MAGIC ```sas
# MAGIC %let prognm=rpt_weekly_telemed;
# MAGIC %let progdescr=All telemed affiliates at all of their facilities;
# MAGIC data _null_;
# MAGIC    call symput('title_dt',strip(put("&sysdate9."d,worddate20.)));
# MAGIC    run;
# MAGIC ```
# MAGIC
# MAGIC `worddate20.` renders "August 17, 2026" — no leading zero on the day, which
# MAGIC is why this is not a bare `format()` call. `%B` is locale-dependent; a
# MAGIC cluster running under a non-English locale produces a translated month name.
# MAGIC Set `Sys.setlocale("LC_TIME", "C")` if that matters.
# MAGIC
# MAGIC `&date8.` is the site macro for the file-name stamp. `%Y%m%d` is used here
# MAGIC because it sorts correctly in a directory listing, which `ddmmmyy` did not.

# COMMAND ----------

prognm    <- "rpt_weekly_telemed"
progdescr <- "All telemed affiliates at all of their facilities"

run_date <- Sys.Date()

worddate <- function(date) {
  paste0(format(date, "%B "), as.integer(format(date, "%d")), ", ", format(date, "%Y"))
}

title_dt <- worddate(run_date)
date8    <- format(run_date, "%Y%m%d")
sysdate9 <- toupper(format(run_date, "%d%b%Y"))

cat("title_dt is ", title_dt, "\n", sep = "")
cat("date8 is    ", date8, "\n", sep = "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The snapshot
# MAGIC
# MAGIC The same three practitioners the golden test uses, plus `dob` and
# MAGIC `state_license_number` — two columns `list_practs` selects that the report
# MAGIC itself never carries. They are here so §5 is a real step rather than a
# MAGIC commented-out one; they do not reach the output, so this snapshot still
# MAGIC produces the frame `tests/testthat/test-weekly_telemed.R` asserts.
# MAGIC
# MAGIC * **101** — primary at SFO, telemedicine at OAK *and* at REG, courtesy at
# MAGIC   SAC. One practitioner covering `P`, `C` and `T`, plus an administrative
# MAGIC   faccode.
# MAGIC * **102** — a current telemedicine affiliate with no live credential at a
# MAGIC   primary facility. Must not appear in the output at all.
# MAGIC * **103** — primary at SAC, telemedicine at SFO, plus a row the
# MAGIC   active-credentialing filter drops.
# MAGIC
# MAGIC `dob` is a `POSIXct`, not a `Date`, because that is what an Oracle `DATE`
# MAGIC arrives as — which is the whole reason `%dttm_to_sasdates` existed.

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

demographics <- tibble::tribble(
  ~pract_id, ~dob,                                          ~state_license_number,
  101L,      as.POSIXct("1974-03-02 00:00:00", tz = "UTC"),  "A55512",
  102L,      as.POSIXct("1981-11-19 00:00:00", tz = "UTC"),  "A61047",
  103L,      as.POSIXct("1969-07-24 00:00:00", tz = "UTC"),  "A48830"
)

base_data <- left_join(base_data, demographics, by = "pract_id")

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
# MAGIC The SAS libnames map like this:
# MAGIC
# MAGIC | SAS | logical name | Oracle object |
# MAGIC |---|---|---|
# MAGIC | `rpt.msow_base_data_hist` | `BASE_DATA_HIST` | `rpt.msow_base_data_hist` |
# MAGIC | `msow.practitioner_facilities` | `PRACTITIONER_FACILITIES` | `msow.practitioner_facilities` |
# MAGIC
# MAGIC Uncomment and nothing below changes: every step takes a frame, and a remote
# MAGIC table is a frame as far as dplyr is concerned.
# MAGIC
# MAGIC `read_source()` is the only thing here that knows a schema name. Do not put
# MAGIC a table name in a step or a notebook cell — add a logical name to
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
# MAGIC ## 4. `telemed_list` — the report population
# MAGIC
# MAGIC ```sas
# MAGIC create table telemed_list as
# MAGIC select distinct mbd.pract_id from
# MAGIC rpt.msow_base_data_hist mbd
# MAGIC join rpt.msow_base_data_hist crd
# MAGIC    on crd.primary_record='Y' and crd.credentialed='Y' and
# MAGIC       upper(crd.current_status)='CURRENT' and mbd.pract_id=crd.pract_id
# MAGIC where upper(mbd.status_category)='TELEMEDICINE AFFILIATE' and
# MAGIC       upper(mbd.current_status)='CURRENT' and mbd.month=0 and crd.month=0;
# MAGIC ```
# MAGIC
# MAGIC A self-join expressing an intersection: a current telemedicine affiliation at
# MAGIC *some* facility, **and** a live credential at the primary one.
# MAGIC `telemed_population()` says that directly, as a semi-join of the two
# MAGIC predicates — which is also why it cannot duplicate a `pract_id` the way the
# MAGIC SAS join would have without its `distinct`.
# MAGIC
# MAGIC 102 has the first and not the second, so it drops out here — before any of
# MAGIC the expensive work.

# COMMAND ----------

telemed_list <- telemed_population(base_data)

telemed_list

# COMMAND ----------

# MAGIC %md
# MAGIC The two halves are exported separately, so you can ask which one a missing
# MAGIC practitioner failed. That question came up every week and the SAS had no
# MAGIC answer for it short of editing the self-join.

# COMMAND ----------

tibble::tibble(pract_id = sort(unique(collect(base_data)$pract_id))) |>
  mutate(
    is_telemed      = pract_id %in% collect(is_current_telemed_row(base_data))$pract_id,
    is_credentialed = pract_id %in%
      collect(is_current_primary_credentialed_row(base_data))$pract_id,
    in_population   = pract_id %in% collect(telemed_list)$pract_id
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `list_practs` — the population at their primary facilities
# MAGIC
# MAGIC ```sas
# MAGIC create table list_practs as
# MAGIC select mbd.pract_id, mbd.last_name, ..., mbd.dob, mbd.state_license_number,
# MAGIC        mbd.faccode as home_facility, mbd.current_status, mbd.status_category,
# MAGIC        mbd.section_name as section1, mpf.section2
# MAGIC from rpt.msow_base_data_hist mbd
# MAGIC left join msow.practitioner_facilities mpf on ...
# MAGIC where mbd.pract_id in (select distinct pds.pract_id from telemed_list pds) and
# MAGIC       mbd.primary_record='Y' and mbd.month=0;
# MAGIC
# MAGIC %dttm_to_sasdates(dsn=list_practs);
# MAGIC ```
# MAGIC
# MAGIC **This dataset feeds nothing.** `list_practs2` filters on
# MAGIC `pract_id in (select ... from list_practs)`, and that set is `telemed_list`
# MAGIC unchanged — every practitioner in the population has a `primary_record='Y'`
# MAGIC row by construction, because that is half of what put them in the population.
# MAGIC So none of the columns selected here reach the report.
# MAGIC
# MAGIC It is worth building anyway: it is the roster view, the thing to look at when
# MAGIC someone asks who is on this week's report and where they sit.

# COMMAND ----------

list_practs <- base_data |>
  restrict_to_practitioners(telemed_list) |>
  filter(primary_record == YES_FLAG, month == CURRENT_MONTH) |>
  rename(section1 = "section_name", home_facility = "faccode") |>
  left_join(
    rename(facilities, home_facility = "faccode"),
    by = c("pract_id", "home_facility")
  ) |>
  select(
    pract_id, last_name, first_name, middle_initial, degree, dob,
    state_license_number, home_facility, current_status, status_category,
    section1, section2
  ) |>
  arrange(home_facility, section1, last_name, first_name) |>
  collect()

step_check("Inital queries", list_practs)  # the SAS typo, preserved

list_practs

# COMMAND ----------

# MAGIC %md
# MAGIC ### `%dttm_to_sasdates`
# MAGIC
# MAGIC An Oracle `DATE` is a datetime. SAS read it as a SAS *datetime* — seconds
# MAGIC since 1960 — and this macro divided each one down to a SAS *date*, so it
# MAGIC would print as `02MAR1974` rather than as a ten-digit number.
# MAGIC
# MAGIC R has the same distinction (`POSIXct` vs `Date`) and the same fix. Two places
# MAGIC to apply it:
# MAGIC
# MAGIC * **after `collect()`**, as below. `as.Date()` has no universal dbplyr
# MAGIC   translation, so this is the safe spot.
# MAGIC * **in the read**, via `apply_schema(data, c(dob = "date"))`, which emits a
# MAGIC   backend-appropriate `CAST` and keeps the conversion in the database.
# MAGIC
# MAGIC Prefer the second when the frame is large: the cast costs nothing and the
# MAGIC narrower column is what crosses the wire.

# COMMAND ----------

dttm_to_dates <- function(data) {
  is_datetime <- vapply(data, inherits, logical(1), what = "POSIXt")

  dplyr::mutate(data, dplyr::across(dplyr::all_of(names(data)[is_datetime]), as.Date))
}

list_practs <- dttm_to_dates(list_practs)

str(list_practs$dob)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `list_practs2` — every facility, classified
# MAGIC
# MAGIC ```sas
# MAGIC create table list_practs2 as
# MAGIC select ..., mbd.expertise,
# MAGIC        case when primary_fac_flag='Y' then 'P'
# MAGIC             when upper(mbd.current_status)='CURRENT' and
# MAGIC                  (upper(mbd.status_category) in ('ACTIVE','COURTESY','CONSULTANT','TEMPORARY')
# MAGIC                   or upper(mbd.status_category) like 'PROVISIONAL%') then 'C'
# MAGIC             when upper(mbd.status_category) like '%TELEMED%' and
# MAGIC                  upper(mbd.current_status)='CURRENT' then 'T'
# MAGIC             else ' ' end as stat
# MAGIC from ...
# MAGIC where mbd.pract_id in (select distinct pract_id from list_practs) and
# MAGIC       (mbd.primary_dea='Y' or missing(mbd.primary_dea)) and
# MAGIC       (mbd.primary_license='Y' or missing(mbd.primary_license)) and
# MAGIC       (mbd.credentialed='Y' or upper(mbd.current_status)='APPLICANT') and mbd.month=0;
# MAGIC ```
# MAGIC
# MAGIC Three rules in one `proc sql`, and each is a separate exported function here:
# MAGIC
# MAGIC * the `where` clause is `is_active_credentialing_row()`
# MAGIC * the `case when` is `classify_facility_status()`
# MAGIC * the `in (select ...)` is `restrict_to_practitioners()`, a semi-join
# MAGIC
# MAGIC Two details of the `case` that the SAS spelling hides. `like 'PROVISIONAL%'`
# MAGIC becomes `substr(x, 1, 11) == "PROVISIONAL"`, because `startsWith()` has no
# MAGIC universal SQL translation. And the `'C'` arm is tested before the `'T'` arm,
# MAGIC so a practitioner who is somehow both reads as credentialed rather than
# MAGIC telemedicine — that ordering is load-bearing, and `facility_status_expr()`
# MAGIC keeps it.

# COMMAND ----------

list_practs2 <- base_data |>
  restrict_to_practitioners(telemed_list) |>
  is_active_credentialing_row() |>
  rename(section1 = "section_name") |>
  left_join(facilities, by = c("pract_id", "faccode")) |>
  classify_facility_status()

step_check("Initial queries", list_practs2)

list_practs2 |>
  select(pract_id, faccode, primary_fac_flag, current_status, status_category, stat)

# COMMAND ----------

# MAGIC %md
# MAGIC The four outcomes, as the legend in §14 will render them:

# COMMAND ----------

tibble::tibble(
  stat  = names(STATUS_LABELS),
  means = unname(STATUS_LABELS)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `list_practs3` — one row per practitioner and facility
# MAGIC
# MAGIC ```sas
# MAGIC proc sort data=list_practs2; by pract_id faccode; run;
# MAGIC data list_practs3;
# MAGIC    set list_practs2;
# MAGIC    by pract_id faccode;
# MAGIC    if first.faccode;
# MAGIC    run;
# MAGIC ```
# MAGIC
# MAGIC The SAS comment on this step says it all: *"Problem with primary license for
# MAGIC some degrees — de-duplicate handful of multiple licenses. FIX IN
# MAGIC NCAL_BASE_DATA."* It is a workaround for duplicate source rows, still here.
# MAGIC
# MAGIC `dedupe_first()` is that idiom. It takes an `order_by` because rows on a
# MAGIC remote table have no inherent order — "first after sort" is not something a
# MAGIC database guarantees unless you name the ranking. It defaults to the group
# MAGIC keys, which is deterministic on those and arbitrary on everything else,
# MAGIC exactly as `first.faccode` was.
# MAGIC
# MAGIC The crosstab needs at most one row per (practitioner, facility), so this has
# MAGIC to happen before the transpose. Skip it and `facility_crosstab()` raises
# MAGIC locally, and silently takes the `MAX` remotely.

# COMMAND ----------

list_practs3 <- dedupe_first(list_practs2, by = c("pract_id", "faccode"))

cat("list_practs2: ", n_rows(list_practs2), " rows\n", sep = "")
cat("list_practs3: ", n_rows(list_practs3), " rows\n", sep = "")

list_practs3 |> select(pract_id, faccode, stat)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. `home_facility`
# MAGIC
# MAGIC ```sas
# MAGIC create table home_facility as
# MAGIC select distinct pract_id, faccode as home_facility, section1, section2
# MAGIC from list_practs2 where stat='P';
# MAGIC ```
# MAGIC
# MAGIC Note that the SAS reads `list_practs2`, not `list_practs3` — the
# MAGIC un-deduplicated frame. Its `select distinct` covers for that.
# MAGIC `home_facility_rows()` is `distinct` too and reads whichever frame it is
# MAGIC handed, so both spellings give the same answer; passing the deduplicated one
# MAGIC makes the guarantee structural rather than incidental.
# MAGIC
# MAGIC The rename map lives in `HOME_FACILITY_COLUMNS`, so the `home_*` prefix is
# MAGIC stated once.

# COMMAND ----------

HOME_FACILITY_COLUMNS

# COMMAND ----------

home_facility <- home_facility_rows(list_practs3)

home_facility

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. `telemed_programs` / `telemed_programs2` — the dominant program
# MAGIC
# MAGIC ```sas
# MAGIC create table telemed_programs as
# MAGIC select pract_id, expertise, count(*) as fac_count
# MAGIC from list_practs2 where stat='T' group by 1,2 order by 1,3 desc;
# MAGIC
# MAGIC data telemed_programs2;
# MAGIC    set telemed_programs;
# MAGIC    retain prev_pract;
# MAGIC    if pract_id ne prev_pract then rank=0;
# MAGIC    rank+1;
# MAGIC    prev_pract=pract_id;
# MAGIC    if rank eq 1;
# MAGIC    run;
# MAGIC ```
# MAGIC
# MAGIC The count, then a `retain`-and-reset ranking on top of it — "of the
# MAGIC telemedicine programs this practitioner participates in, the one covering the
# MAGIC most facilities".
# MAGIC
# MAGIC **`dominant_telemed_program()` deliberately does not reproduce this exactly.**
# MAGIC The SAS `order by 1,3 desc` ranks on the facility count alone; two programs
# MAGIC tied at the same count were separated by whatever order the sort happened to
# MAGIC produce, which made a week-over-week diff of the report flap for no reason.
# MAGIC The R version breaks ties on the program name, so the winner is a function of
# MAGIC the data and nothing else. That is the one intentional behaviour change in
# MAGIC the port, and it changes an output only where the SAS output was arbitrary.
# MAGIC
# MAGIC `count(*)` and not `count(expertise)`: `n()` counts rows, so a group whose
# MAGIC `expertise` is missing is scored by its row count rather than at zero.

# COMMAND ----------

# telemed_programs -- the counts, before the ranking
telemed_programs <- list_practs3 |>
  filter(stat == STATUS_TELEMED) |>
  group_by(pract_id, expertise) |>
  summarise(fac_count = n(), .groups = "drop") |>
  arrange(pract_id, desc(fac_count)) |>
  collect()

telemed_programs

# COMMAND ----------

# telemed_programs2 -- rank 1 per practitioner
telemed_programs2 <- dominant_telemed_program(list_practs3)

step_check("Data preparation", telemed_programs2)

telemed_programs2

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. `list_practs4` — join the two back on
# MAGIC
# MAGIC ```sas
# MAGIC create table list_practs4 as
# MAGIC select pcc.*, hfc.home_facility, hfc.section1 as home_section1,
# MAGIC        hfc.section2 as home_section2, tmp.expertise as telemed_program
# MAGIC from list_practs3 pcc
# MAGIC left join home_facility hfc on pcc.pract_id=hfc.pract_id
# MAGIC left join telemed_programs2 tmp on pcc.pract_id=tmp.pract_id;
# MAGIC ```
# MAGIC
# MAGIC **The ordering constraint — the easiest thing in this notebook to break.**
# MAGIC
# MAGIC §8 and §9 both ran against the frame *before* the administrative faccodes
# MAGIC come out in §12. In the SAS original the exclusion applied only at the
# MAGIC transpose, so an administrative faccode still counted toward a practitioner's
# MAGIC program total. 101's `Tele-Stroke` appears at OAK and at REG; excluding REG
# MAGIC first would change the count, and with it the winner, silently. There is a
# MAGIC test for exactly this — *"excluding faccodes before ranking would change the
# MAGIC answer"*.
# MAGIC
# MAGIC The `compbl(compress(...))` cleanup comes later in the SAS, after the
# MAGIC transpose. It is applied here instead, which is the same thing: it is one
# MAGIC value per practitioner either way, and cleaning before the transpose means
# MAGIC the crosstab's index columns are already in their final form.

# COMMAND ----------

list_practs4 <- list_practs3 |>
  left_join(home_facility, by = "pract_id") |>
  left_join(telemed_programs2, by = "pract_id") |>
  clean_program_text()

step_check("Data preparation", list_practs4)

list_practs4 |>
  select(pract_id, faccode, home_facility, home_section1, home_section2, telemed_program)

# COMMAND ----------

# MAGIC %md
# MAGIC ```sas
# MAGIC telemed_program=compbl(compress(telemed_program,,'kads'));
# MAGIC ```
# MAGIC
# MAGIC `compress(x, , 'kads')` **k**eeps **a**lphabetic, **d**igit and **s**pace
# MAGIC characters; `compbl` collapses runs of blanks. `Tele-Stroke` becomes
# MAGIC `TeleStroke`.
# MAGIC
# MAGIC `clean_program_text_expr()` is that, as two `str_replace_all()` calls plus a
# MAGIC trim. stringr rather than `gsub()` on purpose: several dbplyr backends —
# MAGIC DuckDB among them — have no translation for `gsub()` at all and emit a call
# MAGIC to a scalar function that does not exist.
# MAGIC
# MAGIC The trailing trim has no SAS counterpart. SAS character variables are
# MAGIC blank-padded to their declared width, so trailing blanks were never visible
# MAGIC there; trimming keeps a value from sorting differently than it displays.

# COMMAND ----------

tibble::tibble(
  telemed_program = c("Tele-Stroke", "Tele  ICU  ", "Neuro/Psych #2", NA)
) |>
  mutate(raw = telemed_program) |>
  clean_program_text() |>
  select(raw, cleaned = telemed_program)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. `telemed_participant` — dead in the SAS
# MAGIC
# MAGIC ```sas
# MAGIC create table telemed_participant as
# MAGIC select distinct pract_id from list_practs4
# MAGIC where stat='T' and faccode not in ('REG','RCH','STK');
# MAGIC ```
# MAGIC
# MAGIC Built, and then never read — nothing downstream of this in
# MAGIC `rpt_weekly_telemed.sas` references it. Reproduced here because a reader
# MAGIC comparing the two would otherwise wonder what happened to it, and because it
# MAGIC is a genuinely useful number: telemedicine affiliates at a *reportable*
# MAGIC facility, as opposed to the ones whose only telemedicine row is at REG.
# MAGIC
# MAGIC If it goes from the SAS, delete this cell too.

# COMMAND ----------

telemed_participant <- list_practs4 |>
  filter(stat == STATUS_TELEMED) |>
  exclude_faccodes() |>
  distinct(pract_id) |>
  collect()

telemed_participant

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. `to_transpose` and `proc transpose`
# MAGIC
# MAGIC ```sas
# MAGIC proc sort data=list_practs4 (keep=pract_id last_name ... faccode stat)
# MAGIC           out=to_transpose;
# MAGIC    by pract_id last_name first_name middle_initial degree home_facility
# MAGIC       home_section1 home_section2 telemed_program;
# MAGIC    where faccode not in ('REG','RCH','STK');
# MAGIC    run;
# MAGIC
# MAGIC proc transpose data=to_transpose out=transposed;
# MAGIC    by pract_id last_name ... telemed_program;
# MAGIC    id faccode;
# MAGIC    var stat;
# MAGIC    run;
# MAGIC ```
# MAGIC
# MAGIC The `by` list is `TELEMED_IDENTITY_COLUMNS`, the `id` is `faccode`, the `var`
# MAGIC is `stat`. `facility_crosstab()` takes those three and does two things
# MAGIC `proc transpose` did not:
# MAGIC
# MAGIC * **every** code in `FACILITY_CODES` becomes a column, in registry order,
# MAGIC   whether or not any row mentioned it. `proc transpose` emitted only the
# MAGIC   values it saw, so a facility with no qualifying practitioners this week
# MAGIC   dropped a column — and `proc report` then failed on the missing variable
# MAGIC   rather than printing an empty one.
# MAGIC * gaps are filled with a blank, not `NA`. Blank is a *meaningful* status here
# MAGIC   (see `STATUS_LABELS`), and `NA` would render as the string "NA".
# MAGIC
# MAGIC The `where faccode not in (...)` is `exclude_faccodes()`, and it belongs here
# MAGIC — after §9, as the SAS had it. See the note in §10.

# COMMAND ----------

to_transpose <- list_practs4 |>
  exclude_faccodes() |>
  select(all_of(c(TELEMED_IDENTITY_COLUMNS, FACCODE_COLUMN, STAT_COLUMN)))

transposed <- to_transpose |>
  facility_crosstab(index = TELEMED_IDENTITY_COLUMNS) |>
  select_report_columns()

step_check("Transpose", transposed)

transposed

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. The final `proc sort`
# MAGIC
# MAGIC ```sas
# MAGIC proc sort data=transposed;
# MAGIC    by last_name first_name middle_initial degree home_facility;
# MAGIC    run;
# MAGIC ```
# MAGIC
# MAGIC The stages deliberately do **not** sort a remote result. An `ORDER BY` in a
# MAGIC subquery is something most engines are free to discard, and dbplyr warns when
# MAGIC you ask for one — so sorting an intermediate is at best wasted work and at
# MAGIC worst misleading.
# MAGIC
# MAGIC Sort once, here, immediately before collecting. On a local frame this is
# MAGIC redundant; on a remote one it is the only place the ordering survives.
# MAGIC
# MAGIC This is the report's display order, and it is the SAS one — by name, not by
# MAGIC `pract_id`. The test suite sorts by `pract_id` instead, because it is
# MAGIC asserting content and wants a key that cannot tie.

# COMMAND ----------

report <- transposed |>
  arrange(last_name, first_name, middle_initial, degree, home_facility) |>
  collect()

report

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. `title` and `title2`
# MAGIC
# MAGIC ```sas
# MAGIC title  "Active Telemedicine Affiliates - Status at All Facilities - &title_dt.";
# MAGIC title2 'STATUS CODES:  P - Primary facility where currently privileged, ...';
# MAGIC ```
# MAGIC
# MAGIC The second title is the legend, and it was a hand-typed string sitting a
# MAGIC couple of hundred lines away from the `case when` that assigns the codes.
# MAGIC Here it is generated from `STATUS_LABELS`, which is the same constant
# MAGIC `facility_status_expr()` is written against — so a fifth status code cannot
# MAGIC be added without the legend picking it up.
# MAGIC
# MAGIC The wording is slightly shorter than the SAS: `STATUS_LABELS` says "Primary
# MAGIC facility" where the SAS said "Primary facility where currently privileged".
# MAGIC If the longer phrasing matters to the audience, change it in
# MAGIC `R/report_transforms.R` and the legend, the docs and the help page all
# MAGIC follow.

# COMMAND ----------

report_title <- paste0(
  "Active Telemedicine Affiliates - Status at All Facilities - ", title_dt
)

status_legend <- paste0(
  "STATUS CODES:  ",
  paste0(
    ifelse(trimws(names(STATUS_LABELS)) == "", "Blank", names(STATUS_LABELS)),
    " - ", unname(STATUS_LABELS),
    collapse = ",  "
  )
)

cat(report_title, "\n", status_legend, "\n", sep = "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. `&telemeds_cnt.`
# MAGIC
# MAGIC ```sas
# MAGIC proc sql noprint;
# MAGIC select strip(put(count(*),4.)) into :telemeds_cnt from transposed;
# MAGIC quit;
# MAGIC ```
# MAGIC
# MAGIC One row per practitioner in the transposed frame, so this is a headcount.
# MAGIC The `4.` format would have silently truncated at 9999; `format(big.mark)`
# MAGIC does not.

# COMMAND ----------

telemeds_cnt <- nrow(report)

cat(
  "*** There are ", format(telemeds_cnt, big.mark = ","),
  " active and credentialed telemedicine affiliates in Northern California.\n",
  sep = ""
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. `ods excel` / `proc report`
# MAGIC
# MAGIC ```sas
# MAGIC ods excel file="&caps_outpath./weekly_telemed/NCAL_TELEMEDICINE_&date8..xlsx"
# MAGIC    options(sheet_name="TELEMED_&sysdate9." ... autofilter='all' ...);
# MAGIC proc report data=transposed ...;
# MAGIC    column (pract_id last_name ... vac val wcr);
# MAGIC    define pract_id / display 'Pract\ID' ...;
# MAGIC ods excel close;
# MAGIC ```
# MAGIC
# MAGIC `proc report` did two separable jobs: **layout** — which columns, in what
# MAGIC order, under what headers — and **presentation** — fonts, widths, frozen
# MAGIC headers, autofilter.
# MAGIC
# MAGIC The layout half is already data: `ordered_columns()` and `headers()`, in
# MAGIC `R/report_layout.R`. That is the fix for the specific way these two files
# MAGIC drifted — the SAS carried every header twice, once as a `label=` on the
# MAGIC `proc sql` select item and once as a `define / display` here, and the two
# MAGIC disagreed. A facility's header is its own code, generated rather than listed,
# MAGIC so a code added to the registry cannot be forgotten.
# MAGIC
# MAGIC The presentation half is **not** reproduced. `writexl` writes a plain sheet
# MAGIC and has no styling API at all, which is exactly why it has no compiled
# MAGIC dependencies and installs anywhere. If the fonts and frozen headers turn out
# MAGIC to matter, `openxlsx` does all of it — `freezePane()`, `setColWidths()`,
# MAGIC `addFilter()`, `createStyle()` — at the cost of a heavier dependency.

# COMMAND ----------

# The layout, as the workbook will see it.
tibble::tibble(
  position = seq_along(ordered_columns()),
  column   = ordered_columns(),
  header   = unname(headers()[ordered_columns()])
) |>
  print(n = Inf)

# COMMAND ----------

# MAGIC %md
# MAGIC The titles have nowhere to go in a plain sheet. `embedded_titles='yes'`
# MAGIC needs merged cells above the header row, and putting them in row 1 of the
# MAGIC data sheet would break `autofilter` and every reader that expects row 1 to be
# MAGIC the header. They go on a second sheet instead, along with the legend and the
# MAGIC count — which is also where anyone opening the file next quarter will look
# MAGIC for "what am I reading, and when was it run".

# COMMAND ----------

# SAS: &caps_outpath./weekly_telemed/. On a cluster, point this at
# "/dbfs/FileStore/reports" or a Volume; tempdir() keeps the notebook runnable
# anywhere.
output_root <- file.path(tempdir(), "weekly_telemed")

dir.create(output_root, recursive = TRUE, showWarnings = FALSE)

stem       <- paste0("NCAL_TELEMEDICINE_", date8)
sheet_name <- paste0("TELEMED_", sysdate9)   # Excel caps sheet names at 31 chars

sheets <- list(
  # Display headers, applied at the last possible moment: everything upstream
  # works in column names, which is what a rule can be written against.
  stats::setNames(report, unname(headers()[names(report)])),
  tibble::tibble(
    item  = c("Report", "Run date", "Status codes", "Affiliates", "Source"),
    value = c(report_title, title_dt, status_legend,
              format(telemeds_cnt, big.mark = ","), prognm)
  )
)

names(sheets) <- c(sheet_name, "NOTES")

if (requireNamespace("writexl", quietly = TRUE)) {
  written <- file.path(output_root, paste0(stem, ".xlsx"))
  writexl::write_xlsx(sheets, path = written)
} else {
  # No writexl on this cluster. CSV keeps the notebook runnable and loses only
  # the presentation, which this path was not reproducing anyway.
  written <- file.path(output_root, paste0(stem, ".csv"))
  utils::write.csv(sheets[[sheet_name]], written, row.names = FALSE, na = "")
  utils::write.csv(
    sheets[["NOTES"]],
    file.path(output_root, paste0(stem, "_notes.csv")),
    row.names = FALSE, na = ""
  )
  message("writexl is not installed; wrote CSV instead.")
}

step_check("Report")

cat("wrote ", written, " (", file.size(written), " bytes)\n", sep = "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. `%emlist` and `%send_email`
# MAGIC
# MAGIC ```sas
# MAGIC %emlist(listnm=weekly telemed);
# MAGIC %emlist(listnm=weekly telemed cc, listvar=cclist);
# MAGIC %emlist(listnm=caps admin, listvar=caps_admin);
# MAGIC %send_email(em_to=&emlist., em_cc=&cclist., em_bcc=&caps_admin.,
# MAGIC             em_subject=Telemedicine Affiliates Report for &sysdate9.,
# MAGIC             em_paragraphs=4, em_attach=..., em_content_type='application/xlsx');
# MAGIC ```
# MAGIC
# MAGIC **This cell builds the message and does not send it.** Sending is the one
# MAGIC irreversible thing the macro did, and a notebook that sends on every run is a
# MAGIC notebook nobody dares execute — which is how you end up with a report only
# MAGIC the scheduler has ever run successfully.
# MAGIC
# MAGIC The payload is a plain list, so a job can hand it to whatever transport the
# MAGIC platform provides. Two paths on Databricks:
# MAGIC
# MAGIC * a **Job task email notification**, if a link to the run is enough
# MAGIC * an **SMTP relay** from the notebook, if the workbook has to be attached —
# MAGIC   which is what the SAS did. Credentials come from a secret scope, never from
# MAGIC   a cell.
# MAGIC
# MAGIC `%emlist` read the distribution lists from a table so they could change
# MAGIC without a code deploy. Keep that: the placeholders below are placeholders,
# MAGIC not a suggestion to hardcode addresses.

# COMMAND ----------

# SAS: %emlist(listnm=...) reads these from a distribution-list table. Replace
# with that lookup -- or with dbutils.secrets -- before this runs unattended.
emlist     <- c("weekly-telemed@example.org")
cclist     <- c("weekly-telemed-cc@example.org")
caps_admin <- c("caps-admin@example.org")

email <- list(
  to           = emlist,
  cc           = cclist,
  bcc          = caps_admin,
  subject      = paste0("Telemedicine Affiliates Report for ", sysdate9),
  attach       = written,
  content_type = "application/xlsx",
  paragraphs   = c(
    paste0("Attached is the Active Telemedicine Affiliates Report for ", title_dt, "."),
    paste0(
      "The population of this report is all current Telemedicine Affiliates in ",
      "MSOW who are also credentialed and current at their primary facilities."
    ),
    paste0(
      "There are ", format(telemeds_cnt, big.mark = ","),
      " active and credentialed telemedicine affiliates in MSOW."
    ),
    strrep("_", 68)
  )
)

# COMMAND ----------

# The rendered message, for eyeballing before anyone wires up a transport.
cat(
  "To:      ", paste(email$to, collapse = ", "), "\n",
  "Cc:      ", paste(email$cc, collapse = ", "), "\n",
  "Bcc:     ", paste(email$bcc, collapse = ", "), "\n",
  "Subject: ", email$subject, "\n",
  "Attach:  ", email$attach, "\n\n",
  paste(email$paragraphs, collapse = "\n\n"), "\n",
  sep = ""
)

step_check("Email")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. `%job_monitor(job_event=FINISH)`

# COMMAND ----------

step_check("job finish")

cat(prognm, " -- ", progdescr, "\n", sep = "")
cat("rows:  ", telemeds_cnt, "\n", sep = "")
cat("file:  ", written, "\n", sep = "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is still not here
# MAGIC
# MAGIC | SAS | status |
# MAGIC |---|---|
# MAGIC | `proc report` styling — fonts, widths, frozen headers, autofilter | not reproduced; `openxlsx` would do it |
# MAGIC | `%send_email` | built, not sent — §17 |
# MAGIC | `%emlist` distribution lists | placeholders; wire these to the list table |
# MAGIC | `%job_monitor` | use a Databricks Job task and its alerts |
# MAGIC
# MAGIC And one thing that *is* here but is **not** a faithful port: the tie-break in
# MAGIC §9. Everything else in this notebook produces what the SAS produced.
