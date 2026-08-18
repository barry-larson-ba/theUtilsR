# report_transforms ----------------------------------------------------------
#
# Reusable transforms shared across the credentialing reports.
#
# Each rule is written once, in dplyr, and runs unchanged on a local tibble or
# on a remote table through dbplyr. The Python original writes every rule three
# times (pandas / Polars / PySpark) and uses one shared table of test cases to
# stop the three drifting apart; here there is nothing to drift.
#
# Two forms are provided for the rules that are naturally expressions:
#
#   *_expr()   a quosure, to splice yourself:  filter(df, !!is_..._expr())
#   *_row()    the same rule already applied:  is_..._row(df)
#
# The second is implemented in terms of the first, so the rule still has
# exactly one definition. The quosure carries this package's namespace as its
# environment, so stringr and dplyr resolve whether or not the caller has
# attached them.

# ---------------------------------------------------------------------------
# Business-rule constants
# ---------------------------------------------------------------------------
# Any change to what counts as "credentialed", "telemedicine", or "reportable"
# belongs here, not inside a transform body. Keeping the rule in one place is
# the point of the whole refactor.

#' @rdname transform_constants
#' @export
PRIMARY_FAC_FLAG_YES <- "Y"

#' @rdname transform_constants
#' @export
CURRENT_STATUS_CURRENT <- "CURRENT"

#' @rdname transform_constants
#' @export
CREDENTIALED_CATEGORIES <- c("ACTIVE", "COURTESY", "CONSULTANT", "TEMPORARY")

#' @rdname transform_constants
#' @export
PROVISIONAL_PREFIX <- "PROVISIONAL"

#' @rdname transform_constants
#' @export
TELEMED_SUBSTRING <- "TELEMED"

#' @rdname transform_constants
#' @export
STATUS_PRIMARY <- "P"

#' @rdname transform_constants
#' @export
STATUS_CREDENTIALED <- "C"

#' @rdname transform_constants
#' @export
STATUS_TELEMED <- "T"

#' @rdname transform_constants
#' @export
STATUS_INACTIVE <- " "

#' @rdname transform_constants
#' @export
STATUS_LABELS <- c(
  P   = "Primary facility",
  C   = "Credentialed at non-primary site",
  T   = "Current telemedicine affiliate",
  " " = "Not currently active"
)

#' @rdname transform_constants
#' @export
YES_FLAG <- "Y"

#' @rdname transform_constants
#' @export
APPLICANT_STATUS <- "APPLICANT"

#' @rdname transform_constants
#' @export
CURRENT_MONTH <- 0L

#' @rdname transform_constants
#' @export
TELEMED_CATEGORY <- "TELEMEDICINE AFFILIATE"

#' @rdname transform_constants
#' @export
PROGRAM_TEXT_DROP_PATTERN <- "[^A-Za-z0-9\\s]"

#' @rdname transform_constants
#' @export
PROGRAM_TEXT_COLLAPSE_PATTERN <- "\\s+"

#' @rdname transform_constants
#' @export
FAC_COUNT_COLUMN <- "fac_count"

#' @rdname transform_constants
#' @export
TELEMED_PROGRAM_COLUMN <- "telemed_program"

#' @rdname transform_constants
#' @export
PRACT_ID_COLUMN <- "pract_id"

#' @rdname transform_constants
#' @export
EXPERTISE_COLUMN <- "expertise"

#' @rdname transform_constants
#' @export
STAT_COLUMN <- "stat"

#' @rdname transform_constants
#' @export
HOME_FACILITY_COLUMNS <- c(
  pract_id = "pract_id",
  faccode  = "home_facility",
  section1 = "home_section1",
  section2 = "home_section2"
)

#' Business-rule constants
#'
#' The values every transform in this package is defined against. Change a rule
#' here, never inside a transform body.
#'
#' @section The status codes:
#'
#' `STATUS_PRIMARY` (`"P"`), `STATUS_CREDENTIALED` (`"C"`), `STATUS_TELEMED`
#' (`"T"`) and `STATUS_INACTIVE` (a single space) are the four values a
#' crosstab cell can hold. Blank is a *meaningful* status, not a missing value
#' -- see `STATUS_LABELS` -- which is why the crosstab fills gaps with it
#' rather than leaving them `NA`.
#'
#' @section Two telemedicine constants, deliberately:
#'
#' `TELEMED_SUBSTRING` (`"TELEMED"`) is what the classifier looks for *inside*
#' a status category when deciding a facility's status. `TELEMED_CATEGORY`
#' (`"TELEMEDICINE AFFILIATE"`) is the exact, full category name that defines
#' the report population. The second is stricter than the first on purpose.
#'
#' @section Program text cleanup:
#'
#' `PROGRAM_TEXT_DROP_PATTERN` and `PROGRAM_TEXT_COLLAPSE_PATTERN` are the SAS
#' `compbl(compress(x, , 'kads'))` pair: `kads` keeps alphabetic, digit and
#' space characters and drops everything else; `compbl` then collapses runs of
#' blanks to one.
#'
#' @section HOME_FACILITY_COLUMNS:
#'
#' Source column (the name) to emitted column (the value), for
#' [home_facility_rows()]. The renames are the SAS `faccode as home_facility` /
#' `section1 as home_section1` aliases, applied where the rows are selected
#' rather than in a later join.
#'
#' @name transform_constants
#' @format Character vectors; see the individual values.
NULL


# ===========================================================================
# Classifier
# ===========================================================================

#' Classify a row's facility status as P / C / T / blank
#'
#' `facility_status_expr()` returns the rule as a quosure; the expression form
#' matches the Polars and Spark halves of the Python original.
#' `classify_facility_status()` applies it.
#'
#' Expects the columns `primary_fac_flag`, `current_status` and
#' `status_category`.
#'
#' @section On missing values:
#'
#' An `NA` in any tested column makes its condition `NA`, which `case_when()`
#' treats as "not matched" and falls through -- so a row with nothing known
#' about it lands on `STATUS_INACTIVE`. That is the same answer the pandas
#' original gives, where the comparisons coerce `NaN` to `FALSE`, and the same
#' answer SQL gives, where the `CASE WHEN` branches see `NULL`.
#'
#' @param data A data frame or lazy table.
#' @param into Name of the column to write the status into.
#'
#' @return `facility_status_expr()`: a quosure.
#'   `classify_facility_status()`: `data` with the `into` column added.
#' @export
#' @examples
#' rows <- tibble::tibble(
#'   primary_fac_flag = c("Y", "N", "N", "N"),
#'   current_status   = c("CURRENT", "CURRENT", "CURRENT", "EXPIRED"),
#'   status_category  = c("ACTIVE", "COURTESY", "TELEMEDICINE AFFILIATE", "ACTIVE")
#' )
#' classify_facility_status(rows)$stat
facility_status_expr <- function() {
  # substr(...) == PREFIX rather than startsWith(): the latter has no
  # translation on several backends, while SUBSTR is universal.
  # grepl() for the substring test: dbplyr's base translation covers it, and
  # sparklyr renders it as RLIKE.
  rlang::quo(
    dplyr::case_when(
      primary_fac_flag == !!PRIMARY_FAC_FLAG_YES ~ !!STATUS_PRIMARY,

      toupper(current_status) == !!CURRENT_STATUS_CURRENT &
        (toupper(status_category) %in% !!CREDENTIALED_CATEGORIES |
           substr(toupper(status_category), 1L, !!nchar(PROVISIONAL_PREFIX)) ==
             !!PROVISIONAL_PREFIX) ~ !!STATUS_CREDENTIALED,

      toupper(current_status) == !!CURRENT_STATUS_CURRENT &
        grepl(!!TELEMED_SUBSTRING, toupper(status_category)) ~ !!STATUS_TELEMED,

      TRUE ~ !!STATUS_INACTIVE
    )
  )
}

#' @rdname facility_status_expr
#' @export
classify_facility_status <- function(data, into = STAT_COLUMN) {
  dplyr::mutate(data, !!into := !!facility_status_expr())
}


# ===========================================================================
# Ordering within a group
# ===========================================================================
# This is one of the two places the local/remote abstraction genuinely leaks.
#
# Locally, arrange() fixes the row order and a subsequent row_number() sees it.
# On a lazy table arrange() becomes an ORDER BY in a subquery, which most
# engines are free to ignore; the window frame's ordering has to be set with
# dbplyr::window_order() instead. The dispatch below is the only difference,
# and both branches are covered by the same tests.
#
# Nulls sort last in both branches, in both directions. That is not any
# engine's default -- and it is not consistent between engines either -- so the
# ordering is made explicit by materialising an IS NULL flag as a real column
# and sorting on that first. window_order() accepts only bare column names or
# desc(col), so the flag cannot be an inline expression.

.order_flags <- function(desc, n) {
  if (is.logical(desc) && length(desc) == 1L) {
    return(rep(desc, n))
  }

  flags <- as.logical(desc)

  if (length(flags) != n) {
    rlang::abort(
      paste0(
        "`desc` has ", length(flags), " entries but there are ", n,
        " ordering column(s); pass a single TRUE/FALSE to use one direction ",
        "for all."
      ),
      class = "theUtilsR_bad_desc"
    )
  }

  flags
}

# Sort a local frame; leave a lazy one alone.
#
# Ordering an *intermediate* remote result is not meaningful. The ORDER BY
# lands in a subquery, which most engines are free to discard, and dbplyr warns
# about exactly that. Local frames do carry row order, and the pandas original
# these stages are ported from sorts, so the sort is kept where it means
# something.
#
# On a remote backend, arrange() once at the end of the pipeline, immediately
# before collect() -- that is the only position where a SQL ORDER BY survives.
.arrange_by <- function(data, columns) {
  if (inherits(data, "tbl_lazy")) {
    return(data)
  }

  dplyr::arrange(data, dplyr::across(dplyr::all_of(columns)))
}

.check_columns <- function(data, columns, what) {
  missing <- setdiff(columns, colnames(data))

  if (length(missing)) {
    rlang::abort(
      c(
        paste0(what, " names ", length(missing),
               " column(s) that are not in the data: ",
               paste(missing, collapse = ", ")),
        i = paste0("available columns: ", paste(colnames(data), collapse = ", "))
      ),
      class = "theUtilsR_missing_columns"
    )
  }

  invisible(TRUE)
}

#' Keep the top row per group, ranked by an ordering column
#'
#' `desc` accepts either one value for every ordering column or one per column,
#' so a ranking can mix directions -- "highest count, then earliest name" being
#' the common case for making a tie-break deterministic.
#'
#' Missing values sort **last** in every backend and in both directions. No
#' engine does that by default and no two engines agree, so this asks for it
#' explicitly; without it an `NA` in an ordering column would pick a different
#' winner depending on where the report ran.
#'
#' @param data A data frame or lazy table.
#' @param group Character vector of grouping columns.
#' @param order_by Character vector of ordering columns, most significant
#'   first.
#' @param desc Either a single `TRUE`/`FALSE`, or one per `order_by` column.
#'
#' @return `data`, reduced to one row per `group`, ungrouped.
#' @export
#' @examples
#' counts <- tibble::tibble(
#'   pract_id  = c(1, 1, 2),
#'   expertise = c("TeleICU", "TeleStroke", "TelePsych"),
#'   fac_count = c(2, 2, 1)
#' )
#' # highest count, ties broken on the earliest program name
#' first_per_group(counts, "pract_id", c("fac_count", "expertise"), c(TRUE, FALSE))
first_per_group <- function(data, group, order_by, desc = TRUE) {
  .check_columns(data, c(group, order_by), "first_per_group()")
  flags <- .order_flags(desc, length(order_by))

  na_names <- paste0("tu__na", seq_along(order_by))
  na_exprs <- stats::setNames(
    lapply(order_by, function(column) {
      rlang::quo(as.integer(is.na(!!rlang::sym(column))))
    }),
    na_names
  )

  out <- dplyr::mutate(data, !!!na_exprs)

  ordering <- vector("list", 2L * length(order_by))

  for (i in seq_along(order_by)) {
    ordering[[2L * i - 1L]] <- rlang::sym(na_names[[i]])
    # A bare expression, not a quosure: dbplyr::window_order() inspects the
    # call structure looking for `desc(col)` and errors on a quosure, and it
    # will not see through a `dplyr::desc()` namespace qualifier either. Both
    # branches below evaluate in this function's environment, where `desc` is
    # imported from dplyr.
    ordering[[2L * i]] <- if (flags[[i]]) {
      rlang::expr(desc(!!rlang::sym(order_by[[i]])))
    } else {
      rlang::sym(order_by[[i]])
    }
  }

  out <- dplyr::group_by(out, dplyr::across(dplyr::all_of(group)))

  out <- if (inherits(data, "tbl_lazy")) {
    dbplyr::window_order(out, !!!ordering)
  } else {
    dplyr::arrange(out, !!!ordering, .by_group = TRUE)
  }

  out <- dplyr::mutate(out, tu__rn = dplyr::row_number())
  out <- dplyr::filter(out, .data$tu__rn == 1L)
  out <- dplyr::ungroup(out)

  dplyr::select(out, -dplyr::all_of(c(na_names, "tu__rn")))
}

#' Keep the first row per group after sorting by the group keys
#'
#' The SAS `proc sort; data ...; by X; if first.X;` idiom.
#'
#' Rows on a remote table have no inherent order, so "first after sort" needs
#' an explicit tie-break. `order_by` defaults to `by`, which is deterministic
#' on the keys and arbitrary on every other column -- name the columns you
#' actually want to rank on whenever more than one row per group can survive.
#'
#' @param data A data frame or lazy table.
#' @param by Character vector of grouping columns.
#' @param order_by Character vector of ordering columns. Defaults to `by`.
#'
#' @return `data`, reduced to one row per `by` combination.
#' @export
dedupe_first <- function(data, by, order_by = by) {
  first_per_group(data, group = by, order_by = order_by, desc = FALSE)
}


# ===========================================================================
# Active-credentialing filter
# ===========================================================================
# The definition of "an active credentialing row worth looking at":
#   - primary DEA on file (or missing, treated as OK)
#   - primary license on file (or missing, treated as OK)
#   - credentialed, OR still in APPLICANT status
#   - current month snapshot (month = 0)

#' Reportable active credentialing rows
#'
#' @param data A data frame or lazy table.
#'
#' @return `is_active_credentialing_expr()`: a quosure, for `filter()`.
#'   `is_active_credentialing_row()`: `data`, filtered.
#' @export
#' @examples
#' rows <- tibble::tibble(
#'   primary_dea = "Y", primary_license = NA_character_,
#'   credentialed = c("Y", "N"), current_status = c("CURRENT", "EXPIRED"),
#'   month = 0L
#' )
#' nrow(is_active_credentialing_row(rows))
is_active_credentialing_expr <- function() {
  rlang::quo(
    (primary_dea == !!YES_FLAG | is.na(primary_dea)) &
      (primary_license == !!YES_FLAG | is.na(primary_license)) &
      (credentialed == !!YES_FLAG |
         toupper(current_status) == !!APPLICANT_STATUS) &
      month == !!CURRENT_MONTH
  )
}

#' @rdname is_active_credentialing_expr
#' @export
is_active_credentialing_row <- function(data) {
  dplyr::filter(data, !!is_active_credentialing_expr())
}


# ===========================================================================
# Population predicates
# ===========================================================================
# The two halves of the telemed population. A practitioner qualifies by having
# *both* somewhere in the current snapshot -- a telemedicine affiliation at
# some facility, and a live credential at their primary one. They are separate
# rules because they match different rows of the same table.

#' Current telemedicine-affiliate rows in the current snapshot
#'
#' @param data A data frame or lazy table.
#'
#' @return `is_current_telemed_expr()`: a quosure, for `filter()`.
#'   `is_current_telemed_row()`: `data`, filtered.
#' @export
is_current_telemed_expr <- function() {
  rlang::quo(
    toupper(status_category) == !!TELEMED_CATEGORY &
      toupper(current_status) == !!CURRENT_STATUS_CURRENT &
      month == !!CURRENT_MONTH
  )
}

#' @rdname is_current_telemed_expr
#' @export
is_current_telemed_row <- function(data) {
  dplyr::filter(data, !!is_current_telemed_expr())
}

#' Live credentials at the practitioner's primary facility
#'
#' @param data A data frame or lazy table.
#'
#' @return `is_current_primary_credentialed_expr()`: a quosure, for `filter()`.
#'   `is_current_primary_credentialed_row()`: `data`, filtered.
#' @export
is_current_primary_credentialed_expr <- function() {
  rlang::quo(
    primary_record == !!YES_FLAG &
      credentialed == !!YES_FLAG &
      toupper(current_status) == !!CURRENT_STATUS_CURRENT &
      month == !!CURRENT_MONTH
  )
}

#' @rdname is_current_primary_credentialed_expr
#' @export
is_current_primary_credentialed_row <- function(data) {
  dplyr::filter(data, !!is_current_primary_credentialed_expr())
}


# ===========================================================================
# Telemed population
# ===========================================================================

#' The practitioners a telemedicine report covers
#'
#' A practitioner is in scope when the snapshot holds *both* a current
#' telemedicine affiliation (at any facility) and a live credential at their
#' primary facility -- the intersection of the two population predicates, and
#' the pairing the SAS original expressed as a self-join.
#'
#' Returns a one-column frame rather than a bare vector so the result composes
#' with [restrict_to_practitioners()] on either backend.
#'
#' @param data A data frame or lazy table.
#'
#' @return A one-column frame of distinct `pract_id`, sorted.
#' @export
telemed_population <- function(data) {
  telemed <- dplyr::distinct(
    is_current_telemed_row(data),
    dplyr::across(dplyr::all_of(PRACT_ID_COLUMN))
  )
  credentialed <- dplyr::distinct(
    is_current_primary_credentialed_row(data),
    dplyr::across(dplyr::all_of(PRACT_ID_COLUMN))
  )

  out <- dplyr::semi_join(telemed, credentialed, by = PRACT_ID_COLUMN)

  .arrange_by(out, PRACT_ID_COLUMN)
}

#' Narrow a frame to a population of practitioners
#'
#' A semi-join rather than an inner join: `population` is distinct on
#' `pract_id`, but a semi-join says so to the planner and cannot duplicate rows
#' even if it somehow is not.
#'
#' @param data A data frame or lazy table.
#' @param population A frame with a `pract_id` column, e.g. from
#'   [telemed_population()].
#'
#' @return `data`, keeping only rows whose `pract_id` appears in `population`.
#' @export
restrict_to_practitioners <- function(data, population) {
  dplyr::semi_join(
    data,
    dplyr::select(population, dplyr::all_of(PRACT_ID_COLUMN)),
    by = PRACT_ID_COLUMN
  )
}


# ===========================================================================
# Home facility
# ===========================================================================

#' One home-facility row per practitioner
#'
#' The primary-facility row for each practitioner, renamed to the `home_*`
#' columns that get joined back onto every one of that practitioner's rows.
#'
#' Expects `stat` to have been assigned already by
#' [classify_facility_status()].
#'
#' @param data A data frame or lazy table.
#' @param columns Named character vector of source column -> emitted column.
#'
#' @return One row per practitioner, with the `home_*` column names.
#' @export
home_facility_rows <- function(data, columns = HOME_FACILITY_COLUMNS) {
  .check_columns(data, names(columns), "home_facility_rows()")

  selection <- stats::setNames(rlang::syms(names(columns)), unname(columns))

  out <- dplyr::filter(data, .data[[STAT_COLUMN]] == !!STATUS_PRIMARY)
  out <- dplyr::select(out, !!!selection)
  out <- dplyr::distinct(out)

  .arrange_by(out, PRACT_ID_COLUMN)
}


# ===========================================================================
# Dominant telemedicine program
# ===========================================================================
# Of the telemedicine programs a practitioner participates in, the one covering
# the most facilities. The SAS original ranked on the facility count alone and
# left ties to whatever order the sort happened to produce; this breaks ties on
# the program name so a week-over-week diff of the report does not flap.

#' The telemedicine program a practitioner runs at the most facilities
#'
#' Counted with `n()`, so a group whose `expertise` is `NA` is scored by its
#' row count rather than at zero -- `COUNT(expertise)` would skip the nulls.
#'
#' @param data A data frame or lazy table carrying `stat`, `pract_id` and
#'   `expertise`.
#'
#' @return A frame of `pract_id` and `telemed_program`.
#' @export
dominant_telemed_program <- function(data) {
  counts <- dplyr::filter(data, .data[[STAT_COLUMN]] == !!STATUS_TELEMED)
  counts <- dplyr::group_by(
    counts,
    dplyr::across(dplyr::all_of(c(PRACT_ID_COLUMN, EXPERTISE_COLUMN)))
  )
  counts <- dplyr::summarise(
    counts,
    !!FAC_COUNT_COLUMN := dplyr::n(),
    .groups = "drop"
  )

  top <- first_per_group(
    counts,
    group = PRACT_ID_COLUMN,
    order_by = c(FAC_COUNT_COLUMN, EXPERTISE_COLUMN),
    desc = c(TRUE, FALSE)
  )

  dplyr::select(
    top,
    dplyr::all_of(PRACT_ID_COLUMN),
    !!TELEMED_PROGRAM_COLUMN := dplyr::all_of(EXPERTISE_COLUMN)
  )
}


# ===========================================================================
# Program text cleanup
# ===========================================================================
# SAS: compbl(compress(x, , 'kads')) -- drop everything that is not a letter,
# digit or space, then collapse runs of blanks.
#
# The trailing trim has no SAS counterpart: SAS character variables are
# blank-padded to their declared width, so trailing blanks were never visible
# there. Trimming here keeps a value from sorting or comparing differently than
# it displays. NA passes through as NA on both backends.
#
# stringr rather than gsub(): several dbplyr backends -- DuckDB among them --
# have no translation for gsub() at all, while str_replace_all() and str_trim()
# render as REGEXP_REPLACE and LTRIM(RTRIM(...)) everywhere.

#' Strip punctuation from a program name and collapse blanks
#'
#' @param data A data frame or lazy table.
#' @param column Name of the column to clean.
#'
#' @return `clean_program_text_expr()`: a quosure.
#'   `clean_program_text()`: `data` with `column` cleaned in place.
#' @export
#' @examples
#' clean_program_text(tibble::tibble(telemed_program = "Tele-Stroke  Unit!"))
clean_program_text_expr <- function(column = TELEMED_PROGRAM_COLUMN) {
  rlang::quo(
    stringr::str_trim(
      stringr::str_replace_all(
        stringr::str_replace_all(
          !!rlang::sym(column),
          !!PROGRAM_TEXT_DROP_PATTERN,
          ""
        ),
        !!PROGRAM_TEXT_COLLAPSE_PATTERN,
        " "
      )
    )
  )
}

#' @rdname clean_program_text_expr
#' @export
clean_program_text <- function(data, column = TELEMED_PROGRAM_COLUMN) {
  dplyr::mutate(data, !!column := !!clean_program_text_expr(column))
}
