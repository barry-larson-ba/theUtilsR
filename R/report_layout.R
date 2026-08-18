# report_layout --------------------------------------------------------------
#
# Column order and display headers, held as data.
#
# The SAS originals carried this in two places at once -- a `label=` on every
# `proc sql` select item, and a `define <col> / display '...'` in `proc report`
# -- which is why the two drifted. Here each report gets one ordered mapping of
# column name to header, and both the pipeline and (later) the workbook writer
# read that same mapping.
#
# Nothing in this file imports or touches a dataframe library.

#' Practitioner-level columns of the weekly telemed report, in report order
#'
#' These are also the crosstab index: one output row per distinct combination.
#'
#' @format A character vector of 9 column names.
#' @seealso [TELEMED_LABELS], [ordered_columns()]
#' @export
TELEMED_IDENTITY_COLUMNS <- c(
  "pract_id",
  "last_name",
  "first_name",
  "middle_initial",
  "degree",
  "home_facility",
  "home_section1",
  "home_section2",
  "telemed_program"
)

#' Display headers for the practitioner-level columns
#'
#' The facility columns are deliberately absent: a facility's header is its own
#' code, which [headers()] fills in, so a code added to the registry cannot be
#' forgotten here.
#'
#' @format A named character vector; names are columns, values are headers.
#' @seealso [headers()]
#' @export
TELEMED_LABELS <- c(
  pract_id       = "Pract ID",
  last_name      = "Last Name",
  first_name     = "First Name",
  middle_initial = "Middle Initial",
  degree         = "Degree",
  home_facility  = "Primary Facility",
  home_section1  = "Home Privilege 1",
  home_section2  = "Home Privilege 2",
  telemed_program = "Telemedicine Program 1"
)

#' The report's full column order
#'
#' Identity columns first, then one column per facility code.
#'
#' @param identity Character vector of practitioner-level columns.
#' @param facility_codes Character vector of facility codes, in report order.
#'
#' @return A character vector of column names.
#' @seealso [headers()], [select_report_columns()]
#' @export
#' @examples
#' length(ordered_columns())
#' head(ordered_columns(), 3)
ordered_columns <- function(identity = TELEMED_IDENTITY_COLUMNS,
                            facility_codes = FACILITY_CODES) {
  c(as.character(identity), as.character(facility_codes))
}

#' Column to display header for every column, facilities included
#'
#' A facility's header is its own code, so those entries are generated rather
#' than listed.
#'
#' @param labels Named character vector of column -> header for the identity
#'   columns.
#' @param facility_codes Character vector of facility codes.
#'
#' @return A named character vector covering every reported column.
#' @export
#' @examples
#' headers()[["home_facility"]]
#' headers()[["OAK"]]
headers <- function(labels = TELEMED_LABELS, facility_codes = FACILITY_CODES) {
  facility <- stats::setNames(as.character(facility_codes), as.character(facility_codes))

  c(labels, facility)
}

#' Project a frame into report column order
#'
#' A one-line convenience so a pipeline ends the same way whether it is running
#' on a tibble or on a remote table.
#'
#' @param data A data frame or a lazy table.
#' @param columns Character vector of columns, in order.
#'
#' @return `data`, with `columns` selected in the given order.
#' @export
select_report_columns <- function(data, columns = ordered_columns()) {
  dplyr::select(data, dplyr::all_of(columns))
}

#' Check the report layout for internal drift
#'
#' Called from `.onLoad()`, in the same spirit as [validate_facility_registry()]:
#' a column added to the report order without a header (or a header for a column
#' no longer in the report) fails at load rather than printing a bare variable
#' name in next week's workbook.
#'
#' @param identity Character vector of identity columns.
#' @param labels Named character vector of column -> header.
#'
#' @return `TRUE`, invisibly, if every check passes.
#' @export
#' @examples
#' validate_report_layout()
validate_report_layout <- function(identity = TELEMED_IDENTITY_COLUMNS,
                                   labels = TELEMED_LABELS) {
  unlabelled <- setdiff(identity, names(labels))
  orphans <- setdiff(names(labels), identity)

  if (length(unlabelled) || length(orphans)) {
    rlang::abort(
      c(
        "TELEMED_IDENTITY_COLUMNS and TELEMED_LABELS disagree.",
        i = paste0("unlabelled columns: ", .or_none(unlabelled)),
        i = paste0("orphan labels: ", .or_none(orphans))
      ),
      class = "theUtilsR_layout_drift"
    )
  }

  invisible(TRUE)
}
