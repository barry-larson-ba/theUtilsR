#' theUtilsR: Oracle readers and reusable report transforms
#'
#' The R port of the Python `theUtils` package. Two independent halves, kept
#' separable on purpose:
#'
#' \describe{
#'   \item{`oracle_utils`}{reading Oracle into Spark or a DBI connection}
#'   \item{`sources`}{logical table names -> wherever they live today}
#'   \item{`report_transforms`}{the business-rule transforms}
#'   \item{`crosstab`}{the facility crosstab}
#'   \item{`facility_codes`}{the facility code/name registry}
#'   \item{`report_layout`}{column order and display headers, as data}
#' }
#'
#' @section One implementation, two backends:
#'
#' The Python original writes every rule three times -- pandas, Polars and
#' PySpark -- and leans on a shared table of test cases to keep the three from
#' drifting. R does not need that. dplyr verbs run unchanged on a local tibble
#' and on a remote table, where dbplyr turns them into SQL, so each rule is
#' written **once** and the drift is impossible rather than merely tested for.
#'
#' What survives from the Python design is the shape of the API. There the
#' pandas variant took a data frame and the Polars/Spark variants returned a
#' bare expression to splice into `filter()` or `with_columns()`. Here both
#' forms exist for the rules that have them, and the data-taking form is
#' implemented *in terms of* the expression form, so there is still exactly one
#' copy of the rule:
#'
#' \preformatted{
#'   is_active_credentialing_expr()      # a quosure, for filter(df, !!expr)
#'   is_active_credentialing_row(data)   # filter(data, !!expr) -- same rule
#' }
#'
#' The two places where the abstraction genuinely leaks -- ordering within a
#' group, and pivoting -- are marked in the source and dispatch on whether the
#' input is a lazy table. Both paths are covered by the same tests.
#'
#' @section Translation-sensitive functions:
#'
#' Not every R function has a SQL translation. These are the ones this package
#' depends on, and the ones to re-check when adding a backend: `toupper`,
#' `substr`, `grepl`, `case_when`, `coalesce`, `if_else`, `is.na`,
#' `stringr::str_replace_all` and `stringr::str_trim`. Note that `gsub()` is
#' *not* translated by several backends (DuckDB among them), which is why the
#' text cleanup uses stringr.
#'
#' @keywords internal
"_PACKAGE"

## usethis namespace: start
#' @importFrom dbplyr window_order
#' @importFrom dplyr across all_of arrange case_when coalesce collect desc
#' @importFrom dplyr distinct filter group_by if_else mutate n rename select
#' @importFrom dplyr semi_join summarise ungroup row_number
#' @importFrom rlang .data :=
#' @importFrom stringr str_replace_all str_trim
#' @importFrom tibble as_tibble tibble
## usethis namespace: end
NULL

# The column names the rule expressions in `report_transforms.R` refer to.
#
# They are bare symbols inside quosures, resolved against the data mask by
# dplyr or translated to SQL identifiers by dbplyr -- never looked up as R
# objects. `R CMD check` cannot tell the difference, so it reports each one as
# an undefined global; declaring them here says "these are columns" rather than
# silencing a real problem.
#
# The alternative, writing every reference as `.data$primary_dea`, would make
# the rules markedly harder to read for no gain: `.data` pronouns are not
# needed inside a quosure that is only ever spliced into a dplyr verb.
utils::globalVariables(c(
  "credentialed",
  "current_status",
  "month",
  "primary_dea",
  "primary_fac_flag",
  "primary_license",
  "primary_record",
  "status_category"
))

# Import-time drift checks, the R counterpart of the module-level `assert`
# statements in the Python package. A registry that disagrees with itself
# should stop the package from loading, not surface as a wrong column six
# weeks later in a workbook.
.onLoad <- function(libname, pkgname) {
  validate_facility_registry()
  validate_report_layout()

  invisible(NULL)
}
