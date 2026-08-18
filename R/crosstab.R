# crosstab -------------------------------------------------------------------
#
# The facility crosstab: one row per practitioner, one column per facility,
# holding that practitioner's status at it.
#
# This is the `proc transpose` step of the credentialing reports, plus the two
# things PROC TRANSPOSE left to the report definition:
#
#   * every code in FACILITY_CODES becomes a column, in registry order, whether
#     or not any row mentioned it. A pivot emits only the values it actually
#     saw, so a facility with no qualifying practitioners this week would
#     otherwise silently drop a column -- and PROC REPORT would then fail on
#     the missing variable rather than print an empty one.
#   * gaps are filled with STATUS_INACTIVE, not NA. Blank is a meaningful
#     status here (see STATUS_LABELS), and NA would render as "NA" in most
#     writers.
#
# exclude_faccodes() lives here rather than in report_transforms because it
# exists solely to keep the administrative codes out of those columns; the two
# are always called together.

#' The column the crosstab pivots into column headings
#'
#' The column whose value lands in each cell is `STAT_COLUMN`, which the
#' classifier writes and the crosstab reads -- so there is one name for it.
#'
#' @format A length-one character vector.
#' @export
FACCODE_COLUMN <- "faccode"


# ===========================================================================
# Excluded facilities
# ===========================================================================

#' Drop rows carrying an administrative (non-reportable) faccode
#'
#' The `is.na()` arm is not decoration. `!(faccode %in% excluded)` evaluates to
#' `NA` for a missing faccode, and both `dplyr::filter()` and a SQL `WHERE`
#' drop those rows -- whereas the pandas original keeps them, because
#' `~Series.isin()` returns `TRUE` for `NaN`. Naming the case explicitly keeps
#' the R port answering what the Python one answers.
#'
#' @param data A data frame or lazy table.
#' @param excluded Character vector of administrative codes.
#'
#' @return `exclude_faccodes_expr()`: a quosure, for `filter()`.
#'   `exclude_faccodes()`: `data`, filtered.
#' @export
#' @examples
#' rows <- tibble::tibble(faccode = c("OAK", "REG", NA))
#' exclude_faccodes(rows)
exclude_faccodes_expr <- function(excluded = EXCLUDED_FACCODES) {
  codes <- sort(as.character(excluded))

  rlang::quo(
    is.na(!!rlang::sym(FACCODE_COLUMN)) |
      !(!!rlang::sym(FACCODE_COLUMN) %in% !!codes)
  )
}

#' @rdname exclude_faccodes_expr
#' @export
exclude_faccodes <- function(data, excluded = EXCLUDED_FACCODES) {
  dplyr::filter(data, !!exclude_faccodes_expr(excluded))
}


# ===========================================================================
# The crosstab
# ===========================================================================

#' Pivot faccode into columns, one row per index combination
#'
#' @section Why this function has two bodies:
#'
#' Pivoting is the second place -- after ordering within a group, see
#' [first_per_group()] -- where the local and remote backends genuinely differ.
#' `tidyr::pivot_wider()` has no lazy-table method, and a lazy table cannot be
#' reshaped without knowing the target columns anyway.
#'
#' So the remote branch writes the classic SQL pivot idiom by hand, one
#' conditional aggregate per facility:
#'
#' \preformatted{
#'   MAX(CASE WHEN faccode = 'OAK' THEN stat END) AS OAK
#' }
#'
#' which is the same shape the Polars implementation in the Python package
#' uses, and for the same reason. `MAX` ignores nulls, so with at most one row
#' per (index, faccode) it returns that row's value or null; the `coalesce()`
#' that follows turns null into `fill`. The local branch uses `pivot_wider()`,
#' which is clearer and errors loudly on the duplicate-key case.
#'
#' Both branches emit the full `facility_codes` set in registry order, and both
#' are covered by the same tests.
#'
#' @param data A frame carrying `index`, `faccode` and `stat`. Must hold at
#'   most one row per (index, faccode) pair -- run [dedupe_first()] first.
#' @param index Character vector of the practitioner-level columns that
#'   identify a row.
#' @param facility_codes Character vector of columns to emit, in order.
#' @param fill The value for a practitioner/facility pair with no row.
#'
#' @return A frame of `index` columns followed by one column per facility code.
#'
#' @section On duplicate keys:
#'
#' Locally, a repeated (index, faccode) pair raises an error naming the
#' offending keys -- the de-duplication step was skipped. Remotely, `MAX()`
#' picks the largest value rather than failing, which is what the Spark
#' implementation in the Python package does with `first()`. Run
#' [dedupe_first()] and the question does not arise.
#'
#' @export
#' @examples
#' rows <- tibble::tibble(
#'   pract_id = c(1, 1, 2),
#'   faccode  = c("OAK", "SFO", "OAK"),
#'   stat     = c("T", "P", "P")
#' )
#' facility_crosstab(rows, index = "pract_id", facility_codes = c("OAK", "SFO", "SAC"))
facility_crosstab <- function(data,
                              index,
                              facility_codes = FACILITY_CODES,
                              fill = STATUS_INACTIVE) {
  .check_columns(data, c(index, FACCODE_COLUMN, STAT_COLUMN), "facility_crosstab()")

  codes <- as.character(facility_codes)

  if (inherits(data, "tbl_lazy")) {
    .facility_crosstab_lazy(data, index, codes, fill)
  } else {
    .facility_crosstab_local(data, index, codes, fill)
  }
}

.facility_crosstab_lazy <- function(data, index, codes, fill) {
  aggregates <- stats::setNames(
    lapply(codes, function(code) {
      rlang::quo(max(
        dplyr::if_else(
          !!rlang::sym(FACCODE_COLUMN) == !!code,
          !!rlang::sym(STAT_COLUMN),
          NA_character_
        ),
        na.rm = TRUE
      ))
    }),
    codes
  )

  out <- dplyr::group_by(data, dplyr::across(dplyr::all_of(index)))
  out <- dplyr::summarise(out, !!!aggregates, .groups = "drop")
  out <- dplyr::mutate(out, !!!.fill_exprs(codes, fill))

  .arrange_by(out, index)
}

# One coalesce() per code, with the fill value unquoted into the expression.
# Passing a closure to across() instead would leave `fill` as a bare symbol for
# dbplyr to resolve, and it would go looking for a *column* by that name.
.fill_exprs <- function(codes, fill) {
  stats::setNames(
    lapply(codes, function(code) {
      rlang::quo(dplyr::coalesce(!!rlang::sym(code), !!fill))
    }),
    codes
  )
}

.facility_crosstab_local <- function(data, index, codes, fill) {
  keys <- c(index, FACCODE_COLUMN)
  duplicates <- data[duplicated(data[keys]), keys, drop = FALSE]

  if (nrow(duplicates)) {
    rlang::abort(
      c(
        paste0(
          "facility_crosstab(): ", nrow(duplicates),
          " duplicate (index, faccode) row(s). Each practitioner/facility ",
          "pair must appear at most once."
        ),
        i = "Run dedupe_first(data, by = c(index, \"faccode\")) first.",
        i = paste0(
          "first duplicate: ",
          paste(
            paste0(keys, "=", unlist(lapply(duplicates[1, ], as.character))),
            collapse = ", "
          )
        )
      ),
      class = "theUtilsR_duplicate_crosstab_keys"
    )
  }

  wide <- tidyr::pivot_wider(
    data,
    id_cols = dplyr::all_of(index),
    names_from = dplyr::all_of(FACCODE_COLUMN),
    values_from = dplyr::all_of(STAT_COLUMN)
  )

  # Facilities nobody qualified at this week are absent from the pivot; add
  # them so the column set is the registry's, not this week's data's.
  absent <- setdiff(codes, colnames(wide))

  if (length(absent)) {
    wide[absent] <- NA_character_
  }

  wide <- dplyr::select(wide, dplyr::all_of(c(index, codes)))
  wide <- dplyr::mutate(wide, !!!.fill_exprs(codes, fill))

  .arrange_by(wide, index)
}
