# facility_codes -------------------------------------------------------------
#
# The facility code registry. No dataframe library is touched here, and
# nothing in this file knows what a report looks like.

#' Reportable facility codes, in report order
#'
#' The order is significant -- it drives report column layout -- so do not sort
#' it on the assumption that it is arbitrary.
#'
#' The binding is locked when the package namespace is sealed, which is R's
#' equivalent of the `tuple` and `MappingProxyType` the Python original reaches
#' for: a caller cannot reassign it in place.
#'
#' @format A character vector of 19 three-letter codes.
#' @seealso [FACILITY_NAMES], [EXCLUDED_FACCODES]
#' @export
#' @examples
#' length(FACILITY_CODES)
#' head(FACILITY_CODES, 3)
FACILITY_CODES <- c(
  "ANT", "FRE", "FRS", "MAN", "OAK", "ROS", "RWC", "SAC", "SCL",
  "SFO", "SJO", "SLN", "SRF", "SRO", "SSC", "SSF", "VAC", "VAL", "WCR"
)

#' Facility code to human-readable name
#'
#' A named character vector, which is R's natural form for what the Python
#' package holds in a read-only mapping.
#'
#' @format A named character vector; names are the codes in [FACILITY_CODES].
#' @seealso [FACILITY_CODES]
#' @export
#' @examples
#' FACILITY_NAMES[["OAK"]]
#' unname(FACILITY_NAMES[c("SFO", "SJO")])
FACILITY_NAMES <- c(
  ANT = "Antioch",
  FRE = "Fremont",
  FRS = "Fresno",
  MAN = "Manteca",
  OAK = "Oakland",
  ROS = "Roseville",
  RWC = "Redwood City",
  SAC = "Sacramento",
  SCL = "Santa Clara",
  SFO = "San Francisco",
  SJO = "San Jose",
  SLN = "San Leandro",
  SRF = "San Rafael",
  SRO = "Santa Rosa",
  SSC = "South Sacramento",
  SSF = "South San Francisco",
  VAC = "Vacaville",
  VAL = "Vallejo",
  WCR = "Walnut Creek"
)

#' Administrative facility codes that are never reportable
#'
#' These are real `faccode` values in the source data but are regional and
#' support entities, not hospitals. Reports drop rows carrying them before
#' pivoting, so they never become columns.
#'
#' Unlike [FACILITY_CODES] nothing here is order-sensitive; membership testing
#' is the only thing it is ever used for.
#'
#' @format A character vector.
#' @seealso [exclude_faccodes()]
#' @export
EXCLUDED_FACCODES <- c("REG", "RCH", "STK")

#' Check the facility registry for internal drift
#'
#' Called from `.onLoad()`, so a registry that disagrees with itself stops the
#' package from loading rather than producing a wrong report later. Exported so
#' the check itself can be tested, and so a caller who edits the registry
#' interactively can re-run it.
#'
#' @param codes Character vector of reportable codes.
#' @param names_ Named character vector of code -> facility name.
#' @param excluded Character vector of administrative codes.
#'
#' @return `TRUE`, invisibly, if every check passes.
#' @export
#' @examples
#' validate_facility_registry()
validate_facility_registry <- function(codes = FACILITY_CODES,
                                       names_ = FACILITY_NAMES,
                                       excluded = EXCLUDED_FACCODES) {
  only_codes <- setdiff(codes, names(names_))
  only_names <- setdiff(names(names_), codes)

  if (length(only_codes) || length(only_names)) {
    rlang::abort(
      c(
        "FACILITY_CODES and FACILITY_NAMES disagree.",
        i = paste0("only in codes: ", .or_none(only_codes)),
        i = paste0("only in names: ", .or_none(only_names))
      ),
      class = "theUtilsR_registry_drift"
    )
  }

  if (anyDuplicated(codes)) {
    rlang::abort(
      paste0("FACILITY_CODES contains duplicates: ",
             .or_none(unique(codes[duplicated(codes)]))),
      class = "theUtilsR_registry_drift"
    )
  }

  # A code cannot be both reportable and excluded. If one ever ends up in both,
  # the crosstab would emit a column that the exclusion filter has emptied.
  overlap <- intersect(codes, excluded)

  if (length(overlap)) {
    rlang::abort(
      paste0("EXCLUDED_FACCODES overlaps FACILITY_CODES: ", .or_none(sort(overlap))),
      class = "theUtilsR_registry_drift"
    )
  }

  invisible(TRUE)
}

.or_none <- function(x) {
  if (!length(x)) "<none>" else paste(x, collapse = ", ")
}
