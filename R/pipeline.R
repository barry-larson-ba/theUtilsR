# pipeline -------------------------------------------------------------------
#
# Running a report expressed as an ordered list of named stages.
#
# This is deliberately *not* an orchestrating `build()` for any particular
# report -- it knows nothing about telemedicine, facilities or credentialing.
# It is the applier; the stage list is the composition, and that still lives in
# a notebook where it can be read and edited without a package release.
#
# What the list buys over a plain `|>` chain, and the reason this exists:
#
#   * the report's shape is visible as data -- print it, count it, subset it
#   * a failing stage is named in the error rather than buried in a dplyr
#     backtrace forty frames down
#   * `through =` runs a prefix of the report, which is what you actually want
#     at 7am when Monday's numbers look wrong
#   * the row count after each stage is recorded, so "which stage dropped
#     them?" has an answer instead of a bisect

.trace_env <- new.env(parent = emptyenv())
.trace_env$last <- NULL

#' Run a report expressed as an ordered list of named stages
#'
#' Each stage is a function of one argument that takes a frame and returns a
#' frame. Stages run in list order, each on the previous one's output.
#'
#' @param data The starting frame -- a data frame or a lazy table.
#' @param stages A **named** list of functions, `frame -> frame`.
#' @param count Whether to record the row count after each stage. `NULL`, the
#'   default, counts local frames and skips lazy ones -- see the section below.
#'   `TRUE` forces counting, `FALSE` disables it.
#' @param through Optional stage name. Runs the report up to and including that
#'   stage and stops, which is how you inspect a report mid-flight.
#' @param quiet Suppress the per-stage progress messages.
#'
#' @return The final frame, carrying the trace as an attribute. Read it with
#'   [stage_trace()] -- which also works with no argument, the form to use once
#'   the result has been through `collect()`.
#'
#' @section Counting a lazy table costs a query:
#'
#' There is nothing to count on a lazy table without executing it, and doing so
#' after every stage re-runs the whole lineage each time -- against Oracle over
#' JDBC that is one full round trip per stage, not one extra row scan. So the
#' default counts local frames and leaves lazy ones alone.
#'
#' If you want the trace on a remote report, materialise the read once first
#' and the counts become cheap:
#'
#' \preformatted{
#'   base <- dplyr::compute(read_source(BASE_DATA_HIST, where = "month = 0"))
#'   report <- run_stages(base, stages, count = TRUE)
#' }
#'
#' @section When a stage fails:
#'
#' The error is re-raised naming the stage and its position, with the original
#' condition as the parent. Without that, a mistake in stage nine surfaces as a
#' dplyr error with no indication of which of eleven near-identical joins
#' produced it.
#'
#' @seealso [stage_trace()]
#' @export
#' @examples
#' stages <- list(
#'   positive = function(d) dplyr::filter(d, x > 0),
#'   doubled  = function(d) dplyr::mutate(d, x = x * 2)
#' )
#'
#' result <- run_stages(tibble::tibble(x = c(-1, 1, 2)), stages, quiet = TRUE)
#' stage_trace(result)
run_stages <- function(data, stages, count = NULL, through = NULL,
                       quiet = FALSE) {
  .check_stages(stages)

  if (!is.null(through)) {
    position <- match(through, names(stages))

    if (is.na(position)) {
      rlang::abort(
        c(
          paste0("No stage named ", encodeString(through, quote = '"'), "."),
          i = paste0("stages: ", paste(names(stages), collapse = ", "))
        ),
        class = "theUtilsR_unknown_stage"
      )
    }

    stages <- stages[seq_len(position)]
  }

  counting <- if (is.null(count)) !inherits(data, "tbl_lazy") else isTRUE(count)

  rows <- c(`<input>` = .row_count(data, counting))

  for (i in seq_along(stages)) {
    name <- names(stages)[[i]]

    data <- .run_one_stage(stages[[i]], data, name, i)

    rows[[name]] <- .row_count(data, counting)

    if (!quiet) {
      message(sprintf(
        "  %2d. %-16s %s", i, name,
        if (is.na(rows[[name]])) "" else paste0(format(rows[[name]], big.mark = ","), " rows")
      ))
    }
  }

  trace <- tibble::tibble(
    stage = names(rows),
    rows = unname(rows),
    delta = c(NA_real_, diff(unname(rows)))
  )

  .trace_env$last <- trace
  attr(data, "theUtilsR_trace") <- trace

  data
}

.check_stages <- function(stages) {
  if (!is.list(stages) || !length(stages)) {
    rlang::abort(
      "`stages` must be a non-empty list of functions.",
      class = "theUtilsR_bad_stages"
    )
  }

  names_ <- names(stages)

  if (is.null(names_) || any(!nzchar(names_))) {
    rlang::abort(
      c(
        "Every stage must be named.",
        i = "The names are what the trace and the `through` argument refer to."
      ),
      class = "theUtilsR_bad_stages"
    )
  }

  if (anyDuplicated(names_)) {
    rlang::abort(
      paste0(
        "Duplicate stage name(s): ",
        paste(unique(names_[duplicated(names_)]), collapse = ", "), "."
      ),
      class = "theUtilsR_bad_stages"
    )
  }

  not_functions <- names_[!vapply(stages, is.function, logical(1))]

  if (length(not_functions)) {
    rlang::abort(
      paste0(
        "Every stage must be a function; these are not: ",
        paste(not_functions, collapse = ", "), "."
      ),
      class = "theUtilsR_bad_stages"
    )
  }

  invisible(TRUE)
}

.run_one_stage <- function(stage, data, name, position) {
  out <- rlang::try_fetch(
    stage(data),
    error = function(cnd) {
      rlang::abort(
        paste0("Stage ", position, " (", encodeString(name, quote = '"'), ") failed."),
        class = "theUtilsR_stage_failed",
        parent = cnd
      )
    }
  )

  if (!is.data.frame(out) && !inherits(out, "tbl_lazy")) {
    rlang::abort(
      c(
        paste0(
          "Stage ", position, " (", encodeString(name, quote = '"'),
          ") returned a ", paste(class(out), collapse = "/"),
          ", not a frame."
        ),
        i = "Every stage must take a frame and return a frame."
      ),
      class = "theUtilsR_bad_stage_result"
    )
  }

  out
}

.row_count <- function(data, counting) {
  if (!counting) {
    return(NA_real_)
  }

  if (inherits(data, "tbl_lazy")) {
    return(as.numeric(dplyr::pull(dplyr::collect(dplyr::count(data)), 1L)))
  }

  as.numeric(nrow(data))
}

#' The row counts recorded by the last `run_stages()`
#'
#' @param x A frame returned by [run_stages()]. Omit it to read the trace from
#'   the most recent run in this session.
#'
#' @section Where the attribute goes:
#'
#' dplyr verbs preserve it, on local frames and lazy tables alike. `collect()`
#' does not -- and that is the one operation every remote report ends with, so
#' calling `stage_trace()` with no argument is the reliable form after a
#' collected run rather than a fallback for unusual cases.
#'
#' @return A tibble of `stage`, `rows` and `delta`. `rows` is `NA` when counting
#'   was skipped; see [run_stages()].
#' @seealso [run_stages()]
#' @export
stage_trace <- function(x = NULL) {
  if (!is.null(x)) {
    trace <- attr(x, "theUtilsR_trace", exact = TRUE)

    if (!is.null(trace)) {
      return(trace)
    }
  }

  if (is.null(.trace_env$last)) {
    rlang::abort(
      c(
        "No stage trace recorded in this session.",
        i = "Call run_stages() first."
      ),
      class = "theUtilsR_no_trace"
    )
  }

  .trace_env$last
}
