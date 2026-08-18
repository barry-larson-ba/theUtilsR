# Shared test scaffolding.
#
# The Python package runs one table of cases against three dataframe libraries
# so its three implementations of each rule cannot drift apart. There is only
# one implementation here, so the axis that matters is different: does a rule
# give the same answer when dplyr evaluates it in R as when dbplyr turns it
# into SQL and a database evaluates it?
#
# So every rule test runs twice -- once on a tibble, once on the same rows
# loaded into DuckDB as a lazy table -- and the result is collected before
# comparing. DuckDB stands in for Spark and Oracle: it is the SQL engine that
# happens to be installable as an R package, and it exercises the translation
# layer, which is where a portability bug actually lives.
#
# DuckDB is a Suggests dependency, so a run without it exercises the local
# paths and reports the rest as skips. That is the intended behaviour, not a
# degraded run.

# One connection for the whole suite; opening a DuckDB database per test would
# dominate the runtime otherwise.
#
# Closed by a finalizer on session exit rather than by withr::defer() into
# teardown_env(): helpers are also sourced by pkgload::load_all(), where
# testthat's teardown environment does not exist yet.
.duckdb_state <- new.env(parent = emptyenv())
.duckdb_state$con <- NULL
.duckdb_state$tried <- FALSE

.duckdb_con <- function() {
  if (.duckdb_state$tried) {
    return(.duckdb_state$con)
  }

  .duckdb_state$tried <- TRUE

  if (!requireNamespace("duckdb", quietly = TRUE)) {
    return(NULL)
  }

  .duckdb_state$con <- tryCatch(
    DBI::dbConnect(duckdb::duckdb()),
    error = function(e) NULL
  )

  if (!is.null(.duckdb_state$con)) {
    reg.finalizer(
      .duckdb_state,
      function(e) {
        if (!is.null(e$con)) {
          try(DBI::dbDisconnect(e$con, shutdown = TRUE), silent = TRUE)
        }
      },
      onexit = TRUE
    )
  }

  .duckdb_state$con
}

.table_counter <- new.env(parent = emptyenv())
.table_counter$n <- 0L

#' Load a data frame into DuckDB and hand back a lazy table.
#'
#' Returns NULL when DuckDB is unavailable, which `backends()` turns into a
#' skip.
as_remote <- function(data) {
  con <- .duckdb_con()

  if (is.null(con)) {
    return(NULL)
  }

  .table_counter$n <- .table_counter$n + 1L
  name <- paste0("t", .table_counter$n)

  DBI::dbWriteTable(con, name, as.data.frame(data), overwrite = TRUE)

  dplyr::tbl(con, name)
}

#' The frame constructors to run a rule against.
#'
#' Each is `data.frame -> frame in that backend's form`. Use as:
#'
#'     for (backend in names(backends())) {
#'       make <- backends()[[backend]]
#'       ...
#'     }
backends <- function() {
  out <- list(local = tibble::as_tibble)

  if (!is.null(.duckdb_con())) {
    out$duckdb <- as_remote
  }

  out
}

#' Skip the rest of a test when DuckDB is not installed.
skip_without_remote <- function() {
  testthat::skip_if(
    is.null(.duckdb_con()),
    "duckdb is not available, so the SQL translation is untested here"
  )
}

#' Collect a possibly-lazy result and normalise it for comparison.
#'
#' The two engines disagree about scalar types -- DuckDB hands back BIGINT
#' where R had an integer, and doubles where R had a count -- in ways that say
#' nothing about whether the rule is correct. Every numeric column becomes a
#' double and everything else becomes character, which is exactly the
#' normalisation the Python suite does across pandas, Polars and Spark.
#'
#' Pass `sort_by` for results whose order is not itself under test; only an
#' `arrange()`d result is ordered on a remote backend, and the tests that care
#' about order assert it without sorting first.
normalise <- function(data, sort_by = NULL) {
  out <- as.data.frame(dplyr::collect(data))

  # Drop anything a stage attached along the way -- run_stages() leaves its
  # trace on the result, and a local frame carries it through to here, where it
  # would fail a comparison against a plain expected frame.
  attributes(out) <- attributes(out)[c("names", "row.names", "class")]

  for (column in names(out)) {
    value <- out[[column]]

    out[[column]] <- if (is.logical(value)) {
      value
    } else if (is.numeric(value)) {
      as.numeric(value)
    } else {
      as.character(value)
    }
  }

  if (!is.null(sort_by)) {
    out <- out[do.call(order, unname(as.list(out[sort_by]))), , drop = FALSE]
    rownames(out) <- NULL
  }

  out
}
