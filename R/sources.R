# sources --------------------------------------------------------------------
#
# Where the credentialing tables come from.
#
# Pipeline stages take frames; nothing in them should know a schema name, a
# connection, or which system is authoritative this quarter. This file is the
# one place that does. Stages and notebooks ask for a *logical* name:
#
#     df <- read_source(BASE_DATA_HIST, where = "month = 0")
#
# Today that resolves to a query against whatever connection is configured.
# When the tables land in Unity Catalog, point the resolver at them once and
# every caller follows:
#
#     use_resolver(catalog_reader(sc))
#
# A resolver is just a function with the signature
#
#     function(name, columns = NULL, where = NULL, return_type = "source")
#
# so a test can substitute one returning a fixture frame, with no Oracle and no
# Spark involved.

#' Logical table names
#'
#' Use these constants rather than the bare strings, so a typo is an object-not-
#' found error at parse time rather than a runtime lookup failure.
#'
#' @format Length-one character vectors; `TABLE_NAMES` holds all of them.
#' @export
BASE_DATA_HIST <- "base_data_hist"

#' @rdname BASE_DATA_HIST
#' @export
PRACTITIONER_FACILITIES <- "practitioner_facilities"

#' @rdname BASE_DATA_HIST
#' @export
TABLE_NAMES <- c(BASE_DATA_HIST, PRACTITIONER_FACILITIES)

#' Logical name to Oracle object
#'
#' The SAS libnames `rpt` and `msow` correspond to these two schemas.
#'
#' @format A named character vector.
#' @export
ORACLE_TABLES <- c(
  base_data_hist = "rpt.msow_base_data_hist",
  practitioner_facilities = "msow.practitioner_facilities"
)

#' Logical name to catalog object
#'
#' For when these move to Unity Catalog. Fill in the three-level names and
#' [catalog_reader()] starts working; until then it raises with a message
#' naming this vector.
#'
#' @format A named character vector; both entries currently `NA`.
#' @export
CATALOG_TABLES <- c(
  base_data_hist = NA_character_,
  practitioner_facilities = NA_character_
)

.check_logical_name <- function(name) {
  if (!name %in% TABLE_NAMES) {
    rlang::abort(
      paste0(
        "Unknown logical table ", encodeString(name, quote = '"'),
        "; known names are: ", paste(TABLE_NAMES, collapse = ", "), "."
      ),
      class = "theUtilsR_unknown_table"
    )
  }

  invisible(TRUE)
}

.resolve <- function(name, tables, kind) {
  .check_logical_name(name)

  physical <- if (name %in% names(tables)) tables[[name]] else NA_character_

  if (is.na(physical) || !nzchar(physical)) {
    rlang::abort(
      c(
        paste0(
          encodeString(name, quote = '"'), " has no ", kind, " mapping yet."
        ),
        i = paste0(
          "Set ", toupper(kind), "_TABLES[[",
          encodeString(name, quote = '"'),
          "]] to the physical table name before reading through this resolver."
        )
      ),
      class = "theUtilsR_unmapped_table"
    )
  }

  physical
}

#' Render a SELECT for one table
#'
#' Kept separate from the readers so it can be tested, and read, without a
#' connection. There is deliberately no trailing semicolon: the Spark path
#' wraps the statement as a subquery, which a semicolon would break.
#'
#' @param table The physical table name.
#' @param columns Character vector of columns to project. Omit for `*`.
#' @param where A predicate, without the `WHERE` keyword.
#'
#' @return The statement, as a length-one character vector.
#' @export
#' @examples
#' build_select("rpt.msow_base_data_hist", c("pract_id"), "month = 0")
build_select <- function(table, columns = NULL, where = NULL) {
  projection <- if (length(columns)) paste(columns, collapse = ", ") else "*"
  statement <- paste0("SELECT ", projection, " FROM ", table)

  if (!is.null(where) && nzchar(where)) {
    statement <- paste0(statement, " WHERE ", where)
  }

  statement
}

#' Read a logical table from Oracle
#'
#' Projection and predicate are pushed into the SQL, so Oracle does the work
#' and only the result crosses the wire -- the same thing the SAS `proc sql`
#' steps relied on.
#'
#' Requires [configure()] to have been called.
#'
#' @param name A logical name from [TABLE_NAMES].
#' @param columns Character vector of columns to project.
#' @param where Oracle predicate, without the `WHERE` keyword.
#' @param return_type One of [RETURN_TYPES].
#'
#' @return A table in the requested form.
#' @export
oracle_reader <- function(name, columns = NULL, where = NULL,
                          return_type = "source") {
  table <- .resolve(name, ORACLE_TABLES, "oracle")
  query <- build_select(table, columns = columns, where = where)

  read_oracle(query, return_type = return_type)
}

#' Build a resolver that reads from catalog tables instead of Oracle
#'
#' A factory rather than a plain reader, because the catalog path needs its own
#' connection handle and there is no reason to put a second one into package
#' state -- [configure()] already owns the one used for the JDBC reads.
#'
#' @param con A `spark_connection` or `DBIConnection` to read tables through.
#' @param tables Named character vector of logical -> physical. Defaults to
#'   [CATALOG_TABLES], which ships unmapped.
#'
#' @return A function suitable for [use_resolver()].
#' @export
#' @examples
#' \dontrun{
#' use_resolver(catalog_reader(sc))
#' }
catalog_reader <- function(con, tables = CATALOG_TABLES) {
  force(con)
  force(tables)

  function(name, columns = NULL, where = NULL, return_type = "source") {
    table <- .resolve(name, tables, "catalog")
    data <- dplyr::tbl(con, table)

    if (!is.null(where) && nzchar(where)) {
      data <- dplyr::filter(data, !!rlang::parse_expr(where))
    }

    if (length(columns)) {
      data <- dplyr::select(data, dplyr::all_of(columns))
    }

    switch(
      tolower(return_type),
      source = data,
      tibble = to_tibble(data),
      "data.frame" = to_data_frame(data),
      "data.table" = to_data_table(data),
      rlang::abort(
        paste0(
          "`return_type` must be one of: ",
          paste(RETURN_TYPES, collapse = ", "), "."
        ),
        class = "theUtilsR_bad_return_type"
      )
    )
  }
}

#' Build a resolver that reads from local frames
#'
#' The substitute a test wants: hand it a named list of frames and every
#' [read_source()] call in the pipeline under test resolves against them, with
#' no connection of any kind.
#'
#' @param frames A named list of data frames or lazy tables, keyed by logical
#'   name.
#'
#' @return A function suitable for [use_resolver()].
#' @export
#' @examples
#' previous <- use_resolver(frame_reader(list(base_data_hist = tibble::tibble(x = 1))))
#' read_source("base_data_hist")
#' use_resolver(previous)
frame_reader <- function(frames) {
  force(frames)

  function(name, columns = NULL, where = NULL, return_type = "source") {
    .check_logical_name(name)

    if (!name %in% names(frames)) {
      rlang::abort(
        paste0(
          "frame_reader() has no frame for ", encodeString(name, quote = '"'),
          "; it holds: ", paste(names(frames), collapse = ", "), "."
        ),
        class = "theUtilsR_unmapped_table"
      )
    }

    data <- frames[[name]]

    if (!is.null(where) && nzchar(where)) {
      data <- dplyr::filter(data, !!rlang::parse_expr(where))
    }

    if (length(columns)) {
      data <- dplyr::select(data, dplyr::all_of(columns))
    }

    switch(
      tolower(return_type),
      source = data,
      tibble = to_tibble(data),
      "data.frame" = to_data_frame(data),
      "data.table" = to_data_table(data),
      rlang::abort(
        paste0(
          "`return_type` must be one of: ",
          paste(RETURN_TYPES, collapse = ", "), "."
        ),
        class = "theUtilsR_bad_return_type"
      )
    )
  }
}

# The resolver read_source() currently dispatches to. Environment state, like
# the connection settings, and for the same reason: it is one session-wide
# fact, not a per-call argument.
.settings$resolver <- NULL

#' Install a resolver for `read_source()` to dispatch to
#'
#' Returns the previous resolver, so a caller that needs to swap one in
#' temporarily -- a test, usually -- can put the old one back.
#'
#' @param reader A function
#'   `(name, columns, where, return_type) -> table`.
#'
#' @return The resolver that was in effect, invisibly.
#' @seealso [current_resolver()], [frame_reader()], [catalog_reader()]
#' @export
use_resolver <- function(reader) {
  previous <- current_resolver()
  .settings$resolver <- reader

  invisible(previous)
}

#' The resolver `read_source()` is currently dispatching to
#'
#' @return A function. Defaults to [oracle_reader()].
#' @export
current_resolver <- function() {
  if (is.null(.settings$resolver)) oracle_reader else .settings$resolver
}

#' Read a logical table through the active resolver
#'
#' The entry point for pipeline code. See [oracle_reader()] for the argument
#' meanings; they are the resolver contract, not this function's own.
#'
#' Named `read_source()` rather than `read()`, which the Python package can
#' afford because its modules are namespaces and R's are not.
#'
#' @param name A logical name from [TABLE_NAMES].
#' @param columns Character vector of columns to project.
#' @param where A predicate, without the `WHERE` keyword.
#' @param return_type One of [RETURN_TYPES].
#'
#' @return A table in the requested form.
#'
#' @section Validation comes first:
#'
#' `name` is checked here as well as in the resolver, so a typo fails before
#' any connection is opened.
#'
#' @export
read_source <- function(name, columns = NULL, where = NULL,
                        return_type = "source") {
  .check_logical_name(name)

  current_resolver()(
    name,
    columns = columns,
    where = where,
    return_type = return_type
  )
}
