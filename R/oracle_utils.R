# oracle_utils ---------------------------------------------------------------
#
# Reading Oracle into Spark or into a DBI connection.
#
# The Python original deliberately leaves four names undefined at module level
# -- `spark`, `jdbc_url`, `username`, `password` -- because Python resolves a
# function's globals in the module where the function was defined, so a
# notebook-level assignment would not be seen. R has no such trap: a package
# function's enclosing environment is the package namespace, whose bindings are
# locked when the namespace seals. So the settings live in a dedicated
# environment instead, and configure() writes into it.
#
# The practical convention is unchanged and is the point of the exercise:
# credentials live in the notebook, not in source control, and they are
# *environment* state, so everything sharing an R session shares one
# connection identity.

# The one mutable thing in the package. Kept in an environment rather than in
# options() so that a stray options() call elsewhere cannot clear a session's
# credentials, and so the values never appear in a saved workspace.
.settings <- new.env(parent = emptyenv())

.SETTING_NAMES <- c("con", "jdbc_url", "username", "password")

#' Accepted values for `read_oracle(return_type = )`
#'
#' `"source"` leaves the result where it is -- a `tbl_spark` or a lazy `tbl`
#' over the DBI connection -- so further dplyr verbs still run on the server.
#' The other three collect into driver memory, so filter or aggregate first.
#'
#' @format A character vector.
#' @export
RETURN_TYPES <- c("source", "tibble", "data.frame", "data.table")

#' Decimal scales that `convert_integer_decimals()` recasts by default
#'
#' Only scale 0 -- a decimal that declares no fractional digits at all, so
#' converting it cannot lose anything. Oracle's bare `NUMBER` arrives as
#' `DecimalType(38, 10)` instead and is deliberately excluded; see
#' [convert_integer_decimals()] for why widening this is a per-column decision.
#'
#' @format An integer vector.
#' @export
DEFAULT_DECIMAL_SCALES <- 0L


# ===========================================================================
# Configuration
# ===========================================================================

#' Set the connection and credentials for this session
#'
#' Arguments left `NULL` are not assigned, so an existing value is kept rather
#' than cleared -- call it repeatedly to update settings piecemeal.
#'
#' @section Which settings you need:
#'
#' It depends on what `con` is, and that is the whole reason this port takes a
#' connection object where the Python original took a `SparkSession`:
#'
#' \describe{
#'   \item{a **sparklyr** connection}{`jdbc_url`, `username` and `password` are
#'     also required. Spark opens the Oracle connection itself, per query, from
#'     the JDBC options this package assembles.}
#'   \item{a **DBI** connection}{the connection *is* the credential. Whatever
#'     you passed to `DBI::dbConnect()` already carries the account, so the
#'     three JDBC settings are ignored and need not be supplied.}
#' }
#'
#' @param con A `spark_connection` (from `sparklyr::spark_connect()`) or a
#'   `DBIConnection` (from `DBI::dbConnect()`).
#' @param jdbc_url Oracle JDBC connection string. Spark path only.
#' @param username Oracle account name. Spark path only.
#' @param password Oracle account password. Spark path only.
#'
#' @return `NULL`, invisibly.
#' @seealso [is_configured()], [the_connection()], [reset_configuration()]
#' @export
#' @examples
#' \dontrun{
#' # DBI: the connection carries the credentials
#' configure(con = DBI::dbConnect(odbc::odbc(), dsn = "ORCL"))
#'
#' # sparklyr: Spark needs the JDBC settings to hand to the driver
#' configure(
#'   con = sc,
#'   jdbc_url = "jdbc:oracle:thin:@//host:1521/service",
#'   username = "reporting",
#'   password = Sys.getenv("ORACLE_PASSWORD")
#' )
#' }
configure <- function(con = NULL,
                      jdbc_url = NULL,
                      username = NULL,
                      password = NULL) {
  supplied <- list(
    con = con,
    jdbc_url = jdbc_url,
    username = username,
    password = password
  )

  for (name in .SETTING_NAMES) {
    if (!is.null(supplied[[name]])) {
      assign(name, supplied[[name]], envir = .settings)
    }
  }

  invisible(NULL)
}

#' Report whether the connection settings are complete
#'
#' Useful as a guard before a long-running read, since the error otherwise
#' surfaces only once a query is actually issued.
#'
#' What counts as complete depends on the connection: a DBI connection needs
#' nothing further, a Spark connection needs the three JDBC settings as well.
#'
#' @return `TRUE` or `FALSE`.
#' @seealso [configure()]
#' @export
is_configured <- function() {
  if (!exists("con", envir = .settings, inherits = FALSE)) {
    return(FALSE)
  }

  con <- get("con", envir = .settings, inherits = FALSE)

  if (!.is_spark(con)) {
    return(TRUE)
  }

  all(vapply(
    c("jdbc_url", "username", "password"),
    function(name) exists(name, envir = .settings, inherits = FALSE),
    logical(1)
  ))
}

#' The configured connection
#'
#' @return The connection passed to [configure()].
#' @seealso [configure()]
#' @export
the_connection <- function() {
  if (!exists("con", envir = .settings, inherits = FALSE)) {
    rlang::abort(
      c(
        "No connection has been configured.",
        i = "Call configure(con = ...) with a sparklyr or DBI connection first."
      ),
      class = "theUtilsR_not_configured"
    )
  }

  get("con", envir = .settings, inherits = FALSE)
}

#' Forget every configured setting
#'
#' Mostly for tests, and for a notebook that needs to point at a different
#' account without restarting the session.
#'
#' @return `NULL`, invisibly.
#' @export
reset_configuration <- function() {
  rm(
    list = intersect(.SETTING_NAMES, ls(envir = .settings, all.names = TRUE)),
    envir = .settings
  )

  invisible(NULL)
}

#' Build the JDBC option list for the Oracle connection
#'
#' Reads the configured `jdbc_url`, `username` and `password` each time it is
#' called, so updating them between calls takes effect immediately.
#'
#' Only the Spark path uses this; a DBI connection carries its own credentials.
#'
#' @return A named list of JDBC options.
#' @export
get_oracle_options <- function() {
  missing <- setdiff(
    c("jdbc_url", "username", "password"),
    ls(envir = .settings, all.names = TRUE)
  )

  if (length(missing)) {
    rlang::abort(
      c(
        paste0(
          "The Spark path needs the JDBC settings, and ", length(missing),
          " are unset: ", paste(missing, collapse = ", "), "."
        ),
        i = "Call configure(jdbc_url = , username = , password = )."
      ),
      class = "theUtilsR_not_configured"
    )
  }

  list(
    url = get("jdbc_url", envir = .settings, inherits = FALSE),
    user = get("username", envir = .settings, inherits = FALSE),
    password = get("password", envir = .settings, inherits = FALSE),
    driver = "oracle.jdbc.OracleDriver"
  )
}

.is_spark <- function(con) {
  inherits(con, "spark_connection")
}

.is_dbi <- function(con) {
  inherits(con, "DBIConnection")
}

# Each spark_read_jdbc() registers a named temporary view, so successive reads
# in one session need distinct names.
.read_counter <- new.env(parent = emptyenv())
.read_counter$n <- 0L

.next_view_name <- function() {
  .read_counter$n <- .read_counter$n + 1L

  paste0("theutilsr_jdbc_", .read_counter$n)
}


# ===========================================================================
# Reading
# ===========================================================================

#' Run a query on Oracle and return the result exactly as the driver types it
#'
#' Dispatches on the connection:
#'
#' \describe{
#'   \item{**sparklyr**}{the query is wrapped as `(query) tmp` and passed
#'     through the JDBC `dbtable` option, so any valid SELECT works without
#'     naming a table and the whole statement executes on the Oracle side.}
#'   \item{**DBI**}{the query becomes a lazy `tbl()`, which dbplyr composes
#'     further verbs onto as a subquery.}
#' }
#'
#' Either way the statement becomes a subquery, so it must not end in a
#' semicolon, and row limits use Oracle syntax (`FETCH FIRST n ROWS ONLY` or
#' `ROWNUM`) rather than `LIMIT`.
#'
#' No type normalisation is applied; see [convert_integer_decimals()].
#'
#' @param query An Oracle SELECT statement, without a trailing `;`.
#' @param con A connection. Defaults to the configured one.
#'
#' @return A lazy table.
#' @export
read_oracle_raw <- function(query, con = the_connection()) {
  if (.is_spark(con)) {
    rlang::check_installed("sparklyr", "to read Oracle through Spark.")

    options <- get_oracle_options()
    options[["dbtable"]] <- paste0("(", query, ") tmp")

    return(sparklyr::spark_read_jdbc(
      con,
      name = .next_view_name(),
      options = options,
      memory = FALSE,
      overwrite = TRUE
    ))
  }

  if (.is_dbi(con)) {
    return(dplyr::tbl(con, dbplyr::sql(query)))
  }

  rlang::abort(
    c(
      paste0(
        "Cannot read through a connection of class ",
        paste(class(con), collapse = "/"), "."
      ),
      i = "Expected a sparklyr spark_connection or a DBI DBIConnection."
    ),
    class = "theUtilsR_unsupported_connection"
  )
}

# Canonical type name -> SQL type. Only the character case differs between
# engines, so the map is small: Spark spells it STRING, everyone else VARCHAR.
.SQL_TYPES <- c(
  character = "VARCHAR",
  integer   = "INT",
  bigint    = "BIGINT",
  double    = "DOUBLE",
  logical   = "BOOLEAN",
  date      = "DATE",
  timestamp = "TIMESTAMP"
)

.LOCAL_CASTS <- list(
  character = as.character,
  integer   = as.integer,
  bigint    = as.numeric,
  double    = as.numeric,
  logical   = as.logical,
  date      = as.Date,
  timestamp = as.POSIXct
)

.sql_type <- function(type, con) {
  sql <- .SQL_TYPES[[type]]

  if (identical(type, "character") && .is_spark(con)) "STRING" else sql
}

#' Cast columns to the types named in a schema
#'
#' This casts existing columns rather than replacing the frame's schema: every
#' name in `schema` must already be present as a column, and columns not named
#' keep whatever type they had.
#'
#' `schema` is a named character vector of column name to canonical type, one
#' of `character`, `integer`, `bigint`, `double`, `logical`, `date`,
#' `timestamp`. Canonical rather than backend-native so the same schema
#' definition survives a move from Oracle to Spark -- the only spelling that
#' actually differs is the string type, which Spark calls `STRING` and everyone
#' else calls `VARCHAR`.
#'
#' @param data A data frame or lazy table.
#' @param schema Named character vector of column -> canonical type.
#' @param con The connection, used to pick the SQL spelling. Only consulted for
#'   lazy tables.
#'
#' @return `data`, with the casts applied.
#'
#' @section Why it checks first:
#'
#' A missing column is caught up front so the message names what is missing and
#' what is available. Letting the engine hit it instead produces an
#' `UNRESOLVED_COLUMN` buried in a Java stack trace, which hides the one useful
#' line.
#'
#' @export
#' @examples
#' apply_schema(
#'   tibble::tibble(pract_id = "101", month = "0"),
#'   c(pract_id = "integer", month = "integer")
#' )
apply_schema <- function(data, schema, con = NULL) {
  if (is.null(schema) || !length(schema)) {
    return(data)
  }

  .check_columns(data, names(schema), "apply_schema()")

  unknown <- setdiff(unique(unname(schema)), names(.SQL_TYPES))

  if (length(unknown)) {
    rlang::abort(
      c(
        paste0("apply_schema(): unknown type(s): ",
               paste(unknown, collapse = ", "), "."),
        i = paste0("known types: ", paste(names(.SQL_TYPES), collapse = ", "))
      ),
      class = "theUtilsR_unknown_type"
    )
  }

  if (inherits(data, "tbl_lazy")) {
    if (is.null(con)) {
      con <- dbplyr::remote_con(data)
    }

    casts <- stats::setNames(
      lapply(names(schema), function(column) {
        dplyr::sql(paste0(
          "CAST(", dbplyr::escape(dbplyr::ident(column), con = con),
          " AS ", .sql_type(schema[[column]], con), ")"
        ))
      }),
      names(schema)
    )

    return(dplyr::mutate(data, !!!casts))
  }

  for (column in names(schema)) {
    data[[column]] <- .LOCAL_CASTS[[schema[[column]]]](data[[column]])
  }

  data
}

#' Recast integer-valued decimal columns to a 64-bit integer type
#'
#' Oracle numeric columns arrive over JDBC as decimals, which is faithful but
#' awkward downstream: arithmetic carries 38-digit decimals, joins against
#' genuine integer columns need casts, and collecting to R yields character or
#' list columns rather than numbers.
#'
#' Which decimal you get depends on how the Oracle column was declared, and the
#' distinction matters:
#'
#' \preformatted{
#'   NUMBER(10, 2)   -> DecimalType(10, 2)    a real fixed-point value
#'   NUMBER(38)      -> DecimalType(38, 0)    scale explicitly 0
#'   NUMBER          -> DecimalType(38, 10)   scale not declared at all
#' }
#'
#' That last row is the one that surprises people. A bare `NUMBER` does not
#' report a scale, and Spark's Oracle dialect represents it as
#' `DecimalType(38, 10)` -- so an ID column declared as plain `NUMBER` shows up
#' with ten decimal places it never uses, and is *not* touched by the default
#' `scales = 0`.
#'
#' Pass `scales = c(0, 10)` to catch those as well. That is opt-in rather than
#' the default because scale 10 is ambiguous: it covers both integer IDs and
#' genuinely fractional values, and the cast truncates toward zero, silently.
#' Only widen the scales for columns you know hold whole numbers.
#'
#' @section This is a Spark-path concern only:
#'
#' On a DBI connection there is no Decimal intermediary -- the driver maps
#' Oracle `NUMBER` to an R numeric on the way out -- so there is nothing to
#' recast and this function returns `data` unchanged. It is still called
#' unconditionally by [read_oracle()] so the pipeline reads the same either
#' way.
#'
#' @param data A lazy table.
#' @param scales Integer vector of decimal scales to convert.
#'
#' @return `data`, with qualifying columns recast; unchanged if none qualify.
#' @export
convert_integer_decimals <- function(data, scales = DEFAULT_DECIMAL_SCALES) {
  if (!inherits(data, "tbl_spark")) {
    return(data)
  }

  rlang::check_installed("sparklyr", "to inspect a Spark schema.")

  schema <- sparklyr::sdf_schema(data)
  con <- dbplyr::remote_con(data)

  decimal_scale <- function(type) {
    matched <- regmatches(type, regexec("^DecimalType\\((\\d+),\\s*(\\d+)\\)$", type))[[1]]

    if (length(matched) != 3L) NA_integer_ else as.integer(matched[[3]])
  }

  targets <- Filter(
    function(field) {
      scale <- decimal_scale(field[["type"]])

      !is.na(scale) && scale %in% scales
    },
    schema
  )

  if (!length(targets)) {
    return(data)
  }

  casts <- stats::setNames(
    lapply(names(targets), function(column) {
      dplyr::sql(paste0(
        "CAST(", dbplyr::escape(dbplyr::ident(column), con = con), " AS BIGINT)"
      ))
    }),
    names(targets)
  )

  dplyr::mutate(data, !!!casts)
}


# ===========================================================================
# Collection
# ===========================================================================

#' Collect a table into local memory
#'
#' The entire result is pulled into the driver, so filter or aggregate first.
#'
#' @param data A data frame or lazy table.
#' @param types Optional named character vector of column -> canonical type,
#'   applied by [apply_schema()] after collection.
#'
#' @return A tibble, a base data frame, or a `data.table`.
#' @export
to_tibble <- function(data, types = NULL) {
  apply_schema(tibble::as_tibble(dplyr::collect(data)), types)
}

#' @rdname to_tibble
#' @export
to_data_frame <- function(data, types = NULL) {
  apply_schema(as.data.frame(dplyr::collect(data)), types)
}

#' @rdname to_tibble
#' @export
to_data_table <- function(data, types = NULL) {
  rlang::check_installed("data.table", "to return a data.table.")

  data.table::as.data.table(apply_schema(tibble::as_tibble(dplyr::collect(data)), types))
}


# ===========================================================================
# Reusable schemas
# ===========================================================================

#' Reusable schema definitions, keyed by table nickname
#'
#' A placeholder for schemas worth defining once and sharing across notebooks.
#' Not consulted by [read_oracle()] -- callers pass the value through
#' themselves, e.g. `read_oracle(query, schema = SCHEMAS$member)`.
#'
#' @format A named list; both entries currently `NULL`.
#' @export
SCHEMAS <- list(
  member = NULL,
  provider = NULL
)


# ===========================================================================
# The entry point
# ===========================================================================

#' Read an Oracle query
#'
#' The intended entry point. Stages run in this order:
#'
#' \enumerate{
#'   \item [read_oracle_raw()] -- execute the query
#'   \item [convert_integer_decimals()] -- scale-0 decimals to `BIGINT`
#'     (a no-op off the Spark path)
#'   \item [apply_schema()] -- explicit per-column casts
#'   \item collection -- `source`, `tibble`, `data.frame` or `data.table`
#' }
#'
#' Step 2 precedes step 3 so that a caller-supplied `schema` always wins over
#' the automatic decimal handling. Preserve that ordering when modifying this.
#'
#' @param query An Oracle SELECT statement, without a trailing `;`. See
#'   [read_oracle_raw()] for the syntax constraints.
#' @param con A connection. Defaults to the configured one.
#' @param schema Optional named character vector of per-column casts, applied
#'   as described in [apply_schema()].
#' @param auto_fix_decimals Whether to run [convert_integer_decimals()].
#' @param decimal_scales Passed to [convert_integer_decimals()]; ignored unless
#'   `auto_fix_decimals` is `TRUE`.
#' @param return_type One of [RETURN_TYPES]. `"source"` leaves the result on
#'   the server; the others collect to driver memory.
#' @param types Optional named character vector applied after collection;
#'   ignored when `return_type` is `"source"`.
#'
#' @return A lazy table, tibble, data frame or `data.table`.
#'
#' @section Validation comes first:
#'
#' `return_type` is checked before the query is issued, so a typo raises
#' immediately rather than after a multi-minute Oracle round trip. Keep new
#' argument validation up there with it.
#'
#' @export
#' @examples
#' \dontrun{
#' df <- read_oracle("SELECT * FROM claims.members")
#'
#' ids <- read_oracle(
#'   "SELECT member_id FROM claims.members",
#'   return_type = "tibble",
#'   types = c(member_id = "bigint")
#' )
#' }
read_oracle <- function(query,
                        con = the_connection(),
                        schema = NULL,
                        auto_fix_decimals = TRUE,
                        decimal_scales = DEFAULT_DECIMAL_SCALES,
                        return_type = "source",
                        types = NULL) {
  # Validated before the query runs: a typo here is a programming error, and
  # finding it after a long Oracle round trip helps nobody.
  requested <- tolower(return_type)

  if (!requested %in% RETURN_TYPES) {
    rlang::abort(
      paste0(
        "`return_type` must be one of: ", paste(RETURN_TYPES, collapse = ", "),
        "; got ", encodeString(return_type, quote = '"'), "."
      ),
      class = "theUtilsR_bad_return_type"
    )
  }

  data <- read_oracle_raw(query, con = con)

  if (isTRUE(auto_fix_decimals)) {
    data <- convert_integer_decimals(data, scales = decimal_scales)
  }

  if (!is.null(schema)) {
    data <- apply_schema(data, schema, con = con)
  }

  switch(
    requested,
    source = data,
    tibble = to_tibble(data, types),
    "data.frame" = to_data_frame(data, types),
    "data.table" = to_data_table(data, types)
  )
}
