# The connection layer. Everything here runs against DuckDB standing in for a
# DBI-connected Oracle; the sparklyr branches are exercised only for their
# argument validation, since a Spark connection is not available in a test run.

withr::defer(reset_configuration(), teardown_env())

local_clean_config <- function(env = parent.frame()) {
  withr::defer(reset_configuration(), envir = env)
  reset_configuration()
}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
test_that("is_configured is FALSE before anything is configured", {
  local_clean_config()

  expect_false(is_configured())
})

test_that("a DBI connection is configuration enough on its own", {
  # The connection carries the account, so the three JDBC settings are not
  # required -- that is the difference from the Spark path, and the reason
  # configure() takes a connection rather than a SparkSession.
  skip_without_remote()
  local_clean_config()

  configure(con = .duckdb_con())

  expect_true(is_configured())
})

test_that("configure keeps a setting it is not given", {
  local_clean_config()

  configure(jdbc_url = "jdbc:oracle:thin:@//host:1521/svc", username = "u")
  configure(password = "p")

  expect_equal(get_oracle_options()$url, "jdbc:oracle:thin:@//host:1521/svc")
  expect_equal(get_oracle_options()$user, "u")
  expect_equal(get_oracle_options()$password, "p")
})

test_that("the JDBC options name the Oracle driver", {
  local_clean_config()

  configure(jdbc_url = "url", username = "u", password = "p")

  expect_equal(get_oracle_options()$driver, "oracle.jdbc.OracleDriver")
})

test_that("get_oracle_options names the settings that are missing", {
  local_clean_config()

  configure(jdbc_url = "url")

  expect_error(get_oracle_options(), class = "theUtilsR_not_configured")
  expect_error(get_oracle_options(), "username")
})

test_that("the_connection explains itself when nothing is configured", {
  local_clean_config()

  expect_error(the_connection(), class = "theUtilsR_not_configured")
})

test_that("reset_configuration forgets everything", {
  local_clean_config()

  configure(jdbc_url = "url", username = "u", password = "p")
  reset_configuration()

  expect_error(get_oracle_options(), class = "theUtilsR_not_configured")
})

test_that("an unsupported connection class is named in the error", {
  expect_error(
    read_oracle_raw("SELECT 1", con = structure(list(), class = "not_a_connection")),
    class = "theUtilsR_unsupported_connection"
  )
})


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------
test_that("read_oracle_raw returns a lazy table over a DBI connection", {
  skip_without_remote()

  result <- read_oracle_raw("SELECT 1 AS n", con = .duckdb_con())

  expect_s3_class(result, "tbl_lazy")
  expect_equal(dplyr::collect(result)$n, 1)
})

test_that("further verbs compose onto the query rather than collecting it", {
  skip_without_remote()

  result <- read_oracle_raw("SELECT 1 AS n UNION ALL SELECT 2", con = .duckdb_con())
  result <- dplyr::filter(result, n > 1)

  expect_s3_class(result, "tbl_lazy")
  expect_equal(dplyr::collect(result)$n, 2)
})

test_that("read_oracle validates return_type before issuing the query", {
  # A typo is a programming error, and finding it after a multi-minute Oracle
  # round trip helps nobody. No connection is configured here, so reaching the
  # query at all would raise a different error.
  local_clean_config()

  expect_error(
    read_oracle("SELECT 1", con = NULL, return_type = "polars"),
    class = "theUtilsR_bad_return_type"
  )
})

test_that("read_oracle collects into each requested form", {
  skip_without_remote()

  query <- "SELECT 1 AS n"
  con <- .duckdb_con()

  expect_s3_class(read_oracle(query, con = con), "tbl_lazy")
  expect_s3_class(read_oracle(query, con = con, return_type = "tibble"), "tbl_df")

  plain <- read_oracle(query, con = con, return_type = "data.frame")
  expect_s3_class(plain, "data.frame")
  expect_false(inherits(plain, "tbl_df"))

  skip_if_not_installed("data.table")
  expect_s3_class(
    read_oracle(query, con = con, return_type = "data.table"),
    "data.table"
  )
})

test_that("return_type is case-insensitive", {
  skip_without_remote()

  expect_s3_class(
    read_oracle("SELECT 1 AS n", con = .duckdb_con(), return_type = "TIBBLE"),
    "tbl_df"
  )
})


# ---------------------------------------------------------------------------
# apply_schema
# ---------------------------------------------------------------------------
test_that("apply_schema casts local columns", {
  rows <- tibble::tibble(pract_id = "101", month = "0", name = 5)

  result <- apply_schema(
    rows,
    c(pract_id = "integer", month = "integer", name = "character")
  )

  expect_type(result$pract_id, "integer")
  expect_equal(result$pract_id, 101L)
  expect_type(result$name, "character")
})

test_that("apply_schema casts remote columns", {
  skip_without_remote()

  remote <- as_remote(tibble::tibble(pract_id = "101", flag = 1))
  result <- dplyr::collect(apply_schema(remote, c(pract_id = "bigint", flag = "logical")))

  expect_equal(as.numeric(result$pract_id), 101)
  expect_true(result$flag)
})

test_that("apply_schema leaves unnamed columns alone", {
  rows <- tibble::tibble(a = "1", b = "2")

  result <- apply_schema(rows, c(a = "integer"))

  expect_type(result$b, "character")
})

test_that("apply_schema names the missing column and the available ones", {
  # Letting the engine hit it instead produces an UNRESOLVED_COLUMN buried in a
  # Java stack trace, which hides the one useful line.
  expect_error(
    apply_schema(tibble::tibble(a = 1), c(nope = "integer")),
    class = "theUtilsR_missing_columns"
  )
  expect_error(apply_schema(tibble::tibble(a = 1), c(nope = "integer")), "nope")
})

test_that("apply_schema rejects a type it does not know", {
  expect_error(
    apply_schema(tibble::tibble(a = 1), c(a = "decimal")),
    class = "theUtilsR_unknown_type"
  )
})

test_that("an empty or absent schema is a no-op", {
  rows <- tibble::tibble(a = "1")

  expect_equal(apply_schema(rows, NULL), rows)
  expect_equal(apply_schema(rows, character(0)), rows)
})


# ---------------------------------------------------------------------------
# convert_integer_decimals
# ---------------------------------------------------------------------------
test_that("convert_integer_decimals is a no-op off the Spark path", {
  # There is no Decimal intermediary on a DBI connection, so there is nothing
  # to recast. read_oracle() still calls it unconditionally so the pipeline
  # reads the same either way.
  rows <- tibble::tibble(a = 1)

  expect_equal(convert_integer_decimals(rows), rows)

  skip_without_remote()

  remote <- as_remote(rows)
  expect_equal(
    dplyr::collect(convert_integer_decimals(remote)),
    dplyr::collect(remote)
  )
})


# ---------------------------------------------------------------------------
# Collection helpers
# ---------------------------------------------------------------------------
test_that("to_tibble collects and can recast", {
  skip_without_remote()

  remote <- as_remote(tibble::tibble(n = "7"))

  expect_s3_class(to_tibble(remote), "tbl_df")
  expect_type(to_tibble(remote, c(n = "integer"))$n, "integer")
})

test_that("to_data_frame returns a plain data frame", {
  result <- to_data_frame(tibble::tibble(n = 1))

  expect_s3_class(result, "data.frame")
  expect_false(inherits(result, "tbl_df"))
})

test_that("to_data_table returns a data.table", {
  skip_if_not_installed("data.table")

  expect_s3_class(to_data_table(tibble::tibble(n = 1)), "data.table")
})


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
test_that("the return types are the four documented forms", {
  expect_equal(RETURN_TYPES, c("source", "tibble", "data.frame", "data.table"))
})

test_that("the default decimal scale is 0 only", {
  # Scale 10 -- Oracle's bare NUMBER -- is deliberately excluded: it covers
  # both integer IDs and genuinely fractional values, and the cast truncates.
  expect_equal(DEFAULT_DECIMAL_SCALES, 0L)
})
