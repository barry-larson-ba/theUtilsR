# sources: logical names, the SELECT builder, and the swappable resolver.

# Every test that installs a resolver has to put the old one back, or the rest
# of the suite inherits it.
local_resolver <- function(reader, env = parent.frame()) {
  previous <- use_resolver(reader)
  withr::defer(use_resolver(previous), envir = env)

  invisible(previous)
}


# ---------------------------------------------------------------------------
# build_select
# ---------------------------------------------------------------------------
test_that("build_select projects, filters, and defaults to star", {
  expect_equal(
    build_select("rpt.msow_base_data_hist"),
    "SELECT * FROM rpt.msow_base_data_hist"
  )
  expect_equal(
    build_select("t", c("a", "b")),
    "SELECT a, b FROM t"
  )
  expect_equal(
    build_select("rpt.msow_base_data_hist", "pract_id", "month = 0"),
    "SELECT pract_id FROM rpt.msow_base_data_hist WHERE month = 0"
  )
})

test_that("build_select emits no trailing semicolon", {
  # read_oracle wraps the statement as a subquery, which a semicolon breaks.
  expect_false(grepl(";", build_select("t", where = "a = 1"), fixed = TRUE))
})

test_that("an empty where is the same as none", {
  expect_equal(build_select("t", where = ""), build_select("t"))
})


# ---------------------------------------------------------------------------
# Logical names
# ---------------------------------------------------------------------------
test_that("TABLE_NAMES holds both logical names", {
  expect_setequal(TABLE_NAMES, c(BASE_DATA_HIST, PRACTITIONER_FACILITIES))
})

test_that("every logical name has an Oracle mapping", {
  expect_setequal(names(ORACLE_TABLES), TABLE_NAMES)
  expect_false(anyNA(ORACLE_TABLES))
})

test_that("the catalog mapping ships unmapped", {
  # Fill in the three-level names and catalog_reader() starts working.
  expect_setequal(names(CATALOG_TABLES), TABLE_NAMES)
  expect_true(all(is.na(CATALOG_TABLES)))
})

test_that("read_source rejects an unknown name before opening a connection", {
  expect_error(read_source("nope"), class = "theUtilsR_unknown_table")
})

test_that("oracle_reader rejects an unknown name", {
  expect_error(oracle_reader("nope"), class = "theUtilsR_unknown_table")
})

test_that("an unmapped table names the vector to fill in", {
  reader <- catalog_reader(structure(list(), class = "DBIConnection"))

  expect_error(reader(BASE_DATA_HIST), class = "theUtilsR_unmapped_table")
  expect_error(reader(BASE_DATA_HIST), "CATALOG_TABLES")
})


# ---------------------------------------------------------------------------
# The resolver
# ---------------------------------------------------------------------------
test_that("the default resolver is oracle_reader", {
  expect_identical(current_resolver(), oracle_reader)
})

test_that("use_resolver installs a resolver and returns the previous one", {
  reader <- function(name, columns = NULL, where = NULL, return_type = "source") {
    "sentinel"
  }

  previous <- local_resolver(reader)

  expect_identical(current_resolver(), reader)
  expect_identical(previous, oracle_reader)
  expect_equal(read_source(BASE_DATA_HIST), "sentinel")
})

test_that("read_source passes every argument through to the resolver", {
  seen <- NULL
  reader <- function(name, columns = NULL, where = NULL, return_type = "source") {
    seen <<- list(name = name, columns = columns, where = where, return_type = return_type)
    invisible(NULL)
  }

  local_resolver(reader)

  read_source(BASE_DATA_HIST, columns = c("a", "b"), where = "x = 1", return_type = "tibble")

  expect_equal(seen$name, BASE_DATA_HIST)
  expect_equal(seen$columns, c("a", "b"))
  expect_equal(seen$where, "x = 1")
  expect_equal(seen$return_type, "tibble")
})


# ---------------------------------------------------------------------------
# frame_reader -- the substitute a test wants
# ---------------------------------------------------------------------------
test_that("frame_reader resolves logical names to local frames", {
  frames <- list(
    base_data_hist = tibble::tibble(pract_id = c(1, 2), month = c(0, 1))
  )

  local_resolver(frame_reader(frames))

  expect_equal(nrow(read_source(BASE_DATA_HIST)), 2L)
})

test_that("frame_reader applies columns and where", {
  frames <- list(
    base_data_hist = tibble::tibble(pract_id = c(1, 2), month = c(0, 1))
  )

  local_resolver(frame_reader(frames))

  result <- read_source(BASE_DATA_HIST, columns = "pract_id", where = "month == 0")

  expect_equal(colnames(result), "pract_id")
  expect_equal(result$pract_id, 1)
})

test_that("frame_reader says which frames it holds", {
  local_resolver(frame_reader(list(base_data_hist = tibble::tibble(a = 1))))

  expect_error(
    read_source(PRACTITIONER_FACILITIES),
    class = "theUtilsR_unmapped_table"
  )
})

test_that("frame_reader still rejects a name that is not logical at all", {
  reader <- frame_reader(list(base_data_hist = tibble::tibble(a = 1)))

  expect_error(reader("nope"), class = "theUtilsR_unknown_table")
})

test_that("frame_reader honours return_type", {
  local_resolver(frame_reader(list(base_data_hist = tibble::tibble(a = 1))))

  expect_s3_class(read_source(BASE_DATA_HIST, return_type = "tibble"), "tbl_df")
  expect_error(
    read_source(BASE_DATA_HIST, return_type = "polars"),
    class = "theUtilsR_bad_return_type"
  )
})


# ---------------------------------------------------------------------------
# catalog_reader
# ---------------------------------------------------------------------------
test_that("catalog_reader reads a mapped table through its own connection", {
  skip_without_remote()

  con <- .duckdb_con()
  DBI::dbWriteTable(con, "catalog_base", data.frame(pract_id = c(1, 2), month = c(0, 1)),
                    overwrite = TRUE)

  local_resolver(
    catalog_reader(con, tables = c(base_data_hist = "catalog_base"))
  )

  result <- read_source(BASE_DATA_HIST, columns = "pract_id", where = "month == 0",
                        return_type = "tibble")

  expect_equal(result$pract_id, 1)
})

test_that("catalog_reader leaves the result lazy by default", {
  skip_without_remote()

  con <- .duckdb_con()
  DBI::dbWriteTable(con, "catalog_lazy", data.frame(a = 1), overwrite = TRUE)

  reader <- catalog_reader(con, tables = c(base_data_hist = "catalog_lazy"))

  expect_s3_class(reader(BASE_DATA_HIST), "tbl_lazy")
})
