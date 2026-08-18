# The two data registries and their drift checks.

test_that("the facility registry is internally consistent", {
  expect_true(validate_facility_registry())
})

test_that("every code has a name and every name has a code", {
  expect_setequal(FACILITY_CODES, names(FACILITY_NAMES))
})

test_that("the registry holds the 19 reportable facilities", {
  expect_length(FACILITY_CODES, 19L)
  expect_false(anyDuplicated(FACILITY_CODES) > 0L)
})

test_that("no code is both reportable and excluded", {
  expect_length(intersect(FACILITY_CODES, EXCLUDED_FACCODES), 0L)
})

test_that("validate_facility_registry catches a name without a code", {
  expect_error(
    validate_facility_registry(
      codes = c("ANT", "FRE"),
      names_ = c(ANT = "Antioch")
    ),
    class = "theUtilsR_registry_drift"
  )
})

test_that("validate_facility_registry catches a code without a name", {
  expect_error(
    validate_facility_registry(
      codes = "ANT",
      names_ = c(ANT = "Antioch", FRE = "Fremont")
    ),
    class = "theUtilsR_registry_drift"
  )
})

test_that("validate_facility_registry catches a duplicated code", {
  expect_error(
    validate_facility_registry(
      codes = c("ANT", "ANT"),
      names_ = c(ANT = "Antioch")
    ),
    class = "theUtilsR_registry_drift"
  )
})

test_that("validate_facility_registry catches an overlap with the excluded set", {
  expect_error(
    validate_facility_registry(
      codes = c("ANT", "REG"),
      names_ = c(ANT = "Antioch", REG = "Regional"),
      excluded = "REG"
    ),
    class = "theUtilsR_registry_drift"
  )
})

test_that("the registry bindings cannot be reassigned", {
  # R's namespace sealing is what the Python package gets from tuple and
  # MappingProxyType.
  expect_error(
    assign("FACILITY_CODES", "nope", envir = asNamespace("theUtilsR")),
    "locked"
  )
})


# ---------------------------------------------------------------------------
# report_layout
# ---------------------------------------------------------------------------
test_that("the report layout is internally consistent", {
  expect_true(validate_report_layout())
})

test_that("every identity column has a header", {
  expect_setequal(TELEMED_IDENTITY_COLUMNS, names(TELEMED_LABELS))
})

test_that("validate_report_layout catches an unlabelled column", {
  expect_error(
    validate_report_layout(
      identity = c("pract_id", "last_name"),
      labels = c(pract_id = "Pract ID")
    ),
    class = "theUtilsR_layout_drift"
  )
})

test_that("validate_report_layout catches an orphan label", {
  expect_error(
    validate_report_layout(
      identity = "pract_id",
      labels = c(pract_id = "Pract ID", gone = "Gone")
    ),
    class = "theUtilsR_layout_drift"
  )
})

test_that("ordered_columns is identity columns then facilities", {
  columns <- ordered_columns()

  expect_equal(columns[seq_along(TELEMED_IDENTITY_COLUMNS)], TELEMED_IDENTITY_COLUMNS)
  expect_equal(tail(columns, length(FACILITY_CODES)), FACILITY_CODES)
})

test_that("the report is 28 columns wide", {
  # The SAS `proc report` column list was 9 identity + 19 facility. A drifted
  # registry would change the workbook layout silently.
  expect_length(ordered_columns(), 28L)
  expect_length(
    ordered_columns(),
    length(TELEMED_IDENTITY_COLUMNS) + length(FACILITY_CODES)
  )
})

test_that("headers covers every reported column", {
  expect_setequal(names(headers()), ordered_columns())
})

test_that("a facility's header is its own code", {
  # Generated rather than listed, so a code added to the registry cannot be
  # forgotten here.
  for (code in FACILITY_CODES) {
    expect_equal(unname(headers()[[code]]), code)
  }
})

test_that("select_report_columns projects into report order", {
  rows <- tibble::tibble(!!!stats::setNames(
    as.list(rep("x", length(ordered_columns()))),
    rev(ordered_columns())
  ))

  expect_equal(colnames(select_report_columns(rows)), ordered_columns())
})
