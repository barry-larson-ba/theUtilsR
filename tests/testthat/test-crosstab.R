# The crosstab is the one function with two bodies -- pivot_wider() locally, a
# hand-written MAX(CASE WHEN ...) remotely -- so running every case against
# both backends is what stops those two drifting apart.

CODES <- c("OAK", "SFO", "SAC")

test_that("facility_crosstab emits one column per code, in registry order", {
  rows <- tibble::tibble(
    pract_id = c(1, 1, 2),
    faccode  = c("OAK", "SFO", "OAK"),
    stat     = c("T", "P", "P")
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(
      facility_crosstab(make(rows), index = "pract_id", facility_codes = CODES)
    )

    expect_equal(colnames(result), c("pract_id", CODES), info = backend)
    expect_equal(nrow(result), 2L, info = backend)
  }
})

test_that("a facility nobody qualified at is still a column", {
  # A pivot emits only the values it saw. SAC appears in no row here, and must
  # still be a column -- PROC REPORT failed on the missing variable rather than
  # printing an empty one.
  rows <- tibble::tibble(pract_id = 1, faccode = "OAK", stat = "T")

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(
      facility_crosstab(make(rows), index = "pract_id", facility_codes = CODES)
    )

    expect_true("SAC" %in% colnames(result), info = backend)
    expect_equal(result$SAC, STATUS_INACTIVE, info = backend)
  }
})

test_that("gaps are filled with blank, not NA", {
  # Blank is a meaningful status here, and NA would render as the string "NA"
  # in most writers.
  rows <- tibble::tibble(pract_id = c(1, 2), faccode = c("OAK", "SFO"), stat = c("T", "P"))

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(
      facility_crosstab(make(rows), index = "pract_id", facility_codes = CODES),
      sort_by = "pract_id"
    )

    expect_false(anyNA(result), info = backend)
    expect_equal(result$SFO, c(STATUS_INACTIVE, "P"), info = backend)
  }
})

test_that("the fill value is configurable", {
  rows <- tibble::tibble(pract_id = 1, faccode = "OAK", stat = "T")

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(
      facility_crosstab(make(rows), index = "pract_id", facility_codes = CODES, fill = "-")
    )

    expect_equal(result$SAC, "-", info = backend)
  }
})

test_that("a multi-column index gives one row per combination", {
  rows <- tibble::tibble(
    pract_id  = c(1, 1, 2),
    last_name = c("Alvarez", "Alvarez", "Chen"),
    faccode   = c("OAK", "SFO", "OAK"),
    stat      = c("T", "P", "P")
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(
      facility_crosstab(
        make(rows),
        index = c("pract_id", "last_name"),
        facility_codes = CODES
      ),
      sort_by = "pract_id"
    )

    expect_equal(nrow(result), 2L, info = backend)
    expect_equal(result$last_name, c("Alvarez", "Chen"), info = backend)
  }
})

test_that("the default facility_codes is the whole registry", {
  rows <- tibble::tibble(pract_id = 1, faccode = "OAK", stat = "T")

  result <- facility_crosstab(rows, index = "pract_id")

  expect_equal(colnames(result), c("pract_id", FACILITY_CODES))
})

test_that("facility_crosstab names the columns it cannot find", {
  expect_error(
    facility_crosstab(tibble::tibble(pract_id = 1), index = "pract_id"),
    class = "theUtilsR_missing_columns"
  )
})

test_that("a duplicate (index, faccode) pair is an error locally", {
  # It means dedupe_first() was skipped. The remote branch cannot raise this
  # without a round trip, so MAX() picks one -- which is what the Spark
  # implementation in the Python package does with first().
  rows <- tibble::tibble(
    pract_id = c(1, 1),
    faccode  = c("OAK", "OAK"),
    stat     = c("T", "P")
  )

  expect_error(
    facility_crosstab(rows, index = "pract_id", facility_codes = CODES),
    class = "theUtilsR_duplicate_crosstab_keys"
  )
})


# ---------------------------------------------------------------------------
# Excluded faccodes
# ---------------------------------------------------------------------------
test_that("exclude_faccodes drops the administrative codes", {
  rows <- tibble::tibble(faccode = c("OAK", "REG", "RCH", "STK", "SFO"))

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(exclude_faccodes(make(rows)), sort_by = "faccode")

    expect_equal(result$faccode, c("OAK", "SFO"), info = backend)
  }
})

test_that("a missing faccode is kept, matching the pandas original", {
  # `!(faccode %in% excluded)` is NA for a missing faccode and both filter()
  # and a SQL WHERE would drop the row; ~Series.isin() returns TRUE for NaN and
  # pandas keeps it. The rule names the case rather than inheriting whichever
  # answer the engine happens to give.
  rows <- tibble::tibble(faccode = c("OAK", NA, "REG"))

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(exclude_faccodes(make(rows)))

    expect_equal(nrow(result), 2L, info = backend)
    expect_true(anyNA(result$faccode), info = backend)
  }
})

test_that("the excluded set is configurable", {
  rows <- tibble::tibble(faccode = c("OAK", "SFO"))

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(exclude_faccodes(make(rows), excluded = "OAK"))

    expect_equal(result$faccode, "SFO", info = backend)
  }
})

test_that("exclude_faccodes_expr splices into a caller's filter", {
  rows <- tibble::tibble(faccode = c("OAK", "REG"))

  expect_equal(nrow(dplyr::filter(rows, !!exclude_faccodes_expr())), 1L)
})
