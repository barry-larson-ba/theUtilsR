# run_stages(): the applier. The stage lists it runs are report-specific and
# live in notebooks; what is tested here is the machinery.

DOUBLE_STAGES <- list(
  positive = function(d) dplyr::filter(d, x > 0),
  doubled  = function(d) dplyr::mutate(d, x = x * 2)
)

test_that("stages run in order, each on the previous output", {
  result <- run_stages(tibble::tibble(x = c(-2, 1, 2)), DOUBLE_STAGES, quiet = TRUE)

  expect_equal(result$x, c(2, 4))
})

test_that("it runs the same stages on a remote table", {
  skip_without_remote()

  result <- run_stages(
    as_remote(tibble::tibble(x = c(-2, 1, 2))),
    DOUBLE_STAGES,
    quiet = TRUE
  )

  expect_s3_class(result, "tbl_lazy")
  expect_equal(sort(dplyr::collect(result)$x), c(2, 4))
})

test_that("the trace records a row count per stage, plus the input", {
  result <- run_stages(tibble::tibble(x = c(-2, 1, 2)), DOUBLE_STAGES, quiet = TRUE)
  trace <- stage_trace(result)

  expect_equal(trace$stage, c("<input>", "positive", "doubled"))
  expect_equal(trace$rows, c(3, 2, 2))
  expect_equal(trace$delta, c(NA, -1, 0))
})

test_that("the trace survives further dplyr verbs", {
  result <- run_stages(tibble::tibble(x = c(-2, 1, 2)), DOUBLE_STAGES, quiet = TRUE)

  expect_equal(stage_trace(dplyr::mutate(result, y = 1))$rows, c(3, 2, 2))
  expect_equal(stage_trace(dplyr::select(result, x))$rows, c(3, 2, 2))
})

test_that("stage_trace falls back to the last run after collect() drops it", {
  # collect() is the one operation that loses the attribute, and it is the one
  # every remote report ends with -- so the session-level fallback is what
  # makes the trace readable in production, not a convenience.
  skip_without_remote()

  result <- run_stages(
    as_remote(tibble::tibble(x = c(-2, 1, 2))),
    DOUBLE_STAGES,
    count = TRUE,
    quiet = TRUE
  )
  collected <- dplyr::collect(result)

  expect_null(attr(collected, "theUtilsR_trace", exact = TRUE))
  expect_equal(stage_trace()$rows, c(3, 2, 2))
})

test_that("counting is skipped on a lazy table by default", {
  # Counting after every stage re-runs the whole lineage; against Oracle that
  # is one round trip per stage. Opt in deliberately.
  skip_without_remote()

  result <- run_stages(
    as_remote(tibble::tibble(x = c(-2, 1, 2))),
    DOUBLE_STAGES,
    quiet = TRUE
  )

  expect_true(all(is.na(stage_trace(result)$rows)))
})

test_that("count = TRUE forces counting on a lazy table", {
  skip_without_remote()

  result <- run_stages(
    as_remote(tibble::tibble(x = c(-2, 1, 2))),
    DOUBLE_STAGES,
    count = TRUE,
    quiet = TRUE
  )

  expect_equal(stage_trace(result)$rows, c(3, 2, 2))
})

test_that("count = FALSE disables counting on a local frame", {
  result <- run_stages(
    tibble::tibble(x = c(-2, 1, 2)),
    DOUBLE_STAGES,
    count = FALSE,
    quiet = TRUE
  )

  expect_true(all(is.na(stage_trace(result)$rows)))
})

test_that("through runs a prefix of the report", {
  result <- run_stages(
    tibble::tibble(x = c(-2, 1, 2)),
    DOUBLE_STAGES,
    through = "positive",
    quiet = TRUE
  )

  expect_equal(result$x, c(1, 2))
  expect_equal(stage_trace(result)$stage, c("<input>", "positive"))
})

test_that("through names the stages when it does not recognise one", {
  expect_error(
    run_stages(tibble::tibble(x = 1), DOUBLE_STAGES, through = "nope", quiet = TRUE),
    class = "theUtilsR_unknown_stage"
  )
  expect_error(
    run_stages(tibble::tibble(x = 1), DOUBLE_STAGES, through = "nope", quiet = TRUE),
    "positive"
  )
})

test_that("a failing stage is named in the error", {
  # The whole point: a mistake in stage nine should not surface as a bare dplyr
  # error with no indication of which of eleven similar joins produced it.
  stages <- list(
    fine   = function(d) d,
    broken = function(d) dplyr::filter(d, no_such_column > 0)
  )

  expect_error(
    run_stages(tibble::tibble(x = 1), stages, quiet = TRUE),
    class = "theUtilsR_stage_failed"
  )
  expect_error(
    run_stages(tibble::tibble(x = 1), stages, quiet = TRUE),
    "broken"
  )
})

test_that("a stage that does not return a frame is caught", {
  stages <- list(counted = function(d) nrow(d))

  expect_error(
    run_stages(tibble::tibble(x = 1), stages, quiet = TRUE),
    class = "theUtilsR_bad_stage_result"
  )
})

test_that("stages must be a non-empty, uniquely named list of functions", {
  data <- tibble::tibble(x = 1)

  expect_error(run_stages(data, list(), quiet = TRUE), class = "theUtilsR_bad_stages")
  expect_error(
    run_stages(data, list(function(d) d), quiet = TRUE),
    class = "theUtilsR_bad_stages"
  )
  expect_error(
    run_stages(data, list(a = function(d) d, a = function(d) d), quiet = TRUE),
    class = "theUtilsR_bad_stages"
  )
  expect_error(
    run_stages(data, list(a = "not a function"), quiet = TRUE),
    class = "theUtilsR_bad_stages"
  )
})

test_that("progress messages name each stage, and quiet suppresses them", {
  # capture_messages() rather than expect_message(): the latter consumes only
  # the first condition and lets the rest through to the console.
  emitted <- paste(
    testthat::capture_messages(
      run_stages(tibble::tibble(x = c(-2, 1, 2)), DOUBLE_STAGES)
    ),
    collapse = ""
  )

  expect_match(emitted, "positive")
  expect_match(emitted, "doubled")
  expect_match(emitted, "2 rows")

  expect_no_message(
    run_stages(tibble::tibble(x = c(-2, 1, 2)), DOUBLE_STAGES, quiet = TRUE)
  )
})
