# One table of cases per rule, run against every backend in `backends()`.
#
# The cases are the ones the Python suite asserts, so a disagreement between
# the two ports shows up here rather than in a report.

# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------
CLASSIFIER_CASES <- tibble::tribble(
  ~case,                    ~primary_fac_flag, ~current_status, ~status_category,         ~expected,
  "primary flag wins",      "Y",               "EXPIRED",       "SOMETHING ELSE",         "P",
  "primary beats telemed",  "Y",               "CURRENT",       "TELEMEDICINE AFFILIATE", "P",
  "current + active",       "N",               "CURRENT",       "ACTIVE",                 "C",
  "current + courtesy",     "N",               "CURRENT",       "COURTESY",               "C",
  "current + consultant",   "N",               "CURRENT",       "CONSULTANT",             "C",
  "current + temporary",    "N",               "CURRENT",       "TEMPORARY",              "C",
  "provisional prefix",     "N",               "CURRENT",       "PROVISIONAL ACTIVE",     "C",
  "telemed substring",      "N",               "CURRENT",       "TELEMEDICINE AFFILIATE", "T",
  "telemed anywhere",       "N",               "CURRENT",       "REGIONAL TELEMED UNIT",  "T",
  "not current",            "N",               "EXPIRED",       "ACTIVE",                 " ",
  "current but unlisted",   "N",               "CURRENT",       "HONORARY",               " ",
  "lowercase is upcased",   "N",               "current",       "active",                 "C",
  "null category",          "N",               "CURRENT",       NA,                       " ",
  "null status",            "N",               NA,              "ACTIVE",                 " ",
  "all null",               NA,                NA,              NA,                       " "
)

test_that("classify_facility_status assigns P/C/T/blank", {
  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(classify_facility_status(make(CLASSIFIER_CASES)), sort_by = "case")
    expected <- CLASSIFIER_CASES[order(CLASSIFIER_CASES$case), ]

    expect_equal(result$stat, expected$expected, info = backend)
  }
})

test_that("PROVISIONAL matches only as a prefix, not anywhere", {
  rows <- tibble::tibble(
    primary_fac_flag = "N",
    current_status = "CURRENT",
    # "PROVISIONAL" appears, but not at the start
    status_category = "NOT PROVISIONAL"
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    expect_equal(
      normalise(classify_facility_status(make(rows)))$stat,
      STATUS_INACTIVE,
      info = backend
    )
  }
})

test_that("facility_status_expr can be spliced into a caller's mutate", {
  rows <- tibble::tibble(
    primary_fac_flag = "Y", current_status = "CURRENT", status_category = "ACTIVE"
  )

  spliced <- dplyr::mutate(rows, my_own_name = !!facility_status_expr())

  expect_equal(spliced$my_own_name, STATUS_PRIMARY)
})

test_that("classify_facility_status writes to the column it is told to", {
  rows <- tibble::tibble(
    primary_fac_flag = "Y", current_status = "CURRENT", status_category = "ACTIVE"
  )

  expect_true("elsewhere" %in% colnames(classify_facility_status(rows, into = "elsewhere")))
})


# ---------------------------------------------------------------------------
# first_per_group / dedupe_first
# ---------------------------------------------------------------------------
test_that("first_per_group takes the highest ranked row per group", {
  rows <- tibble::tibble(
    pract_id = c(1, 1, 1, 2, 2),
    program  = c("A", "B", "C", "D", "E"),
    n        = c(1, 3, 2, 5, 4)
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(
      first_per_group(make(rows), "pract_id", "n"),
      sort_by = "pract_id"
    )

    expect_equal(result$program, c("B", "D"), info = backend)
    expect_equal(nrow(result), 2L, info = backend)
  }
})

test_that("first_per_group breaks ties in the direction asked for", {
  rows <- tibble::tibble(
    pract_id = c(1, 1),
    program  = c("Zebra", "Alpha"),
    n        = c(2, 2)
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    ascending <- first_per_group(
      make(rows), "pract_id", c("n", "program"), c(TRUE, FALSE)
    )
    descending <- first_per_group(
      make(rows), "pract_id", c("n", "program"), c(TRUE, TRUE)
    )

    expect_equal(normalise(ascending)$program, "Alpha", info = backend)
    expect_equal(normalise(descending)$program, "Zebra", info = backend)
  }
})

test_that("nulls in an ordering column sort last in both directions", {
  # Without an explicit null placement this is exactly where two engines give
  # different answers: no two SQL dialects agree on where NULL belongs, and
  # none of them agrees with R.
  rows <- tibble::tibble(
    pract_id = c(1, 1, 2, 2),
    program  = c("has-value", "is-null", "has-value", "is-null"),
    n        = c(1, NA, 1, NA)
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    for (direction in c(TRUE, FALSE)) {
      result <- normalise(
        first_per_group(make(rows), "pract_id", "n", desc = direction),
        sort_by = "pract_id"
      )

      expect_equal(
        result$program,
        c("has-value", "has-value"),
        info = paste(backend, "desc =", direction)
      )
    }
  }
})

test_that("first_per_group leaves no working columns behind", {
  rows <- tibble::tibble(pract_id = 1, n = 1)

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- first_per_group(make(rows), "pract_id", "n")

    expect_equal(sort(colnames(result)), c("n", "pract_id"), info = backend)
  }
})

test_that("first_per_group rejects a desc vector of the wrong length", {
  rows <- tibble::tibble(pract_id = 1, a = 1, b = 2)

  expect_error(
    first_per_group(rows, "pract_id", c("a", "b"), desc = c(TRUE, FALSE, TRUE)),
    class = "theUtilsR_bad_desc"
  )
})

test_that("first_per_group names the columns it cannot find", {
  expect_error(
    first_per_group(tibble::tibble(a = 1), "a", "nope"),
    class = "theUtilsR_missing_columns"
  )
})

test_that("dedupe_first keeps one row per key", {
  rows <- tibble::tibble(
    pract_id = c(1, 1, 2),
    faccode  = c("OAK", "OAK", "SFO"),
    other    = c("first", "second", "only")
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(
      dedupe_first(make(rows), by = c("pract_id", "faccode")),
      sort_by = c("pract_id", "faccode")
    )

    expect_equal(nrow(result), 2L, info = backend)
  }
})

test_that("dedupe_first ranks on order_by when it is given one", {
  rows <- tibble::tibble(
    pract_id = c(1, 1),
    faccode  = c("OAK", "OAK"),
    rank     = c(2, 1),
    other    = c("loser", "winner")
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- dedupe_first(
      make(rows),
      by = c("pract_id", "faccode"),
      order_by = "rank"
    )

    expect_equal(normalise(result)$other, "winner", info = backend)
  }
})


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------
ACTIVE_CASES <- tibble::tribble(
  ~case,               ~primary_dea, ~primary_license, ~credentialed, ~current_status, ~month, ~keep,
  "all yes",           "Y",          "Y",              "Y",           "CURRENT",       0L,     TRUE,
  "missing dea ok",    NA,           "Y",              "Y",           "CURRENT",       0L,     TRUE,
  "missing licence ok", "Y",         NA,               "Y",           "CURRENT",       0L,     TRUE,
  "applicant counts",  "Y",          "Y",              "N",           "APPLICANT",     0L,     TRUE,
  "applicant lower",   "Y",          "Y",              "N",           "applicant",     0L,     TRUE,
  "dea no",            "N",          "Y",              "Y",           "CURRENT",       0L,     FALSE,
  "licence no",        "Y",          "N",              "Y",           "CURRENT",       0L,     FALSE,
  "not credentialed",  "Y",          "Y",              "N",           "EXPIRED",       0L,     FALSE,
  "wrong month",       "Y",          "Y",              "Y",           "CURRENT",       1L,     FALSE
)

test_that("is_active_credentialing_row keeps exactly the reportable rows", {
  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(
      is_active_credentialing_row(make(ACTIVE_CASES)),
      sort_by = "case"
    )
    expected <- sort(ACTIVE_CASES$case[ACTIVE_CASES$keep])

    expect_equal(result$case, expected, info = backend)
  }
})

TELEMED_CASES <- tibble::tribble(
  ~case,             ~status_category,          ~current_status, ~month, ~keep,
  "exact category",  "TELEMEDICINE AFFILIATE",  "CURRENT",       0L,     TRUE,
  "lowercase",       "telemedicine affiliate",  "current",       0L,     TRUE,
  "substring only",  "REGIONAL TELEMED UNIT",   "CURRENT",       0L,     FALSE,
  "not current",     "TELEMEDICINE AFFILIATE",  "EXPIRED",       0L,     FALSE,
  "wrong month",     "TELEMEDICINE AFFILIATE",  "CURRENT",       1L,     FALSE
)

test_that("is_current_telemed_row is stricter than the classifier", {
  # The classifier accepts any category *containing* TELEMED; the population is
  # defined on the full category name. "substring only" is the case that pins
  # the difference.
  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(is_current_telemed_row(make(TELEMED_CASES)), sort_by = "case")

    expect_equal(result$case, sort(TELEMED_CASES$case[TELEMED_CASES$keep]), info = backend)
  }
})

PRIMARY_CASES <- tibble::tribble(
  ~case,             ~primary_record, ~credentialed, ~current_status, ~month, ~keep,
  "all yes",         "Y",             "Y",           "CURRENT",       0L,     TRUE,
  "lowercase",       "Y",             "Y",           "current",       0L,     TRUE,
  "not primary",     "N",             "Y",           "CURRENT",       0L,     FALSE,
  "not credentialed", "Y",            "N",           "CURRENT",       0L,     FALSE,
  "not current",     "Y",             "Y",           "EXPIRED",       0L,     FALSE,
  "wrong month",     "Y",             "Y",           "CURRENT",       1L,     FALSE
)

test_that("is_current_primary_credentialed_row keeps live primary credentials", {
  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(
      is_current_primary_credentialed_row(make(PRIMARY_CASES)),
      sort_by = "case"
    )

    expect_equal(result$case, sort(PRIMARY_CASES$case[PRIMARY_CASES$keep]), info = backend)
  }
})

test_that("the predicate expressions splice into a caller's filter", {
  rows <- ACTIVE_CASES

  expect_equal(
    nrow(dplyr::filter(rows, !!is_active_credentialing_expr())),
    sum(ACTIVE_CASES$keep)
  )
})


# ---------------------------------------------------------------------------
# Population
# ---------------------------------------------------------------------------
POPULATION_ROWS <- tibble::tribble(
  ~pract_id, ~status_category,         ~current_status, ~primary_record, ~credentialed, ~month,
  # 1: telemed somewhere and credentialed at primary -- in scope
  1,         "TELEMEDICINE AFFILIATE", "CURRENT",       "N",             "Y",           0L,
  1,         "ACTIVE",                 "CURRENT",       "Y",             "Y",           0L,
  # 2: telemed but no live primary credential -- out
  2,         "TELEMEDICINE AFFILIATE", "CURRENT",       "N",             "Y",           0L,
  # 3: credentialed at primary but no telemed row -- out
  3,         "ACTIVE",                 "CURRENT",       "Y",             "Y",           0L
)

test_that("telemed_population is the intersection of the two predicates", {
  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(telemed_population(make(POPULATION_ROWS)))

    expect_equal(result$pract_id, 1, info = backend)
    expect_equal(colnames(result), "pract_id", info = backend)
  }
})

test_that("telemed_population returns each practitioner exactly once", {
  rows <- dplyr::bind_rows(
    POPULATION_ROWS,
    dplyr::mutate(POPULATION_ROWS[1:2, ], pract_id = 0)
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(telemed_population(make(rows)), sort_by = "pract_id")

    expect_equal(result$pract_id, c(0, 1), info = backend)
  }
})

test_that("telemed_population sorts a local result", {
  # Only local frames carry row order. On a lazy table the sort would land in a
  # subquery that the engine is free to discard, so the stages skip it and the
  # caller arranges once, right before collect(). See `.arrange_by()`.
  rows <- dplyr::bind_rows(
    POPULATION_ROWS,
    dplyr::mutate(POPULATION_ROWS[1:2, ], pract_id = 0)
  )

  expect_equal(telemed_population(rows)$pract_id, c(0, 1))
})

test_that("restrict_to_practitioners cannot duplicate rows", {
  data <- tibble::tibble(pract_id = c(1, 1, 2), value = c("a", "b", "c"))
  # deliberately not distinct, to prove the semi-join still cannot fan out
  population <- tibble::tibble(pract_id = c(1, 1))

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(
      restrict_to_practitioners(make(data), make(population)),
      sort_by = c("pract_id", "value")
    )

    expect_equal(result$value, c("a", "b"), info = backend)
  }
})


# ---------------------------------------------------------------------------
# Home facility
# ---------------------------------------------------------------------------
test_that("home_facility_rows renames to the home_* columns", {
  rows <- tibble::tibble(
    pract_id = c(1, 1, 2),
    faccode  = c("SFO", "OAK", "SAC"),
    section1 = c("Cardiology", "Cardiology", "Neurology"),
    section2 = c("Echo", NA, "EEG"),
    stat     = c("P", "T", "P")
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(home_facility_rows(make(rows)), sort_by = "pract_id")

    expect_equal(
      colnames(result),
      c("pract_id", "home_facility", "home_section1", "home_section2"),
      info = backend
    )
    expect_equal(result$home_facility, c("SFO", "SAC"), info = backend)
    expect_equal(result$home_section2, c("Echo", "EEG"), info = backend)
  }
})

test_that("home_facility_rows collapses identical duplicate rows", {
  rows <- tibble::tibble(
    pract_id = c(1, 1),
    faccode  = c("SFO", "SFO"),
    section1 = c("Cardiology", "Cardiology"),
    section2 = c("Echo", "Echo"),
    stat     = c("P", "P")
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    expect_equal(nrow(normalise(home_facility_rows(make(rows)))), 1L, info = backend)
  }
})


# ---------------------------------------------------------------------------
# Dominant telemed program
# ---------------------------------------------------------------------------
test_that("dominant_telemed_program picks the program with the most facilities", {
  rows <- tibble::tibble(
    pract_id  = c(1, 1, 1, 2),
    expertise = c("TeleICU", "TeleICU", "TeleStroke", "TelePsych"),
    stat      = "T"
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(dominant_telemed_program(make(rows)), sort_by = "pract_id")

    expect_equal(colnames(result), c("pract_id", "telemed_program"), info = backend)
    expect_equal(result$telemed_program, c("TeleICU", "TelePsych"), info = backend)
  }
})

test_that("dominant_telemed_program breaks a tie on the program name", {
  # The SAS original left ties to whatever order the sort produced, so a
  # week-over-week diff of the report flapped. This pins it.
  rows <- tibble::tibble(
    pract_id  = c(1, 1),
    expertise = c("Zebra", "Alpha"),
    stat      = "T"
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    expect_equal(
      normalise(dominant_telemed_program(make(rows)))$telemed_program,
      "Alpha",
      info = backend
    )
  }
})

test_that("dominant_telemed_program counts rows, not non-null expertise", {
  # COUNT(expertise) would score the null group at zero and hand the win to the
  # single-facility program.
  rows <- tibble::tibble(
    pract_id  = c(1, 1, 1),
    expertise = c(NA, NA, "TeleICU"),
    stat      = "T"
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    expect_true(
      is.na(normalise(dominant_telemed_program(make(rows)))$telemed_program),
      info = backend
    )
  }
})

test_that("dominant_telemed_program ignores rows that are not telemed", {
  rows <- tibble::tibble(
    pract_id  = c(1, 1, 1),
    expertise = c("TeleICU", "Cardiology", "Cardiology"),
    stat      = c("T", "P", "C")
  )

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    expect_equal(
      normalise(dominant_telemed_program(make(rows)))$telemed_program,
      "TeleICU",
      info = backend
    )
  }
})


# ---------------------------------------------------------------------------
# Program text cleanup
# ---------------------------------------------------------------------------
CLEAN_CASES <- tibble::tribble(
  ~telemed_program,      ~expected,
  "Tele-Stroke",         "TeleStroke",
  "Tele / Stroke",       "Tele Stroke",
  "A  B",                "A B",
  "  leading",           "leading",
  "trailing  ",          "trailing",
  "Punct!@#uation",      "Punctuation",
  "Digits 123 kept",     "Digits 123 kept",
  "",                    "",
  NA,                    NA
)

test_that("clean_program_text strips punctuation and collapses blanks", {
  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(clean_program_text(make(CLEAN_CASES)))
    # Row order is not guaranteed on a remote backend; match on the input.
    ordered <- result[order(match(result$expected, CLEAN_CASES$expected)), ]

    expect_equal(ordered$telemed_program, ordered$expected, info = backend)
  }
})

test_that("clean_program_text cleans the column it is pointed at", {
  rows <- tibble::tibble(other_name = "A-B")

  expect_equal(clean_program_text(rows, "other_name")$other_name, "AB")
})
