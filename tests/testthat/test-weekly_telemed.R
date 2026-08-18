# End-to-end test of the weekly telemed pipeline.
#
# This runs the exact stage sequence that `notebooks/weekly_telemed_pipeline.r`
# documents, on both backends, against one hand-built snapshot -- and asserts
# the whole output frame, not a row count.
#
# There is no orchestrating `build()` function in the package on purpose: the
# stages are meant to be composed and inspected one at a time. That makes this
# test the thing keeping the documented order honest. If a stage's signature or
# output columns drift from what the notebook calls, it fails here.
#
# The snapshot is small but every row earns its place:
#
#   101  in scope; telemed at OAK and at an excluded code, primary at SFO,
#        courtesy at SAC -- exercises P, C and T in one practitioner
#   102  telemed but no live primary credential -- must not appear at all
#   103  in scope; also carries a row the active-credentialing filter drops

BASE_ROWS <- tibble::tribble(
  ~pract_id, ~last_name, ~first_name, ~middle_initial, ~degree, ~faccode, ~section_name, ~expertise, ~primary_fac_flag, ~primary_record, ~credentialed, ~current_status, ~status_category, ~primary_dea, ~primary_license, ~month,
  # 101 -- primary at SFO, telemed at OAK and REG, courtesy at SAC
  101L, "Alvarez", "Rosa", "M", "MD", "SFO", "Cardiology", NA, "Y", "Y", "Y", "CURRENT", "ACTIVE", "Y", "Y", 0L,
  101L, "Alvarez", "Rosa", "M", "MD", "OAK", "Cardiology", "Tele-Stroke", "N", "N", "Y", "CURRENT", "TELEMEDICINE AFFILIATE", "Y", "Y", 0L,
  101L, "Alvarez", "Rosa", "M", "MD", "SAC", "Cardiology", NA, "N", "N", "Y", "CURRENT", "COURTESY", "Y", "Y", 0L,
  # REG is an administrative code: it still counts toward the dominant program
  # (as it did in SAS, where the exclusion applied only at the transpose) but
  # must not become a column.
  101L, "Alvarez", "Rosa", "M", "MD", "REG", "Cardiology", "Tele-Stroke", "N", "N", "Y", "CURRENT", "TELEMEDICINE AFFILIATE", "Y", "Y", 0L,

  # 102 -- telemed, but nothing credentialed at a primary facility
  102L, "Brennan", "Ida", "R", "DO", "OAK", "Neurology", "TelePsych", "N", "N", "Y", "CURRENT", "TELEMEDICINE AFFILIATE", "Y", "Y", 0L,

  # 103 -- primary at SAC, telemed at SFO
  103L, "Chen", "Wei", "L", "MD", "SAC", "Neurology", NA, "Y", "Y", "Y", "CURRENT", "ACTIVE", "Y", "Y", 0L,
  103L, "Chen", "Wei", "L", "MD", "SFO", "Neurology", "TeleICU", "N", "N", "Y", "CURRENT", "TELEMEDICINE AFFILIATE", "Y", "Y", 0L,
  # dropped by is_active_credentialing_row(): not credentialed, not an applicant
  103L, "Chen", "Wei", "L", "MD", "VAL", "Neurology", NA, "N", "N", "N", "EXPIRED", "ACTIVE", "Y", "Y", 0L
)

FACILITY_ROWS <- tibble::tribble(
  ~pract_id, ~faccode, ~section2,
  101L,      "SFO",    "Echo",
  101L,      "OAK",    NA,
  103L,      "SAC",    "EEG"
)


# ---------------------------------------------------------------------------
# The pipeline -- the notebook's stage list, in order
# ---------------------------------------------------------------------------
# Written as one function rather than one per backend, which is the whole
# payoff of the R port: the Python suite needs three copies of this because it
# has three implementations of every stage.
run_pipeline <- function(base, facilities) {
  population <- telemed_population(base)

  rows <- restrict_to_practitioners(base, population)
  rows <- is_active_credentialing_row(rows)
  rows <- dplyr::rename(rows, section1 = "section_name")
  rows <- dplyr::left_join(rows, facilities, by = c("pract_id", "faccode"))
  rows <- dedupe_first(rows, by = c("pract_id", "faccode"))
  rows <- classify_facility_status(rows)

  home <- home_facility_rows(rows)
  programs <- dominant_telemed_program(rows)

  rows <- dplyr::left_join(rows, home, by = "pract_id")
  rows <- dplyr::left_join(rows, programs, by = "pract_id")
  rows <- clean_program_text(rows)

  # Order matters here: dominant_telemed_program() and home_facility_rows() run
  # *before* exclude_faccodes(). In the SAS original the exclusion applied only
  # at the transpose, so an administrative faccode still counted toward a
  # practitioner's program total. Swapping these changes the answer silently.
  rows <- exclude_faccodes(rows)

  wide <- facility_crosstab(rows, index = TELEMED_IDENTITY_COLUMNS)

  select_report_columns(wide)
}

# The same report as an ordered list of named stages -- the form
# `notebooks/weekly_telemed_job.r` runs. Restated here rather than imported,
# because a notebook is not importable; the test below asserts the two
# formulations agree, which is what stops the job notebook drifting.
SITE_KEY <- c("pract_id", "faccode")

telemed_stages <- function(facilities) list(
  population     = function(d) restrict_to_practitioners(d, telemed_population(d)),
  reportable     = is_active_credentialing_row,
  privileges     = function(d) dplyr::left_join(
                     dplyr::rename(d, section1 = "section_name"),
                     facilities,
                     by = SITE_KEY
                   ),
  one_per_site   = function(d) dedupe_first(d, by = SITE_KEY),
  classify       = classify_facility_status,
  attach_home    = function(d) dplyr::left_join(d, home_facility_rows(d), by = "pract_id"),
  attach_program = function(d) dplyr::left_join(d, dominant_telemed_program(d), by = "pract_id"),
  clean_program  = clean_program_text,
  drop_admin     = exclude_faccodes,
  transpose      = function(d) facility_crosstab(d, index = TELEMED_IDENTITY_COLUMNS),
  layout         = select_report_columns
)


# ---------------------------------------------------------------------------
# The expectation
# ---------------------------------------------------------------------------
expected_row <- function(pract_id, last, first, mi, degree, home, sec1, sec2,
                         program, marked) {
  identity <- list(
    pract_id = as.numeric(pract_id), last_name = last, first_name = first,
    middle_initial = mi, degree = degree, home_facility = home,
    home_section1 = sec1, home_section2 = sec2, telemed_program = program
  )
  facilities <- stats::setNames(
    lapply(FACILITY_CODES, function(code) {
      if (code %in% names(marked)) marked[[code]] else STATUS_INACTIVE
    }),
    FACILITY_CODES
  )

  as.data.frame(c(identity, facilities), stringsAsFactors = FALSE)
}

EXPECTED <- rbind(
  expected_row(
    101, "Alvarez", "Rosa", "M", "MD", "SFO", "Cardiology", "Echo",
    # "Tele-Stroke" -- the hyphen is stripped by clean_program_text()
    "TeleStroke",
    list(SFO = "P", OAK = "T", SAC = "C")
  ),
  expected_row(
    103, "Chen", "Wei", "L", "MD", "SAC", "Neurology", "EEG",
    "TeleICU",
    list(SAC = "P", SFO = "T")
  )
)


# ---------------------------------------------------------------------------
# The tests
# ---------------------------------------------------------------------------
test_that("the weekly telemed pipeline produces the expected report", {
  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(
      run_pipeline(make(BASE_ROWS), make(FACILITY_ROWS)),
      sort_by = "pract_id"
    )

    expect_equal(colnames(result), ordered_columns(), info = backend)
    expect_equal(result, EXPECTED, info = backend)
  }
})

test_that("the stage-list form produces the same report as the chain", {
  # notebooks/weekly_telemed_job.r runs the stage list; the other two telemed
  # notebooks run the chain. They are the same report, and this is what says so.
  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(
      run_stages(
        make(BASE_ROWS),
        telemed_stages(make(FACILITY_ROWS)),
        count = TRUE,
        quiet = TRUE
      ),
      sort_by = "pract_id"
    )

    expect_equal(colnames(result), ordered_columns(), info = backend)
    expect_equal(result, EXPECTED, info = backend)
  }
})

test_that("the stage trace accounts for every row the report drops", {
  trace <- stage_trace(
    run_stages(BASE_ROWS, telemed_stages(FACILITY_ROWS), quiet = TRUE)
  )

  counts <- stats::setNames(trace$rows, trace$stage)

  expect_equal(unname(counts[["<input>"]]), 8)
  # 102 has no live primary credential: four rows in, three practitioners' worth
  # out, and 102's single row gone.
  expect_equal(unname(counts[["population"]]), 7)
  # 103's expired VAL row
  expect_equal(unname(counts[["reportable"]]), 6)
  # REG is dropped only here, after it has counted toward the program
  expect_equal(unname(counts[["drop_admin"]]), 5)
  expect_equal(unname(counts[["layout"]]), 2)
})

test_that("running through a stage stops there", {
  partial <- run_stages(
    BASE_ROWS,
    telemed_stages(FACILITY_ROWS),
    through = "classify",
    quiet = TRUE
  )

  expect_true(STAT_COLUMN %in% colnames(partial))
  expect_false("home_facility" %in% colnames(partial))
  expect_setequal(unique(partial$stat), c("P", "C", "T"))
})

test_that("moving drop_admin before the program choice changes the answer", {
  # The ordering constraint in the stage list, stated as a test. Same stages,
  # one moved up two places.
  stages <- telemed_stages(FACILITY_ROWS)
  reordered <- stages[c(
    "population", "reportable", "privileges", "one_per_site", "classify",
    "drop_admin", "attach_home", "attach_program", "clean_program",
    "transpose", "layout"
  )]

  rows <- dplyr::bind_rows(
    BASE_ROWS,
    dplyr::mutate(BASE_ROWS[4, ], faccode = "STK", expertise = "Tele-Stroke")
  )
  rows <- dplyr::mutate(
    rows,
    expertise = dplyr::if_else(
      .data$faccode == "OAK" & .data$pract_id == 101L,
      "AAA Rival",
      .data$expertise
    )
  )

  correct <- normalise(run_stages(rows, stages, quiet = TRUE), sort_by = "pract_id")
  wrong <- normalise(run_stages(rows, reordered, quiet = TRUE), sort_by = "pract_id")

  expect_equal(correct$telemed_program[[1]], "TeleStroke")
  expect_equal(wrong$telemed_program[[1]], "AAA Rival")
})

test_that("a practitioner without a live primary credential is excluded", {
  # 102 is a current telemedicine affiliate but has no credentialed primary
  # record, which is exactly the pairing the SAS self-join required.
  expect_false(102 %in% EXPECTED$pract_id)

  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(run_pipeline(make(BASE_ROWS), make(FACILITY_ROWS)))

    expect_false(102 %in% result$pract_id, info = backend)
  }
})

test_that("an administrative faccode counts toward the program but is not a column", {
  # 101's Tele-Stroke appears at OAK and at REG. Dropping REG before the
  # program is chosen would still give Tele-Stroke here (it is the only
  # program), so the sharper check is that REG never becomes a column while the
  # row it carried was still counted.
  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- normalise(run_pipeline(make(BASE_ROWS), make(FACILITY_ROWS)))

    expect_false(any(EXCLUDED_FACCODES %in% colnames(result)), info = backend)
  }
})

test_that("excluding faccodes before ranking would change the answer", {
  # The ordering constraint above, stated as a test rather than a comment.
  # Give 101 a second program that wins only when the REG row is counted.
  rows <- dplyr::bind_rows(
    BASE_ROWS,
    dplyr::mutate(BASE_ROWS[4, ], faccode = "STK", expertise = "Tele-Stroke")
  )
  rows <- dplyr::mutate(
    rows,
    expertise = dplyr::if_else(
      .data$faccode == "OAK" & .data$pract_id == 101L,
      "AAA Rival",
      .data$expertise
    )
  )

  # Counted with the administrative rows: Tele-Stroke has REG + STK = 2,
  # AAA Rival has OAK = 1. Tele-Stroke wins.
  with_admin <- normalise(run_pipeline(rows, FACILITY_ROWS), sort_by = "pract_id")
  expect_equal(with_admin$telemed_program[[1]], "TeleStroke")

  # Excluding first would leave Tele-Stroke at 0 and hand it to AAA Rival.
  excluded_first <- exclude_faccodes(rows)
  expect_equal(
    sort(unique(dplyr::filter(excluded_first, .data$pract_id == 101L)$faccode)),
    c("OAK", "SAC", "SFO")
  )
})

test_that("the report is the identity block plus one column per facility", {
  for (backend in names(backends())) {
    make <- backends()[[backend]]

    result <- run_pipeline(make(BASE_ROWS), make(FACILITY_ROWS))

    expect_length(colnames(result), 28L)
    expect_equal(
      length(colnames(result)),
      length(TELEMED_IDENTITY_COLUMNS) + length(FACILITY_CODES),
      info = backend
    )
  }
})

test_that("the pipeline runs through a resolver, with no connection at all", {
  # What frame_reader() is for: the notebook's read_source() calls resolve
  # against fixtures.
  previous <- use_resolver(frame_reader(list(
    base_data_hist = BASE_ROWS,
    practitioner_facilities = FACILITY_ROWS
  )))
  withr::defer(use_resolver(previous))

  result <- normalise(
    run_pipeline(
      read_source(BASE_DATA_HIST),
      read_source(PRACTITIONER_FACILITIES)
    ),
    sort_by = "pract_id"
  )

  expect_equal(result, EXPECTED)
})
