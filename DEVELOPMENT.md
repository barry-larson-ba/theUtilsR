# DEVELOPMENT.md

Notes for anyone working on this repository — the constraints and conventions that are load-bearing but
not obvious from reading the source. `README.md` is the user-facing documentation; this file is the
maintenance manual.

## What this is

An R package of helpers for pulling Oracle tables into Spark or a DBI connection, plus the reusable
credentialing-report transforms. It is the R port of the Python package at `I:\2026\p\projects\theUtils`;
that project in turn ports `I:\2026\p\projects\SasUtils` (github.com/barry-larson-ba). The context is a
Databricks notebook importing the library to help write report code. The code largely focuses on
transforming tables into other tables.

**When changing a business rule, change it in both ports or neither.** The two share one set of
expectations — `tests/testthat/test-weekly_telemed.R` here and `tests/test_weekly_telemed.py` there assert
the same output frame from the same input snapshot. A rule that diverges silently is the failure mode
this whole arrangement exists to prevent.

## Layout

A standard R package. Nothing clever, and deliberately so — the Python original uses a flat layout as a
concession to Databricks `sys.path` behaviour, and R has no equivalent problem to concede to.

```
R/                oracle_utils.R, sources.R, report_transforms.R,
                  crosstab.R, facility_codes.R, report_layout.R,
                  pipeline.R, theUtilsR-package.R
tests/testthat/   the suite; helper-backends.R holds the DuckDB fixture
notebooks/        Databricks R notebooks (.r, Databricks source format)
man/              roxygen-generated -- never edit by hand
DESCRIPTION       version lives here
```

Run the suite with `devtools::test()`. Regenerate docs with `roxygen2::roxygenise(".")` after touching any
`#'` block, and **commit `man/` and `NAMESPACE` along with the source** — they are generated but tracked.

`devtools::check()` is currently clean: 0 errors, 0 warnings, 0 notes. Keep it that way.

## The central design decision

**Every rule is written once, in dplyr, and runs on both a local frame and a remote table.**

The Python package implements each rule three times — pandas, Polars, PySpark — and leans on a shared table
of test cases to stop the three drifting apart. Do not port that structure. dplyr verbs translate to SQL via
dbplyr, so one implementation covers both backends and drift is impossible rather than merely tested for.

If you find yourself writing `*_local()` and `*_remote()` variants of a rule, stop: that is the Python
structure sneaking back in. There are exactly two functions where the dispatch is genuinely unavoidable,
and both are marked in the source:

- **`first_per_group()`** — local `arrange()` fixes row order for a later `row_number()`; a lazy table needs
  `dbplyr::window_order()`, because an `ORDER BY` in a subquery is something engines may discard.
- **`facility_crosstab()`** — `tidyr::pivot_wider()` has no lazy-table method, so the remote branch writes
  `MAX(CASE WHEN faccode = 'OAK' THEN stat END)` by hand.

### The expression / data pair

Each rule that is naturally an expression has two exported forms:

```r
is_active_credentialing_expr()      # a quosure, for filter(df, !!expr)
is_active_credentialing_row(data)   # filter(data, !!expr)
```

The second is implemented in terms of the first. **Keep it that way** — the moment the data-taking form
re-states the rule, there are two copies of it. This pair is the R rendering of the Python asymmetry where
the pandas variant took a DataFrame and the Polars/Spark variants returned an `Expr`/`Column`.

The `*_expr()` functions return **quosures** built with `rlang::quo()`, not bare expressions. The quosure
carries the package namespace as its environment, so `str_replace_all` and friends resolve whether or not
the caller has attached stringr.

One exception, and it matters: the ordering expressions inside `first_per_group()` are built with
`rlang::expr()`, **not** `quo()`. `dbplyr::window_order()` inspects the call structure looking for
`desc(col)`, errors on a quosure, and will not see through a `dplyr::desc()` namespace qualifier either.

## Translation-sensitive functions

Not every R function survives the trip to SQL. These are the ones the package depends on:

`toupper`, `substr`, `grepl`, `case_when`, `coalesce`, `if_else`, `is.na`, `stringr::str_replace_all`,
`stringr::str_trim`.

**`gsub()` is not translated** by several backends (DuckDB among them) — it renders as a call to a scalar
function named `gsub` that does not exist. That is why the program-text cleanup uses stringr. Similarly,
`startsWith()` has no universal translation, so the `PROVISIONAL` prefix test uses
`substr(x, 1, n) == prefix` instead.

When adding a rule, verify it collects on DuckDB before committing. The test suite does this automatically;
a rule that only ever ran locally would pass a lazily-written test and fail on a cluster.

## Sorting

The stages sort a local frame and **deliberately leave a lazy one alone** — see `.arrange_by()` in
`report_transforms.R`. Ordering an intermediate remote result is meaningless: the `ORDER BY` lands in a
subquery, most engines discard it, and dbplyr warns about exactly that. Sort once, at the end, immediately
before `collect()`.

Do not "fix" a test that sorts its result before comparing. On a remote backend only an `arrange()`d final
result is ordered, and the tests that genuinely assert order do so on a local frame.

## Values that leak into SQL as column names

A local variable referenced inside a dplyr verb that runs remotely gets treated as a **column reference**
by dbplyr, not as a value. `dplyr::coalesce(x, fill)` sends `COALESCE(x, fill)` and the database goes
looking for a column called `fill`. Unquote it: `rlang::quo(dplyr::coalesce(!!sym(code), !!fill))`.

This bites hardest inside a closure passed to `across()`, where there is no obvious place to unquote. Build
the named list of expressions explicitly instead — `.fill_exprs()` in `crosstab.R` is the pattern.

## Execution model

Connection settings live in `.settings`, an environment inside the package, written only by `configure()`.
The Python original goes to some trouble here because Python resolves a function's globals in the module
where the function was defined; R has no such trap, since a package function's environment is the sealed
package namespace. **Do not port the "these four names are deliberately undefined" convention** — it solves
a problem R does not have.

They are still *session* state, so everything sharing an interpreter shares one connection identity, and
credentials still belong in the notebook rather than in source control.

`read_oracle()` dispatches on the connection class:

- `spark_connection` → `sparklyr::spark_read_jdbc()`, query wrapped as `(query) tmp`. Needs `jdbc_url`,
  `username`, `password`.
- `DBIConnection` → `dplyr::tbl(con, dbplyr::sql(query))`. The connection *is* the credential; the three
  JDBC settings are neither needed nor consulted.

`sparklyr` is in **Suggests**, not Imports — the package must install and test without it, mirroring the
Python rule that `import theUtils` never requires pyspark. Guard every use with `rlang::check_installed()`.

## Pipeline shape

`read_oracle()` is the only entry point users should call; everything else is a stage it composes:

1. `read_oracle_raw()` — execute the query
2. `convert_integer_decimals()` — scale-0 decimals to `BIGINT`; a **no-op off the Spark path**, since a DBI
   driver maps Oracle `NUMBER` to an R numeric with no Decimal intermediary. Called unconditionally anyway
   so the pipeline reads the same either way.
3. `apply_schema()` — explicit per-column casts
4. collection — `"source"` (default), `"tibble"`, `"data.frame"`, `"data.table"`

The decimal fix runs *before* the explicit schema so a caller-supplied schema always wins. Preserve that
ordering.

`return_type` is validated *before* the query is issued, so a typo costs nothing. Keep new argument
validation up there with it.

`SCHEMAS` is a placeholder list (`member`, `provider` both `NULL`) for reusable schema definitions; it is
not wired into `read_oracle()`.

`apply_schema()` takes **canonical** type names (`character`, `integer`, `bigint`, `double`, `logical`,
`date`, `timestamp`), not backend-native ones, so one schema definition survives a move between engines.
The only spelling that actually differs is the string type — Spark says `STRING`, everyone else `VARCHAR`.

## Registries

`facility_codes.R` and `report_layout.R` hold data and nothing else. Both are checked for internal drift by
`validate_facility_registry()` and `validate_report_layout()`, called from `.onLoad()` — so a registry that
disagrees with itself stops the package loading rather than producing a wrong column six weeks later. That
is the R counterpart of the Python module-level `assert` statements. Keep the checks in `.onLoad()`.

`FACILITY_CODES` is **order-sensitive** — it drives report column layout. Do not sort it.

## Testing

Every rule test runs against both entries in `backends()`: a tibble, and the same rows in DuckDB as a lazy
table. That is the R analogue of the Python suite's three-backend table — the axis that matters here is
"does dplyr's answer match SQL's answer", not "do three implementations agree".

```r
for (backend in names(backends())) {
  make <- backends()[[backend]]
  result <- normalise(some_rule(make(rows)), sort_by = "pract_id")
  expect_equal(result$stat, expected, info = backend)
}
```

`normalise()` collects and flattens scalar types, because the two engines disagree about `integer` vs
`BIGINT` in ways that say nothing about correctness. Pass `sort_by` unless the test is specifically about
ordering.

DuckDB is in Suggests; a run without it skips the remote half rather than failing. That is intended.

`test-weekly_telemed.R` is the end-to-end golden test and the thing keeping `notebooks/weekly_telemed_pipeline.r`
and `notebooks/weekly_telemed_report.r` honest. If you change a stage's signature or output columns, that is what catches the notebook drifting.

## Notebooks

Databricks R source format (`# Databricks notebook source`, `# COMMAND ----------`, `# MAGIC %md`), with
a `.r` extension. Each opens with a `find_package_root()` cell that walks up looking for `DESCRIPTION` and
calls `devtools::load_all()`, falling back to `library(theUtilsR)`. Keep that cell and keep it inlined —
it is what makes the package available, so it cannot come from the package.

`loading_in_databricks.r` is the one that explains the `find_package_root()` cell the other five open with,
plus the install routes, the dependency check and the failure modes. When that cell changes, update the
explanation there rather than adding a note to each notebook.

All six run top to bottom with no Oracle connection. `oracle_utils_demo.r` uses an in-process DuckDB as
the stand-in, which exercises the same DBI branch a real Oracle ODBC connection would. Preserve that
property when editing: a notebook nobody can run is a notebook nobody reads.

Cells needing a live connection are commented out and marked `[NEEDS ORACLE]`.

There are three telemed notebooks and they are not redundant — `pipeline` teaches the stages, `report`
maps SAS line to replacement, `job` is the one you schedule. Before adding a fourth, check which of those
three the change belongs in.

`weekly_telemed_job.r` calls `dbutils` through a `dbutils_call()` shim that resolves the function by name
at call time and falls back to a default. That is what lets the notebook run in RStudio. **Do not replace
it with direct `dbutils.*` calls** — an unrunnable job notebook cannot be tested before it is scheduled.

## run_stages

`R/pipeline.R` holds `run_stages()`, which applies an ordered, named list of `frame -> frame` stages.

It is **not** the orchestrating `build()` the design rules out. It knows nothing about telemedicine; the
stage list — the actual composition — stays in the notebook. Keep that line: report-specific knowledge does
not belong in `R/pipeline.R`.

Two things about it that are easy to get wrong:

- **Counting a lazy table costs a query per stage.** `count` defaults to `NULL`, meaning "count local
  frames, skip lazy ones". Against Oracle, forcing it without a `dplyr::compute()` first issues one full
  round trip per stage. The default is not timidity.
- **The trace attribute survives dplyr verbs but not `collect()`**, which is how every remote report ends.
  That is why `stage_trace()` also reads a session-level copy when called with no argument.

`tests/testthat/test-weekly_telemed.R` restates the job notebook's stage list and asserts it produces the
same frame as the chain form. A notebook is not importable, so that restatement is the only thing holding
the two together — update it when you add a stage.
