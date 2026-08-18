# Databricks notebook source
# MAGIC %md
# MAGIC # Loading theUtilsR in Databricks
# MAGIC
# MAGIC How to get `library(theUtilsR)` — or the dev-loop equivalent — working from
# MAGIC a Git folder checkout. This is the cell every other notebook in
# MAGIC `notebooks/` opens with, explained once so it does not have to be explained
# MAGIC four more times.
# MAGIC
# MAGIC Run this notebook top to bottom on the cluster you intend to use. It
# MAGIC diagnoses before it prescribes: the first section reports what your
# MAGIC environment actually is rather than assuming, because the answers vary by
# MAGIC runtime version, workspace settings and cluster access mode.
# MAGIC
# MAGIC ### The one-paragraph version
# MAGIC
# MAGIC Clone the repo as a Git folder. In a notebook inside it, call
# MAGIC `pkgload::load_all(<repo root>)` while you are developing, or install it
# MAGIC once with `install.packages(<repo root>, repos = NULL, type = "source")`
# MAGIC and then `library(theUtilsR)` for a scheduled job. There is no third
# MAGIC mechanism and nothing happens automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Why this needs explaining at all
# MAGIC
# MAGIC Because the Python answer does not carry over, and people reasonably expect
# MAGIC it to.
# MAGIC
# MAGIC Databricks puts a Git folder's root on `sys.path`, so a Python notebook in a
# MAGIC checkout can `import theUtils` with no install and no setup. **R has no
# MAGIC equivalent.** There is no automatic search path for source trees, no
# MAGIC per-directory convention, nothing that makes an uninstalled package visible
# MAGIC to `library()`.
# MAGIC
# MAGIC So an R package in a Git folder has to be loaded on purpose, every session,
# MAGIC by one of the two mechanisms in §4 and §5. The upside is that there is no
# MAGIC ambiguity about *which* copy you got — the recurring failure mode on the
# MAGIC Python side, where an unnoticed `sys.path` entry silently shadows the
# MAGIC checkout you are editing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Where am I?
# MAGIC
# MAGIC Run this first. Everything below depends on the answers, and hardcoding a
# MAGIC path from someone else's workspace is the most common way this goes wrong.

# COMMAND ----------

report_environment <- function() {
  on_databricks <- nzchar(Sys.getenv("DATABRICKS_RUNTIME_VERSION"))

  cat("R version      : ", R.version.string, "\n", sep = "")
  cat("Databricks DBR : ",
      if (on_databricks) Sys.getenv("DATABRICKS_RUNTIME_VERSION") else "(not on a cluster)",
      "\n", sep = "")
  cat("working dir    : ", getwd(), "\n", sep = "")
  cat("\n.libPaths(), in search order:\n")
  for (p in .libPaths()) {
    cat("  ", p, if (file.access(p, 2) == 0) "  [writable]" else "  [read-only]", "\n", sep = "")
  }
  cat("\ntooling:\n")
  for (pkg in c("pkgload", "devtools", "remotes", "pak")) {
    cat("  ", format(pkg, width = 9), " ",
        if (requireNamespace(pkg, quietly = TRUE)) "installed" else "MISSING",
        "\n", sep = "")
  }

  invisible(NULL)
}

report_environment()

# COMMAND ----------

# MAGIC %md
# MAGIC ### What to look for
# MAGIC
# MAGIC **`working dir`** — in a Git folder with workspace files enabled this is the
# MAGIC notebook's own directory, something like
# MAGIC `/Workspace/Repos/you@example.com/theUtilsR/notebooks` or, on newer
# MAGIC workspaces, `/Workspace/Users/you@example.com/theUtilsR/notebooks`. Which of
# MAGIC those you get depends on the workspace, which is exactly why §3 walks up to
# MAGIC find the root instead of naming it.
# MAGIC
# MAGIC If the working directory is `/databricks/driver` instead, workspace files are
# MAGIC off for this workspace or runtime — the notebook is running detached from its
# MAGIC folder, `find_package_root()` will return `NULL`, and you will have to give
# MAGIC the path explicitly.
# MAGIC
# MAGIC **`.libPaths()`** — the first *writable* entry is where an install will land.
# MAGIC If none is writable, installs fail; see §8.
# MAGIC
# MAGIC **`pkgload` MISSING** — install it, or use the install route in §5, which
# MAGIC needs no tooling beyond base R.
# MAGIC
# MAGIC ### If R does not run here at all
# MAGIC
# MAGIC Worth ruling out before anything else: R is not available on every cluster
# MAGIC configuration. In particular R is generally unavailable on **Standard**
# MAGIC (formerly Shared) access-mode clusters and on serverless compute — you want
# MAGIC **Dedicated** (formerly Single user) access mode. Note also that SparkR is
# MAGIC deprecated in recent runtimes and removed in DBR 17+; this package uses
# MAGIC **sparklyr** for the Spark path and never SparkR. These details move between
# MAGIC runtime versions, so treat this as a checklist, not a specification.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Finding the repo root
# MAGIC
# MAGIC This is the helper every other notebook opens with. It walks up from the
# MAGIC working directory looking for the directory that holds `DESCRIPTION`, which
# MAGIC is the file that makes a directory an R package.
# MAGIC
# MAGIC Walking up rather than hardcoding matters for three reasons: the Git folder
# MAGIC path differs between workspaces, it contains the user's own email, and the
# MAGIC notebooks sit one level below the root. A literal path in a committed
# MAGIC notebook works for exactly one person.

# COMMAND ----------

find_package_root <- function(start = getwd(), max_up = 6L) {
  path <- normalizePath(start, winslash = "/", mustWork = FALSE)

  for (i in seq_len(max_up)) {
    if (file.exists(file.path(path, "DESCRIPTION"))) return(path)

    parent <- dirname(path)

    if (identical(parent, path)) break   # reached the filesystem root

    path <- parent
  }

  NULL
}

root <- find_package_root()

if (is.null(root)) {
  cat("No DESCRIPTION found above ", getwd(), "\n",
      "Set `root` by hand -- see the note below.\n", sep = "")
} else {
  cat("package root: ", root, "\n", sep = "")
  cat("DESCRIPTION  :\n")
  cat(paste0("  ", readLines(file.path(root, "DESCRIPTION"), n = 3)), sep = "\n")
}

# COMMAND ----------

# MAGIC %md
# MAGIC If that printed `NULL`, set it explicitly. Copy the path from the Git folder
# MAGIC in the workspace sidebar:
# MAGIC
# MAGIC ```r
# MAGIC root <- "/Workspace/Repos/you@example.com/theUtilsR"
# MAGIC ```
# MAGIC
# MAGIC A Git folder is a normal directory on the driver's filesystem, so any R
# MAGIC function that takes a path works on it. You do **not** need `dbutils.fs`,
# MAGIC and you must not use a `dbfs:/` URL — that scheme means nothing to R.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Option A — `load_all()`, for developing
# MAGIC
# MAGIC Source-loads the package straight from the checkout. Nothing is built,
# MAGIC nothing is installed, and **your edits are picked up by re-running the cell**
# MAGIC — no reinstall, no cluster restart, no detach-and-reattach.
# MAGIC
# MAGIC This is the right choice while you are changing `R/`. It is the wrong choice
# MAGIC for a scheduled job: it is slower to start, it depends on tooling being
# MAGIC present, and it loads whatever is in the working tree, including edits you
# MAGIC have not committed.
# MAGIC
# MAGIC Use `pkgload` rather than `devtools`: `devtools::load_all()` *is*
# MAGIC `pkgload::load_all()` re-exported, and pkgload is a far smaller install if
# MAGIC the cluster has neither.

# COMMAND ----------

if (!requireNamespace("pkgload", quietly = TRUE)) {
  install.packages("pkgload")
}

pkgload::load_all(root, quiet = TRUE)

cat("loaded ", length(getNamespaceExports("theUtilsR")), " exported objects\n", sep = "")
head(FACILITY_CODES)

# COMMAND ----------

# MAGIC %md
# MAGIC ### `load_all()` does not put the package on the search path the usual way
# MAGIC
# MAGIC It attaches a shim environment rather than an installed namespace. That is
# MAGIC almost always invisible, but two things follow:
# MAGIC
# MAGIC * `packageVersion("theUtilsR")` and `installed.packages()` will not see it —
# MAGIC   it is not installed, and they are telling the truth
# MAGIC * internal (non-exported) objects are reachable, which they would not be
# MAGIC   after a real install. Code that works under `load_all()` and fails after
# MAGIC   installing is nearly always reaching for something unexported.
# MAGIC
# MAGIC If a notebook has to work both ways, test it once with §5 before scheduling
# MAGIC it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Option B — install it, for jobs
# MAGIC
# MAGIC Builds and installs into the library, after which `library(theUtilsR)` works
# MAGIC like any other package. Slower once, then normal.
# MAGIC
# MAGIC All three of the following take the **directory** — you do not need to build
# MAGIC a `.tar.gz` first. The first needs no tooling at all beyond base R, which is
# MAGIC why it is the one to reach for on a bare cluster.
# MAGIC
# MAGIC ```r
# MAGIC install.packages(root, repos = NULL, type = "source")   # base R
# MAGIC remotes::install_local(root, upgrade = "never")         # resolves deps
# MAGIC devtools::install(root, upgrade = "never")              # dev workflow
# MAGIC ```
# MAGIC
# MAGIC `upgrade = "never"` on the latter two is not optional in practice. Without
# MAGIC it they will offer to upgrade dplyr and friends mid-run, which on a cluster
# MAGIC means either a hang waiting for input or a surprise rebuild of half the
# MAGIC tidyverse.

# COMMAND ----------

# Uncomment to install. Left commented so that re-running this notebook during a
# load_all() session does not swap the package out from under you.
#
# install.packages(root, repos = NULL, type = "source")
#
# library(theUtilsR)
# packageVersion("theUtilsR")

# COMMAND ----------

# MAGIC %md
# MAGIC ### This does not survive a cluster restart
# MAGIC
# MAGIC A notebook-driven install lands in a library on the driver's local disk and
# MAGIC is gone when the cluster terminates. For something that runs every week,
# MAGIC pick one of:
# MAGIC
# MAGIC | | how | good for |
# MAGIC |---|---|---|
# MAGIC | install cell at the top of the job notebook | the cell above, uncommented | simple, costs a rebuild per run |
# MAGIC | cluster-scoped init script | `R CMD INSTALL` against the checkout | every notebook on the cluster, no per-run cost |
# MAGIC | build a tarball, add as a cluster library | `pkgbuild::build(root)`, upload, attach in the UI | pinned versions, no build at runtime |
# MAGIC
# MAGIC The middle one needs the Git folder to exist when the cluster boots, which
# MAGIC it will, but it also pins you to whatever the folder contained at boot. The
# MAGIC last one is the most reproducible and the most ceremony.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Which one
# MAGIC
# MAGIC | | `load_all()` | install |
# MAGIC |---|---|---|
# MAGIC | picks up edits | yes, re-run the cell | no, reinstall |
# MAGIC | needs pkgload/devtools | yes | no (base R route) |
# MAGIC | `packageVersion()` works | no | yes |
# MAGIC | hides unexported objects | no | yes |
# MAGIC | startup cost | small each time | one build |
# MAGIC | use for | editing `R/` | scheduled jobs |
# MAGIC
# MAGIC The other notebooks in this folder try `load_all()` and fall back to
# MAGIC `library()`, so they work either way:
# MAGIC
# MAGIC ```r
# MAGIC root <- find_package_root()
# MAGIC
# MAGIC if (!is.null(root) && requireNamespace("devtools", quietly = TRUE)) {
# MAGIC   devtools::load_all(root, quiet = TRUE)
# MAGIC } else {
# MAGIC   library(theUtilsR)
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Keep that cell inlined in any new notebook. It cannot be imported *from* the
# MAGIC package, because it is what makes the package importable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Dependencies
# MAGIC
# MAGIC Loading the package does not install what it depends on. `load_all()` fails
# MAGIC outright if an `Imports` package is missing; an install will try to fetch
# MAGIC them from CRAN, which needs egress from the cluster.
# MAGIC
# MAGIC Required (`Imports`): **dbplyr, dplyr, rlang, stringr, tibble, tidyr**.
# MAGIC
# MAGIC Optional (`Suggests`), each needed only for the feature that uses it:
# MAGIC **sparklyr** for the Spark path, **DBI** for the DBI path, **writexl** to
# MAGIC write the workbook, **data.table** for `return_type = "data.table"`,
# MAGIC **duckdb**/**testthat**/**withr** for the test suite.
# MAGIC
# MAGIC `sparklyr` being a Suggests is deliberate — the package installs, loads and
# MAGIC tests without it, mirroring the Python rule that `import theUtils` must never
# MAGIC require pyspark.

# COMMAND ----------

check_dependencies <- function(root) {
  fields <- read.dcf(file.path(root, "DESCRIPTION"), fields = c("Imports", "Suggests"))

  parse_field <- function(x) {
    if (is.na(x)) return(character(0))
    trimws(sub("\\(.*\\)", "", strsplit(x, ",")[[1]]))
  }

  status <- function(pkgs, kind) {
    if (!length(pkgs)) return(NULL)
    data.frame(
      package = pkgs,
      kind = kind,
      installed = vapply(pkgs, requireNamespace, logical(1), quietly = TRUE),
      row.names = NULL
    )
  }

  rbind(
    status(parse_field(fields[1, "Imports"]), "required"),
    status(parse_field(fields[1, "Suggests"]), "optional")
  )
}

deps <- check_dependencies(root)
print(deps)

missing_required <- deps$package[deps$kind == "required" & !deps$installed]

if (length(missing_required)) {
  cat("\nMissing and required:\n  install.packages(c(",
      paste0('"', missing_required, '"', collapse = ", "), "))\n", sep = "")
} else {
  cat("\nall required dependencies present\n")
}

# COMMAND ----------

# MAGIC %md
# MAGIC The tidyverse pieces ship with Databricks Runtime for **Machine Learning**;
# MAGIC standard DBR carries a smaller R library and may not have them. The cell
# MAGIC above tells you which you are on more reliably than the release notes do.
# MAGIC
# MAGIC For anything scheduled, prefer **cluster libraries** (Compute → Libraries →
# MAGIC Install new → CRAN) over an `install.packages()` cell: they are installed
# MAGIC once at cluster start, they are visible in the UI, and they do not silently
# MAGIC change version between runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Things that go wrong
# MAGIC
# MAGIC | Symptom | Cause | Fix |
# MAGIC |---|---|---|
# MAGIC | `there is no package called 'theUtilsR'` | never loaded — nothing is automatic in R | §4 or §5 |
# MAGIC | `find_package_root()` returns `NULL` | working dir is `/databricks/driver`; workspace files off, or notebook outside the folder | set `root` explicitly (§3) |
# MAGIC | `cannot open file '.../DESCRIPTION'` | `root` points at `notebooks/`, not the repo root | drop the last path element |
# MAGIC | `unable to create ... permission denied` | no writable entry in `.libPaths()` | install to a writable lib, or use a cluster library |
# MAGIC | `there is no package called 'dplyr'` | dependencies not installed | §7 |
# MAGIC | works under `load_all()`, fails after install | using an unexported object | export it, or stop using it |
# MAGIC | `could not find function "spark_read_jdbc"` | sparklyr is a Suggests and is not installed | install sparklyr |
# MAGIC | package loads but edits do nothing | you installed it; `library()` reads the installed copy | use `load_all()` while editing |
# MAGIC | `%md` cells render as code | notebook language is not R | set the notebook language, or the file needs a `.r` extension |
# MAGIC
# MAGIC A note on that last-but-one row, because it costs people an afternoon: if you
# MAGIC have *both* installed the package and called `load_all()`, which one you get
# MAGIC depends on the order the cells ran in. `unloadNamespace("theUtilsR")` before
# MAGIC re-loading settles it.

# COMMAND ----------

# Which copy of the package is actually loaded right now?
if ("theUtilsR" %in% loadedNamespaces()) {
  path <- getNamespaceInfo("theUtilsR", "path")
  cat("loaded from: ", path, "\n", sep = "")
  cat("mechanism  : ",
      if (identical(normalizePath(path, winslash = "/", mustWork = FALSE),
                    normalizePath(root, winslash = "/", mustWork = FALSE))) {
        "load_all() -- the working tree"
      } else {
        "installed"
      },
      "\n", sep = "")
} else {
  cat("theUtilsR is not loaded\n")
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Confirm it works
# MAGIC
# MAGIC A smoke test that touches the registry, a transform and the crosstab without
# MAGIC needing a connection. If this prints a two-row report, the package is loaded
# MAGIC correctly and you can move on to `weekly_telemed_job.r`.

# COMMAND ----------

suppressPackageStartupMessages(library(dplyr))

smoke <- tibble::tibble(
  pract_id         = c(1L, 1L, 2L),
  faccode          = c("SFO", "OAK", "SAC"),
  primary_fac_flag = c("Y", "N", "Y"),
  current_status   = "CURRENT",
  status_category  = c("ACTIVE", "TELEMEDICINE AFFILIATE", "ACTIVE")
)

result <- smoke |>
  classify_facility_status() |>
  exclude_faccodes() |>
  facility_crosstab(index = "pract_id", facility_codes = c("SFO", "OAK", "SAC"))

stopifnot(
  nrow(result) == 2L,
  identical(colnames(result), c("pract_id", "SFO", "OAK", "SAC"))
)

cat("theUtilsR is loaded and working\n\n")
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC | Notebook | For |
# MAGIC |---|---|
# MAGIC | `weekly_telemed_job.r` | the report as a scheduled job |
# MAGIC | `weekly_telemed_pipeline.r` | the report, one stage per cell |
# MAGIC | `weekly_telemed_report.r` | the SAS macro, step by step |
# MAGIC | `api_reference.r` | every exported function |
# MAGIC | `oracle_utils_demo.r` | connections and `read_oracle()` |
# MAGIC
# MAGIC All of them open with the fallback cell from §6, so once this notebook runs
# MAGIC cleanly they will too.
