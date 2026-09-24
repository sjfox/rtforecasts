## Pulls the latest RSV Forecast Hub target data from the local hub repo clone,
## joins it with the hub's jurisdiction population data, and writes the set of
## CSVs the microhub tool needs to run INFLAenza with its spatial and seasonal
## options:
##
##   <slug>-latest.csv                  date / target_group / value / population
##   <slug>-neighbors-latest.csv        target_group / neighbor
##   <slug>-season-groups-latest.csv    target_group / season_group
##
## plus a date-stamped copy of each. The three upload separately in the app: the
## target data on the Data tab's main upload, the other two in the "Neighbor
## Graph" and "Seasonal Groups" sections below it (or the equivalent controls on
## the Retrospective tab).
##
## One set per target -- hospital admission counts and the ED-visit proportion
## series are kept apart since they're on different scales (a count vs. a 0-1
## proportion) and cover different history. The neighbor and season files are
## rebuilt per target too, because the two targets don't always report the same
## set of jurisdictions: the contiguity graph is computed on the jurisdictions
## actually present, and both files must name only those groups (microhub's
## validators reject unrecognized names).
##
## Assumes rtforecasts, rsv-forecast-hub, and microhub are checked out as
## sibling directories (e.g. ~/projects/rtforecasts, ~/projects/rsv-forecast-hub,
## ~/projects/microhub). Run this with the rtforecasts project as the working
## directory (e.g. via the .Rproj, or `Rscript R/rsv-hub-data-processing.R`
## from the repo root).

library(dplyr)
library(readr)
library(lubridate)  # needed by microhub's parse_microhub_dates(), sourced below

required_packages <- c("arrow", "sf", "spdep")
missing_packages <- required_packages[
  !vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)
]
if (length(missing_packages) > 0) {
  install.packages(missing_packages)
}

# Configuration ================================================================

rsv_hub_repo <- "../rsv-forecast-hub"  # path to the local rsv-forecast-hub clone
hub_remote   <- "upstream"             # remote to pull from before processing
hub_remote_url <- "https://github.com/CDCgov/rsv-forecast-hub.git"
hub_branch   <- "main"

microhub_repo <- "../microhub"  # used only to reuse microhub's upload validators

# State/territory boundaries, used to derive the contiguity graph. This is the
# same shapefile the UGA/NAU INLA pipelines use via load_us_graph().
boundary_shapefile <- "raw-data/us-geography/us-state-boundaries.shp"

# Column in the shapefile holding the jurisdiction name to match against the
# hub's `location_name`. For this shapefile that's `name` (full names, e.g.
# "District of Columbia"); `stusab` holds two-letter abbreviations instead.
boundary_name_col <- "name"

# Both latest files use the same fully observed starting window. Dated
# snapshots retain all earlier observations (but omit rows with no value).
latest_start_date <- as.Date("2024-10-12")

# One output set per target. `slug` names the files; `label` is for console
# messages; `data_type` is what microhub should be told the values are, and is
# used both for validation here and for the Data Type radio in the app.
# `latest_start_date` limits only the undated latest file; dated snapshots
# preserve the full observed history available in the selected hub snapshot.
rsv_targets <- list(
  list(target = "wk inc rsv hosp",           slug = "rsv-hosp",
       label = "Hospitalizations", data_type = "count",
       latest_start_date = latest_start_date),
  list(target = "wk inc rsv prop ed visits", slug = "rsv-ed-visits",
       label = "ED Visits (proportion)", data_type = "proportion",
       latest_start_date = latest_start_date)
)

# target-data/time-series.parquet is a stack of historical snapshots (one per
# Wednesday pull, in the `as_of` column), not a single current table.
# "latest" uses the most recent snapshot (today's best available data). Set
# this to a specific "YYYY-MM-DD" string instead to reproduce an earlier pull.
as_of_choice <- "latest"

include_national <- TRUE  # include the national row

# What to call the national row. microhub's INFLAenza derives its aggregate
# group by summing the other groups' posterior samples rather than fitting it,
# and the group it looks for is hardcoded as "Overall" (fit_process_inla()'s
# `agg_group` default, which no caller overrides). Leaving this as "US" would
# make the national series an ORDINARY target group -- fit directly, and, under
# the spatial model, fit as a region with no neighbors. Renaming it here is what
# makes the aggregate behave as an aggregate.
#
# Note this only takes effect for count data: aggregation is Poisson-only in
# fit_process_inla(), so for the proportion target "Overall" is fit directly
# regardless (a national ED-visit percentage is its own series, not a sum).
national_target_group <- "Overall"

# Seasonal grouping. Target groups NOT listed here share one common seasonal
# curve; those listed get a curve per distinct label. Deliberately declared by
# hand rather than derived from the contiguity graph -- spatial isolation and
# seasonal regime are different things. Alaska has no land border but is
# temperate; Hawaii and Puerto Rico are tropical and pool together well, which
# matters because each alone has few seasons of history.
#
# This mirrors the grouping in the UGA flusight pipeline
# (scripts/flusight-25-26/inflaenza-forecast.R), except that HI and PR share a
# curve here rather than each having their own.
season_group_overrides <- c(
  "Alaska"      = "Alaska",
  "Hawaii"      = "Hawaii",
  "Puerto Rico" = "Puerto Rico"
)

# Jurisdictions whose computed neighbors are printed for eyeballing. Small or
# enclosed jurisdictions are the ones worth checking: DC is a tiny polygon
# wedged between Maryland and Virginia, so it's the clearest test that
# contiguity is being picked up rather than silently dropped.
spot_check_groups <- c("District of Columbia")

# ED-visit values: the hub has published this target both as a 0-1 proportion
# and as a 0-100 percentage at different times. microhub's proportion data type
# requires 0-1 and its validator rejects anything above 1, so "auto" rescales a
# percentage-looking series and says so. Force with "proportion" or "percent".
proportion_scale <- "auto"

output_dir <- "processed-data"

# Step 1: update the local hub clone ==========================================

pull_hub_repo <- function(repo_path, remote, remote_url, branch) {
  repo_path <- normalizePath(repo_path, mustWork = FALSE)

  if (!dir.exists(file.path(repo_path, ".git"))) {
    stop(
      "'", repo_path, "' doesn't look like a git repo. Update `rsv_hub_repo` ",
      "at the top of this script to point at your local rsv-forecast-hub clone.",
      call. = FALSE
    )
  }

  configured_url <- system2(
    "git", c("-C", repo_path, "remote", "get-url", remote),
    stdout = TRUE, stderr = TRUE
  )
  remote_exists <- is.null(attr(configured_url, "status"))

  if (!remote_exists) {
    message("Adding git remote '", remote, "' -> ", remote_url)
    remote_output <- system2(
      "git", c("-C", repo_path, "remote", "add", remote, remote_url),
      stdout = TRUE, stderr = TRUE
    )
  } else if (!identical(sub("\\.git/?$", "", configured_url[[1]]),
                        sub("\\.git/?$", "", remote_url))) {
    message(
      "Correcting git remote '", remote, "' from ", configured_url[[1]],
      " to ", remote_url
    )
    remote_output <- system2(
      "git", c("-C", repo_path, "remote", "set-url", remote, remote_url),
      stdout = TRUE, stderr = TRUE
    )
  } else {
    remote_output <- character(0)
  }

  remote_status <- attr(remote_output, "status")
  if (!is.null(remote_status) && remote_status != 0) {
    stop(
      "Couldn't configure git remote '", remote, "' as ", remote_url, ":\n",
      paste(remote_output, collapse = "\n"),
      call. = FALSE
    )
  }

  local_changes <- system2(
    "git", c("-C", repo_path, "status", "--porcelain"),
    stdout = TRUE, stderr = TRUE
  )

  if (length(local_changes) > 0) {
    stop(
      "'", repo_path, "' has uncommitted changes, so it can't be updated ",
      "safely. Commit or stash them and re-run this script. Refusing to ",
      "continue with potentially stale target data.",
      call. = FALSE
    )
  }

  message("Pulling ", remote, "/", branch, " into ", repo_path, " ...")
  pull_output <- system2(
    "git", c("-C", repo_path, "pull", "--ff-only", remote, branch),
    stdout = TRUE, stderr = TRUE
  )
  status <- attr(pull_output, "status")
  message(paste(pull_output, collapse = "\n"))

  if (!is.null(status) && status != 0) {
    stop(
      "`git pull --ff-only ", remote, " ", branch, "` failed in ",
      repo_path, call. = FALSE
    )
  }

  invisible(TRUE)
}

pull_hub_repo(rsv_hub_repo, hub_remote, hub_remote_url, hub_branch)

# Step 2: load the hub's target data and resolve the as_of snapshot ==========

target_path <- file.path(rsv_hub_repo, "target-data", "time-series.parquet")
if (!file.exists(target_path)) {
  stop("Couldn't find ", target_path, ". Check `rsv_hub_repo` at the top of this script.", call. = FALSE)
}

rsv_raw <- arrow::read_parquet(target_path)

resolved_as_of <- if (identical(as_of_choice, "latest")) {
  max(rsv_raw$as_of)
} else {
  requested <- as.Date(as_of_choice)
  if (!(requested %in% rsv_raw$as_of)) {
    stop(
      "as_of_choice = '", as_of_choice, "' isn't in the data. Available as_of ",
      "dates range from ", min(rsv_raw$as_of), " to ", max(rsv_raw$as_of), ".",
      call. = FALSE
    )
  }
  requested
}

message("Using as_of = ", resolved_as_of)

# Step 3: bring in jurisdiction population ====================================

locations <- read_csv(
  file.path(rsv_hub_repo, "auxiliary-data", "locations.csv"),
  col_types = cols(
    location = col_character(),
    location_name = col_character(),
    population = col_double(),
    .default = col_guess()
  )
)

# Identify the national row. Hubs differ in whether the national jurisdiction
# is marked by its location CODE, its display name, or both, so match on either
# rather than betting on one -- a missed match here is silent and costly: the
# national series would be fit as an ordinary target group, and under the
# spatial model as a region with no neighbors.
national_aliases <- c("US", "USA", "United States", "National")

locations <- locations |>
  mutate(
    is_national = location %in% national_aliases | location_name %in% national_aliases
  )

n_national <- sum(locations$is_national)

if (!include_national) {
  locations <- locations |> filter(!is_national)
} else if (n_national == 0) {
  warning(
    "Couldn't find a national row in auxiliary-data/locations.csv (looked for ",
    paste(national_aliases, collapse = "/"), " in `location` or `location_name`). ",
    "No group will be named '", national_target_group, "', so INFLAenza will have ",
    "no aggregate group to derive. Add the hub's national identifier to ",
    "`national_aliases` if it uses a different one.",
    call. = FALSE
  )
} else if (n_national > 1) {
  warning(
    n_national, " rows look national (",
    paste(locations$location_name[locations$is_national], collapse = ", "),
    ") and would collapse into one '", national_target_group, "' group. ",
    "Narrow `national_aliases`.",
    call. = FALSE
  )
} else {
  message(
    "National row: ", locations$location_name[locations$is_national],
    " (code '", locations$location[locations$is_national], "') -> target_group '",
    national_target_group, "'"
  )
}

# Rename the national row so INFLAenza treats it as its aggregate group rather
# than as an ordinary jurisdiction.
locations <- locations |>
  mutate(
    target_group = if_else(is_national, national_target_group, location_name)
  ) |>
  select(-is_national)

# Step 4: load boundaries and microhub's validators ===========================

microhub_sources <- c(
  data_utils     = file.path(microhub_repo, "R", "data_utils.R"),
  neighbor_graph = file.path(microhub_repo, "R", "validate_neighbor_graph.R"),
  season_groups  = file.path(microhub_repo, "R", "validate_season_groups.R")
)

for (nm in names(microhub_sources)) {
  path <- microhub_sources[[nm]]
  if (file.exists(path)) {
    source(path)
  } else {
    message(
      "Note: couldn't find ", path, " -- the matching validation step will be ",
      "skipped. (Check `microhub_repo` at the top of this script.)"
    )
  }
}

us_boundaries <- if (file.exists(boundary_shapefile)) {
  b <- sf::read_sf(boundary_shapefile)
  if (!boundary_name_col %in% names(b)) {
    stop(
      "Shapefile '", boundary_shapefile, "' has no column '", boundary_name_col,
      "'. Available columns: ", paste(names(b), collapse = ", "),
      ". Set `boundary_name_col` at the top of this script.",
      call. = FALSE
    )
  }
  message("Loaded ", nrow(b), " boundary polygons from ", boundary_shapefile)
  b
} else {
  message(
    "Note: couldn't find ", boundary_shapefile, " -- neighbor graph files will ",
    "be skipped. Set `boundary_shapefile` at the top of this script."
  )
  NULL
}

#' Derive an undirected contiguity edge list for a set of jurisdictions.
#'
#' Mirrors the UGA/NAU pipelines' load_us_graph() + sf2mat(): subset the
#' boundaries to the jurisdictions in the data, then take QUEEN contiguity (two
#' regions are neighbors if their boundaries share at least one point, so the
#' Four Corners states are mutual diagonal neighbors). The one difference is the
#' output shape -- an edge list rather than a matrix -- because microhub keys
#' the graph by target group NAME and builds its own matrix internally, which
#' removes any chance of a row-order mismatch against the model's group index.
#'
#' Subsetting BEFORE computing contiguity is deliberate: if a state is absent
#' from the data, its neighbors must not become connected through it.
#'
#' Regions with no neighbors (Alaska, Hawaii, island territories) simply appear
#' in no edge. That is the correct representation -- microhub treats an unlisted
#' group as isolated, and INLA's besagproper model keeps a proper prior for a
#' zero-degree node.
compute_neighbor_edges <- function(boundaries, present_groups, name_col = "name") {
  empty <- data.frame(
    target_group = character(0), neighbor = character(0), stringsAsFactors = FALSE
  )
  if (is.null(boundaries) || length(present_groups) == 0) {
    return(empty)
  }

  poly_names <- as.character(boundaries[[name_col]])
  keep <- poly_names %in% present_groups

  subset_polys <- boundaries[keep, ]
  subset_names <- poly_names[keep]

  # Sort for a deterministic, diff-friendly edge list. Order is irrelevant to
  # correctness here precisely because the output is keyed by name.
  ord <- order(subset_names)
  subset_polys <- subset_polys[ord, ]
  subset_names <- subset_names[ord]

  unmatched <- setdiff(present_groups, subset_names)
  if (length(unmatched) > 0) {
    warning(
      "No boundary polygon for ", length(unmatched), " target group(s); they ",
      "will be modelled as isolated: ", paste(unmatched, collapse = ", "),
      ". If that is a naming mismatch rather than a genuine island, fix it -- ",
      "a silently isolated region gets no spatial pooling.",
      call. = FALSE
    )
  }

  if (nrow(subset_polys) < 2) {
    return(empty)
  }

  # poly2nb warns about zero-neighbor regions and sub-graphs; both are expected
  # here (Alaska, Hawaii, Puerto Rico) and are reported properly by the caller.
  nb <- suppressWarnings(spdep::poly2nb(subset_polys, queen = TRUE))

  # nb[[i]] holds neighbor indices, or a single 0 when region i has none.
  # Keeping only j > i emits each undirected pair exactly once and drops the
  # zero marker without needing a special case.
  edges <- do.call(rbind, lapply(seq_along(nb), function(i) {
    js <- nb[[i]]
    js <- js[js > i]
    if (length(js) == 0) {
      return(NULL)
    }
    data.frame(
      target_group = subset_names[i],
      neighbor = subset_names[js],
      stringsAsFactors = FALSE
    )
  }))

  if (is.null(edges)) {
    return(empty)
  }

  edges[order(edges$target_group, edges$neighbor), , drop = FALSE]
}

# Report what a generated file looks like, using microhub's own validator so the
# check here is exactly the check the app will apply on upload.
report_validation <- function(label, kind, result) {
  if (is.null(result)) return(invisible(NULL))

  if (length(result$errors) > 0) {
    message(label, ": ", kind, " FAILED validation --")
    message(paste(" -", unlist(result$errors, recursive = TRUE), collapse = "\n"))
  } else {
    message(label, ": ", kind, " passed validation.")
  }
  if (length(result$warnings) > 0) {
    message(paste("   note:", unlist(result$warnings, recursive = TRUE), collapse = "\n"))
  }
  invisible(NULL)
}

write_pair <- function(df, slug_suffix, spec, latest_df = df) {
  dated  <- file.path(output_dir, paste0(Sys.Date(), "-", spec$slug, slug_suffix, ".csv"))
  latest <- file.path(output_dir, paste0(spec$slug, slug_suffix, "-latest.csv"))
  write_csv(df, dated)
  write_csv(latest_df, latest)
  list(dated = dated, latest = latest)
}

# Step 5: build + write one set of files per target ===========================

dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

for (spec in rsv_targets) {

  snapshot <- rsv_raw |>
    filter(target == spec$target, as_of == resolved_as_of) |>
    transmute(
      location = location,
      date = as.Date(target_end_date),
      value = observation
    )

  if (nrow(snapshot) == 0) {
    warning(spec$label, ": no rows for target '", spec$target, "' at as_of ",
            resolved_as_of, " -- skipping.")
    next
  }

  missing_observations <- sum(is.na(snapshot$value))
  if (missing_observations > 0) {
    message(
      spec$label, ": dropping ", missing_observations,
      " source row(s) with no observed value."
    )
    snapshot <- snapshot |>
      filter(!is.na(value))
  }

  if (nrow(snapshot) == 0) {
    warning(
      spec$label, ": every row at as_of ", resolved_as_of,
      " has a missing observation -- skipping."
    )
    next
  }

  unmatched <- setdiff(unique(snapshot$location), locations$location)
  if (length(unmatched) > 0) {
    warning(
      spec$label, ": dropping ", length(unmatched), " location code(s) with no ",
      "match in auxiliary-data/locations.csv: ", paste(unmatched, collapse = ", ")
    )
  }

  formatted <- snapshot |>
    inner_join(
      locations |> select(location, target_group, population),
      by = "location"
    ) |>
    select(date, target_group, value, population) |>
    arrange(target_group, date)

  # -- proportion scale ------------------------------------------------------
  # microhub's proportion data type is 0-1 and its validator rejects values
  # above 1, so a percentage series has to be rescaled before upload rather
  # than after.
  if (identical(spec$data_type, "proportion")) {
    max_value <- max(formatted$value, na.rm = TRUE)
    rescale <- switch(
      proportion_scale,
      "auto"       = isTRUE(max_value > 1),
      "percent"    = TRUE,
      "proportion" = FALSE,
      stop("proportion_scale must be one of 'auto', 'percent', 'proportion'.", call. = FALSE)
    )
    if (rescale) {
      message(
        spec$label, ": values look like percentages (max = ",
        signif(max_value, 4), "); dividing by 100 to put them on microhub's 0-1 scale."
      )
      formatted$value <- formatted$value / 100
    }
  }

  latest_formatted <- formatted
  if (!is.null(spec$latest_start_date)) {
    latest_formatted <- formatted |>
      filter(date >= spec$latest_start_date)
  }

  target_paths <- write_pair(formatted, "", spec, latest_df = latest_formatted)

  present_groups <- sort(unique(formatted$target_group))

  # The rename above happens on the locations table; this confirms it survived
  # the join into the data actually written out.
  if (include_national && !(national_target_group %in% present_groups)) {
    warning(
      spec$label, ": no '", national_target_group, "' group in the output -- the ",
      "national row either isn't in this target's data or wasn't matched by ",
      "`national_aliases`. INFLAenza will have no aggregate group to derive.",
      call. = FALSE
    )
  }
  # The aggregate group is derived, not fit, so it is neither a polygon nor a
  # node in the graph.
  spatial_groups <- setdiff(present_groups, national_target_group)

  message(
    spec$label, ": wrote full snapshot (", nrow(formatted), " rows, ",
    min(formatted$date), " to ", max(formatted$date), ") to:\n  ",
    target_paths$dated, "\n",
    "  latest (", nrow(latest_formatted), " rows, ",
    min(latest_formatted$date), " to ", max(latest_formatted$date), ") to:\n  ",
    target_paths$latest
  )

  # -- neighbor graph, computed from the boundaries --------------------------
  neighbor_paths <- NULL
  if (!is.null(us_boundaries)) {
    target_neighbors <- compute_neighbor_edges(
      us_boundaries, spatial_groups, name_col = boundary_name_col
    )

    neighbor_paths <- write_pair(target_neighbors, "-neighbors", spec)

    connected <- unique(c(target_neighbors$target_group, target_neighbors$neighbor))
    isolated <- setdiff(spatial_groups, connected)

    message(
      spec$label, ": computed ", nrow(target_neighbors), " neighbor pairs covering ",
      length(connected), " of ", length(spatial_groups), " groups (",
      length(isolated), " isolated",
      if (length(isolated) > 0) paste0(": ", paste(isolated, collapse = ", ")) else "",
      ") to:\n  ", neighbor_paths$dated, "\n  ", neighbor_paths$latest
    )

    for (g in intersect(spot_check_groups, spatial_groups)) {
      nbrs <- sort(unique(c(
        target_neighbors$neighbor[target_neighbors$target_group == g],
        target_neighbors$target_group[target_neighbors$neighbor == g]
      )))
      message(
        "   spot check -- ", g, " borders: ",
        if (length(nbrs) > 0) paste(nbrs, collapse = ", ") else "(nothing -- check this)"
      )
    }
  }

  # -- seasonal groups -------------------------------------------------------
  # Only the exceptions are listed; everything else shares one curve.
  season_assignments <- tibble(
    target_group = names(season_group_overrides),
    season_group = unname(season_group_overrides)
  ) |>
    filter(target_group %in% spatial_groups)

  season_paths <- NULL
  if (nrow(season_assignments) > 0) {
    season_paths <- write_pair(season_assignments, "-season-groups", spec)

    message(
      spec$label, ": wrote ", nrow(season_assignments), " seasonal assignments across ",
      length(unique(season_assignments$season_group)), " named group(s), ",
      length(setdiff(spatial_groups, season_assignments$target_group)),
      " group(s) sharing the default curve, to:\n  ",
      season_paths$dated, "\n  ", season_paths$latest
    )
  } else {
    message(
      spec$label, ": none of the seasonal overrides (",
      paste(names(season_group_overrides), collapse = ", "),
      ") are present in this target's data -- no seasonal group file written."
    )
  }

  # -- validate everything with microhub's own validators --------------------
  if (exists("validate_data")) {
    # Passing data_type matters: the default is "count", under which the 0-1
    # range check for proportions never runs.
    errs <- validate_data(target_paths$latest, data_type = spec$data_type)
    report_validation(spec$label, "target data", list(errors = errs, warnings = list()))
  }

  if (exists("validate_neighbor_graph") && !is.null(neighbor_paths)) {
    report_validation(
      spec$label, "neighbor graph",
      validate_neighbor_graph(
        neighbor_paths$latest,
        target_groups = present_groups,
        agg_group = national_target_group
      )
    )
  }

  if (exists("validate_season_groups") && !is.null(season_paths)) {
    report_validation(
      spec$label, "seasonal groups",
      validate_season_groups(
        season_paths$latest,
        target_groups = present_groups,
        agg_group = national_target_group
      )
    )
  }

  message(
    "\n", spec$label, " -- to run in microhub:\n",
    "  Data tab -> Upload Data:      ", basename(target_paths$latest), "\n",
    "  Data tab -> Neighbor Graph:   ",
    if (!is.null(neighbor_paths)) basename(neighbor_paths$latest) else "(skipped)", "\n",
    "  Data tab -> Seasonal Groups:  ",
    if (!is.null(season_paths)) basename(season_paths$latest) else "(skipped)", "\n",
    "  Data tab -> Data Type:        ", spec$data_type, "\n",
    "  INFLAenza -> Group structure: Spatial (neighbor graph)\n",
    "  INFLAenza -> Seasonality:     One curve per seasonal group"
  )

  cat("\n")
}
