# --- Libraries ---
library(data.table)
library(here)
library(DBI)
library(RSQLite)
library(stringr)
library(glue)

#' Align column names to reference using GloVe embeddings
#'
#' Matches and renames columns in a target table to align with a reference,
#' using GloVe word embeddings and cosine similarity.
#' @param con Active SQL database connection.
#' @param reference_table Table to match against (keeps original names).
#' @param target_table Target table (renamed to match reference).
#' @param output_table Name of output table (renamed copy of target).
#' @param models_dir Path to GloVe embeddings.
#' @param threshold Cosine similarity cutoff for matches.
#' @return Invisibly returns the name of the output table.
align_columns <- function(
  con,
  reference_table,
  target_table,
  output_table,
  models_dir = here("models"),
  threshold = 0.75
) {
  # Load GloVe embeddings from file (word vectors for semantic similarity)
  glove_txt <- file.path(models_dir, "glove.6B.300d.txt")
  message("📖 Loading GloVe vectors...")
  glove <- fread(glove_txt, header = FALSE, quote = "", data.table = FALSE)
  words <- glove[[1]]
  embeddings <- as.matrix(glove[, -1])
  rownames(embeddings) <- words

  # Split a column name into lowercase tokens (words) for embedding lookup
  split_name <- function(x) {
    x <- gsub("([a-z])([A-Z])", "\\1_\\2", x)
    x <- gsub("[^a-zA-Z0-9]+", "_", x)
    x <- tolower(x)
    tokens <- unlist(strsplit(x, "_"))
    tokens[nchar(tokens) > 0]
  }

  # Get the average embedding for a column name (averages word vectors)
  get_embedding <- function(colname) {
    tokens <- split_name(colname)
    vecs <- embeddings[tokens[tokens %in% rownames(embeddings)], , drop = FALSE]
    if (nrow(vecs) == 0) return(rep(NA, ncol(embeddings)))
    colMeans(vecs)
  }

  # Cosine similarity measures how similar two vectors are (1 = identical)
  cosine_sim <- function(a, b) sum(a * b) / (sqrt(sum(a^2)) * sqrt(sum(b^2)))

  # Get column names and their embeddings for both tables
  cols_ref <- dbListFields(con, reference_table)
  cols_target <- dbListFields(con, target_table)
  emb_ref <- t(sapply(cols_ref, get_embedding))
  emb_target <- t(sapply(cols_target, get_embedding))

  # Compute similarity matrix:
  # each [i, j] is similarity between target i and reference j
  sim_matrix <- matrix(NA, nrow = length(cols_target), ncol = length(cols_ref))
  for (i in seq_along(cols_target)) {
    for (j in seq_along(cols_ref)) {
      sim_matrix[i, j] <- cosine_sim(emb_target[i, ], emb_ref[j, ])
    }
  }

  # For each target column, find the reference column with highest similarity
  # above the threshold
  matches <- rep(NA, length(cols_target))
  for (i in seq_along(cols_target)) {
    best_idx <- which.max(sim_matrix[i, ])
    sim_val <- sim_matrix[i, best_idx]
    if (!is.na(sim_val) && sim_val >= threshold) {
      matches[i] <- cols_ref[best_idx]
    }
  }
  names(matches) <- cols_target
  matches <- na.omit(matches)
  matches <- matches[!duplicated(matches)]  # Remove duplicate matches

  message("✅ Matching complete. Here are the results:")
  print(matches)

  # Rename columns in the target table based on matches found above
  table_data <- dbReadTable(con, target_table)
  colnames(table_data) <- sapply(colnames(table_data), function(x) {
    if (x %in% names(matches)) matches[[x]] else x
  })

  dbWriteTable(con, output_table, table_data, overwrite = TRUE)
  message(glue("✅ Done. Renamed table saved as {output_table}."))
  invisible(output_table)
}