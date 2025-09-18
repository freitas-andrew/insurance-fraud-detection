# --- Libraries ---
library(data.table)
library(here)
library(DBI)
library(RSQLite)

# --- Function to align column names of two tables using GloVe embeddings ---
align_columns <- function(
  con,                          # active SQL database connection
  reference_table,              # table to match against (keeps original names)
  target_table,                 # target table (renamed to match table 1)
  output_table,                 # name of output table (renamed copy of table 2)
  models_dir = here("models"),  # path to GloVe embeddings
  threshold = 0.75              # cosine similarity cutoff for matches
) {

  # --- Load GloVe embeddings ---
  glove_txt <- file.path(models_dir, "glove.6B.300d.txt")
  cat("📖 Loading GloVe vectors...\n")
  glove <- fread(glove_txt, header = FALSE, quote = "", data.table = FALSE)
  words <- glove[, 1]
  embeddings <- as.matrix(glove[, -1])
  rownames(embeddings) <- words

  # --- Helper: split column names into tokens ---
  split_name <- function(x) {
    x <- gsub("([a-z])([A-Z])", "\\1_\\2", x)
    x <- gsub("[^a-zA-Z0-9]+", "_", x)
    x <- tolower(x)
    tokens <- unlist(strsplit(x, "_"))
    tokens[nchar(tokens) > 0]
  }

  # --- Helper: get average embedding for a column name ---
  get_embedding <- function(colname) {
    tokens <- split_name(colname)
    vecs <- embeddings[tokens[tokens %in% rownames(embeddings)], , drop = FALSE]
    if (nrow(vecs) == 0) return(rep(NA, ncol(embeddings)))
    colMeans(vecs)
  }

  # --- Helper: cosine similarity between two vectors ---
  cosine_sim <- function(a, b) sum(a * b) / (sqrt(sum(a^2)) * sqrt(sum(b^2)))

  # --- Get columns from the tables ---
  cols_1 <- dbListFields(con, reference_table)
  cols_2 <- dbListFields(con, target_table)

  # --- Compute embeddings ---
  emb_1 <- t(sapply(cols_1, get_embedding))
  emb_2 <- t(sapply(cols_2, get_embedding))

  # --- Compute cosine similarity matrix ---
  sim_matrix <- outer(
    seq_len(nrow(emb_2)),
    seq_len(nrow(emb_1)),
    Vectorize(function(i, j) {
      a <- emb_2[i, ]
      b <- emb_1[j, ]
      if (any(is.na(a)) || any(is.na(b))) return(NA)
      cosine_sim(a, b)
    })
  )

  # --- Find best matches above threshold ---
  matches <- apply(sim_matrix, 1, function(row) {
    if (all(is.na(row))) return(NA)
    best_idx <- which.max(row)
    if (row[best_idx] >= threshold) cols_1[best_idx] else NA
  })
  names(matches) <- cols_2
  matches <- na.omit(matches)
  matches <- matches[!duplicated(matches)]

  cat("✅ Matching complete. Here are the results:\n")
  print(matches)

  # --- Rename columns in second table based on matches ---
  table_data <- dbReadTable(con, target_table)
  colnames(table_data) <- sapply(colnames(table_data), function(x) {
    if (x %in% names(matches)) matches[[x]] else x
  })

  # --- Write aligned table to DB ---
  dbWriteTable(
    con,
    output_table,
    table_data,
    overwrite = TRUE
  )

  cat(paste0("✅ Done. Renamed table saved as ", output_table, ".\n"))
}
