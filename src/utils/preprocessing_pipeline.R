# --- Libraries ---
library(DBI)
library(dplyr)
library(janitor)
library(lubridate)
library(rlang)
library(RSQLite)
library(stringr)

# --- Importing external utility: GloVeb-based column alignment ---
source(here::here("src", "utils", "column_align.R"))
utils::globalVariables("align_columns")

# --- Function: standardise_bools ---
# Converts boolean-like columns (e.g., Yes/No, Y/N, T/F) to 0/1 integers.
# Binary encodes two value categorical data (e.g., Male/Female)
# Also, renames binary encoded variables to the Is<Value> format (PascalCase)
# (i.e., Male/Female → IsMale), keeping standard boolean column names unchanged
standardise_bools <- function(
  con,                  # active database connection
  table_name,           # table to encode
  output_table = NULL   # optional: specify name of table saved to DB
) {

  df <- dbReadTable(con, table_name)

  # --- Nested helper: sanitize/format column names ---
  sanitize_name <- function(x) {
    x <- gsub("[^a-zA-Z0-9]", "", x)       # Remove non-alphanumeric
    paste0(toupper(substring(x, 1, 1)), substring(x, 2)) # PascalCase
  }

  # --- Priority mapping for which value → 1 ---
  preferred_priority <- c("TRUE", "T", "YES", "Y", "MALE", "M", "1")

  df_colnames <- names(df)

  for (col_name in df_colnames) {
    column_values <- na.omit(unique(df[[col_name]]))
    # Only process columns with exactly 2 unique values
    if (length(column_values) != 2) next

    # Convert values to upper-case strings for consistent matching
    column_values_upper <- toupper(as.character(column_values))

    # --- Determine which value should be 1 ---
    priority_match <- intersect(preferred_priority, column_values_upper)
    if (length(priority_match) > 0) {
      value_for_1 <- priority_match[1]
    } else {
      # fallback: first alphabetically
      value_for_1 <- sort(column_values_upper)[1]
    }

    # --- Map original values to 0/1 ---
    df[[col_name]] <- ifelse(
      toupper(as.character(df[[col_name]])) == value_for_1,
      1L,
      0L
    )

    # --- Decide on new column name ---
    # Detect whether column was already standard boolean
    # (0/1, TRUE/FALSE, Y/N, etc.)
    original_values_standard <- all(column_values %in% c(0, 1)) ||
      all(column_values_upper %in% c(
        "TRUE", "FALSE", "T", "F", "YES", "NO", "Y", "N"
      ))

    if (original_values_standard) {
      # Keep original column name (standard boolean)
      new_col_name <- col_name

    } else if (all(column_values_upper %in% c("M", "F"))) {
      # Special hardcoded edge case: M/F → IsMale
      new_col_name <- "IsMale"

    } else {
      # Non-standard boolean → dynamic rename using the chosen "1" value
      new_col_name <- paste0("Is", sanitize_name(value_for_1))
    }

    # Apply renaming if necessary
    if (new_col_name != col_name) {
      df[[new_col_name]] <- df[[col_name]]
      df[[col_name]] <- NULL
    }

    message(paste0("✅ Column processed: '", col_name, "' → '", new_col_name,
                   "' | 1 = '", value_for_1, "'"))
  }

  # Determine output table name
  if (is.null(output_table)) {
    output_table <- paste0(table_name, "_encoded")
  }
  message(paste0(
    "🔹 Writing processed table to database as '",
    output_table, "'..."
  ))
  dbWriteTable(con, output_table, df, overwrite = TRUE)

  message(paste0("✅ Table saved successfully as '", output_table, "'"))

  # Return dataframe
  df
}


# --- Function: prepare_column_names ---
# Standardises and cleans column names of a database table,
# consolidates weekday/month/day triplets into proper date columns,
# optionally applies a user-defined renaming map
prepare_column_names <- function(
  con,                  # active database connection
  table_name,           # name of the table to standardise
  default_year = 2000,  # fallback year if no year column found (default = 2000)
  rename_map = NULL,    # optional: vector for manual renaming (original = new)
  output_table = NULL   # optional: specify name of table saved to DB
) {
  # --- Nested helper: collapse weekday/month/week/day triplets ---
  combine_date_columns <- function(df) {
    month_cols <- names(df)[str_detect(names(df), "_month$")]

    # Ensure logical output with vapply
    triplets_to_process <- month_cols[vapply(month_cols, function(m_col) {
      d_col <- paste0(str_remove(m_col, "_month$"), "_day")
      d_col %in% names(df)
    }, logical(1))]


    if (length(triplets_to_process) == 0) {
      message("⚠️ No date columns to consolidate; ",
        "skipping collapse_weekday_triplets."
      )
      return(df)
    }

    for (m_col in triplets_to_process) {
      prefix <- str_remove(m_col, "_month$")
      w_col <- paste0(prefix, "_week")
      d_col <- paste0(prefix, "_day")
      y_col <- paste0(prefix, "_year")

      message(paste0("🔹 Processing date columns for prefix '", prefix, "'..."))

      df <- df %>%
        mutate(
          # Parse year: prefer [prefix]_year, else general "year" column, else fallback # nolint
          year_val = if (y_col %in% names(df)) {
            .data[[y_col]]
          } else if ("year" %in% names(df)) {
            .data$year
          } else {
            default_year
          },

          # Parse month: numeric ("01") or text ("Dec", "December")
          month_val = suppressWarnings(
            case_when(
              str_detect(str_trim(!!sym(m_col)), "^[0-9]+$") ~
                as.integer(str_remove(str_trim(!!sym(m_col)), "^0+")),
              TRUE ~ match(
                str_to_title(str_sub(str_trim(!!sym(m_col)), 1, 3)),
                c(month.abb, "Sept")
              )
            )
          ),

          # Parse day: numeric ("15", "1st") or weekday names ("Mon")
          day_val = suppressWarnings(
            case_when(
              str_detect(str_trim(!!sym(d_col)), "^[0-9]+(st|nd|rd|th)?$") ~
                as.integer(
                  str_remove(
                    str_trim(!!sym(d_col)),
                    "(st|nd|rd|th)$"
                  )
                ),

              TRUE ~ match(
                str_to_title(str_sub(str_trim(!!sym(d_col)), 1, 3)),
                c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
              )
            )
          ),

          # Flag whether parsed day was numeric or weekday
          day_is_weekday = !str_detect(str_trim(!!sym(d_col)), "^[0-9]")
        ) %>%
        rowwise() %>%
        mutate(
          # --- Final date construction ---
          !!paste0(prefix, "_date") := {
            if (!is.na(.data$day_val) && !.data$day_is_weekday) {
              # Case 1: numeric day
              day <- min(
                .data$day_val,
                days_in_month(make_date(.data$year_val, .data$month_val, 1))
              )
              make_date(.data$year_val, .data$month_val, day)

            } else if (!is.na(.data$day_val) && .data$day_is_weekday) {
              # Case 2: weekday name -> requires week number
              if (!(w_col %in% names(df))) {
                NA_Date_
              } else {
                week_val <- suppressWarnings(as.integer(.data[[w_col]]))
                if (is.na(week_val)) {
                  NA_Date_
                } else {
                  first_day <- make_date(.data$year_val, .data$month_val, 1)
                  first_wd <- as.integer(format(first_day, "%u")) # Monday - 1
                  offset <- (.data$day_val - (first_wd - 1)) %% 7
                  day1 <- first_day + days(offset)
                  candidate <- day1 + weeks(week_val - 1)

                  if (month(candidate) == .data$month_val) {
                    candidate
                  } else {
                    NA_Date_
                  }
                }
              }
            } else {
              NA_Date_
            }
          }
        ) %>%
        ungroup() %>%
        # Drop staging columns and originals
        select(
          -any_of(c(m_col, w_col, d_col, y_col)),
          -all_of(c("year_val", "month_val", "day_val", "day_is_weekday"))
        )
    }

    message("✅ Date columns successfully processed and consolidated.")
    df
  }

  # --- Pipeline workflow ---
  df_clean <- dbReadTable(con, table_name) %>%
    (\(df) if (!is.null(rename_map)) rename(df, !!!rename_map) else df) %>%
    clean_names(case = "snake") %>%
    combine_date_columns()

  message(paste0(
    "✅ prepare_column_names completed successfully for table '",
    table_name, "'."
  ))

  # Saving cleaned table back to DB
  output_table <- output_table %||% paste0(table_name, "_clean")
  dbWriteTable(con, output_table, df_clean, overwrite = TRUE)

  # Return df
  df_clean
}

# --- Function: collapse_table ---
# Collapses a table to only the columns it shares with a reference table,
# and ensures supplementary columns are present (filled with NA if missing).
collapse_table <- function(
  con,                 # active database connection
  target_table,        # table to collapse
  reference_table,     # table to match columns with
  output_table = NULL, # optional: specify name of table saved to DB
  supplementary_cols = NULL # optional: columns to always include
) {
  message(paste0(
    "🔹 Starting collapse of table '", target_table,
    "' with reference '", reference_table, "'..."
  ))

  # Identify shared columns
  shared_cols <- intersect(
    dbListFields(con, target_table),
    dbListFields(con, reference_table)
  )
  message(paste0(
    "✅ Shared columns identified: ",
    paste(shared_cols, collapse = ", ")
  ))

  # --- Collapsing table ---
  query <- paste0(
    "SELECT ", paste(shared_cols, collapse = ", "),
    " FROM ", target_table, ";"
  )
  df_collapsed <- dbGetQuery(con, query)
  message(paste0(
    "✅ Table '", target_table, "' collapsed to ", length(shared_cols),
    " shared columns"
  ))

  # --- Add supplementary columns if missing ---
  if (!is.null(supplementary_cols)) {
    for (col in supplementary_cols) {
      if (!(col %in% names(df_collapsed))) {
        df_collapsed[[col]] <- NA
        message(paste0("➕ Added supplementary column: '", col, "' (all NA)"))
      }
    }
  }

  # Saving collapsed table back to DB
  output_table <- output_table %||% paste0(target_table, "_collapsed")
  dbWriteTable(con, output_table, df_collapsed, overwrite = TRUE)

  # Return df
  df_collapsed
}


# --- Final preprocessing pipeline ---
# Applies the full preprocessing pipeline to a given database table
# Steps:
# skip_align - TRUE:
#   1. Standardises column names (snake_case, rename map, date consolidation)
#   2. Standardises boolean-like/binary columns into consistent 0/1 encoding
# skip_align - FALSE:
#   3. Aligns target table column names to match reference using embeddings
#   4. Optionally collapses the table to match the schema of a reference table
preprocessing_pipeline <- function(
  con,                    # active database connection
  input_table,            # raw input table name
  output_table,           # optional: final processed table name saved to DB
  default_year = 2000,    # fallback year when constructing dates
  rename_map = NULL,      # optional: vector for manual column renames
  reference_table = NULL, # optional: collapse against this reference schema
  threshold = 0.75,       # optional: set threshold for cosine similarity
  skip_align = TRUE,      # optional: TRUE: skip column alignment
  supplementary_cols = NULL # NEW: columns to always include in output
) {
  # Default output name if none provided
  output_table <- output_table %||% paste0(input_table, "_processed")

  if (skip_align) {
    # --- Run preprocessing but skip alignment
    # Step 1: standardise booleans (overwrites same output_table)
    standardise_bools(
      con,
      table_name = input_table,
      output_table
    )

    # Step 2: clean + standardise column names
    prepare_column_names(
      con,
      table_name = output_table,
      default_year,
      rename_map,
      output_table
    )

  } else {
    # --- Skip column name preprocessing, just run alignment ---
    # Step 3: align columns with GloVe embeddings
    if (!skip_align && !is.null(reference_table)) {
      align_columns(
        con,
        reference_table,
        target_table = output_table,
        output_table,
        threshold = threshold
      )
    }
  }

  # Step 4: collapse with reference table (if provided)
  if (!is.null(reference_table)) {
    df <- collapse_table(
      con,
      target_table = output_table,
      reference_table,
      output_table,
      supplementary_cols = supplementary_cols # Pass through
    )
  } else {
    # If no collapsing, just read the current table
    df <- dbReadTable(con, output_table)
    # Add supplementary columns if needed
    if (!is.null(supplementary_cols)) {
      for (col in supplementary_cols) {
        if (!(col %in% names(df))) {
          df[[col]] <- NA
          message(paste0("➕ Added supplementary column: '", col, "' (all NA)"))
        }
      }
      dbWriteTable(con, output_table, df, overwrite = TRUE)
    }
  }

  message(
    paste0("✅ Preprocessing completed. Final table: '", output_table, "'")
  )

  # Return processed dataframe
  df
}
