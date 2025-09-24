library(DBI)
library(dplyr)
library(mice)
library(here)

impute_missing_data <- function(
  con,
  table1,
  table2,
  supplementary_cols,
  target_col = "fraud_found"
) {
  df1 <- dbReadTable(con, table1)
  df2 <- dbReadTable(con, table2)

  df1$source <- table1
  df2$source <- table2

  # --- Feature Engineering: report_lag and clean-up ---
  for (df_name in c("df1", "df2")) {
    df <- get(df_name)
    df$claim_date[df$claim_date == ""] <- NA
    df$incident_date[df$incident_date == ""] <- NA
    df$claim_date <- as.numeric(df$claim_date)
    df$incident_date <- as.numeric(df$incident_date)
    df$report_lag <- df$claim_date - df$incident_date
    df$report_lag[df$report_lag < 0] <- NA  # Set negative lags to NA
    df$claim_date <- NULL
    df$incident_date <- NULL
    assign(df_name, df)
  }

  # Ensure numeric columns are truly numeric and blanks are NA
  num_cols <- c("annual_premium", "total_claim", "claim_date", "deductible")
  for (col in num_cols) {
    if (col %in% names(df1)) {
      df1[[col]][df1[[col]] == ""] <- NA
      df1[[col]] <- as.numeric(df1[[col]])
    }
    if (col %in% names(df2)) {
      df2[[col]][df2[[col]] == ""] <- NA
      df2[[col]] <- as.numeric(df2[[col]])
    }
  }

  # Ensure categorical columns are character before merging
  char_cols <- c("days_policy_claim", "vehicle_price")
  for (col in char_cols) {
    if (col %in% names(df1)) {
      df1[[col]] <- as.character(df1[[col]])
    }
    if (col %in% names(df2)) {
      df2[[col]] <- as.character(df2[[col]])
    }
  }

  # Ensure past_number_of_claims is integer in both tables before merging
  if ("past_number_of_claims" %in% names(df1)) {
    df1$past_number_of_claims[df1$past_number_of_claims == ""] <- NA
    df1$past_number_of_claims <- as.integer(df1$past_number_of_claims)
  }
  if ("past_number_of_claims" %in% names(df2)) {
    df2$past_number_of_claims[df2$past_number_of_claims == ""] <- NA
    df2$past_number_of_claims <- as.integer(df2$past_number_of_claims)
  }

  # Merge both datasets for joint imputation
  combined <- bind_rows(df1, df2)

  # Exclude supplementary columns and target for first imputation (MAR)
  impute_cols <- setdiff(
    names(combined),
    c(supplementary_cols, target_col)
  )
  combined_mar <- combined %>% select(all_of(impute_cols))

  # --- Set column types for MICE ---
  # 1. Set binary columns as factors with two levels (0, 1)
  binary_cols <- c("is_male", "witness_present", "police_report")
  for (col in binary_cols) {
    if (col %in% names(combined_mar)) {
      combined_mar[[col]] <- factor(combined_mar[[col]], levels = c(0, 1))
    }
  }

  # 2. Set other categorical columns as factors (unordered)
  categorical_cols <- c(
    "marital_status", "number_of_cars", "past_number_of_claims",
    "vehicle_price", "auto_make", "days_policy_claim"
  )
  for (col in categorical_cols) {
    if (col %in% names(combined_mar)) {
      combined_mar[[col]] <- as.factor(combined_mar[[col]])
    }
  }

  # 3. Set all character/factor columns as factors (catch any missed above)
  cat_cols <- names(combined_mar)[
    sapply(combined_mar, function(x) is.character(x) | is.factor(x))
  ]
  for (col in cat_cols) {
    n_unique <- length(unique(combined_mar[[col]]))
    if (n_unique < 50) {
      combined_mar[[col]] <- as.factor(combined_mar[[col]])
    }
  }

  # 4. Set binned columns as ordered factors from highest to lowest
  make_ordered_bin <- function(x) {
    bins <- unique(na.omit(as.character(x)))
    bin_order <- order(
      sapply(bins, function(bin) {
        if (tolower(bin) == "none") return(-Inf)
        nums <- as.numeric(unlist(regmatches(bin, gregexpr("\\d+", bin))))
        if (grepl("^>", bin)) return(max(nums, na.rm = TRUE) + 0.1)
        if (grepl("^<", bin)) return(min(nums, na.rm = TRUE) - 0.1)
        if (length(nums) > 0) return(mean(nums))
        NA
      }),
      decreasing = TRUE
    )
    bins <- bins[bin_order]
    factor(x, levels = bins, ordered = TRUE)
  }
  binned_cols <- c(
    "number_of_cars", "past_number_of_claims",
    "vehicle_price", "days_policy_claim"
  )
  for (col in binned_cols) {
    if (col %in% names(combined_mar)) {
      combined_mar[[col]] <- make_ordered_bin(combined_mar[[col]])
    }
  }

  cat("🚦 Starting Step 1: Impute MAR values with MICE...\n")
  set.seed(123)
  imputed_mar <- mice(
    combined_mar, m = 5, maxit = 10, seed = 123
  )
  completed_mar <- complete(imputed_mar, 1)

  # Add back supplementary columns for second imputation step
  completed_full <- completed_mar %>%
    bind_cols(combined %>% select(all_of(supplementary_cols)))

  cat("🚦 Starting Step 2: Impute supplementary columns with MICE...\n")
  set.seed(456)
  imputed_full <- mice(
    completed_full, m = 5, maxit = 10, seed = 456
  )
  completed_final <- complete(imputed_full, 1)

  # Add back the target variable after imputation
  completed_final[[target_col]] <- combined[[target_col]]

  cat("✅ Imputation complete. Final data shape:\n")
  print(dim(completed_final))

  # Linear model example with pooled results (optional, for calculating C.I.)
  try({
    fit <- with(imputed_full, lm(fraud_found ~ .))
    print(summary(pool(fit)))
  }, silent = TRUE)

  invisible(completed_final)
}