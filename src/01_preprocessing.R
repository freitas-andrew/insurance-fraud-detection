library(dplyr)
library(lubridate)
library(reticulate)

# Import the preprocessing pipeline function
source(here::here("src/utils/preprocessing_pipeline.R"))

db_path <- here::here("db", "insurance.db")

# Establish database connection
con <- DBI::dbConnect(RSQLite::SQLite(), db_path)

# --- Deriving variables shared with fraud_oracle for insurance_claims ---
# Estimating Claims History, deriving age of vehicle, and days_policy_claim
# Read the insurance_claims table
insurance_claims <- dbReadTable(con, "insurance_claims")

# Using assumed claim frequency of 0.08 claims per year (details in report)
claim_frequency_per_year <- 0.08

insurance_claims <- insurance_claims %>%
  mutate(
    # Ensure numeric types
    months_as_customer = as.numeric(months_as_customer),
    auto_year = as.numeric(auto_year),

    # Estimate past claims
    past_number_of_claims = round(
      (months_as_customer / 12) * claim_frequency_per_year,
      0
    ),

    # Vehicle age at incident
    age_of_vehicle = year(suppressWarnings(dmy(incident_date))) - auto_year,

    # Days between policy bind and incident
    days_policy_claim = as.integer(
      difftime(
        suppressWarnings(dmy(incident_date)),
        suppressWarnings(dmy(policy_bind_date)),
        units = "days"
      )
    )
  )

# Write the updated table back to the database
dbWriteTable(con, "insurance_claims", insurance_claims, overwrite = TRUE)
# Confirmation message
message(
  paste(
    "✅ Columns 'past_number_of_claims' and 'age_of_vehicle'",
    "were added/estimated successfully."
  )
)

# --- Preprocessing column names ---
# Renaming map for fraud_oracle
rename_map <- c(
  IncidentMonth = "Month",
  IncidentWeek = "WeekOfMonth",
  IncidentDay = "DayOfWeek",
  AutoMake = "Make",
  ClaimDay = "DayOfWeekClaimed",
  ClaimMonth = "MonthClaimed",
  ClaimWeek = "WeekOfMonthClaimed",
  FraudFound = "FraudFound_P",
  PoliceReport = "PoliceReportFiled"
)

# Preprocessing fraud_oracle columns
preprocessing_pipeline(
  con,
  input_table = "fraud_oracle",
  output_table = "fraud_oracle_prepared",
  rename_map = rename_map,
  skip_align = TRUE
)

# Renaming map for insurance_claims
rename_map <- c(
  marital_status = "insured_relationship",
  witness_present = "witnesses",
  total_claim = "total_claim_amount",
  annual_premium = "policy_annual_premium"
)

# Preprocessing insurance_claims columns
preprocessing_pipeline(
  con,
  input_table = "insurance_claims",
  output_table = "insurance_claims_prepared",
  rename_map = rename_map,
  skip_align = TRUE
)

# --- ALigning tables ---
# (done separately so matching is improved)

supplementary_cols <- c(
  "vehicle_price", "annual_premium", "total_claim", "claim_date"
)

# Aligning insurance_claims columns w/ fraud_oracle
preprocessing_pipeline(
  con,
  input_table = "insurance_claims_prepared",
  output_table = "insurance_claims_prepared",
  reference_table = "fraud_oracle_prepared",
  skip_align = FALSE,
  supplementary_cols = supplementary_cols
)
# Aligning fraud_oracle columns w/ insurance_claims
collapse_table(
  con,
  target_table = "fraud_oracle_prepared",
  reference_table = "insurance_claims_prepared",
  output_table = "fraud_oracle_prepared",
  supplementary_cols = supplementary_cols
)

# --- Standardise schema using Python ---
# Use the project's .venv for Python
reticulate::use_virtualenv(here::here(".venv"), required = TRUE)
standardise_schema <- reticulate::import_from_path(
  "standardise_schema",
  path = here::here("src/utils")
)

# Standardise fraud_oracle_prepared
standardise_schema$standardise_schema(
  db_path = db_path,
  tables = list("fraud_oracle_prepared"),
  ref_table = "fraud_oracle_prepared",
  output_table = "fraud_oracle_prepared"
)

# Standardise insurance_claims_prepared
standardise_schema$standardise_schema(
  db_path = db_path,
  tables = list("insurance_claims_prepared"),
  ref_table = "fraud_oracle_prepared",
  output_table = "insurance_claims_prepared"
)

# --- Impute missing values and save as combined_imputed ---
source(here::here("src/utils/impute_missing.R"))

# Specify supplementary columns (not to impute in first pass)

# Impute missing values and get the combined imputed dataframe
combined_imputed <- impute_missing_data(
  con,
  table1 = "fraud_oracle_prepared",
  table2 = "insurance_claims_prepared",
  supplementary_cols = supplementary_cols,
  target_col = "fraud_found"
)

# Save the imputed combined dataset to the database
dbWriteTable(
  con,
  "combined_imputed",
  combined_imputed,
  overwrite = TRUE
)
message("✅ Table 'combined_imputed' written to database successfully.")

dbDisconnect(con)