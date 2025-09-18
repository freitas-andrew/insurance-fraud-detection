library(dplyr)
library(lubridate)

# Importing the preprocessing pipeline function
source(here::here("src/utils/preprocessing_pipeline.R"))

# Establishing the connection to the DB
con <- dbConnect(RSQLite::SQLite(), here::here("db", "insurance.db"))

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

    # Dynamic binning of vehicle age so it matches fraud_oracle
    AgeOfVehicle_Bin = case_when(
      is.na(age_of_vehicle) ~ "new",
      age_of_vehicle <= 1 ~ "new",
      age_of_vehicle >= 2 & age_of_vehicle <= 7 ~
        paste0(age_of_vehicle, " years"),
      age_of_vehicle > 7 ~ "more than 7"
    ),

    # Days between policy bind and incident
    days_policy_claim = as.integer(
      difftime(
        suppressWarnings(dmy(incident_date)),
        suppressWarnings(dmy(policy_bind_date)),
        units = "days"
      )
    ),

    # Binned days_policy_claim (matches fraud_oracle bins)
    days_policy_claim = case_when(
      is.na(days_policy_claim) ~ "none",
      days_policy_claim >= 1 & days_policy_claim <= 7 ~ "1 to 7",
      days_policy_claim >= 8 & days_policy_claim <= 15 ~ "8 to 15",
      days_policy_claim >= 16 & days_policy_claim <= 30 ~ "15 to 30",
      days_policy_claim > 30 ~ "more than 30",
      TRUE ~ "none"
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
  is_male = "insured_sex"
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

# Aligning insurance_claims columns w/ fraud_oracle
preprocessing_pipeline(
  con,
  input_table = "insurance_claims_prepared",
  output_table = "insurance_claims_prepared",
  reference_table = "fraud_oracle_prepared",
  skip_align = FALSE
)
# Aligning fraud_oracle columns w/ insurance_claims
collapse_table(
  con,                 # active database connection
  target_table = "fraud_oracle_prepared",
  reference_table = "insurance_claims_prepared",
  output_table = "fraud_oracle_prepared"
)

dbDisconnect(con)