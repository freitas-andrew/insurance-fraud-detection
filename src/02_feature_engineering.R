library(dplyr)
library(glue)

# Load fully cleaned data
con <- DBI::dbConnect(
  RSQLite::SQLite(),
  here::here("db", "insurance.db")
)
df <- DBI::dbReadTable(con, "combined_imputed")

# Function to convert bins in text format to numeric midpoints/bounds,
# To derive features later.
extract_midpoint <- function(bin) {
  # Handle bins like "20000-29000"
  if (grepl("-", bin)) {
    midpoint <- as.numeric(unlist(strsplit(bin, "-")))
    return(mean(midpoint))
  }
  # Handle bins like ">69000"
  if (grepl("^>", bin)) {
    return(as.numeric(gsub(">", "", bin)))  # Use lower bound (i..e, 69000)
  }
  # Handle bins like "<20000"
  if (grepl("^<", bin)) {
    return(as.numeric(gsub("<", "", bin)))  # Use upper bound (i.e., 2000)
  }
  NA_real_
}
df <- df %>%
  mutate(
    vehicle_price_num = sapply(as.character(vehicle_price), extract_midpoint),
    number_of_cars_num = sapply(as.character(number_of_cars), extract_midpoint)
  )

# --- A. Ratios and Buckets ---
# Claim-to-premium ratio: Indicates if the claim is disproportionately high
# relative to premium paid.
df$claim_to_premium_ratio <- round(df$total_claim / df$annual_premium, 2)
message("✅ Feature created: claim_to_premium_ratio")

# Claim-to-vehicle value ratio: High values may indicate inflated claims.
df$claim_to_vehicle_value_ratio <- round(
  df$total_claim / df$vehicle_price_num, 2
)
message("✅ Feature created: claim_to_vehicle_value_ratio (using bin midpoints)")

# Deductible-to-claim ratio: Small claims above deductible may be suspicious.
df$deductible_to_claim_ratio <- round(df$deductible / df$total_claim, 2)
message("✅ Feature created: deductible_to_claim_ratio")

# --- B. Interactions ---
# Young driver, high claim: Young drivers with high claims are higher risk.
df$young_driver_high_claim <- (
  df$age < 25 &
    (round(df$total_claim / df$vehicle_price_num, 2) > 0.8)
)
message("✅ Feature created: young_driver_high_claim")

# Single, late reporting: Singles reporting late may be higher risk.
df$single_late_reporting <- (
  df$marital_status == "Single" &
    df$days_policy_claim > 7
)
message("✅ Feature created: single_late_reporting")

# High claim, no police report: Large claims without police involvement
# are suspicious.
high_claim_threshold <- quantile(
  df$total_claim, 0.95, na.rm = TRUE
)
df$high_claim_no_police_report <- (
  df$total_claim > high_claim_threshold &
    df$police_report == 0
)
message("✅ Feature created: high_claim_no_police_report")

# Witness present, no police report: Inconsistency flag.
df$witness_no_police_report <- (
  df$witness_present == 1 &
    df$police_report == 0
)
message("✅ Feature created: witness_no_police_report")

# --- C. Flags ---
# High claim flag: Top 5% of claims.
df$high_claim_flag <- df$total_claim > high_claim_threshold
message("✅ Feature created: high_claim_flag")

# Late reporting flag: Claims reported more than 30 days after incident.
df$late_reporting_flag <- df$days_policy_claim > 30
message("✅ Feature created: late_reporting_flag")

# --- D. Combined Features ---
# Claim per car: High values may indicate suspicious claims.
df$claim_per_car <- ifelse(
  df$number_of_cars_num > 0,
  round(df$total_claim / df$number_of_cars_num, 2),
  NA
)
message("✅ Feature created: claim_per_car")

# Premium per car: Useful for normalizing premium by household fleet size.
df$premium_per_car <- ifelse(
  df$number_of_cars_num > 0,
  round(df$annual_premium / df$number_of_cars_num, 2),
  NA
)
message("✅ Feature created: premium_per_car")

# Output table name
table_name <- "combined_featured"

# --- Save engineered features back to the database ---
DBI::dbWriteTable(
  con,
  table_name,
  df,
  overwrite = TRUE
)

# Drop intermediary columns
df <- df %>%
  select(-vehicle_price_num, -number_of_cars_num)

message(glue("✅ Feature engineering complete: '{table_name}' table saved."))

DBI::dbDisconnect(con)