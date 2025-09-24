# --- Insurance Fraud Detection: XGBoost Model Training ---
# This script loads engineered features, splits data (with real/synthetic logic),
# encodes variables, applies sample weighting, trains XGBoost, fine-tunes on real data,
# and saves the final model.

library(DBI)
library(dplyr)
library(xgboost)
library(Matrix)
library(caret)

# --- 1. Load Data ---
con <- DBI::dbConnect(
  RSQLite::SQLite(),
  here::here("db", "insurance.db")
)
print(here::here("db", "insurance.db"))
df <- dbReadTable(con, "combined_featured")
dbDisconnect(con)

# --- 2. Remove Non-Feature Columns ---
# Remove 'source' and any columns not used for training
non_feature_cols <- c("source") # add others if needed
target_col <- "fraud_found"
feature_cols <- setdiff(names(df), c(non_feature_cols, target_col))

# --- 3. Split Data: Train/Validation/Test ---
set.seed(42)
real_idx <- which(df$source == "insurance_claims")
synthetic_idx <- which(df$source == "fraud_oracle")

n_real <- length(real_idx)
n_val <- floor(0.20 * n_real)   # 20% of real data for validation
n_test <- floor(0.10 * n_real)  # 10% of real data for testing
n_train_real <- n_real - n_val - n_test  # Remaining real data for training

# Shuffle real data indices for unbiased splitting
real_idx <- sample(real_idx)

# Assign indices for train, validation, and test sets
train_real_idx <- real_idx[1:n_train_real]
val_idx <- real_idx[(n_train_real + 1):(n_train_real + n_val)]
test_idx <- real_idx[(n_train_real + n_val + 1):(n_train_real + n_val + n_test)]

# Training set includes all synthetic data and most real data
train_idx <- c(synthetic_idx, train_real_idx)

# --- 4. Encode Categorical Variables ---
# XGBoost in R prefers numeric input, so use label encoding for factors/characters
encode_label <- function(x) {
  if (is.character(x) || is.factor(x)) {
    as.integer(as.factor(x))
  } else {
    x
  }
}
df_xgb <- as.data.frame(lapply(df[, feature_cols], encode_label))

# --- 5. Handle Missing Values ---
# Warn if missing values remain after imputation, and impute with -999
if (anyNA(df_xgb)) {
  warning("Missing values detected after feature engineering. Imputing with -999.")
  df_xgb[is.na(df_xgb)] <- -999
}

# --- 6. Prepare Data for XGBoost ---
# Create DMatrix objects for train, validation, and test sets
dtrain <- xgb.DMatrix(
  data = as.matrix(df_xgb[train_idx, ]),
  label = df[train_idx, target_col]
)
dval <- xgb.DMatrix(
  data = as.matrix(df_xgb[val_idx, ]),
  label = df[val_idx, target_col]
)
dtest <- xgb.DMatrix(
  data = as.matrix(df_xgb[test_idx, ]),
  label = df[test_idx, target_col]
)

# --- 7. Set Sample Weights (Real data weighted higher) ---
# Give real data higher weight to prioritize learning from real-world patterns
weights <- rep(1, nrow(df))
weights[train_real_idx] <- 3  # Real data gets 3x weight
setinfo(dtrain, "weight", weights[train_idx])

# --- 8. Handle Class Imbalance ---
# Compute scale_pos_weight for XGBoost if classes are imbalanced
n_pos <- sum(df[train_idx, target_col] == 1)
n_neg <- sum(df[train_idx, target_col] == 0)
scale_pos_weight <- n_neg / n_pos

# --- 9. Train XGBoost Model with Early Stopping ---
# Use early stopping on validation set to avoid overfitting
params <- list(
  objective = "binary:logistic",
  eval_metric = "auc",
  scale_pos_weight = scale_pos_weight,
  eta = 0.1,
  max_depth = 6,
  subsample = 0.8,
  colsample_bytree = 0.8
)

watchlist <- list(val = dval)
set.seed(42)
model <- xgb.train(
  params = params,
  data = dtrain,
  nrounds = 1000,
  watchlist = watchlist,
  early_stopping_rounds = 20,
  print_every_n = 10
)

# --- 10. Fine-tune on Real Data Only ---
# Continue training for a few rounds on real data only (lower learning rate)
# This helps the model adapt to real-world patterns after learning from synthetic data
dtrain_real <- xgb.DMatrix(
  data = as.matrix(df_xgb[train_real_idx, ]),
  label = df[train_real_idx, target_col]
)
setinfo(dtrain_real, "weight", rep(3, length(train_real_idx)))

params_finetune <- params
params_finetune$eta <- 0.02  # Lower learning rate for fine-tuning

model <- xgb.train(
  params = params_finetune,
  data = dtrain_real,
  nrounds = 50,
  xgb_model = model,
  watchlist = list(val = dval),
  early_stopping_rounds = 10,
  print_every_n = 10
)

# --- 11. Evaluate Model on Test Set ---
preds <- predict(model, dtest)

obs_raw <- df[test_idx, target_col]
obs <- as.character(obs_raw)
if (any(is.na(obs))) {
  warning("NAs found in test labels; dropping those rows for evaluation.")
  keep <- !is.na(obs)
  obs <- obs[keep]
  preds <- preds[keep]
}
keep <- obs %in% c("0", "1")
if (!all(keep)) {
  warning("Non-binary labels found in test set; dropping those rows.")
  obs <- obs[keep]
  preds <- preds[keep]
}
obs <- factor(obs, levels = c("0", "1"))

# Calculate predicted class (as factor)
pred_class <- factor(ifelse(preds > 0.5, "1", "0"), levels = c("0", "1"))

# Build the data frame as caret expects:
df_eval <- data.frame(
  obs = obs,           # true labels, factor
  pred = pred_class,   # predicted class, factor
  "1" = preds          # predicted probability for class "1"
)

auc <- caret::twoClassSummary(df_eval, lev = c("0", "1"))["ROC"]
cat(glue::glue("Test AUC: {round(auc, 4)}\n"))

# --- 12. Save Model ---
# Save the trained model to disk for future use
dir.create("models", showWarnings = FALSE)
xgb.save(model, "models/insurance_fraud_detector.model")
cat("✅ Model saved as models/insurance_fraud_detector.model\n")

# --- 13. Save Feature Names for Later Use ---
# Save the feature names used for training, for use in prediction scripts
saveRDS(feature_cols, "models/detector_features.rds")
cat("✅ Feature names saved for future use\n")