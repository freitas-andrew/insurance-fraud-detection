import pandas as pd
import numpy as np
import re
from datetime import datetime
import sqlite3

def parse_age(val):
    """
    Parse age values, handling 'new', numerics, and missing values.
    """
    if pd.isna(val):
        return np.nan
    if isinstance(val, (int, float)):
        return int(val)
    val_str = str(val).strip().lower()
    if val_str == 'new':
        return 0
    m = re.match(r'(\d+)', val_str)
    if m:
        return int(m.group(1))
    return np.nan

def get_bins_and_edges(con, ref_table, column):
    """
    Retrieve unique bin labels and parse them into intervals for binning.
    """
    # Query all unique, non-null values for the column from the reference table
    query = f"SELECT DISTINCT {column} FROM {ref_table} WHERE {column} IS NOT NULL"
    bins = pd.read_sql_query(query, con)[column].dropna().unique()

    concise_bins = []
    parsed = []

    for b in bins:
        if pd.isna(b):
            continue
        # Clean up and lowercase the bin label
        s = re.sub(r'\s+', ' ', str(b).strip().lower())

        # Handle special cases for bin labels
        if s == "none":
            concise_bins.append('none')
            parsed.append(('none', None, None))

        # Handle "more than X" bins
        elif re.search(r'more\s+than\s+(\d+)', s):
            num = re.search(r'more\s+than\s+(\d+)', s).group(1)
            concise_bins.append(f">{num}")
            parsed.append((f">{num}", int(num), np.inf))

        # Handle "less than X" bins
        elif re.search(r'less\s+than\s+(\d+)', s):
            num = re.search(r'less\s+than\s+(\d+)', s).group(1)
            concise_bins.append(f"<{num}")
            parsed.append((f"<{num}", -np.inf, int(num)))

        # Handle "X to Y" bins
        elif 'to' in s:
            nums = re.findall(r'\d+', s)
            if len(nums) == 2:
                concise_bins.append(f"{nums[0]}-{nums[1]}")
                parsed.append((f"{nums[0]}-{nums[1]}", int(nums[0]), int(nums[1])))

        # Handle numeric bins or fallback to string
        else:
            try:
                val = float(s)
                concise_label = str(int(val)) if val.is_integer() else str(val)
                concise_bins.append(concise_label)
                parsed.append((concise_label, val, val))
            except:
                concise_bins.append(s)
                parsed.append((s, None, None))

    # Sort bins, putting 'none' last for consistency
    concise_bins = sorted(
        set(concise_bins),
        key=lambda x: (
            x == 'none',
            int(re.sub(r'\D', '', x)) if re.sub(r'\D', '', x) else 0
        )
    )
    return concise_bins, parsed

def bin_column(df, column, bins, parsed_bins):
    """
    Bin a column's values according to provided bins and parsed intervals.
    """
    if column not in df.columns:
        return df

    bins_set = set(bins)

    def assign_bin(val):
        # Return 'none' for missing values
        if pd.isna(val):
            return 'none'

        # Clean and lowercase value for matching
        val_str = str(val).strip().lower()

        # Try to match special bin patterns
        m = re.search(r'more\s+than\s+(\d+)', val_str)
        if m:
            concise_val = f">{m.group(1)}"
        elif re.search(r'less\s+than\s+(\d+)', val_str):
            concise_val = f"<{re.search(r'less\s+than\s+(\d+)', val_str).group(1)}"
        elif re.search(r'(\d+)\s*to\s*(\d+)', val_str):
            nums = re.search(r'(\d+)\s*to\s*(\d+)', val_str)
            concise_val = f"{nums.group(1)}-{nums.group(2)}"
        elif re.search(r'(\d+)\s*vehicles?', val_str):
            concise_val = re.search(r'(\d+)\s*vehicles?', val_str).group(1)
        elif val_str == "none":
            concise_val = "none"
        else:
            concise_val = val_str

        # If already a known bin, return it
        if concise_val in bins_set:
            return concise_val

        # Try to convert to float for interval binning
        try:
            v = float(concise_val)
        except Exception:
            return 'none'

        # Assign bin by interval (e.g., for numeric ranges)
        for label, low, high in parsed_bins:
            if label == 'none':
                continue
            if low is not None and high is not None and low <= v <= high:
                return label

        # Handle open-ended bins (e.g., >X or <Y)
        for label, low, high in parsed_bins:
            if label.startswith(">") and low is not None and v > low:
                return label
        for label, low, high in parsed_bins:
            if label.startswith("<") and high is not None and v < high:
                return label

        # Fallback if no bin matched
        return 'none'

    # Apply binning function to the column
    df[column] = df[column].apply(assign_bin)
    return df

def binarise_column(df, column):
    """
    Binarise a column to 0/1, treating NA/0 as 0 and anything else as 1.
    """
    if column in df.columns:
        unique_vals = set(df[column].dropna().unique())
        if unique_vals <= {0, 1}:
            return df
        df[column] = df[column].apply(lambda x: 0 if pd.isna(x) or x == 0 else 1)
    return df

def infer_date_fmt(series):
    """
    Infer the date format for a pandas Series of date strings.
    """
    # Try several common date formats
    formats = ["%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y"]

    for val in series.dropna():
        val_str = str(val).strip()
        for fmt in formats:
            try:
                dt = datetime.strptime(val_str, fmt)
                # Heuristic: if day > 12, likely day-first format
                if fmt in ("%d/%m/%Y", "%d-%m-%Y") and dt.day > 12:
                    return fmt
                # If month > 12, likely month-first format
                if fmt == "%m/%d/%Y" and dt.month > 12:
                    return fmt
                # ISO format
                if fmt == "%Y-%m-%d":
                    return fmt
            except Exception:
                continue
    # Default to day/month/year if nothing else matches
    return "%d/%m/%Y"

def convert_to_unix(series):
    """
    Convert a pandas Series of date strings to unix days using inferred format.
    """
    if pd.api.types.is_numeric_dtype(series):
        return series.astype('Int64')
    fmt = infer_date_fmt(series)
    def parse(val):
        if pd.isna(val):
            return np.nan
        try:
            dt = datetime.strptime(str(val).strip(), fmt)
            return (dt - datetime(1970, 1, 1)).days
        except Exception:
            return np.nan
    return series.apply(parse)

def standardise_marital(df):
    """
    Standardise the marital_status column to a fixed set of values.
    """
    target_set = {"Married", "Single", "Divorced", "Widow"}
    insurance_claims_set = {
        'husband', 'wife', 'unmarried', 'not-in-family', 'own-child', 'other-relative'
    }
    mapping = {
        'husband': 'Married',
        'wife': 'Married',
        'unmarried': 'Single',
        'not-in-family': 'Single',
        'own-child': 'Single',
        'other-relative': 'Single'
    }
    if 'marital_status' in df.columns:
        uniques = set(df['marital_status'].dropna().unique())
        if uniques <= target_set:
            return df
        if uniques & insurance_claims_set:
            present_mapping = {k: v for k, v in mapping.items() if k in uniques}
            df['marital_status'] = (
                df['marital_status']
                .str.strip().str.lower()
                .map(present_mapping)
            )
        df.loc[~df['marital_status'].isin(target_set), 'marital_status'] = np.nan
    return df

def cast_to_schema(df, schema):
    """
    Cast DataFrame columns to types specified in schema dict.
    Only casts columns present in both df and schema.
    For bool: ensures 0/1 integer (handles 0.0/1.0/True/False/0/1).
    """
    for col, typ in schema.items():
        if col in df.columns:
            if typ == "int":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif typ == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
            elif typ == "bool":
                # Cast to int first to handle 0.0/1.0, then to Int64
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
                df[col] = df[col].apply(lambda x: 1 if x != 0 else 0).astype("Int64")
            elif typ == "Int64":
                df[col] = df[col].astype(float).astype("Int64")
            elif typ == "str":
                df[col] = df[col].astype(str)
    return df

# Pre-defined schema
schema = {
    "age": "int",
    "age_of_vehicle": "int",
    "annual_premium": "float",
    "auto_make": "str",
    "days_policy_claim": "str",
    "claim_date": "int",
    "deductible": "int",
    "incident_date": "int",
    "is_male": "bool",
    "marital_status": "str",
    "number_of_cars": "str",
    "past_number_of_claims": "str",
    "police_report": "bool",
    "policy_number": "int",
    "total_claim": "int",
    "vehicle_price": "str",
    "witness_present": "bool",
    "fraud_found": "bool"
}

def standardise_schema(db_path, tables, ref_table="fraud_oracle_prepared", output_table=None):
    """
    Main pipeline: standardise and clean all relevant columns for a list of tables.
    Opens and closes its own SQLite connection.
    """
    con = sqlite3.connect(db_path)
    for table in tables:
        # --- Load table ---
        df = pd.read_sql_query(f"SELECT * FROM {table}", con)

        # --- Drop policy_number and order columns alphabetically ---
        if 'policy_number' in df.columns:
            df = df.drop(columns=['policy_number'])
        df = df[sorted(df.columns)]

        # --- Standardise and clean age and age_of_vehicle columns ---
        for col in ['age', 'age_of_vehicle']:
            if col in df.columns:
                df[col] = df[col].apply(parse_age).astype('Int64')
        print(f"✅ Standardisation of age columns in '{table}' completed.")

        # --- Standardise marital_status column ---
        df = standardise_marital(df)
        print(f"✅ Standardisation of marital_status in '{table}' completed.")

        # --- Standardise all pre-defined binned columns ---
        for col, special in [
            ('days_policy_claim', False),
            ('number_of_cars', True),
            ('past_number_of_claims', False),
            ('vehicle_price', False)
        ]:
            # Only bin if column exists and, for vehicle_price, if it has any non-null values
            if col in df.columns and (col != 'vehicle_price' or df[col].notna().sum() > 0):
                # Get bin labels and intervals from reference table
                bins, parsed_bins = get_bins_and_edges(con, ref_table, col)
                # For number_of_cars, ensure bins for '1' and '2' always exist
                if col == "number_of_cars":
                    for v in ["1", "2"]:
                        if v not in bins:
                            bins.append(v)
                            parsed_bins.append((v, int(v), int(v)))
                # Bin the column values
                df = bin_column(df, col, bins, parsed_bins)
        print(f"✅ Standardisation of bins for columns in '{table}' completed.")

        # --- Binarise witness_present column ---
        df = binarise_column(df, column='witness_present')
        print(f"✅ Binarisation of witness_present in '{table}' completed.")

        # --- Standardise incident_date and claim_date columns to unix timestamp ---
        for col in ['incident_date', 'claim_date']:
            if col in df.columns:
                df[col] = convert_to_unix(df[col])
        print(f"✅ Date columns standardised in '{table}'.")

        # --- Cast columns to schema types ---
        df = cast_to_schema(df, schema)

        # --- Dynamically determine output table name ---
        if output_table is not None:
            out_table = output_table
        else:
            # If table ends with _prepared, replace with _standardised
            if table.endswith("_prepared"):
                out_table = table[:-9] + "_standardised"
            else:
                out_table = table + "_standardised"

        # --- Save the cleaned and standardised table ---
        df.to_sql(out_table, con, if_exists='replace', index=False)
        print(f"✅ Table '{out_table}' saved.")

    con.close()