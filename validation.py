import pandas as pd
import sqlite3


# Load data
legacy = pd.read_csv("legacy_data.csv")
new = pd.read_csv("new_data.csv")

# Create SQLite database
conn = sqlite3.connect("insurance.db")

# Load CSV into SQL tables
legacy.to_sql("legacy_table", conn, if_exists="replace", index=False)
new.to_sql("new_table", conn, if_exists="replace", index=False)

print("\nData loaded into SQLite database")

print("=== Data Loaded ===")

# 1. Row count comparison
print("\nRow Count:")
print("Legacy:", len(legacy))
print("New:", len(new))

# 2. Merge datasets
merged = legacy.merge(new, on="policy_id", how="outer", suffixes=('_old', '_new'))

# SQL: Find mismatches
query = """
SELECT l.policy_id, l.premium AS old_premium, n.premium AS new_premium
FROM legacy_table l
JOIN new_table n ON l.policy_id = n.policy_id
WHERE l.premium != n.premium
"""

sql_mismatch = pd.read_sql(query, conn)

print("\nSQL Mismatch Results:")
print(sql_mismatch)

# 3. Find premium mismatches
mismatch = merged[
    (merged['premium_old'] != merged['premium_new']) &
    (~merged['premium_old'].isna()) &
    (~merged['premium_new'].isna())
]

# 4. Find missing in new
missing_in_new = merged[merged['premium_new'].isna()]

# 5. Find extra in new
extra_in_new = merged[merged['premium_old'].isna()]

# 6. Find duplicates in new
duplicates = new[new.duplicated(subset='policy_id', keep=False)]

print("\n=== Validation Results ===")

print("\nPremium Mismatches:")
print(mismatch[['policy_id', 'premium_old', 'premium_new']])

print("\nMissing in New System:")
print(missing_in_new[['policy_id']])

print("\nExtra in New System:")
print(extra_in_new[['policy_id']])

print("\nDuplicate Records in New System:")
print(duplicates)

# 7. Save report
report = pd.concat([mismatch, missing_in_new, extra_in_new])
report.to_csv("validation_report.csv", index=False)

print("\nValidation report saved as validation_report.csv")
print("\nSummary:")
print("Total mismatches:", len(mismatch))
print("Missing records:", len(missing_in_new))
print("Extra records:", len(extra_in_new))
print("Duplicates:", len(duplicates))