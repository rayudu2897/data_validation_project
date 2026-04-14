import pandas as pd

# Load data
legacy = pd.read_csv("legacy_data.csv")
new = pd.read_csv("new_data.csv")

print("=== Data Loaded ===")

# 1. Row count comparison
print("\nRow Count:")
print("Legacy:", len(legacy))
print("New:", len(new))

# 2. Merge datasets
merged = legacy.merge(new, on="policy_id", how="outer", suffixes=('_old', '_new'))

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