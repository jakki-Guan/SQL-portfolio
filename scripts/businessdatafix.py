# scripts/fix_business_only.py
import sys, json, ast, pandas as pd

# Print the arguments to see what is being received
print(f"sys.argv: {sys.argv}")
print(f"Number of arguments: {len(sys.argv)}")
# Set the source and destination paths using absolute paths
src = 'C:/Users/Administrator/sql-portfolio/data/business.csv'
dst = 'C:/Users/Administrator/sql-portfolio/data/business_clean.csv'

def to_json_str(s):
    if pd.isna(s) or s == "" or s == "None":
        return None
    try:
        obj = ast.literal_eval(s)  # parse Python dict safely
        # Check if the dictionary is empty and return None if so
        if not obj:
            return None
        return json.dumps(obj, ensure_ascii=False)  # valid JSON
    except Exception as e:
        print(f"Error processing value: {s}, Error: {e}")
        return None

df = pd.read_csv(src, dtype=str, keep_default_na=False, na_values=[""])
if "attributes" in df.columns:
    df["attributes"] = df["attributes"].apply(to_json_str)
if "hours" in df.columns:
    df["hours"] = df["hours"].apply(to_json_str)
df.to_csv(dst, index=False)
print(f"Wrote {dst}")

