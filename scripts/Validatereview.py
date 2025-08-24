import pandas as pd
from datetime import datetime

# ==============================================================================
# Step 1: Load the dataset
# Assumes the 'review.csv' file has been uploaded to the Colab environment.
# ==============================================================================
try:
    df = pd.read_csv('C:/Users/Administrator/sql-portfolio/data/review.csv', low_memory=False)
    print("Successfully loaded the review.csv file.")
    print("Initial DataFrame shape:", df.shape)
except FileNotFoundError:
    print("Error: review.csv not found. Please make sure the file is uploaded to the Colab session.")
    # Exit if file is not found
    exit()

# ==============================================================================
# Step 2: Reformat and clean the date column
# We will parse the existing date string and then reformat it to only include
# the date portion (YYYY-MM-DD), effectively dropping the time component.
# ==============================================================================
print("\nReformatting and cleaning date column...")

# Explicitly replace the problematic "0" with a string that can be treated as null
df['date'] = df['date'].replace('0', None)

# Create a new column by parsing the date and then reformatting it.
# This approach handles any minor inconsistencies while ensuring a uniform output format.
df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')

# After reformatting, we drop any rows that had an invalid date
# that could not be parsed.
initial_count = len(df)
df.dropna(subset=['date'], inplace=True)
dropped_count = initial_count - len(df)

if dropped_count > 0:
    print(f"Found and dropped {dropped_count} rows with invalid dates during reformatting.")
else:
    print("All date entries were successfully reformatted. No invalid data found.")

# ==============================================================================
# Step 3: Prepare the final cleaned DataFrame and save it
# ==============================================================================
print(f"\nFinal cleaned DataFrame shape: {df.shape}")

# Save the cleaned data to a new CSV file
output_path = 'C:/Users/Administrator/sql-portfolio/data/review_clean.csv'
df.to_csv(output_path, index=False)
print(f"Successfully wrote the cleaned data to '{output_path}'.")
