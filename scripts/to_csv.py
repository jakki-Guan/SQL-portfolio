import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__),"..","data")


def to_csv_jsonl(json_path, csv_path, use_chunks=False, chunksize=250000, nrows=None):
    """
    Converts a JSON file to a CSV file.

    This version includes explicit formatting for list-based columns to
    ensure they are correctly processed by the PostgreSQL COPY command.

    Args:
        json_path (str): Path to the input JSON file.
        csv_path (str): Path to the output CSV file.
        use_chunks (bool): Whether to process the file in chunks.
        chunksize (int): The number of lines to process in each chunk.
        nrows (int): The number of lines to read from the JSON file.
    """
    print(f"Converting JSON file: {json_path}")
    src_path = os.path.join(DATA_DIR, json_path)
    dst_path = os.path.join(DATA_DIR, csv_path)

    os.makedirs(os.path.dirname(dst_path), exist_ok=True)

    if use_chunks:
        first = True
        for chunk in pd.read_json(src_path, lines=True, chunksize=chunksize):
            # Special handling for user data to format array columns correctly
            if 'user.csv' in dst_path:
                # Convert list columns to a simple comma-separated string
                # This ensures the data can be correctly loaded into a staging table
                if 'friends' in chunk.columns:
                    chunk['friends'] = chunk['friends'].apply(
                        lambda x: ','.join(map(str, x)) if isinstance(x, list) and x else ''
                    )
                if 'elite' in chunk.columns:
                    chunk['elite'] = chunk['elite'].apply(
                        lambda x: ','.join(map(str, x)) if isinstance(x, list) and x else ''
                    )

            if nrows and len(chunk) > nrows:
                chunk = chunk.iloc[:max(0, nrows - 1)]
                first = False

            chunk.to_csv(dst_path, index=False, mode="a" if not first else "w", header=first)

            if nrows and len(chunk) > nrows:
                break

            first = False

    else:
        df = pd.read_json(src_path, lines=True, nrows=nrows)

        # Special handling for user data to format array columns correctly
        if 'user.csv' in dst_path:
            if 'friends' in df.columns:
                df['friends'] = df['friends'].apply(
                    lambda x: ','.join(map(str, x)) if isinstance(x, list) and x else ''
                )
            if 'elite' in df.columns:
                df['elite'] = df['elite'].apply(
                    lambda x: ','.join(map(str, x)) if isinstance(x, list) and x else ''
                )
        if 'review.csv' in dst_path:
            # Coerce invalid dates (like '0') to NaT, which will be written as empty strings to CSV
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

        df.to_csv(dst_path, index=False)

    print(f"Done converting to CSV: {dst_path}")


def main():
    to_csv_jsonl("yelp_academic_dataset_business.json", "business.csv")
    to_csv_jsonl("yelp_academic_dataset_user.json", "user.csv", use_chunks=True, chunksize=200000)
    to_csv_jsonl("yelp_academic_dataset_review.json", "review.csv", use_chunks=True, chunksize=200000)
    to_csv_jsonl("yelp_academic_dataset_tip.json", "tip.csv")
    to_csv_jsonl("yelp_academic_dataset_checkin.json", "checkin.csv")

if __name__ == "__main__":
    main()
