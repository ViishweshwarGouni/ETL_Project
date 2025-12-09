import os
import time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))


def load_nasa_to_supabase():
    csv_path = "../data/staged/nasa_cleaned.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing file: {csv_path}")

    df = pd.read_csv(csv_path)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["inserted_at"] = pd.to_datetime(df["inserted_at"]).dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    df = df.where(pd.notnull(df), None)
    batch_size = 1
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i : i + batch_size].to_dict("records")
        supabase.table("nasa_apod").insert(batch).execute()
        print(f"Inserted rows {i+1} → {min(i + batch_size, len(df))}")
        time.sleep(0.3)
    print("Finished loading NASA data.")


if __name__ == "__main__":
    load_nasa_to_supabase()