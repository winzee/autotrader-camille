"""One-off migration: add last_scrape_timestamp and is_deleted columns."""
import pandas as pd

CSV = "used_suv_listings.csv"

df = pd.read_csv(CSV)
df["last_scrape_timestamp"] = "2026-03-28"
df["is_deleted"] = None
df.to_csv(CSV, index=False)

print(f"Done — added last_scrape_timestamp (set to 2026-03-28) and is_deleted (null) to {len(df)} rows.")
