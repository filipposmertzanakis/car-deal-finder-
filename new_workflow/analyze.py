import pandas as pd
from supabase import create_client
import numpy as np
from datetime import datetime, timezone
import argparse
import logging

from dotenv import load_dotenv
import os
load_dotenv()

# --- Setup logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# --- Supabase setup ---
***REMOVED*** = os.getenv("***REMOVED***")
***REMOVED*** = os.getenv("***REMOVED***")
supabase = create_client(***REMOVED***, ***REMOVED***)

# --- Model configuration ---
MODELS = {
    "Yaris": "yaris_price_stats_by_mileage",
    "Corsa": "corsa_price_stats_by_mileage",
    "Swift": "swift_price_stats_by_mileage",
    "208": "208_price_stats_by_mileage",
    "Clio": "clio_price_stats_by_mileage",
    "i10" : "i10_price_stats_by_mileage",
    "i20" : "i20_price_stats_by_mileage",
    "C3": "c3_price_stats_by_mileage",
    "Fiesta": "fiesta_price_stats_by_mileage",
    "Polo": "polo_price_stats_by_mileage",
}

# --- Step 1: Fetch listings for a specific model ---
def fetch_listings(model):
    logger.info(f"Fetching {model} listings from Supabase...")
    response = supabase.table("listings").select("*").eq("model", model).execute()
    if not response.data:
        logger.warning(f"No listings found for {model}")
        return pd.DataFrame()
    logger.info(f"Fetched {len(response.data)} listings for {model}")
    return pd.DataFrame(response.data)

# --- Step 2: Clean the data ---
def clean_data(df, model):
    if df.empty:
        logger.warning(f"No data to clean for {model}")
        return df
    
    logger.info(f"Cleaning {model} data...")
    df = df.dropna(subset=["mileage", "price", "year", "source_id"])
    df = df.drop_duplicates(subset=["source_id"])
    
    # Remove price outliers using IQR per year
    clean_df = pd.DataFrame()
    for year, group in df.groupby("year"):
        Q1 = group["price"].quantile(0.25)
        Q3 = group["price"].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        group_filtered = group[(group["price"] >= lower) & (group["price"] <= upper)]
        clean_df = pd.concat([clean_df, group_filtered])
    
    logger.info(f"Cleaned down to {len(clean_df)} listings for {model}")
    return clean_df

# --- Step 3: Compute statistics per mileage bin and year ---
def compute_stats_by_year_and_mileage(df, model, bin_size=25000, min_count_threshold=5):
    if df.empty:
        logger.warning(f"No data to compute stats for {model}")
        return pd.DataFrame()
    
    logger.info(f"Computing stats for {model}...")
    # Use cut to create mileage bins
    df["mileage_bin"] = pd.cut(
        df["mileage"],
        bins=range(0, int(df["mileage"].max() + bin_size), bin_size),
        right=False,
        labels=[f"{i}-{i + bin_size}" for i in range(0, int(df["mileage"].max()), bin_size)]
    )

    grouped = df.groupby(["year", "mileage_bin"]).agg(
        median_price=("price", "median"),
        p25_price=("price", lambda x: np.percentile(x, 25)),
        p75_price=("price", lambda x: np.percentile(x, 75)),
        min_price=("price", "min"),
        max_price=("price", "max"),
        count=("price", "count"),
    ).reset_index()

    # Filter out sparse bins
    grouped = grouped[grouped["count"] >= min_count_threshold]
    grouped["last_updated"] = datetime.now(timezone.utc).isoformat()

    logger.info(f"Computed stats for {len(grouped)} bins for {model}")
    return grouped

# --- Step 4: Upload to Supabase ---
def upload_stats(stats_df, model, table_name):
    if stats_df.empty:
        logger.warning(f"No stats to upload for {model}")
        return

    logger.info(f"Uploading stats to {table_name}...")
    for _, row in stats_df.iterrows():
        data = {
            "year": int(row["year"]),
            "mileage_bin": str(row["mileage_bin"]),
            "median_price": float(row["median_price"]),
            "p25_price": float(row["p25_price"]),
            "p75_price": float(row["p75_price"]),
            "min_price": float(row["min_price"]),
            "max_price": float(row["max_price"]),
            "count": int(row["count"]),
            "last_updated": row["last_updated"],
        }

        try:
            response = supabase.table(table_name).insert([data]).execute()
            logger.info(f"Inserted: {data}")
        except Exception as e:
            logger.error(f"Error inserting row for {model}: {data}, Error: {e}")

# --- Process a single model ---
def process_model(model, table_name):
    logger.info(f"Processing model: {model}")
    listings_df = fetch_listings(model)
    if listings_df.empty:
        return
    
    cleaned_df = clean_data(listings_df, model)
    if cleaned_df.empty:
        return
    
    stats_df = compute_stats_by_year_and_mileage(cleaned_df, model)
    if stats_df.empty:
        return
    
    upload_stats(stats_df, model, table_name)

# --- Main ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze car listings and compute price statistics")
    parser.add_argument("--model", choices=["Yaris", "Corsa", "Swift", "208" , "Clio" , "C3" , "i10" , "i20" , "Fiesta" , "Polo" , "all"], default="all", help="Model to process (or 'all' for all models)")
    args = parser.parse_args()

    if args.model == "all":
        for model, table_name in MODELS.items():
            process_model(model, table_name)
    else:
        table_name = MODELS.get(args.model)
        if table_name:
            process_model(args.model, table_name)
        else:
            logger.error(f"Invalid model: {args.model}")

    logger.info("Processing complete.")