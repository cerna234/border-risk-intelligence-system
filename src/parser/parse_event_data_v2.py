"""
Border Risk Intelligence System
This script processes conflict event data, filters it geographically and temporally,
and engineers spatial and temporal risk features for analysis.

Author: Miguel Cerna Benitez
Program: MS National Cyber Security Studies
Project Context: Developed as part of an academic competition sponsored by NGA and T-REX
Date: 2026


"""

import pandas as pd
import json
import numpy as np
from pathlib import Path


# Load configuration settings
# Reads runtime parameters such as input file path,
# schema selection, filtering options, and output location.

with open("Configurations/config.json", "r") as f:
    config = json.load(f)

file_path = Path(config["input_file"])
schema_key = config["schema_type"]
countries_filter = [c.lower() for c in config["countries_filter"]]
border_coordinates_path = Path(config["border_coordinates_file"])
output_file = config["output_file"]


# Import input dataset
# Supports CSV and Excel formats.
# Raises an error if another file type is provided.

if file_path.suffix.lower() == ".csv":
    df = pd.read_csv(file_path)
elif file_path.suffix.lower() in [".xlsx", ".xls"]:
    df = pd.read_excel(file_path)
else:
    raise ValueError("Unsupported file type")


# Standardize column names
# Normalizes column headers for consistency by:
# - Removing extra spaces
# - Converting to lowercase
# - Replacing spaces with underscores


df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)


# Apply schema mapping
# Renames dataset columns according to the selected
# schema defined in column_mapping.json.

with open("Configurations/column_mapping.json", "r") as f:
    schema = json.load(f)

column_mapping = schema[schema_key]
df = df.rename(columns=column_mapping)


# Filter dataset by country
# Ensures the required 'country' column exists,
# standardizes values, and filters to selected countries.

if "country" not in df.columns:
    raise ValueError("Expected 'country' column not found after schema mapping.")


df["country"] = (
    df["country"]
    .astype(str)
    .str.strip()
    .str.lower()
)

df = df[df["country"].isin(countries_filter)]


# Retain required analytical columns
# Keeps only the standardized fields necessary for
# spatial and temporal risk modeling.

columns_to_keep = [
    "event_id",
    "event_date",
    "year",
    "event_type",
    "actor_1",
    "actor_2",
    "country",
    "fatalities",
    "location",
    "latitude",
    "longitude",
    "notes"
]

df = df[columns_to_keep]


# Clean and validate numeric fields
# Converts latitude, longitude, and fatalities to numeric.
# Drops rows missing coordinates and ensures fatalities
# are non-negative integers.


df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
df["fatalities"] = pd.to_numeric(df["fatalities"], errors="coerce")

df = df.dropna(subset=["latitude", "longitude"])

df.loc[df["fatalities"] < 1, "fatalities"] = 0
df["fatalities"] = df["fatalities"].fillna(0).astype(int)


# Define Haversine distance function
# Computes great-circle distance between two latitude
# and longitude points in kilometers.


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))

    return R * c


# Load and process border coordinates
# Imports predefined border points and calculates
# the closest distance from each event to the border.

border_coordinates_data = pd.read_csv(border_coordinates_path)
border_coordinates_data = border_coordinates_data[["Longitude", "Latitude"]]

border_lats = border_coordinates_data["Latitude"].values
border_lons = border_coordinates_data["Longitude"].values


def closest_border_distance(row):
    distances = haversine(
        row["latitude"],
        row["longitude"],
        border_lats,
        border_lons
    )
    return np.min(distances)


df["closest_border_km"] = df.apply(closest_border_distance, axis=1)

BORDER_BUFFER_KM = 50 #Agreed on by team

df = df[df["closest_border_km"] <= BORDER_BUFFER_KM]


# Calculate distance to Preah Vihear Temple
# Measures proximity of each event to the temple location.

temple_lat = 14.3906
temple_lon = 104.6803

df["closest_border_temple_km"] = haversine(
    temple_lat,
    temple_lon,
    df["latitude"],
    df["longitude"]
)


# Parse and standardize event dates
# Attempts direct parsing first. If unsuccessful for many
# rows, combines event_date with year field.


df["event_date_parsed"] = pd.to_datetime(
    df["event_date"],
    errors="coerce"
)

if df["event_date_parsed"].isna().mean() > 0.5:
    df["event_date_parsed"] = pd.to_datetime(
        df["event_date"].astype(str).str.strip() + " " + df["year"].astype(str),
        errors="coerce"
    )


df = df.dropna(subset=["event_date_parsed"])
df["event_date"] = df["event_date_parsed"]
df = df.drop(columns=["event_date_parsed"])

START_YEAR = 2003
END_YEAR = 2026

df = df[
    (df["event_date"].dt.year >= START_YEAR) &
    (df["event_date"].dt.year <= END_YEAR)
]

if df.empty:
    raise ValueError("No rows remaining after year filtering.")


# Construct true 10 km spatial grid
# Converts geographic coordinates to planar kilometer space
# and assigns each event to a 10 km grid cell.

EARTH_RADIUS_KM = 6371
GRID_SIZE_KM = 10

lat_rad = np.radians(df["latitude"])
lon_rad = np.radians(df["longitude"])


df["y_km"] = EARTH_RADIUS_KM * lat_rad
df["x_km"] = EARTH_RADIUS_KM * lon_rad * np.cos(lat_rad)

df["grid_x"] = (df["x_km"] // GRID_SIZE_KM) * GRID_SIZE_KM
df["grid_y"] = (df["y_km"] // GRID_SIZE_KM) * GRID_SIZE_KM

df["grid_id"] = df["grid_x"].astype(str) + "_" + df["grid_y"].astype(str)


# Engineer temporal escalation and intensity features
# Computes recency, cumulative counts, and rolling
# event activity within each spatial grid.


df = df.sort_values(["grid_id", "event_date"]).reset_index(drop=True)

df["days_since_last_event_in_area"] = (
    df.groupby("grid_id")["event_date"]
      .diff()
      .dt.days
)

df["days_since_last_event_in_area"] = (
    df["days_since_last_event_in_area"]
    .fillna(365)
)


df["total_events_in_area"] = (
    df.groupby("grid_id")["event_id"]
      .transform("count")
)


df["total_fatalities_in_area"] = (
    df.groupby("grid_id")["fatalities"]
      .transform("sum")
)


events_30_list = []
events_90_list = []

for grid, group in df.groupby("grid_id"):
    group = group.sort_values("event_date").copy()
    group = group.set_index("event_date")

    rolling_30 = group["event_id"].rolling("30D").count() - 1
    rolling_90 = group["event_id"].rolling("90D").count() - 1

    events_30_list.append(rolling_30)
    events_90_list.append(rolling_90)


df["events_last_30_days_in_area"] = (
    pd.concat(events_30_list).sort_index().values
)

df["events_last_90_days_in_area"] = (
    pd.concat(events_90_list).sort_index().values
)


# Generate risk-aligned features
# Inverts distance and inactivity measures so that
# smaller distances and shorter gaps produce higher risk values.


df["tempo_risk"] = 1 / (df["days_since_last_event_in_area"] + 1)
df["border_risk"] = 1 / (df["closest_border_km"] + 1)
df["temple_risk"] = 1 / (df["closest_border_temple_km"] + 1)


# Export processed dataset
# Saves final feature-enriched dataset to the configured
# output file location.

if "grid_id" not in df.columns:
    raise ValueError("grid_id missing before export.")


df.to_csv(output_file, index=False)

print("Processing complete.")
print(f"Output saved to: {output_file}")
