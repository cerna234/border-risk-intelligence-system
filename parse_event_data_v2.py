"""
Border Risk Intelligence System
"""

import pandas as pd
import json
import numpy as np
from pathlib import Path


# --------------------------------------------------
# 1. Load Config File
# --------------------------------------------------

with open("Configurations/config.json", "r") as f:
    config = json.load(f)

file_path = Path(config["input_file"])
schema_key = config["schema_type"]
countries_filter = [c.lower() for c in config["countries_filter"]]
border_coordinates_path = Path(config["border_coordinates_file"])
output_file = config["output_file"]


# --------------------------------------------------
# 2. Import Data
# --------------------------------------------------

if file_path.suffix.lower() == ".csv":
    df = pd.read_csv(file_path)
elif file_path.suffix.lower() in [".xlsx", ".xls"]:
    df = pd.read_excel(file_path)
else:
    raise ValueError("Unsupported file type")


# --------------------------------------------------
# 3. Standardize Column Names
# --------------------------------------------------

df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)


# --------------------------------------------------
# 4. Schema Mapping
# --------------------------------------------------

with open("Configurations/column_mapping.json", "r") as f:
    schema = json.load(f)

column_mapping = schema[schema_key]
df = df.rename(columns=column_mapping)


# --------------------------------------------------
# 5. Filter by Country
# --------------------------------------------------

if "country" not in df.columns:
    raise ValueError("Expected 'country' column not found after schema mapping.")

df["country"] = (
    df["country"]
    .astype(str)
    .str.strip()
    .str.lower()
)

df = df[df["country"].isin(countries_filter)]


# --------------------------------------------------
# 6. Keep Required Columns
# --------------------------------------------------




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


# --------------------------------------------------
# 7. Clean Numeric Columns
# --------------------------------------------------

df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
df["fatalities"] = pd.to_numeric(df["fatalities"], errors="coerce")

df = df.dropna(subset=["latitude", "longitude"])

df.loc[df["fatalities"] < 1, "fatalities"] = 0
df["fatalities"] = df["fatalities"].fillna(0).astype(int)


# --------------------------------------------------
# 8. Haversine Distance Function
# --------------------------------------------------

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


# --------------------------------------------------
# 9. Load Border Coordinates
# --------------------------------------------------

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


# --------------------------------------------------
# 10. Temple Distance
# --------------------------------------------------

temple_lat = 14.3906
temple_lon = 104.6803

df["closest_border_temple_km"] = haversine(
    temple_lat,
    temple_lon,
    df["latitude"],
    df["longitude"]
)


print(df)
# --------------------------------------------------
# 11. Proper Date Handling
# --------------------------------------------------

df["event_date"] = df["event_date"].astype(str).str.strip()

df["event_date"] = pd.to_datetime(
    df["event_date"] + " " + df["year"].astype(str),
    errors="coerce"
)

df = df.dropna(subset=["event_date"])

df["day"] = df["event_date"].dt.day
df["month"] = df["event_date"].dt.month
df["month_name"] = df["event_date"].dt.month_name()
df["week"] = df["event_date"].dt.isocalendar().week


START_YEAR = 2025
END_YEAR = 2026

df = df[
    (df["event_date"].dt.year >= START_YEAR) &
    (df["event_date"].dt.year <= END_YEAR)
]

if df.empty:
    raise ValueError("No rows remaining after year filtering.")

# --------------------------------------------------
# 12. Create TRUE 10km Grid
# --------------------------------------------------

EARTH_RADIUS_KM = 6371 #approx radius of earth
GRID_SIZE_KM = 10  #size of each grid cell


'''
distance = radius × angle_in_radians

So we convert degrees → radians.
'''

lat_rad = np.radians(df["latitude"])
lon_rad = np.radians(df["longitude"])


df["y_km"] = EARTH_RADIUS_KM * lat_rad
df["x_km"] = EARTH_RADIUS_KM * lon_rad * np.cos(lat_rad)

df["grid_x"] = (df["x_km"] // GRID_SIZE_KM) * GRID_SIZE_KM
df["grid_y"] = (df["y_km"] // GRID_SIZE_KM) * GRID_SIZE_KM

df["grid_id"] = df["grid_x"].astype(str) + "_" + df["grid_y"].astype(str)


# --------------------------------------------------
# 13. Temporal Escalation + Intensity Features
# --------------------------------------------------

df = df.sort_values(["grid_id", "event_date"]).reset_index(drop=True)

# Days since last event in grid
df["days_since_last_event_in_area"] = (
    df.groupby("grid_id")["event_date"]
      .diff()
      .dt.days
)

# Fill first-event NaNs with long inactivity assumption
df["days_since_last_event_in_area"] = (
    df["days_since_last_event_in_area"]
    .fillna(365)
)

# Total lifetime events in grid
df["total_events_in_area"] = (
    df.groupby("grid_id")["event_id"]
      .transform("count")
)

# Total lifetime fatalities in grid
df["total_fatalities_in_area"] = (
    df.groupby("grid_id")["fatalities"]
      .transform("sum")
)

# Rolling event intensity
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


# --------------------------------------------------
# 14. Risk-Aligned Feature Engineering
# --------------------------------------------------

#Small values mean high risk, Large values mean low risk.Below code reverses those to correctly show lower values being higher risk



df["tempo_risk"] = 1 / (df["days_since_last_event_in_area"] + 1)
df["border_risk"] = 1 / (df["closest_border_km"] + 1)
df["temple_risk"] = 1 / (df["closest_border_temple_km"] + 1)


# --------------------------------------------------
# 15. Export Output
# --------------------------------------------------

if "grid_id" not in df.columns:
    raise ValueError("grid_id missing before export.")

df.to_csv(output_file, index=False)

print("Processing complete.")
print(f"Output saved to: {output_file}")