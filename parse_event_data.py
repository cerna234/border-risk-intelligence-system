"""
Border Risk Intelligence System
"""

import pandas as pd
import json
import os
import numpy as np
from pathlib import Path


#  Load Config File

with open("Configurations/config.json", "r") as f:
    config = json.load(f)

file_path = Path(config["input_file"])
schema_key = config["schema_type"]
present_date_column_name = config["present_date_column_name"]
month_column_name = config["month_column_name"]
day_column_name = config["day_column_name"]
countries_filter = [c.lower() for c in config["countries_filter"]]
border_coordinates_path = Path(config["border_coordinates_file"])
output_file = config["output_file"]
data_source = config["source_name"]


# Import Data

if file_path.suffix.lower() == ".csv":
    df = pd.read_csv(file_path)
    original_data = pd.read_csv(file_path)

elif file_path.suffix.lower() in [".xlsx", ".xls"]:
    df = pd.read_excel(file_path)
    original_data = pd.read_excel(file_path)

else:
    raise ValueError("Unsupported file type")


# 
# standardize column names


df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)


# 4. schema column mapping from present to expected column headers


with open("Configurations/column_mapping.json", "r") as f:
    schema = json.load(f)

column_mapping = schema[schema_key]
df = df.rename(columns=column_mapping)



#  filter and normalize by country


if "country_name" in df.columns:
    df["country_name"] = (
        df["country_name"]
        .astype(str)
        .str.strip()
        .str.lower()
    )
else:
    raise ValueError("Expected 'country_name' column not found after schema mapping.")

df = df[df["country_name"].isin(countries_filter)]



# keep only required columns

columns_to_keep = [
    "event_id",
    "year",
    "event_type",
    "actor_1",
    "actor_2",
    "country_name",
    "fatalities",
    "location",
    "latitude",
    "longitude",
    "notes"
]



df = df[columns_to_keep]




# keep only rows with correct lat, long, and fatalities values


df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
df["fatalities"] = pd.to_numeric(df["fatalities"], errors="coerce")

df = df.dropna(subset=["latitude", "longitude"])

df.loc[df["fatalities"] < 1, "fatalities"] = 0
df["fatalities"] = df["fatalities"].fillna(0).astype(int)


# loading border coordinates from shapefile generated coordinates

border_coordinates_data = pd.read_csv(border_coordinates_path)
border_coordinates_data = border_coordinates_data[["Longitude", "Latitude"]]

border_lats = border_coordinates_data["Latitude"].values
border_lons = border_coordinates_data["Longitude"].values


# calculate distances

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km

    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))

    return R * c


def closest_border_distance(row):
    distances = haversine(
        row["latitude"],
        row["longitude"],
        border_lats,
        border_lons
    )
    return np.min(distances)


df["closest_border_km"] = df.apply(closest_border_distance, axis=1)


# Temple of Preah Vihear
temple_lat = 14.3906
temple_long = 104.6803

df["closest_border_temple_km"] = haversine(
    temple_lat,
    temple_long,
    df["latitude"],
    df["longitude"]
)


#Create date ony if not present in file already

def create_date_features():
    if present_date_column_name in original_data.columns:

        original_data[present_date_column_name] = pd.to_datetime(
            original_data[present_date_column_name],
            errors="coerce"
        )

        original_data["month_day"] = (
            original_data[present_date_column_name]
            .dt.strftime("%b %d")
            .str.upper()
        )

    else:
        original_data[month_column_name] = pd.to_numeric(original_data[month_column_name], errors="coerce")
        original_data[day_column_name] = pd.to_numeric(original_data[day_column_name], errors="coerce")

        original_data.loc[original_data[month_column_name] == 0, month_column_name] = np.nan
        original_data.loc[original_data[day_column_name] == 0, day_column_name] = np.nan

        original_data["month_day"] = (
            pd.to_datetime(
                dict(
                    year=2000,
                    month=original_data[month_column_name],
                    day=original_data[day_column_name]
                ),
                errors="coerce"
            )
            .dt.strftime("%b %d")
            .str.upper()
        )


create_date_features()

df["month_day"] = original_data.loc[df.index, "month_day"]

df["Source"] = data_source

# export output


df.to_csv(output_file, index=False)

print("Processing complete.")
print(f"Output saved to: {output_file}")