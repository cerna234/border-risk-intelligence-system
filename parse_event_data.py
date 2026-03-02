import pandas as pd
import json
import os
import numpy as np
import calendar






# Import data

file_path = "//Users//miguelcerna//Desktop//border-risk-intelligence-system//Sample Data//USD 20 Data//MAJIC_Raw_Data_Prio.Org.xlsx"

#determine files extension

extension = os.path.splitext(file_path)[1].lower()


if file_path.lower().endswith(".csv"):
    df = pd.read_csv(file_path)
    original_data = pd.read_csv(file_path)

elif file_path.lower().endswith((".xlsx", ".xls")):
    df = pd.read_excel(file_path)
    original_data = pd.read_excel(file_path)

else:
    raise ValueError("Unsupported file type")










#standardize columns 
df.columns = (
    df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
)


#convert headers from input data -> expected headers 
    # this is going to be ahcieved through a mapping schema json file with format
    # expected : input file column name

    #Ex: if column = Summary - Convert name to "Notes"

schema_file_path = "//Users//miguelcerna//Desktop//border-risk-intelligence-system//column_mapping.json"
with open(schema_file_path, "r") as f:
    schema = json.load(f)


column_mapping = schema["column_mapping2"] #specified column mapping from mapping json

df = df.rename(columns=column_mapping)




#normalize country values column to lower case to avoid case sensitive when filtering

df['country'] = df['country'].str.strip().str.lower()

#Filter data by country (Only for Cambodia and Thailand)

filtered_df = df[df["country"].isin(["cambodia", "thailand"])]

df = filtered_df





#drop non required columns

columns_to_keep = [
    "event_id",
    #"event_date", # This will be added later since some data keeps it seperatd by day,month,year
    "year",
    "event_type",
    "actor_1",
    "actor_2",
    "country",
    "fatalities",
    "location",
    "latitude",
    "longitude",
    #"source", #will be added later
    "notes"
]

df = df[columns_to_keep]






# create functions:
#   add column to add distance from event to Border
    # Shapefile with lat and long of border value will be closest value from border


border_coordinates_data = pd.read_csv("//Users//miguelcerna//Desktop//border-risk-intelligence-system//border_coordinates.csv")

border_coordinates_data = border_coordinates_data[['Longitude', 'Latitude']]
print(border_coordinates_data)



# Haversine function
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



border_lats = border_coordinates_data["Latitude"].values
border_lons = border_coordinates_data["Longitude"].values
# Function to compute closest border point + distance
def closest_border_info(row):
    distances = haversine(
        row["latitude"],
        row["longitude"],
        border_lats,
        border_lons
    )

    idx = np.argmin(distances)

    return pd.Series({
        "closest_border_km": distances[idx],
        #"closest_border_lat": border_lats[idx], Uncomment to validate Data
        #"closest_border_lon": border_lons[idx] Uncomment to validate Data
    })


# Apply to dataframe
df[[
    "closest_border_km",
   # "closest_border_lat", Uncomment to validate Data
    #"closest_border_lon" Uncomment to validate Data
]] = df.apply(closest_border_info, axis=1)


#  add column to add distance from event to preah vihar temple
    # value will be with closest distsnce in km to preah vihar temple

#Temple of Preah Vihear lat,long
temple_lat = 14.3906
temple_long = 104.6803

distances_to_temple = haversine(
        temple_lat,
        temple_long,
        border_lats,
        border_lons
    )

print(distances_to_temple)

df["closest_border_temple_km"] = haversine(
    temple_lat,
    temple_long,
    df["latitude"],
    df["longitude"]
)



#remove any columns with blank data or normalize ie Text to numerical values

#update fatalities under 0 to 0
df["fatalities"] = pd.to_numeric(df["fatalities"], errors="coerce")
df.loc[df["fatalities"] < 1, "fatalities"] = 0
df["fatalities"] = df["fatalities"].fillna(0).astype(int)



#create event date if not present or seperated values are present
# ----- CREATE OR USE event_date -----

if "event_date" in original_data.columns:

    # Convert existing event_date to datetime
    original_data["event_date"] = pd.to_datetime(
        original_data["event_date"],
        errors="coerce"
    )

    # Format as "DEC 10"
    original_data["month_day"] = (
        original_data["event_date"]
            .dt.strftime("%b %d")
            .str.upper()
    )

else:
    # Build from EMONTH + EDAY
    original_data["EMONTH"] = pd.to_numeric(original_data["EMONTH"], errors="coerce")
    original_data["EDAY"] = pd.to_numeric(original_data["EDAY"], errors="coerce")

    original_data["month_day"] = (
        original_data["EMONTH"]
            .apply(lambda x: calendar.month_abbr[int(x)] if pd.notna(x) and 1 <= int(x) <= 12 else "")
            .str.upper()
        + " "
        + original_data["EDAY"].astype("Int64").astype(str)
    )

# ----- ADD TO FILTERED DF -----

df["month_day"] = original_data.loc[df.index, "month_day"]
df.to_csv("output2.csv")