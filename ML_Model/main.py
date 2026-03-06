from sklearn.preprocessing import MinMaxScaler
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

# Load dataset
df = pd.read_csv("C:\\Users\\Miguel Cerna\\OneDrive\\Desktop\\border-risk-intelligence-system\\test5_v2_file2ACLED.csv")

# Clean whitespace in actor columns
df["actor_1"] = df["actor_1"].str.strip()
df["actor_2"] = df["actor_2"].str.strip()

# -------- STEP 1: Separate numeric features --------
numeric_features = [
    "total_events_in_area",
    "events_last_30_days_in_area",
    "total_fatalities_in_area",
    "tempo_risk",
    "border_risk",
    "temple_risk", 
    #"closest_border_km",                 # raw version of border_risk
    #"closest_border_temple_km",          # raw temple distance
    #"days_since_last_event_in_area",     # raw version of tempo_risk
    #"events_last_90_days_in_area",       # redundant with 30-day in small dataset


]

X_numeric = df[numeric_features]

# -------- STEP 2: Scale numeric features --------
scaler = MinMaxScaler()
X_scaled = scaler.fit_transform(X_numeric)

df_scaled = pd.DataFrame(X_scaled, columns=numeric_features)

# -------- STEP 3: One-hot encode actors --------
actor_encoded = pd.get_dummies(df[["actor_1", "actor_2","grid_id"]])

# -------- STEP 4: Combine scaled numeric + encoded actors --------
X_final = pd.concat([df_scaled, actor_encoded], axis=1)

# -------- STEP 5: Correlation matrix (numeric only makes sense here) --------
corr_matrix = df_scaled.corr()

print(corr_matrix)

plt.figure(figsize=(10,10))
sns.heatmap(corr_matrix, annot=True, cmap="coolwarm")
plt.title("Numeric Feature Correlation Matrix")
plt.show()