"""
Border Risk Intelligence System
DBSCAN Clustering Module

Author: Miguel Cerna Benitez
Program: MS National Cyber Security Studies
Project Context: Developed as part of an academic competition sponsored by NGA and T-REX
Date: 2026

"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
# from sklearn.preprocessing import MinMaxScaler  # kept for variation testing
# from sklearn.decomposition import PCA  # optional dimensionality reduction (testing)


# Load dataset
# Reads preprocessed model input data generated from the
# Border Risk Intelligence pipeline.

# NOTE: Hardcoded path used for local development
# Consider moving to config file later if productionizing

df = pd.read_csv(
    r"C:\Users\Miguel Cerna\OneDrive\Desktop\border-risk-intelligence-system\ML_Model\Model Input Data\acledDataParse(Allyears).csv"
)

# Clean whitespace from actor fields to prevent encoding mismatches

df["actor_1"] = df["actor_1"].astype(str).str.strip()
df["actor_2"] = df["actor_2"].astype(str).str.strip()


# Select features for clustering
# Using spatial + temporal escalation momentum features

numeric_features = [
    "x_km",
    "y_km",
    "events_last_30_days_in_area",
    "events_last_90_days_in_area",
    "days_since_last_event_in_area"
]

X_numeric = df[numeric_features]


# Scale numeric features
# StandardScaler used for Variation 2 (All Years Momentum)
# MinMaxScaler kept commented for Variation 1 (2025-2026 window testing)

# scaler = MinMaxScaler()  # Variation 1: 2025-2026 (testing only)
scaler = StandardScaler()  # Variation 2: All Years Momentum

X_scaled = scaler.fit_transform(X_numeric)
X_final = X_scaled  # DBSCAN uses scaled feature space


# Optional actor encoding ( For Testing )
# Actor encoding was tested but excluded from final clustering
# to preserve spatial-temporal structure without high-dimensional noise

# actor_encoded = pd.get_dummies(df[["actor_1", "actor_2"]])
# X_final = pd.concat(
#     [pd.DataFrame(X_scaled, columns=numeric_features), actor_encoded],
#     axis=1
# )


# Correlation matrix 
# Used for feature sanity-checking during experimentation
# Can be commented out if running in non-visual environments

corr_matrix = X_numeric.corr()

plt.figure(figsize=(8, 8))
sns.heatmap(corr_matrix, annot=True, cmap="coolwarm")
plt.title("Numeric Feature Correlation Matrix")
plt.show()


# PCA testing block 
# Used for dimensionality reduction experiments
# Uncomment if testing cluster separability in 2D

# pca = PCA(n_components=2)
# X_pca = pca.fit_transform(X_scaled)
# X_final = X_pca


# Apply DBSCAN clustering

db = DBSCAN(eps=0.9, min_samples=10)
clusters = db.fit_predict(X_final)

df["cluster"] = clusters

labels = db.labels_
n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
n_noise = np.sum(labels == -1)


#output for testing
#print("clusters:", n_clusters)
#print("noise points:", n_noise)
#print("total points:", len(labels))


# Export clustered output
# --------------------------------------------------
# Output used for ArcGIS spatial visualization and
# escalation momentum mapping


df.to_csv("clustered_output_Escalation_Momentum(AllYears).csv", index=False)

print("\nClustered dataset exported successfully.")