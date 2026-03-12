# Border-risk-intelligence-system


## Background

This project was developed as part of a National Geospatial-Intelligence Agency (NGA) and T-REX sponsored academic competition focused on analyzing escalation dynamics along the Thai–Cambodian border.

The objective was to model instability along a contested international boundary using structured spatial and temporal analysis rather than isolated incident reporting.

To accomplish this, our team designed a full analytical pipeline that ingests conflict event data, normalizes it into a standardized schema, engineers spatial and escalation features, and detects structured patterns of instability.

Raw event data collected by the team was processed through a custom parser that:

- Standardized heterogeneous source formats
- Normalized actor and event classifications
- Calculated geodesic distances to the international border
- Measured proximity to key strategic landmarks
- Engineered temporal escalation indicators within defined geographic areas

The processed dataset was then ingested into a DBSCAN clustering model to identify density-based escalation zones without predefining the number of clusters. This allowed us to differentiate between sustained escalation environments and isolated incidents.

Clustered outputs were exported into ArcGIS, where spatial patterns were visualized and analyzed to develop a structured geospatial narrative of border instability.

The result was an end-to-end intelligence workflow:

data ingestion → structured parsing → feature engineering → density-based clustering → geospatial visualization → analytical interpretation

This project demonstrates how spatial-temporal modeling can transform raw event data into interpretable escalation insights within a national security context.
