import pandas as pd

API_KEYS_DF = pd.read_csv("api_keys.csv")
GOOGLE_MAPS_API = API_KEYS_DF[API_KEYS_DF["api_type"] == "google_maps"]["key"][0]
