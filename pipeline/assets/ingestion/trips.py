"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
  - name: dropoff_datetime
    type: timestamp
@bruin"""

import os
import json
import pandas as pd

def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    data = []
    for t in taxi_types:
        data.append({
            "pickup_datetime": pd.to_datetime(start_date),
            "dropoff_datetime": pd.to_datetime(start_date),
            "pickup_location_id": 1,
            "dropoff_location_id": 2,
            "fare_amount": 10.5,
            "payment_type": 1,
            "taxi_type": t
        })

    return pd.DataFrame(data)