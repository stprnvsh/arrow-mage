from arrow_cache.threading import safe_to_arrow_table
import tempfile
import os
import requests

# Download to temp file
url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet'
response = requests.get(url, stream=True)
response.raise_for_status()
temp = tempfile.NamedTemporaryFile(delete=False)
for chunk in response.iter_content(chunk_size=8192):
    temp.write(chunk)
temp_path = temp.name
temp.close()

