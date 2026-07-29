# File where the data from Kaggle gets extracted and put into a volume
import os
from datetime import datetime

try:
    import kaggle
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "kaggle", "-q"])
    import kaggle

# MAGIC %run "/Users/alexandruborduz@gmail.com/World Happiness/config"

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# Build volume path
volume_path = f"/Volumes/{CONFIG['catalog']}/bronze/{CONFIG['volume']}"

# Skip if files already exist
existing_files = os.listdir(volume_path) if os.path.exists(volume_path) else []
if existing_files:
    log(f"Files already present in volume, skipping download:")
    for f in existing_files:
        log(f"  - {f}")
else:
    # Retrieve Kaggle credentials from Databricks secrets
    try:
        os.environ["KAGGLE_USERNAME"] = dbutils.secrets.get(scope="etl-secrets", key="kaggle-username")
        os.environ["KAGGLE_KEY"] = dbutils.secrets.get(scope="etl-secrets", key="kaggle-token")
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve Kaggle credentials from secrets: {e}")

    # Authenticate and download
    try:
        kaggle.api.authenticate()
        log(f"Downloading dataset '{CONFIG['kaggle_dataset']}' to {volume_path}...")
        kaggle.api.dataset_download_files(CONFIG["kaggle_dataset"], path=volume_path, unzip=True)
    except Exception as e:
        raise RuntimeError(f"Failed to download dataset from Kaggle: {e}")

    # Report
    files = os.listdir(volume_path)
    log(f"Downloaded {len(files)} file(s):")
    for f in files:
        log(f"  - {f}")
