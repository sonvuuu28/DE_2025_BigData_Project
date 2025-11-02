import requests
import time
import json
import os
from datetime import datetime

API_URL = "http://localhost:1111/votes"
OUTPUT_DIR = "raw_data"

os.makedirs(OUTPUT_DIR, exist_ok=True)

while True:
    try:
        res = requests.get(API_URL)
        data = res.json()

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        file_path = f"{OUTPUT_DIR}/votes_{timestamp}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

        print(f"✅ Saved: {file_path} ({data['count']} votes)")
    except Exception as e:
        print("❌ Error:", e)

    time.sleep(10)  # poll mỗi 2 giây
