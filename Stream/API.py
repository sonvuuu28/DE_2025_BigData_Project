from flask import Flask, jsonify, request
import random
import time
from datetime import datetime, timezone
import uuid
import json

app = Flask(__name__)

# =========================
# Danh sách bài hát mẫu
# =========================
SONGS = [
    "Shape of You - Ed Sheeran",
    "Blinding Lights - The Weeknd",
    "Someone Like You - Adele",
    "Uptown Funk - Mark Ronson ft. Bruno Mars",
    "Bad Guy - Billie Eilish",
    "See You Again - Wiz Khalifa ft. Charlie Puth",
    "Levitating - Dua Lipa",
    "Believer - Imagine Dragons",
    "Love Story - Taylor Swift",
    "Counting Stars - OneRepublic",
]


# Hàm tạo 1 lượt bình chọn ngẫu nhiên
def generate_vote():
    return {
        "user_id": str(uuid.uuid4()),
        "song": random.choice(SONGS),
        "vote": True,
        "timestamp": datetime.now().isoformat(),
    }


# =========================
# API GET /votes
# Trả về danh sách bình chọn ngẫu nhiên
# =========================
@app.route("/votes", methods=["GET"])
def get_votes():
    # Nếu không truyền count → random 1-20
    count = request.args.get("count")
    count = int(count) if count is not None else random.randint(20, 80)

    # Nếu không truyền delay → random 0-2 giây
    delay = request.args.get("delay")
    delay = float(delay) if delay is not None else random.uniform(0, 2)

    time.sleep(delay)  # mô phỏng server chậm

    votes = [generate_vote() for _ in range(count)]

    # Trả về JSON thủ công (đảm bảo không bị encode Unicode)
    return app.response_class(
        response=json.dumps(
            {
                "status": "ok",
                "count": len(votes),
                "delay_used": round(delay, 3),
                "votes": votes,
            },
            ensure_ascii=False,
        ),
        status=200,
        mimetype="application/json",
    )


# Trang chủ để giới thiệu API
@app.route("/", methods=["GET"])
def info():
    return jsonify(
        {
            "service": "billboard-music-api",
            "description": "API mô phỏng bình chọn bài hát US-UK.",
            "endpoints": {
                "/votes": "GET → Trả về danh sách bình chọn ngẫu nhiên",
            },
            "example": "http://localhost:1111/votes?count=5&delay=1",
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=1111, debug=True)
