import pandas as pd
import time
import json
import os
import requests
from rapidfuzz import process, fuzz
import unicodedata


def read_file(path):
    df = pd.read_csv(path, nrows=300)
    return df


def normalize_text(s):
    """Chuẩn hoá: viết thường + bỏ dấu + bỏ khoảng trắng thừa"""
    if not s:
        return ""
    s = s.lower().strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s


def fuzzy_map(original_list, ai_result, threshold=85):
    """
    original_list: danh sách tên gốc
    ai_result: dict {tên AI trả về: category}
    threshold: độ tương đồng tối thiểu (%)
    """
    # Normalize ai_result keys
    normalized_ai = {normalize_text(k): v for k, v in ai_result.items()}

    mapped = {}
    for name in original_list:
        n_name = normalize_text(name)

        # Tìm tên AI gần nhất
        match = process.extractOne(n_name, normalized_ai.keys(), scorer=fuzz.ratio)
        if match is None:
            mapped[name] = "Other"
            continue

        best_match, score, *_ = match
        if score >= threshold:
            mapped[name] = normalized_ai[best_match]
        else:
            mapped[name] = "Other"
    return mapped


def classify_category(movie_list):
    if not movie_list:
        return {}

    system_prompt = """
    Bạn là chuyên gia phân loại nội dung phim, chương trình truyền hình và các loại nội dung giải trí.  
    Bạn sẽ nhận một danh sách tên có thể viết sai, viết liền không dấu, viết tắt, hoặc chỉ là cụm từ liên quan đến nội dung.

    Nguyên tắc:
    - Không được trả về "Other" nếu có thể đoán được dù chỉ một phần ý nghĩa.  
    - Luôn cố gắng sửa lỗi, nhận diện tên gần đúng hoặc đoán thể loại gần đúng.  
    - Nếu không chắc → chọn thể loại gần nhất (VD: mô tả tình cảm → Romance, thể thao → Sports, v.v.)

    Nhiệm vụ của bạn:
    1. **Chuẩn hoá tên**: thêm dấu tiếng Việt nếu cần, tách từ, chỉnh chính tả (vd: "thuyếtminh" → "Thuyết minh", "tramnamu" → "Trăm năm hữu duyên", "capdoi" → "Cặp đôi").
    2. **Nhận diện tên hoặc ý nghĩa gốc gần đúng nhất**. Bao gồm:
    - Tên phim, series, show, chương trình
    - Quốc gia / đội tuyển (→ "Sports" hoặc "News")
    - Từ khoá mô tả nội dung (→ phân loại theo ý nghĩa, ví dụ "thuyếtminh" → "Other" hoặc "Drama", "bigfoot" → "Horror")
    3. **Gán thể loại phù hợp nhất** trong các nhóm sau:  
    - Action  
    - Romance  
    - Comedy  
    - Horror  
    - Animation  
    - Drama  
    - C Drama  
    - K Drama  
    - Sports  
    - Music  
    - Reality Show  
    - TV Channel  
    - News  
    - Other

    Một số quy tắc gợi ý nhanh:
    - Có từ “VTV”, “HTV”, “Channel” → TV Channel  
    - Có “running”, “master key”, “reality” → Reality Show  
    - Quốc gia, CLB bóng đá, sự kiện thể thao → Sports hoặc News  
    - “sex”, “romantic”, “love” → Romance  
    - “potter”, “hogwarts” → Drama / Fantasy  
    - Tên phim Việt/Trung/Hàn → ưu tiên Drama / C Drama / K Drama

    Hãy chỉ trả về **một JSON hợp lệ duy nhất**, không kèm giải thích, không có markdown, không có gạch đầu dòng.

    Ví dụ:
    {
      "cẩm tú nam ca": "Romance",
      "killing eve": "Drama",
      "fairy tail": "Animation"
    }
    """
    user_prompt = f"Danh sách: {movie_list}"

    try:
        response = requests.post(
            "http://127.0.0.1:1234/v1/chat/completions",
            headers={"Content-Type": "application/json"},
            json={
                "model": "llama-3-groq-8b-tool-use",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0,
                # "max_tokens": 1024
            },
            timeout=120,
        )

        result = response.json()["choices"][0]["message"]["content"].strip()
        result = json.loads(result)
        print(result)

        return result

    except Exception as e:
        print("❌ Error:", e)
        return {m: "Other" for m in movie_list}


def seperate_batch(df, output):
    batch_size = 20

    header_exist = not os.path.exists(output)
    for i in range(0, len(df), batch_size):
        ## Copy kiểu nhiều cột
        mini_df = df.iloc[i : i + batch_size].copy()
        movies = mini_df["most_search"].tolist()

        result = classify_category(movies)

        # fuzzy map giữa gốc và AI result
        result = fuzzy_map(movies, result)

        # Gắn category
        mini_df["category"] = mini_df["most_search"].map(
            lambda x: result.get(x, "Other")
        )

        # Write to disk, append
        mini_df.to_csv(
            output, mode="a", index=False, header=header_exist, encoding="utf-8-sig"
        )
        header_exist = False

        print(
            f"💾 Đã lưu batch {i//batch_size + 1}/{len(df)//batch_size} {len(mini_df)} dòng vào {output}"
        )
        time.sleep(1)


def main_AI(month):
    df = read_file(f"./output/t{month}")
    output = f"./output_AI/t{month}.csv"

    if not os.path.exists("./output_AI"):
        os.makedirs("./output_AI")

    seperate_batch(df, output)


main_AI(6)
main_AI(7)
