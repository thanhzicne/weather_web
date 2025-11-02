# data_cleaning.py
# Trách nhiệm: Nhận JSON, xử lý, và trả về DataFrame sạch.

import pandas as pd

HOURLY_PARAMS = [
    "temperature_2m", "relative_humidity_2m", "precipitation",
    "rain", "showers", "weather_code", "pressure_msl",
    "wind_speed_10m", "wind_direction_10m"
]

def clean_api_data(data_json, province_id, province_name):
    """
    Chuyển đổi JSON thô thành DataFrame sạch.
    Trả về DataFrame hoặc None nếu lỗi.
    """
    if not data_json or "hourly" not in data_json:
        print(f"  Không có dữ liệu 'hourly' cho {province_name}.")
        return None
    
    try:
        df = pd.DataFrame(data_json["hourly"])
        df.rename(columns={"time": "timestamp"}, inplace=True)
        
        # Kiểm tra xem có thiếu cột không
        if not all(col in df.columns for col in HOURLY_PARAMS + ["timestamp"]):
            print(f"  Dữ liệu API trả về thiếu cột cho {province_name}.")
            return None

        df["province_id"] = province_id
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        
        # Bỏ qua các dòng có giá trị NaN (rỗng)
        df.dropna(inplace=True)
        
        if df.empty:
            print(f"  Không có dữ liệu hợp lệ (sau khi lọc NaN) cho {province_name}.")
            return None

        # Sắp xếp lại cột cho đúng thứ tự
        columns = ["province_id", "timestamp"] + HOURLY_PARAMS
        df = df[columns]
        
        return df
        
    except Exception as e:
        print(f"  Lỗi khi xử lý dữ liệu {province_name}: {e}")
        return None
