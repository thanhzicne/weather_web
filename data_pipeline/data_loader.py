import requests

def fetch_weather_api(lat, lon, start_date, end_date):
    """
    Tải dữ liệu thời tiết lịch sử từ Open-Meteo.
    Hàm này sẽ raise Exception nếu API call thất bại (vd: 404, 500, 429).
    """
    BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,rain,showers,weather_code,pressure_msl,wind_speed_10m,wind_direction_10m",
        "timezone": "Asia/Bangkok"
    }
    
    response = requests.get(BASE_URL, params=params)
    
    # Dòng này sẽ tự động "văng" lỗi (raise Exception) 
    # cho các mã trạng thái 4xx (như 429) hoặc 5xx.
    response.raise_for_status() 
    
    # Nếu không có lỗi, trả về JSON
    return response.json()