# backend_api/models/storm_model.py
# File này sẽ chứa logic nghiệp vụ (business logic)
# để xử lý dữ liệu cho trang bão.

# (Ví dụ: một hàm logic)
def get_storm_warning_level(wind_speed):
    if wind_speed > 117:
        return "Cực kỳ nguy hiểm"
    elif wind_speed > 88:
        return "Nguy hiểm"
    else:
        return "Cảnh báo"
