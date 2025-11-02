# backend_api/controllers/storm_controller.py
# Xử lý các route cho trang Bão và API Bão.

from flask import Blueprint, render_template, jsonify
import pandas as pd
import sys, os

# Thêm thư mục gốc vào path để import predictor
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from machine_learning.predictor import predict_storm

storm_bp = Blueprint('storm_bp', __name__)

@storm_bp.route('/storm')
def route_storm():
    """Phục vụ trang dự báo bão."""
    return render_template('storm.html', nav_active='storm')

@storm_bp.route('/api/storm_track')
def api_get_storm_track():
    """
    API dự đoán đường đi của bão.
    Sẽ gọi hàm predict_storm() từ ML.
    """
    
    # 1. Thu thập dữ liệu đầu vào (Input Features)
    # Ví dụ: lấy dữ liệu 1h trước từ DB (tạm thời hardcode)
    input_df = pd.DataFrame(
        [[25.0, 1008.0, 15.0]], 
        columns=['temp_lag1', 'pressure_lag1', 'wind_lag1']
    )
    
    # 2. Gọi hàm dự đoán từ predictor
    prediction_result = predict_storm(input_df)
    
    # 3. Kiểm tra xem ML có chạy không
    if "error" in prediction_result:
        # Nếu mô hình lỗi, trả về dữ liệu giả
        print(f"Lỗi predictor: {prediction_result['error']}. Trả về dữ liệu giả.")
        return jsonify(get_mock_storm_track())

    # 4. Nếu thành công, xử lý kết quả (hiện tại đang ví dụ)
    # prediction_result['predicted_temperature']
    # TODO: Chuyển đổi kết quả dự đoán thành GeoJSON
    
    # Tạm thời vẫn trả về dữ liệu giả
    return jsonify(get_mock_storm_track())


def get_mock_storm_track():
    """Tạo dữ liệu bão giả."""
    return {
        "type": "FeatureCollection",
        "features": [
            { "type": "Feature", "geometry": { "type": "LineString", "coordinates": [[118.0, 12.0], [117.0, 12.5], [116.0, 13.0], [114.5, 14.0]] }},
            { "type": "Feature", "geometry": { "type": "Point", "coordinates": [114.5, 14.0] }, "properties": { "name": "Tâm bão (t+0)" } }
        ]
    }
