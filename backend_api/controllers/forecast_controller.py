# backend_api/controllers/forecast_controller.py
# Xử lý các route cho trang Dự báo và API thời tiết.

from flask import Blueprint, render_template, request, jsonify
from ..models.weather_model import Provinces
import requests

forecast_bp = Blueprint('forecast_bp', __name__)

@forecast_bp.route('/forecast')
def route_forecast():
    """Phục vụ trang dự báo."""
    return render_template('forecast.html', nav_active='forecast')


@forecast_bp.route('/api/provinces')
def api_get_provinces():
    """API lấy danh sách 63 tỉnh."""
    try:
        provinces = Provinces.query.order_by(Provinces.name).all()
        return jsonify([p.to_dict() for p in provinces])
    except Exception as e:
        print(f"Lỗi /api/provinces: {e}")
        return jsonify({"error": "Không thể lấy danh sách tỉnh"}), 500

@forecast_bp.route('/api/forecast')
def api_get_forecast():
    """API lấy dữ liệu thời tiết (từ Open-Meteo)."""
    province_name = request.args.get('name')
    if not province_name:
        return jsonify({"error": "Thiếu tham số 'name'"}), 400

    province = Provinces.query.filter_by(name=province_name).first()
    if not province:
        return jsonify({"error": "Không tìm thấy tỉnh"}), 404

    try:
        # Gọi API Open-Meteo để lấy DỰ BÁO 7 NGÀY
        forecast_url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": province.latitude,
            "longitude": province.longitude,
            "hourly": "temperature_2m,relative_humidity_2m,precipitation,rain,showers,weather_code,pressure_msl,wind_speed_10m",
            "daily": "weather_code,temperature_2m_max,temperature_2m_min",
            "timezone": "Asia/Bangkok",
            "forecast_days": 7
        }
        response = requests.get(forecast_url, params=params)
        response.raise_for_status()
        forecast_data = response.json()

        response_data = {
            "location": province.name,
            "daily": forecast_data.get("daily", {}),
            "hourly_detailed": {
                "labels": forecast_data.get("hourly", {}).get("time", [])[:48],
                "temperature_2m": forecast_data.get("hourly", {}).get("temperature_2m", [])[:48],
                "relative_humidity_2m": forecast_data.get("hourly", {}).get("relative_humidity_2m", [])[:48],
                "precipitation": forecast_data.get("hourly", {}).get("precipitation", [])[:48],
                "showers": forecast_data.get("hourly", {}).get("showers", [])[:48],
                "wind_speed_10m": forecast_data.get("hourly", {}).get("wind_speed_10m", [])[:48],
                "pressure_msl": forecast_data.get("hourly", {}).get("pressure_msl", [])[:48],
            },
        }
        return jsonify(response_data)

    except Exception as e:
        print(f"Lỗi /api/forecast: {e}")
        return jsonify({"error": f"Lỗi khi lấy dữ liệu: {e}"}), 500