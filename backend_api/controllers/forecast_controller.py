# backend_api/controllers/forecast_controller.py
# Xử lý các route cho trang Dự báo và API thời tiết.

from flask import Blueprint, render_template, request, jsonify
import sys
import os
import numpy as np

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend_api.models.weather_model import Provinces
from data_pipeline.data_storage import connect_to_db, get_last_timestamp
from machine_learning.predictor import predict_storm

import requests
import pandas as pd
from datetime import datetime, timedelta

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
        return jsonify([{
            'province_id': p.province_id,
            'name': p.name,
            'latitude': p.latitude,
            'longitude': p.longitude
        } for p in provinces])
    except Exception as e:
        print(f"Lỗi /api/provinces: {e}")
        return jsonify({"error": "Không thể lấy danh sách tỉnh"}), 500

def merge_api_and_ml_data(api_data, ml_data, province_name):
    """
    Merge dữ liệu từ Open-Meteo API và ML predictions
    Ưu tiên API cho giờ hiện tại và các giờ có sẵn,
    dùng ML để bổ sung các giờ còn thiếu
    """
    merged_data = {
        "location": province_name,
        "current": api_data.get("current", {}),
        "daily": {},
        "hourly": {},
        "ml_prediction": ml_data
    }
    
    # Merge hourly data
    api_hourly = api_data.get("hourly", {})
    api_times = api_hourly.get('time', [])
    
    if ml_data and 'hourly_predictions' in ml_data:
        ml_hourly = ml_data['hourly_predictions']
        
        # Tạo dict để dễ merge
        hourly_dict = {
            'time': [],
            'temperature_2m': [],
            'relative_humidity_2m': [],
            'precipitation': [],
            'rain': [],
            'showers': [],
            'weather_code': [],
            'pressure_msl': [],
            'wind_speed_10m': [],
            'wind_direction_10m': [],
            'visibility': [],
            'uv_index': []
        }
        
        # Lấy tất cả thời gian cần thiết (API + ML)
        all_times = []
        now = datetime.now()
        
        # Thêm từ API (nếu có)
        for i, time_str in enumerate(api_times):
            time_obj = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            if time_obj >= now:
                all_times.append({
                    'time': time_str,
                    'source': 'api',
                    'index': i
                })
        
        # Thêm từ ML cho các giờ còn thiếu
        for ml_hour in ml_hourly:
            ml_time = ml_hour['time']
            # Kiểm tra xem thời gian này đã có trong API chưa
            if ml_time not in [t['time'] for t in all_times]:
                all_times.append({
                    'time': ml_time,
                    'source': 'ml',
                    'data': ml_hour
                })
        
        # Sort theo thời gian
        all_times.sort(key=lambda x: x['time'])
        
        # Merge data
        for time_info in all_times[:48]:  # Chỉ lấy 48 giờ
            hourly_dict['time'].append(time_info['time'])
            
            if time_info['source'] == 'api':
                idx = time_info['index']
                hourly_dict['temperature_2m'].append(api_hourly.get('temperature_2m', [])[idx] if idx < len(api_hourly.get('temperature_2m', [])) else 0)
                hourly_dict['relative_humidity_2m'].append(api_hourly.get('relative_humidity_2m', [])[idx] if idx < len(api_hourly.get('relative_humidity_2m', [])) else 0)
                hourly_dict['precipitation'].append(api_hourly.get('precipitation', [])[idx] if idx < len(api_hourly.get('precipitation', [])) else 0)
                hourly_dict['rain'].append(api_hourly.get('rain', [])[idx] if idx < len(api_hourly.get('rain', [])) else 0)
                hourly_dict['showers'].append(api_hourly.get('showers', [])[idx] if idx < len(api_hourly.get('showers', [])) else 0)
                hourly_dict['weather_code'].append(api_hourly.get('weather_code', [])[idx] if idx < len(api_hourly.get('weather_code', [])) else 0)
                hourly_dict['pressure_msl'].append(api_hourly.get('pressure_msl', [])[idx] if idx < len(api_hourly.get('pressure_msl', [])) else 0)
                hourly_dict['wind_speed_10m'].append(api_hourly.get('wind_speed_10m', [])[idx] if idx < len(api_hourly.get('wind_speed_10m', [])) else 0)
                hourly_dict['wind_direction_10m'].append(api_hourly.get('wind_direction_10m', [])[idx] if idx < len(api_hourly.get('wind_direction_10m', [])) else 0)
                hourly_dict['visibility'].append(api_hourly.get('visibility', [])[idx] if idx < len(api_hourly.get('visibility', [])) else 0)
                hourly_dict['uv_index'].append(api_hourly.get('uv_index', [])[idx] if idx < len(api_hourly.get('uv_index', [])) else 0)
            else:  # ML data
                ml_hour = time_info['data']
                hourly_dict['temperature_2m'].append(ml_hour['temperature_2m'])
                hourly_dict['relative_humidity_2m'].append(ml_hour['relative_humidity_2m'])
                hourly_dict['precipitation'].append(ml_hour['precipitation'])
                hourly_dict['rain'].append(ml_hour['precipitation'])
                hourly_dict['showers'].append(0)
                hourly_dict['weather_code'].append(ml_hour['weather_code'])
                hourly_dict['pressure_msl'].append(ml_hour['pressure_msl'])
                hourly_dict['wind_speed_10m'].append(ml_hour['wind_speed_10m'])
                hourly_dict['wind_direction_10m'].append(0)
                hourly_dict['visibility'].append(ml_hour['visibility'])
                hourly_dict['uv_index'].append(ml_hour['uv_index'])
        
        merged_data['hourly'] = hourly_dict
    else:
        merged_data['hourly'] = api_hourly
    
    # Merge daily data
    api_daily = api_data.get("daily", {})
    
    if ml_data and 'daily_forecast' in ml_data:
        ml_daily = ml_data['daily_forecast']
        
        # Nếu API có ít hơn 7 ngày, bổ sung từ ML
        api_daily_times = api_daily.get('time', [])
        
        if len(api_daily_times) < 7:
            # Merge daily data
            daily_dict = {
                'time': list(api_daily.get('time', [])),
                'weather_code': list(api_daily.get('weather_code', [])),
                'temperature_2m_max': list(api_daily.get('temperature_2m_max', [])),
                'temperature_2m_min': list(api_daily.get('temperature_2m_min', [])),
                'precipitation_sum': list(api_daily.get('precipitation_sum', [])),
                'wind_speed_10m_max': list(api_daily.get('wind_speed_10m_max', [])),
                'sunrise': list(api_daily.get('sunrise', [])),
                'sunset': list(api_daily.get('sunset', []))
            }
            
            # Thêm từ ML
            for ml_day in ml_daily:
                if ml_day['time'] not in daily_dict['time']:
                    daily_dict['time'].append(ml_day['time'])
                    daily_dict['weather_code'].append(ml_day['weather_code'])
                    daily_dict['temperature_2m_max'].append(ml_day['temperature_2m_max'])
                    daily_dict['temperature_2m_min'].append(ml_day['temperature_2m_min'])
                    daily_dict['precipitation_sum'].append(ml_day['precipitation_sum'])
                    daily_dict['wind_speed_10m_max'].append(ml_day['wind_speed_10m_max'])
                    daily_dict['sunrise'].append(ml_day['sunrise'])
                    daily_dict['sunset'].append(ml_day['sunset'])
                    
                    if len(daily_dict['time']) >= 7:
                        break
            
            merged_data['daily'] = daily_dict
        else:
            merged_data['daily'] = api_daily
    else:
        merged_data['daily'] = api_daily
    
    return merged_data

@forecast_bp.route('/api/forecast')
def api_get_forecast():
    """API lấy dữ liệu thời tiết (từ Open-Meteo + ML nếu cần + AQI)."""
    province_name = request.args.get('province', '')
    days = int(request.args.get('days', 7))
    
    if not province_name:
        return jsonify({"error": "Thiếu province"}), 400

    try:
        province = Provinces.query.filter_by(name=province_name).first()
        if not province:
            return jsonify({"error": "Không tìm thấy tỉnh"}), 404

        # Fetch weather from Open-Meteo
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": province.latitude,
            "longitude": province.longitude,
            "hourly": "temperature_2m,relative_humidity_2m,precipitation,rain,showers,weather_code,pressure_msl,wind_speed_10m,wind_direction_10m,visibility,uv_index",
            "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,sunrise,sunset",
            "current": "temperature_2m,apparent_temperature,relative_humidity_2m,precipitation,wind_speed_10m,pressure_msl,visibility,uv_index,weather_code",
            "timezone": "Asia/Bangkok",
            "forecast_days": min(days, 16)
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        api_data = response.json()

        # Dự đoán ML
        ml_data = None
        try:
            current_weather_data = {
                'temperature_2m': api_data.get("current", {}).get('temperature_2m', 25),
                'relative_humidity_2m': api_data.get("current", {}).get('relative_humidity_2m', 70),
                'pressure_msl': api_data.get("current", {}).get('pressure_msl', 1013),
                'wind_speed_10m': api_data.get("current", {}).get('wind_speed_10m', 5)
            }
            
            ml_data = predict_storm(province.province_id, current_weather_data)
            
            if 'error' in ml_data:
                print(f"ML prediction error: {ml_data['error']}")
                ml_data = None
                
        except Exception as e:
            print(f"Lỗi ML prediction: {e}")
            import traceback
            traceback.print_exc()
            ml_data = None

        # Merge API và ML data
        forecast_data = merge_api_and_ml_data(api_data, ml_data, province_name)

        # Fetch AQI
        try:
            aqi_url = f"https://api.waqi.info/feed/geo:{province.latitude};{province.longitude}/?token=demo"
            aqi_response = requests.get(aqi_url, timeout=5)
            if aqi_response.status_code == 200:
                aqi_json = aqi_response.json()
                if aqi_json.get('status') == 'ok':
                    aqi_data = aqi_json.get('data', {})
                    forecast_data['aqi'] = {
                        'index': aqi_data.get('aqi', 0),
                        'components': aqi_data.get('iaqi', {})
                    }
                else:
                    forecast_data['aqi'] = {'index': 0, 'components': {}}
            else:
                forecast_data['aqi'] = {'index': 0, 'components': {}}
        except Exception as e:
            print(f"Lỗi AQI: {e}")
            forecast_data['aqi'] = {'index': 0, 'components': {}}

        return jsonify(forecast_data)
        
    except requests.RequestException as e:
        print(f"Lỗi request: {e}")
        return jsonify({"error": f"Lỗi khi gọi API thời tiết: {str(e)}"}), 500
    except Exception as e:
        print(f"Lỗi tổng quát: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Lỗi khi lấy dữ liệu: {str(e)}"}), 500