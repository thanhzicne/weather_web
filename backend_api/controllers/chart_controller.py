# backend_api/controllers/chart_controller.py
from flask import Blueprint, render_template, jsonify, request
from data_pipeline.data_storage import connect_to_db, get_monthly_weather_stats # Import hàm mới
from datetime import datetime

chart_bp = Blueprint('chart_bp', __name__, template_folder='../templates')

@chart_bp.route('/chart')
def route_chart():
    # Render file HTML giao diện
    return render_template('chart.html', nav_active='chart')

@chart_bp.route('/api/weather-monthly')
def api_weather_monthly():
    # Lấy tham số từ URL, ví dụ: /api/weather-monthly?province_id=1&year=2024
    province_id = request.args.get('province_id', 1, type=int) # Mặc định ID=1
    year = request.args.get('year', datetime.now().year, type=int) # Mặc định năm hiện tại

    conn = connect_to_db()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    data = get_monthly_weather_stats(conn, province_id, year)
    conn.close()
    
    return jsonify(data)