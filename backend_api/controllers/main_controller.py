# backend_api/controllers/main_controller.py
# Xử lý các route cho trang Home và News.

from flask import Blueprint, render_template
from ..models.news_model import get_latest_news

# Tạo một Blueprint
main_bp = Blueprint('main_bp', __name__)

@main_bp.route('/')
def route_home():
    """Phục vụ trang chủ."""
    return render_template('index.html', nav_active='home')

@main_bp.route('/news')
def route_news():
    """Phục vụ trang tin tức."""
    news_list = get_latest_news()
    return render_template('news.html', nav_active='news', news_list=news_list)
