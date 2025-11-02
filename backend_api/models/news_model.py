# backend_api/models/news_model.py
# File này chứa logic nghiệp vụ cho trang tin tức.

def get_latest_news():
    """Lấy tin tức (dữ liệu giả)."""
    return [
        {
            "id": 1,
            "title": "Hiện tượng El Niño ảnh hưởng đến Việt Nam",
            "category": "Biến đổi khí hậu",
            "image": "https://placehold.co/600x400/1f2937/9ca3af?text=Ảnh+Tin+Tức"
        },
        {
            "id": 2,
            "title": "Tiềm năng điện gió ngoài khơi tại Bình Thuận",
            "category": "Năng lượng tái tạo",
            "image": "https://placehold.co/600x400/1f2937/9ca3af?text=Ảnh+Tin+Tức"
        }
    ]