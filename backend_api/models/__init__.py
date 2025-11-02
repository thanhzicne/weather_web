# backend_api/models/__init__.py
# Khởi tạo đối tượng DB để các model khác có thể import
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()
