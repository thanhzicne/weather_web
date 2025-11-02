# backend_api/models/weather_model.py
# Định nghĩa các bảng liên quan đến tỉnh và thời tiết.

from . import db # Import db từ file __init__.py cùng cấp

class Provinces(db.Model):
    __tablename__ = 'provinces'
    
    province_id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    latitude = db.Column(db.Float, nullable=False)
    longitude = db.Column(db.Float, nullable=False)
    
    weather_data = db.relationship('WeatherData', backref='province', lazy=True, cascade="all, delete-orphan")

    def to_dict(self):
        return {
            'province_id': self.province_id,
            'name': self.name,
            'lat': self.latitude,
            'lon': self.longitude
        }

class WeatherData(db.Model):
    __tablename__ = 'weather_data'
    
    data_id = db.Column(db.BigInteger, primary_key=True)
    province_id = db.Column(db.Integer, db.ForeignKey('provinces.province_id'), nullable=False)
    timestamp = db.Column(db.TIMESTAMP, nullable=False)
    
    temperature_2m = db.Column(db.Float)
    relative_humidity_2m = db.Column(db.Float)
    precipitation = db.Column(db.Float)
    rain = db.Column(db.Float)
    showers = db.Column(db.Float)
    weather_code = db.Column(db.Integer)
    pressure_msl = db.Column(db.Float)
    wind_speed_10m = db.Column(db.Float)
    wind_direction_10m = db.Column(db.Float)
