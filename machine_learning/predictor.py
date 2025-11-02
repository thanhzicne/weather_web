# predictor.py
# Trách nhiệm: Tải mô hình đã huấn luyện và cung cấp hàm .predict()
# File này sẽ được controller gọi.

import joblib
import os
import pandas as pd

MODEL_PATH = os.path.join(os.path.dirname(__file__), 'models/storm_model.pkl')
ml_model = None

def load_model():
    """Tải mô hình vào bộ nhớ."""
    global ml_model
    if os.path.exists(MODEL_PATH):
        try:
            ml_model = joblib.load(MODEL_PATH)
            print(f"Tải mô hình ML thành công từ: {MODEL_PATH}")
            return True
        except Exception as e:
            print(f"Lỗi khi tải mô hình ML: {e}")
            return False
    else:
        print(f"Không tìm thấy file mô hình tại {MODEL_PATH}")
        return False

def predict_storm(input_data):
    """
    Nhận dữ liệu đầu vào (ví dụ: 1 hàng pandas) và trả về dự đoán.
    LƯU Ý: Đây là hàm VÍ DỤ. Dữ liệu đầu vào và đầu ra
    cần khớp với mô hình bạn huấn luyện (ví dụ: LinearRegression)
    """
    if ml_model is None:
        if not load_model():
            return {"error": "Mô hình không khả dụng"}

    try:
        # Ví dụ: input_data là 1 DataFrame có 1 dòng
        # input_data = pd.DataFrame([[25.0, 1012.0, 5.0]], 
        #                           columns=['temp_lag1', 'pressure_lag1', 'wind_lag1'])
        
        prediction = ml_model.predict(input_data)
        
        # Xử lý kết quả dự đoán
        # (Ví dụ: Trả về nhiệt độ dự đoán)
        return {"predicted_temperature": prediction[0]}
        
    except Exception as e:
        return {"error": f"Lỗi khi dự đoán: {e}"}

# Tải mô hình ngay khi file này được import
load_model()
