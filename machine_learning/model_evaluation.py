# model_evaluation.py
# Trách nhiệm: Tải model và đánh giá trên tập test.

import joblib
import os
from sklearn.metrics import mean_squared_error
from model_training import load_data_for_training, feature_engineering, MODEL_PATH

def evaluate():
    print("--- BẮT ĐẦU ĐÁNH GIÁ MÔ HÌNH ---")
    
    # 1. Tải mô hình
    if not os.path.exists(MODEL_PATH):
        print(f"Không tìm thấy mô hình tại {MODEL_PATH}. Vui lòng chạy model_training.py trước.")
        return
        
    print(f"Đang tải mô hình từ {MODEL_PATH}...")
    model = joblib.load(MODEL_PATH)
    
    # 2. Tải và xử lý dữ liệu (chỉ để lấy tập test)
    # Lý tưởng nhất, bạn nên lưu tập test riêng
    df = load_data_for_training()
    if df.empty:
        return
        
    X, y = feature_engineering(df)
    if X.empty:
        return
        
    # Tái tạo lại tập test (dùng cùng random_state)
    # (Đây là cách làm đơn giản, thực tế nên lưu X_test, y_test)
    from sklearn.model_selection import train_test_split
    _, X_test, _, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # 3. Đánh giá
    print("Đang dự đoán trên tập test...")
    preds = model.predict(X_test)
    mse = mean_squared_error(y_test, preds)
    
    print("\n--- KẾT QUẢ ĐÁNH GIÁ ---")
    print(f"  Mean Squared Error (MSE): {mse}")
    print(f"  Root Mean Squared Error (RMSE): {mse**0.5}")
    print("------------------------")

if __name__ == "__main__":
    evaluate()
