import psycopg2
import psycopg2.extras as extras
import pandas as pd
from io import StringIO
import traceback

# --- CẤU HÌNH DATABASE ---
# Cần được thay đổi cho phù hợp
DB_CONFIG = {
    "host": "localhost",         # Chạy trên máy cá nhân
    "port": 5432,                # Cổng mặc định PostgreSQL
    "dbname": "weather_project", # Tên database bạn vừa tạo trong pgAdmin
    "user": "thanh",          # Username mặc định
    "password": "matkhaula123"         # Mật khẩu của bạn (hãy đổi nếu khác)
}

# --- HẰNG SỐ CẢNH BÁO ---
# Ngưỡng (Thresholds) dùng để xác định cảnh báo
RAIN_THRESHOLD = 50.0  # mm/h (Ví dụ: Mưa lớn > 50 mm/h)
WIND_THRESHOLD = 20.0  # m/s (Ví dụ: Gió mạnh > 20 m/s)

def connect_to_db():
    """
    Kết nối đến PostgreSQL database và trả về connection object.
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Kết nối database thành công.")
        return conn
    except Exception as e:
        print(f"LỖI KẾT NỐI DATABASE: {e}\n\nVui lòng kiểm tra lại DB_CONFIG và đảm bảo PostgreSQL đang chạy.")
        return None

def get_provinces_from_db(conn):
    """
    Lấy danh sách các tỉnh (ID, Tên, Lat, Lon) từ bảng provinces.
    """
    provinces = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT province_id, name, latitude, longitude FROM provinces")
            provinces = cur.fetchall()
    except Exception as e:
        print(f"LỖI khi lấy danh sách tỉnh: {e}")
        conn.rollback() # Hoàn tác nếu có lỗi
    return provinces

# --- THỜI TIẾT (Cập nhật get_last_timestamp để truy vấn cả hai bảng) ---

def get_last_timestamp(conn, province_id, table_name='weather_data'):
    """
    Lấy timestamp mới nhất của một tỉnh đã có trong database từ bảng được chỉ định.
    Mặc định: weather_data
    """
    try:
        with conn.cursor() as cur:
            # Lưu ý: "timestamp" cần dấu ngoặc kép nếu tên cột là từ khóa
            cur.execute(
                f'SELECT MAX("timestamp") FROM {table_name} WHERE province_id = %s',
                (province_id,)
            )
            result = cur.fetchone()[0] 
            return result
    except Exception as e:
        # print(f"  Lỗi khi lấy timestamp cuối cùng từ {table_name}: {e}") # Có thể bỏ qua vì lỗi này xảy ra khi bảng trống
        conn.rollback()
        return None

def insert_weather_data(conn, df):
    """
    Chèn (hoặc cập nhật) dữ liệu thời tiết từ DataFrame vào database
    sử dụng ON CONFLICT DO UPDATE, bao gồm tính toán cờ cảnh báo.
    """
    if df.empty:
        return 0
    
    # --- BƯỚC 1: TÍNH TOÁN VÀ THÊM CỜ CẢNH BÁO (FLAGGING) ---
    print("-> Đang tính toán cờ cảnh báo thời tiết...")
    
    # 1. Tính toán Heavy Rain Flag (Mưa lớn)
    if 'precipitation' in df.columns:
        df['heavy_rain_flag'] = df['precipitation'] > RAIN_THRESHOLD
    else:
        df['heavy_rain_flag'] = False
        print("   Cảnh báo: Không tìm thấy cột 'precipitation' để tính cờ mưa lớn.")
    
    # 2. Tính toán Strong Wind Flag (Gió mạnh)
    if 'wind_speed_10m' in df.columns:
        df['strong_wind_flag'] = df['wind_speed_10m'] > WIND_THRESHOLD
    else:
        df['strong_wind_flag'] = False
        print("   Cảnh báo: Không tìm thấy cột 'wind_speed_10m' để tính cờ gió mạnh.")
    
    # --- BƯỚC 2: CHÈN DỮ LIỆU VÀO DATABASE ---

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0) 
    
    cols = list(df.columns)
    try:
        cols[cols.index('timestamp')] = '"timestamp"'
    except ValueError:
        pass 
        
    cols_sql = ', '.join(cols)
    
    # Tạo danh sách các cột để UPDATE
    update_cols = ", ".join([f'{col} = EXCLUDED.{col}' for col in cols if col not in ['province_id', '"timestamp"', 'heavy_rain_flag', 'strong_wind_flag']])
    
    # Phải thêm các cột FLAG vào UPDATE vì chúng không phải cột GENERATED ALWAYS
    update_cols += ", heavy_rain_flag = EXCLUDED.heavy_rain_flag, strong_wind_flag = EXCLUDED.strong_wind_flag"

    sql_insert = f"""
    INSERT INTO weather_data ({cols_sql})
    VALUES %s
    ON CONFLICT (province_id, "timestamp") DO UPDATE SET
        {update_cols}
    """
    
    cursor = None
    try:
        cursor = conn.cursor()
        extras.execute_values(
            cursor,
            sql_insert,
            df.values,
            page_size=1000
        )
        conn.commit()
        return len(df)
    except Exception as e:
        print(f"LỖI khi chèn dữ liệu thời tiết: {e}")
        conn.rollback()
        traceback.print_exc()
        return 0
    finally:
        if cursor:
            cursor.close()

# --- CHẤT LƯỢNG KHÔNG KHÍ ---

def insert_air_quality_data(conn, df):
    """
    Chèn (hoặc cập nhật) dữ liệu chất lượng không khí từ DataFrame vào database
    sử dụng ON CONFLICT DO UPDATE.
    """
    if df.empty:
        return 0
    
    print("-> Đang chuẩn bị chèn dữ liệu chất lượng không khí...")

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0) 
    
    # Tên cột trong DataFrame phải khớp với tên cột trong DB
    cols = list(df.columns)
    try:
        cols[cols.index('timestamp')] = '"timestamp"'
    except ValueError:
        pass 
        
    cols_sql = ', '.join(cols)
    
    # Tạo danh sách các cột để UPDATE
    update_cols = ", ".join([f'{col} = EXCLUDED.{col}' for col in cols if col not in ['province_id', '"timestamp"']])
    
    sql_insert = f"""
    INSERT INTO air_quality_data ({cols_sql})
    VALUES %s
    ON CONFLICT (province_id, "timestamp") DO UPDATE SET
        {update_cols}
    """
    
    cursor = None
    try:
        cursor = conn.cursor()
        extras.execute_values(
            cursor,
            sql_insert,
            df.values,
            page_size=1000
        )
        conn.commit()
        return len(df)
    except Exception as e:
        print(f"LỖI khi chèn dữ liệu AQI: {e}")
        conn.rollback()
        traceback.print_exc()
        return 0
    finally:
        if cursor:
            cursor.close()
# data_pipeline/data_storage.py
# Hàm lấy thống kê thời tiết theo tháng cho một tỉnh trong một năm cụ thể

# data_pipeline/data_storage.py

def get_monthly_weather_stats(conn, province_id, year):
    """
    Lấy thống kê thời tiết theo tháng.
    Sử dụng COALESCE để đảm bảo không bị lỗi NULL nếu dữ liệu thiếu.
    """
    sql = """
        SELECT 
            EXTRACT(MONTH FROM "timestamp") as month,
            -- Nhiệt độ trung bình (Làm tròn 1 số thập phân)
            ROUND(AVG(COALESCE(temperature_2m, 0))::numeric, 1) as avg_temp,
            
            -- Tổng lượng mưa (SUM)
            ROUND(SUM(COALESCE(precipitation, 0))::numeric, 1) as total_rain,
            
            -- Độ ẩm trung bình
            ROUND(AVG(COALESCE(relative_humidity_2m, 0))::numeric, 1) as avg_humidity,
            
            -- Tốc độ gió trung bình
            ROUND(AVG(COALESCE(wind_speed_10m, 0))::numeric, 1) as avg_wind
        FROM weather_data 
        WHERE province_id = %s 
        AND EXTRACT(YEAR FROM "timestamp") = %s
        GROUP BY month
        ORDER BY month ASC;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (province_id, year))
            rows = cur.fetchall()
            
            result = []
            for row in rows:
                result.append({
                    "month": int(row[0]),
                    "avg_temp": float(row[1]) if row[1] is not None else 0,
                    "total_rain": float(row[2]) if row[2] is not None else 0,
                    "avg_humidity": float(row[3]) if row[3] is not None else 0,
                    "avg_wind": float(row[4]) if row[4] is not None else 0
                })
            return result
    except Exception as e:
        print(f"Lỗi lấy thống kê tháng: {e}")
        return []

# =============================================================================
# --- VÍ DỤ MINH HỌA (Giữ lại nếu bạn muốn test logic flagging/insert) ---
if __name__ == '__main__':
    # ... (giữ nguyên hoặc xóa phần test tùy ý)
    pass