import sys
import os
import time
from datetime import datetime, timedelta
import requests

# Thêm thư mục gốc vào path để import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pipeline.data_loader import fetch_weather_api
from data_pipeline.data_cleaning import clean_api_data
# --- SỬA LỖI ---
# Thêm 'get_provinces_from_db' vào import
from data_pipeline.data_storage import connect_to_db, get_last_timestamp, insert_weather_data, get_provinces_from_db

# Thời gian nghỉ giữa các tỉnh (giây)
# Tăng thời gian nghỉ giữa các năm, không cần nghỉ giữa các tỉnh
PROVINCE_DELAY_SECONDS = 0 
YEAR_DELAY_SECONDS = 10 # Nghỉ 10 giây giữa mỗi NĂM
RETRY_DELAY_START = 10 # Bắt đầu chờ 10s nếu lỗi 429
MAX_RETRIES = 10 # Số lần thử lại tối đa nếu lỗi 429

def process_province_range(conn, province_id, province_name, lat, lon, start_date, end_date):
    """
    Hàm helper: Lấy và lưu dữ liệu cho một tỉnh trong một khoảng thời gian
    Bao gồm logic thử lại (retry)
    """
    print(f"  Đang xử lý khoảng: {start_date} đến {end_date}...")
    
    retries = 0
    current_delay = RETRY_DELAY_START
    api_data = None
    
    while retries < MAX_RETRIES:
        try:
            # 1. Tải dữ liệu từ API
            print(f"  Đang gọi API (Lần {retries + 1}/{MAX_RETRIES})...")
            api_data = fetch_weather_api(lat, lon, start_date, end_date)
            
            # 2. Xử lý (Clean) dữ liệu
            # Cần cả province_name để in log nếu có lỗi
            cleaned_df = clean_api_data(api_data, province_id, province_name)
            
            # 3. Lưu vào DB
            if not cleaned_df.empty:
                insert_weather_data(conn, cleaned_df)
                print(f"  Đã lưu/cập nhật {len(cleaned_df)} dòng cho khoảng {start_date} - {end_date}.")
            else:
                print(f"  Không có dữ liệu mới trong khoảng {start_date} - {end_date}.")
            
            # Nếu thành công, thoát khỏi vòng lặp retry
            break 
            
        except requests.exceptions.HTTPError as e:
            # Bắt lỗi 429 (Too Many Requests)
            if e.response.status_code == 429:
                print(f"  !!! Lỗi 429: Too Many Requests. Đang chờ {current_delay} giây để thử lại...")
                time.sleep(current_delay)
                retries += 1
                current_delay *= 2 # Tăng gấp đôi thời gian chờ
            else:
                # Nếu là lỗi HTTP khác (404, 500...), văng ra
                print(f"  !!! LỖI HTTP KHÁC: {e}")
                break # Bỏ qua khoảng này
        except Exception as e:
            print(f"  LỖI KHI XỬ LÝ {province_name} ({start_date}-{end_date}): {e}")
            break # Bỏ qua khoảng này
            
    if retries == MAX_RETRIES:
        print(f"  !!! ĐÃ THỬ LẠI {MAX_RETRIES} LẦN. BỎ QUA khoảng {start_date}-{end_date} cho tỉnh này.")

def run_pipeline():
    """Hàm chính chạy toàn bộ pipeline."""
    conn = connect_to_db()
    if not conn:
        return
    
    # --- SỬA LỖI ---
    # Lấy danh sách tỉnh (chưa có timestamp)
    provinces = get_provinces_from_db(conn) 
    if not provinces:
        print("Không thể lấy danh sách tỉnh từ DB.")
        return
        
    print(f"Lấy thành công {len(provinces)} tỉnh.")
    
    end_date_today_str = datetime.now().strftime('%Y-%m-%d')
    target_year = datetime.now().year

    for province in provinces:
        # --- SỬA LỖI (TypeError) ---
        # Hàm get_provinces_from_db trả về list of tuples
        # Chúng ta phải truy cập bằng index (0, 1, 2, 3)
        # thay vì tên cột (string key)
        province_id = province[0]
        name = province[1]
        lat = province[2]
        lon = province[3]
        
        # --- SỬA LỖI ---
        # Lấy last_timestamp cho TỪNG tỉnh MỘT ở đây
        last_ts = get_last_timestamp(conn, province_id) 
        
        print(f"\n--- Bắt đầu xử lý: {name} (ID: {province_id}) ---")

        if last_ts is None:
            # --- TRƯỜNG HỢP 1: LẤY LẦN ĐẦU (CHIA THEO NĂM) ---
            print("  Chưa có dữ liệu. Bắt đầu lấy theo từng năm từ 2020...")
            
            current_year = 2020
            while current_year <= target_year:
                loop_start_date = f"{current_year}-01-01"
                
                if current_year < target_year:
                    loop_end_date = f"{current_year}-12-31"
                else:
                    # Nếu là năm hiện tại, chỉ lấy đến hôm nay
                    loop_end_date = end_date_today_str
                
                # Gọi hàm xử lý cho từng NĂM
                process_province_range(conn, province_id, name, lat, lon, loop_start_date, loop_end_date)
                
                # Nghỉ giữa các NĂM
                if current_year < target_year:
                    print(f"  Nghỉ {YEAR_DELAY_SECONDS} giây trước khi lấy năm tiếp theo...")
                    time.sleep(YEAR_DELAY_SECONDS)

                current_year += 1
            
        else:
            # --- TRƯỜNG HỢP 2: CẬP NHẬT HÀNG NGÀY ---
            start_date_obj = last_ts.date() + timedelta(days=1)
            start_date_str = start_date_obj.strftime('%Y-%m-%d')
            
            if start_date_str > end_date_today_str:
                print(f"  Dữ liệu đã được cập nhật đến hôm qua. Bỏ qua.")
                continue
            
            # Gọi hàm xử lý cho khoảng cập nhật
            process_province_range(conn, province_id, name, lat, lon, start_date_str, end_date_today_str)

        if PROVINCE_DELAY_SECONDS > 0:
            print(f"  Nghỉ {PROVINCE_DELAY_SECONDS} giây trước khi sang tỉnh tiếp theo...")
            time.sleep(PROVINCE_DELAY_SECONDS)

    print("\n--- HOÀN TẤT TOÀN BỘ QUÁ TRÌNH ---")
    conn.close()

if __name__ == "__main__":
    run_pipeline()