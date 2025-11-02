// main.js
// Chứa toàn bộ logic JS cho tất cả các trang.

// --- LOGIC CHUNG (chạy trên mọi trang) ---
document.addEventListener('DOMContentLoaded', () => {
    lucide.createIcons();

    const mobileMenuButton = document.getElementById('mobile-menu-button');
    const mobileMenu = document.getElementById('mobile-menu');
    
    if (mobileMenuButton && mobileMenu) {
        mobileMenuButton.addEventListener('click', () => {
            mobileMenu.classList.toggle('hidden');
        });
    }
});

// --- LOGIC TRANG DỰ BÁO (/forecast) ---
// Hàm này được gọi từ template forecast.html
function initForecastPage() {
    console.log("Khởi tạo trang Dự báo...");
    
    const dom = {
        provinceSelect: document.getElementById('province-select'),
        geolocateBtn: document.getElementById('geolocate-btn'),
        loadingSpinner: document.getElementById('loading-spinner'),
        forecastResults: document.getElementById('forecast-results'),
        forecastLocation: document.getElementById('forecast-location'),
        messageBox: document.getElementById('message-box'),
        dailyForecastContainer: document.getElementById('daily-forecast-container'),
    };
    
    let charts = {};
    let provincesData = [];

    async function loadProvinces() {
        try {
            // API này được định nghĩa trong forecast_controller.py
            const response = await fetch('/api/provinces'); 
            if (!response.ok) throw new Error('Không thể tải danh sách tỉnh');
            
            provincesData = await response.json();
            
            dom.provinceSelect.innerHTML = ''; 
            provincesData.forEach(province => {
                const option = document.createElement('option');
                option.value = province.name;
                option.textContent = province.name;
                dom.provinceSelect.appendChild(option);
            });
            handleGeolocation();
            
        } catch (error) {
            console.error(error);
            dom.provinceSelect.innerHTML = '<option>Lỗi tải danh sách tỉnh</option>';
        }
    }
    
    async function fetchWeather(provinceName) {
        console.log(`Đang lấy dữ liệu cho: ${provinceName}`);
        dom.loadingSpinner.classList.remove('hidden');
        dom.forecastResults.classList.add('hidden');
        dom.messageBox.classList.add('hidden');
        dom.forecastLocation.textContent = `Dự báo cho ${provinceName}`;
        
        try {
            // API này được định nghĩa trong forecast_controller.py
            const response = await fetch(`/api/forecast?name=${encodeURIComponent(provinceName)}`);
            if (!response.ok) {
                const err = await response.json();
                throw new Error(err.error || 'Lỗi không xác định từ server');
            }
            const data = await response.json();
            updateUI(data);
            dom.loadingSpinner.classList.add('hidden');
            dom.forecastResults.classList.remove('hidden');
            
        } catch (error) {
            console.error("Lỗi khi fetch dữ liệu:", error);
            showMessage(dom, `Không thể tải dữ liệu cho ${provinceName}. (${error.message})`);
        }
    }

    function updateUI(data) {
        dom.dailyForecastContainer.innerHTML = '';
        const daily = data.daily;
        daily.time.forEach((day, index) => {
            const { icon } = getWeatherIconAndText(daily.weather_code[index]);
            const dayFormatted = new Date(day).toLocaleDateString('vi-VN', { weekday: 'short', day: '2-digit', month: '2-digit' });
            
            dom.dailyForecastContainer.innerHTML += `
                <div class="bg-gray-800 p-3 rounded-lg text-center shadow">
                    <p class="font-semibold text-sm text-gray-300">${dayFormatted}</p>
                    <i data-lucide="${icon}" class="w-10 h-10 text-blue-400 mx-auto my-2"></i>
                    <p class="font-bold text-xl text-white">${Math.round(daily.temperature_2m_max[index])}°</p>
                    <p class="text-sm text-gray-400">${Math.round(daily.temperature_2m_min[index])}°</p>
                </div>
            `;
        });
        
        const hourly = data.hourly_detailed;
        const labels = hourly.labels.map(t => new Date(t));
        Object.values(charts).forEach(chart => chart.destroy());

        charts.tempHumidity = new Chart(document.getElementById('chart-temp-humidity'), {
            type: 'line', data: { labels: labels, datasets: [
                { label: 'Nhiệt độ (°C)', data: hourly.temperature_2m, borderColor: '#facc15', yAxisID: 'yTemp' },
                { label: 'Độ ẩm (%)', data: hourly.relative_humidity_2m, borderColor: '#38bdf8', yAxisID: 'yHumidity' }
            ]}, options: chartOptions('yTemp', '°C', 'yHumidity', '%')
        });
        
        charts.precipitation = new Chart(document.getElementById('chart-precipitation'), {
            type: 'bar', data: { labels: labels, datasets: [
                { label: 'Lượng mưa (mm)', data: hourly.precipitation, backgroundColor: '#60a5fa' },
                { label: 'Mưa rào (mm)', data: hourly.showers, backgroundColor: '#2563eb' }
            ]}, options: chartOptions('yPrecip', 'mm')
        });
        
        charts.wind = new Chart(document.getElementById('chart-wind'), {
            type: 'line', data: { labels: labels, datasets: [{ label: 'Tốc độ gió (km/h)', data: hourly.wind_speed_10m, borderColor: '#4ade80' }] },
            options: chartOptions('yWind', 'km/h')
        });

        charts.pressure = new Chart(document.getElementById('chart-pressure'), {
            type: 'line', data: { labels: labels, datasets: [{ label: 'Áp suất (hPa)', data: hourly.pressure_msl, borderColor: '#f472b6' }] },
            options: chartOptions('yPressure', 'hPa')
        });

        lucide.createIcons();
    }
    
    function handleGeolocation() {
        if ('geolocation' in navigator) {
            showMessage(dom, 'Đang xin quyền truy cập vị trí...', false);
            navigator.geolocation.getCurrentPosition(
                (position) => {
                    const { latitude, longitude } = position.coords;
                    const nearest = findNearestProvince(provincesData, latitude, longitude);
                    dom.provinceSelect.value = nearest.name;
                    fetchWeather(nearest.name);
                },
                (error) => {
                    console.warn("Lỗi định vị:", error.message);
                    showMessage(dom, `Không thể lấy vị trí. Mặc định hiển thị Hà Nội.`, false);
                    fetchWeather("Hà Nội");
                }
            );
        } else {
            showMessage(dom, 'Trình duyệt không hỗ trợ định vị. Mặc định hiển thị Hà Nội.', false);
            fetchWeather("Hà Nội");
        }
    }
    
    dom.provinceSelect.addEventListener('change', (e) => fetchWeather(e.target.value));
    dom.geolocateBtn.addEventListener('click', handleGeolocation);
    loadProvinces();
}

// --- LOGIC TRANG BÃO (/storm) ---
// Hàm này được gọi từ template storm.html
async function initStormMap() {
    console.log("Khởi tạo trang Bão...");
    const mapContainer = document.getElementById('map-container');
    if (!mapContainer) return;
    
    let stormMap = L.map('map-container').setView([15.0, 115.0], 5);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; OpenStreetMap &copy; CARTO',
    }).addTo(stormMap);

    try {
        // API này được định nghĩa trong storm_controller.py
        const response = await fetch('/api/storm_track');
        const stormData = await response.json();
        
        L.geoJSON(stormData, {
            style: (feature) => (feature.geometry.type === 'LineString') ? { color: 'red', weight: 3, opacity: 0.7 } : {},
            pointToLayer: (feature, latlng) => {
                return L.circleMarker(latlng, { radius: 8, color: 'red', fillColor: '#f03', fillOpacity: 0.8 })
                         .bindPopup(feature.properties.name || 'Tâm bão');
            }
        }).addTo(stormMap);
        
        const lastFeature = stormData.features.find(f => f.geometry.type === 'Point');
        if(lastFeature) {
            const [lon, lat] = lastFeature.geometry.coordinates;
            L.popup().setLatLng([lat, lon]).setContent(lastFeature.properties.name || 'Tâm bão').openOn(stormMap);
        }
    } catch (error) {
        console.error("Lỗi khi tải dữ liệu bão:", error);
    }
}

// --- CÁC HÀM TIỆN ÍCH (dùng chung) ---
function getWeatherIconAndText(code) {
    const map = { 0: { icon: 'sun' }, 1: { icon: 'sun' }, 2: { icon: 'cloud-sun' }, 3: { icon: 'cloud' }, 45: { icon: 'cloud-fog' }, 48: { icon: 'cloud-fog' }, 51: { icon: 'drizzle' }, 53: { icon: 'drizzle' }, 55: { icon: 'drizzle' }, 61: { icon: 'cloud-rain' }, 63: { icon: 'cloud-rain' }, 65: { icon: 'cloud-rain' }, 80: { icon: 'cloud-drizzle' }, 81: { icon: 'cloud-drizzle' }, 82: { icon: 'cloud-drizzle' }, 95: { icon: 'cloud-lightning' }, 96: { icon: 'cloud-lightning' }, 99: { icon: 'cloud-lightning' }};
    return map[code] || { icon: 'cloud-question' };
}

function showMessage(dom, message, isError = true) {
    if (!dom.messageBox) return;
    dom.messageBox.innerHTML = message;
    dom.messageBox.className = isError ? 'block bg-red-800 text-red-100 px-4 py-3 rounded-lg mb-6' : 'block bg-blue-800 text-blue-100 px-4 py-3 rounded-lg mb-6';
    dom.loadingSpinner.classList.add('hidden');
    dom.forecastResults.classList.add('hidden');
}

function findNearestProvince(provincesData, lat, lon) {
    let minDistance = Infinity;
    let nearestProvince = provincesData[0];
    provincesData.forEach(province => {
        const dLat = (province.lat - lat) * Math.PI / 180;
        const dLon = (province.lon - lon) * Math.PI / 180;
        const a = 0.5 - Math.cos(dLat) / 2 + Math.cos(lat * Math.PI / 180) * Math.cos(province.lat * Math.PI / 180) * (1 - Math.cos(dLon)) / 2;
        const distance = 12742 * Math.asin(Math.sqrt(a));
        if (distance < minDistance) { minDistance = distance; nearestProvince = province; }
    });
    return nearestProvince;
}

function chartOptions(y1ID, y1Label, y2ID = null, y2Label = null) {
    const options = {
        responsive: true, maintainAspectRatio: false,
        scales: {
            x: { type: 'time', time: { unit: 'hour', tooltipFormat: 'HH:mm dd/MM' }, ticks: { color: '#9ca3af' }, grid: { color: '#374151' } },
            [y1ID]: { type: 'linear', position: 'left', title: { display: true, text: y1Label, color: '#e5e7eb' }, ticks: { color: '#9ca3af' }, grid: { color: '#374151' } }
        },
        plugins: { legend: { labels: { color: '#e5e7eb' } }, tooltip: { mode: 'index', intersect: false } }
    };
    if(y2ID) {
        options.scales[y2ID] = { type: 'linear', position: 'right', title: { display: true, text: y2Label, color: '#e5e7eb' }, ticks: { color: '#9ca3af' }, grid: { drawOnChartArea: false } };
    }
    return options;
}
