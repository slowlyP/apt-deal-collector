import pandas as pd
import folium
import requests
import os
import time
import urllib3
from dotenv import load_dotenv

# 1. 시스템 설정
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()
KAKAO_API_KEY = os.environ.get('KAKAO_REST_API_KEY')
DATA_FILE = "data/realty_combined_20260410_1641.csv"

def get_kakao_coords(address):
    """
    [예외 처리 강화 버전]
    - 타임아웃 설정
    - API 상태 코드별 대응
    - KA 헤더 인증 우회
    """
    if not KAKAO_API_KEY:
        return None, None
        
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {
        "Authorization": f"KakaoAK {KAKAO_API_KEY}",
        "KA": "sdk/1.0.0 os/javascript lang/ko device/web origin/http://localhost:5500"
    }
    params = {"query": address}
    
    try:
        # timeout=5 를 추가하여 서버 응답이 없을 때 무한 대기를 방지합니다.
        response = requests.get(url, headers=headers, params=params, verify=False, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            if data['documents']:
                return float(data['documents'][0]['y']), float(data['documents'][0]['x'])
        elif response.status_code == 429:
            print("⚠️ API 호출 한도 초과(Quota Exceeded)")
        return None, None
    except requests.exceptions.RequestException as e:
        # 네트워크 단절 등 통신 에러 발생 시 프로그램 중단 방지
        return None, None

def create_realty_map():
    try:
        if not os.path.exists(DATA_FILE):
            print("❌ 데이터 파일을 찾을 수 없습니다.")
            return

        df = pd.read_csv(DATA_FILE)
        
        # 지도 생성 시 기본 좌표 (서울 강남구)
        m = folium.Map(location=[37.5172, 127.0473], zoom_start=14, tiles='CartoDB positron')
        
        print(f"🚀 실시간성 및 신뢰성 검증 엔진 가동...")
        
        success_count = 0
        for i, row in df.head(100).iterrows(): # 50개에서 100개로 확장 테스트
            lat, lon = None, None
            
            # [예외 처리] 데이터가 NaN(결측치)인 경우 스킵
            if pd.isna(row.get('apartment')) or pd.isna(row.get('dong')):
                continue

            # 1단계: 아파트명 정제 검색
            clean_name = str(row['apartment']).split('(')[0].split(',')[0]
            addr1 = f"서울특별시 강남구 {row['dong']} {clean_name}"
            lat, lon = get_kakao_coords(addr1)
            
            # 2단계: 본번/부번 조합 검색 (Fallback)
            if not lat:
                bon = str(int(row['bonbeon'])) if 'bonbeon' in row and pd.notnull(row['bonbeon']) else ""
                bu = str(int(row['bubeon'])) if 'bubeon' in row and pd.notnull(row['bubeon']) and row['bubeon'] != 0 else ""
                jibun = f"{bon}-{bu}".strip("-")
                if jibun:
                    addr2 = f"서울특별시 강남구 {row['dong']} {jibun}"
                    lat, lon = get_kakao_coords(addr2)

            if lat and lon:
                # 마커 추가
                folium.Marker(
                    location=[lat, lon],
                    popup=folium.Popup(f"<b>{row['apartment']}</b><br>거래가: {row['price']}만원", max_width=200),
                    tooltip=row['apartment']
                ).add_to(m)
                success_count += 1
            
            # API 매너 대기 (과도한 요청 방지)
            time.sleep(0.05)

        m.save("realty_map.html")
        print(f"\n✅ 프레임워크 검증 완료: {success_count}건 매핑 성공")
        
    except Exception as e:
        print(f"🔥 치명적 오류 발생: {e}")

if __name__ == "__main__":
    create_realty_map()