import pandas as pd
import folium
from folium.plugins import MarkerCluster
import requests
import os
import time
import urllib3
import glob
from dotenv import load_dotenv
from news_collector import get_realtime_news

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()
KAKAO_API_KEY = os.environ.get('KAKAO_REST_API_KEY')

def get_kakao_coords(address):
    """카카오 위치 변환 (Circuit Breaker 및 Timeout 적용)"""
    if not KAKAO_API_KEY: return None, None
        
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    headers = {
        "Authorization": f"KakaoAK {KAKAO_API_KEY}",
        "KA": "sdk/1.0.0 os/javascript lang/ko device/web origin/http://localhost:5500"
    }
    params = {"query": address}
    
    try:
        # [Pro 예외처리 1] Timeout으로 서버 무한 대기 차단
        response = requests.get(url, headers=headers, params=params, verify=False, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            if data['documents']:
                return float(data['documents'][0]['y']), float(data['documents'][0]['x'])
                
        # [Pro 예외처리 2] Circuit Breaker: 하루 할당량 초과 시 API 호출 강제 중단
        elif response.status_code == 429:
            print("\n🚨 [치명적 오류] 카카오 API 일일 호출 한도(Quota) 초과!")
            return "QUOTA_EXCEEDED", "QUOTA_EXCEEDED"
            
    except requests.exceptions.RequestException:
        pass
    return None, None

def create_pro_map():
    # 가장 최근에 생성된 csv 파일 자동으로 찾기
    csv_files = glob.glob('data/realty_national_*.csv')
    if not csv_files:
        print("❌ 데이터 파일이 존재하지 않습니다.")
        return
    
    DATA_FILE = max(csv_files, key=os.path.getctime)
    print(f"📊 최신 데이터 로드 중: {DATA_FILE}")
    
    try:
        df = pd.read_csv(DATA_FILE)
    except pd.errors.EmptyDataError:
        print("❌ CSV 파일이 비어있습니다.")
        return

    # [Pro 예외처리 3] 데이터 클렌징 (결측치 제거)
    df = df.dropna(subset=['apartment', 'dong', 'price'])
    
    # 지도 초기화 (한국 중심)
    m = folium.Map(location=[36.3, 127.5], zoom_start=7, tiles='CartoDB positron')
    
    # [Pro 성능 최적화] 다방/직방 스타일 마커 클러스터링 적용
    marker_cluster = MarkerCluster(
        name="전국 실거래가 현황",
        overlay=True,
        control=True
    ).add_to(m)

    print(f"🚀 카카오 로컬 엔진 & 클러스터링 매핑 가동 (총 {len(df)}건)")
    
    success_count = 0
    quota_hit = False

    for i, row in df.iterrows():
        if quota_hit: break # 한도 초과 시 즉시 루프 탈출
        
        # 1차 정제 주소
        clean_name = str(row['apartment']).split('(')[0].split(',')[0].strip()
        addr = f"서울특별시 강남구 {row['dong']} {clean_name}" if "강남구" in row.get('region', '') else f"{row.get('region', '').split('_')[0]} {row['dong']} {clean_name}"
        
        lat, lon = get_kakao_coords(addr)
        
        # 한도 초과 감지
        if lat == "QUOTA_EXCEEDED":
            quota_hit = True
            break
            
        # 2차 지번 주소 (Fallback)
        if not lat:
            # int() 강제 변환을 없애고 문자열 분리(split)로 소수점만 제거
            bon = str(row.get('bonbeon')).split('.')[0] if pd.notnull(row.get('bonbeon')) else ""
            bu = str(row.get('bubeon')).split('.')[0] if pd.notnull(row.get('bubeon')) else ""
            
            if bu in ['0', '0000', 'nan', 'None']:
                bu = ""
                
            jibun = f"{bon}-{bu}".strip("-")
            # 'nan' 같은 가짜 데이터가 아닐 때만 주소 검색
            if jibun and jibun != "nan":
                lat, lon = get_kakao_coords(f"{row.get('region', '').split('_')[0]} {row['dong']} {jibun}")

        if lat and lon:
            # 개별 마커가 아닌 '클러스터'에 데이터 삽입
            folium.Marker(
                location=[lat, lon],
                popup=folium.Popup(f"<div style='width:200px'><b>{row['apartment']}</b><br>지역: {row.get('region', '')}<br>거래가: {row['price']}만원</div>", max_width=300),
                tooltip=f"{row['apartment']} ({row['price']}만)",
                icon=folium.Icon(color='blue', icon='home', prefix='fa')
            ).add_to(marker_cluster)
            success_count += 1
            
            # 진행상황 로깅 (50건마다 출력하여 로그 폭주 방지)
            if success_count % 50 == 0:
                print(f"🔄 매핑 진행 중... ({success_count}건 완료)")
                
        time.sleep(0.04) # 초당 약 25회 요청 제한 준수
# ---------------------------------------------------------
    # [수정] 실시간 뉴스 이슈 자동 좌표 매핑 로직
    # ---------------------------------------------------------
    print("⚡ 실시간 뉴스에서 지역을 분석하여 자동으로 마킹합니다...")
    news_df = get_realtime_news()

    if news_df is not None and not news_df.empty:
        # 서울 25개 구 대표 좌표 (이 목록에 있으면 자동으로 찍힙니다)
        seoul_gu_coords = {
            '강남': [37.495, 127.066], '서초': [37.483, 127.032], '송파': [37.514, 127.106],
            '강동': [37.530, 127.123], '마포': [37.566, 126.901], '용산': [37.532, 126.990],
            '성동': [37.563, 127.036], '광진': [37.538, 127.082], '동대문': [37.574, 127.039],
            '중랑': [37.606, 127.092], '성북': [37.589, 127.016], '강북': [37.639, 127.025],
            '도봉': [37.668, 127.047], '노원': [37.654, 127.056], '은평': [37.602, 126.929],
            '서대문': [37.579, 126.936], '양천': [37.516, 126.866], '강서': [37.550, 126.849],
            '구로': [37.495, 126.887], '금천': [37.456, 126.895], '영등포': [37.526, 126.896],
            '동작': [37.512, 126.939], '관악': [37.478, 126.951], '종로': [37.573, 126.979],
            '중구': [37.563, 126.997], '여의도': [37.521, 126.924], '반포': [37.504, 126.994],
            '잠실': [37.513, 127.094], '성수': [37.543, 127.044]
        }

        for _, row in news_df.iterrows():
            # 뉴스 제목에서 지역명 찾기
            region = row.get('region')
            
            # 1. 수집된 region이 좌표 목록에 있는지 확인
            if region in seoul_gu_coords:
                target_pos = seoul_gu_coords[region]
                
                popup_html = f"""
                <div style="width:250px; font-family: 'Malgun Gothic', sans-serif;">
                    <div style="background-color:#ff4757; color:white; padding:5px; border-radius:5px; text-align:center; font-weight:bold; margin-bottom:10px;">
                        🚨 실시간 핫이슈: {region}
                    </div>
                    <a href="{row['link']}" target="_blank" style="text-decoration:none; color:#2f3542; font-size:13px; font-weight:bold;">
                        {row['title']}
                    </a>
                    <div style="margin-top:5px; font-size:11px; color:#747d8c;">
                        출처: 네이버 뉴스 검색 API
                    </div>
                </div>
                """
                
                folium.Marker(
                    location=target_pos,
                    popup=folium.Popup(popup_html, max_width=300),
                    icon=folium.Icon(color='red', icon='bolt', prefix='fa'), # 번개 아이콘으로 변경
                    tooltip=f"🔥 {region} 실시간 이슈"
                ).add_to(m)
    # ---------------------------------------------------------

    # [저장 경로 설정]
    # Flask 서버를 쓰고 계시다면 templates 폴더 안에 저장해야 웹에서 보입니다.
    m.save("templates/realty_map.html") 
    print(f"\n✨ 하이브리드 지도 생성 완료! (경로: templates/realty_map.html)")
    if quota_hit:
        print("⚠️ API 한도 초과로 일부 데이터만 매핑되었습니다. 내일 이어서 자동으로 수행됩니다.")

if __name__ == "__main__":
    create_pro_map()