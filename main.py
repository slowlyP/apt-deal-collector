import requests
import pandas as pd
import os
import time
from datetime import datetime
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

load_dotenv()
SERVICE_KEY = os.environ.get('SERVICE_KEY')

# 전국 주요 15개 시/도/구 법정동 코드 (Pro버전 확장 리스트)
# 원하시면 이 리스트를 250개 전국 시군구 코드로 늘리기만 하면 됩니다.
REGION_CODES = {
    "서울_강남구": "11680", "서울_서초구": "11650", "서울_송파구": "11710",
    "경기_성남분당구": "41135", "경기_수원영통구": "41117", "경기_과천시": "41290",
    "부산_해운대구": "26350", "대구_수성구": "27260", "인천_연수구": "28185"
}

def fetch_realty_data(lawd_cd, deal_ym):
    """국토부 아파트 실거래가 API 호출 (재시도 및 예외처리 포함)"""
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    params = {
        "serviceKey": requests.utils.unquote(SERVICE_KEY),
        "pageNo": "1",
        "numOfRows": "1000", # 한 번에 최대한 많이
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ym
    }
    
    # [Pro 예외처리 1] 일시적 네트워크 장애 방어 (최대 3번 재시도)
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.content
            elif response.status_code == 503:
                print("⚠️ 공공데이터 포털 서버 과부하. 5초 후 재시도...")
                time.sleep(5)
        except requests.exceptions.RequestException as e:
            print(f"⚠️ 네트워크 오류 발생 ({attempt+1}/3): {e}")
            time.sleep(3)
            
    return None

def parse_xml_to_df(xml_data):
    """XML 데이터를 Pandas DataFrame으로 안전하게 변환"""
    if not xml_data: return pd.DataFrame()
    
    try:
        root = ET.fromstring(xml_data)
        items = root.findall('.//item')
        
        # [Pro 예외처리 2] 해당 월에 거래가 아예 없는 지역 처리
        if not items:
            return pd.DataFrame()
            
        data_list = []
        for item in items:
            data = {
                'apartment': item.findtext('아파트'),
                'dong': item.findtext('법정동'),
                'price': item.findtext('거래금액'),
                'area': item.findtext('전용면적'),
                'floor': item.findtext('층'),
                'bonbeon': item.findtext('본번'),
                'bubeon': item.findtext('부번'),
            }
            data_list.append(data)
        return pd.DataFrame(data_list)
        
    except ET.ParseError:
        print("❌ XML 파싱 에러: 공공데이터 응답 형식이 올바르지 않습니다.")
        return pd.DataFrame()

def run_collector():
    if not SERVICE_KEY:
        print("🚨 치명적 오류: SERVICE_KEY가 설정되지 않았습니다.")
        return

    # [Pro 동적 날짜] 하드코딩 없이 실행되는 날짜의 '지난 달' 데이터를 자동으로 가져옴
    current_date = datetime.now()
    deal_ym = current_date.strftime("%Y%m") # 예: 202604
    
    all_data = pd.DataFrame()
    
    print(f"🚀 [Pro] 전국 데이터 수집 파이프라인 가동 (기준월: {deal_ym})")
    
    for name, code in REGION_CODES.items():
        print(f"📡 수집 중: {name} (코드: {code})...", end="")
        xml_data = fetch_realty_data(code, deal_ym)
        df = parse_xml_to_df(xml_data)
        
        if not df.empty:
            df['region'] = name # 지역 태그 추가
            all_data = pd.concat([all_data, df], ignore_index=True)
            print(f" ✅ {len(df)}건 완료")
        else:
            print(" ⚠️ 거래 데이터 없음")
            
        # [Pro 예외처리 3] 공공데이터 API 호출 제한 방어 (Traffic Throttle)
        time.sleep(1.5) 

    # 디렉토리 확인 및 생성
    os.makedirs('data', exist_ok=True)
    file_name = f"data/realty_national_{deal_ym}.csv"
    all_data.to_csv(file_name, index=False, encoding='utf-8-sig')
    print(f"✨ 전국 수집 완료! 총 {len(all_data)}건 저장됨 ({file_name})")

if __name__ == "__main__":
    run_collector()