import requests
import pandas as pd
import os
import time
from datetime import datetime
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import create_engine, text  # text를 추가합니다.

# [수정] 현재 실행 중인 main.py의 절대 경로를 찾아서 그 옆에 있는 .env를 로드합니다.
current_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(current_dir, '.env')
load_dotenv(dotenv_path)

# 로드가 잘 되었는지 확인하기 위한 디버깅 코드 (잠깐 추가)
SERVICE_KEY = os.environ.get('SERVICE_KEY')
DB_URL = os.environ.get('DB_URL')

if not DB_URL:
    print(f"--- 디버깅 정보 ---")
    print(f"현재 파일 위치: {current_dir}")
    print(f"찾고 있는 .env 경로: {dotenv_path}")
    print(f".env 존재 여부: {os.path.exists(dotenv_path)}")
    # 여기서 False가 뜬다면 파일 이름이 .env가 맞는지, 확장자가 숨겨진 건 아닌지 확인해야 합니다.

REGION_CODES = {
    "서울_강남구": "11680", "서울_서초구": "11650", "서울_송파구": "11710",
    "경기_성남분당구": "41135", "경기_수원영통구": "41117", "경기_과천시": "41290",
    "부산_해운대구": "26350", "대구_수성구": "27260", "인천_연수구": "28185"
}

def fetch_realty_data(lawd_cd, deal_ym):
    """국토부 아파트 실거래가 API 호출"""
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    params = {
        "serviceKey": requests.utils.unquote(SERVICE_KEY) if SERVICE_KEY else "",
        "pageNo": "1",
        "numOfRows": "1000",
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ym
    }
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.content
            elif response.status_code == 503:
                print("⚠️ 서버 과부하. 재시도...")
                time.sleep(5)
        except requests.exceptions.RequestException as e:
            print(f"⚠️ 네트워크 오류 ({attempt+1}/3): {e}")
            time.sleep(3)
    return None

def parse_xml_to_df(xml_data):
    """XML 데이터를 Pandas DataFrame으로 변환 (태그 매핑 및 취소 로직 강화)"""
    if not xml_data: return pd.DataFrame()
    try:
        root = ET.fromstring(xml_data)
        items = root.findall('.//item')
        if not items: return pd.DataFrame()
        
        data_list = []
        for item in items:
            # 해제사유발생일(취소일) 추출
            raw_cancel = item.findtext('cdealDay') or item.findtext('해제사유발생일')
            cancel_dt = raw_cancel.strip() if raw_cancel else ""
            
            data = {
                'apt_name': item.findtext('aptNm') or item.findtext('아파트') or item.findtext('aptName'),
                'city': item.findtext('umdNm') or item.findtext('법정동') or item.findtext('dong'),
                'deal_amount': item.findtext('dealAmount') or item.findtext('거래금액') or item.findtext('amount'),
                'exclusive_area': item.findtext('excluArea') or item.findtext('전용면적') or item.findtext('area'),
                'floor': item.findtext('floor') or item.findtext('층') or "0",
                'deal_year': item.findtext('dealYear') or item.findtext('년') or "0",
                'deal_month': item.findtext('dealMonth') or item.findtext('월') or "0",
                'deal_day': item.findtext('dealDay') or item.findtext('일') or "0",
                'build_year': item.findtext('buildYear') or item.findtext('건축년도') or "0",
                'regional_code': item.findtext('sggCd') or item.findtext('지역코드') or "00000",
                
                # --- 여기입니다! ---
                'cancel_date': cancel_dt,
                'is_cancelled': True if cancel_dt else False 
                # ------------------
            }
            data_list.append(data)
        return pd.DataFrame(data_list)
    except ET.ParseError:
        print("❌ XML 파싱 중 오류가 발생했습니다.")
        return pd.DataFrame()

def save_to_mysql(df):
    if df.empty:
        print("📥 저장할 데이터가 없습니다.")
        return

    try:
        temp_df = df.copy()
        
        # 1. 데이터 타입 정제 (기존 로직 유지)
        temp_df['deal_amount'] = temp_df['deal_amount'].str.replace(',', '').fillna("0").astype(int)
        temp_df['exclusive_area'] = pd.to_numeric(temp_df['exclusive_area'], errors='coerce').fillna(0.0)
        temp_df['deal_year'] = pd.to_numeric(temp_df['deal_year'], errors='coerce').fillna(0).astype(int)
        temp_df['deal_month'] = pd.to_numeric(temp_df['deal_month'], errors='coerce').fillna(0).astype(int)
        temp_df['deal_day'] = pd.to_numeric(temp_df['deal_day'], errors='coerce').fillna(0).astype(int)
        temp_df['floor'] = pd.to_numeric(temp_df['floor'], errors='coerce').fillna(0).astype(int)
        temp_df['build_year'] = pd.to_numeric(temp_df['build_year'], errors='coerce').fillna(0).astype(int)

        # 2. DB 연결
        engine = create_engine(DB_URL)
        
        # 3. 한 건씩 순회하며 INSERT 또는 IGNORE (취소 데이터 반영)
        new_count = 0
        with engine.begin() as connection:
            for _, row in temp_df.iterrows():
                try:
                    # 데이터 삽입 (is_cancelled 값이 포함된 채로 들어감)
                    row_df = pd.DataFrame([row])
                    row_df.to_sql('apt_deals', con=connection, if_exists='append', index=False)
                    new_count += 1
                except Exception as e:
                    # 중복 에러(1062) 발생 시 취소 여부만 업데이트 하는 로직 추가 가능
                    if '1062' in str(e) and row['is_cancelled']:
                        # 만약 이미 있는 데이터인데 '취소' 데이터라면 상태를 업데이트
                        update_sql = text("""
                            UPDATE apt_deals 
                            SET is_cancelled = TRUE, cancel_date = :cancel_date 
                            WHERE apt_name = :apt_name AND exclusive_area = :exclusive_area 
                            AND deal_year = :deal_year AND deal_month = :deal_month 
                            AND deal_day = :deal_day AND floor = :floor AND deal_amount = :deal_amount
                        """)
                        connection.execute(update_sql, row.to_dict())
                    continue
        
        print(f"✅ DB 연동 완료! (신규 적재 및 취소 상태 업데이트 완료)")
        
    except Exception as e:
        print(f"❌ DB 저장 중 치명적 오류: {e}")

def run_collector():
    if not SERVICE_KEY:
        print("🚨 SERVICE_KEY가 설정되지 않았습니다.")
        return

    # 기준 날짜 설정 (현재 날짜 기준)
    current_date = datetime.now()
    deal_ym = current_date.strftime("%Y%m")
    all_data = pd.DataFrame()
    
    print(f"🚀 전국 데이터 수집 시작: {deal_ym}")
    
    for name, code in REGION_CODES.items():
        print(f"📡 수집 중: {name}...", end="")
        xml_data = fetch_realty_data(code, deal_ym)
        df = parse_xml_to_df(xml_data)
        
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
            print(f" ✅ {len(df)}건 완료")
        else:
            print(" ⚠️ 데이터 없음")
        
        # API 과부하 방지를 위한 대기
        time.sleep(1.2) 

    # CSV 백업 저장
    os.makedirs('data', exist_ok=True)
    all_data.to_csv(f"data/realty_{deal_ym}.csv", index=False, encoding='utf-8-sig')

    # MySQL 저장 실행
    save_to_mysql(all_data)

if __name__ == "__main__":
    run_collector()