import sys
import os
from dotenv import load_dotenv  # [추가] .env 파일을 읽기 위한 라이브러리

# .env 파일 활성화 (이 줄이 있어야 SERVICE_KEY를 읽어옵니다)
load_dotenv()

# 현재 폴더를 파이썬 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from datetime import datetime
from src.scraper import fetch_api_data
from src.parser import parse_xml_to_list

# 설정: .env 파일에서 SERVICE_KEY를 가져옵니다.
SERVICE_KEY = os.environ.get('SERVICE_KEY')
TARGET_REGIONS = ["11680", "11110", "11710"]

def main():
    # 1. 키 확인 로직
    if not SERVICE_KEY:
        print("❌ SERVICE_KEY가 설정되지 않았습니다. .env 파일을 확인해주세요.")
        return

    # 2. 날짜 설정 (데이터가 확실히 있는 3월로 테스트)
    #deal_ymd = datetime.now().strftime("%Y%m")
    deal_ymd = "202401"
    all_data = []

    for lawd_cd in TARGET_REGIONS:
        print(f"🚀 {lawd_cd} 지역 데이터 수집 중...")
        xml_res = fetch_api_data(SERVICE_KEY, lawd_cd, deal_ymd)
        
        # 3. 데이터 확인 로그 추가
        if xml_res:
            parsed_items = parse_xml_to_list(xml_res)
            if parsed_items:
                print(f"✅ {lawd_cd} 지역 수집 성공: {len(parsed_items)}건")
                all_data.extend(parsed_items)
            else:
                print(f"⚠️ {lawd_cd} 지역: 수집된 데이터가 없습니다.")
        else:
            print(f"❌ {lawd_cd} 지역: API 요청 실패 (키 활성화 대기일 수 있음)")

    # 4. 파일 저장
    if all_data:
        df = pd.DataFrame(all_data)
        os.makedirs('data', exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"data/realty_combined_{timestamp}.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"🎉 모든 수집 완료! 저장됨: {filename} (총 {len(df)}건)")
    else:
        print("❌ 최종 수집된 데이터가 0건입니다.")

if __name__ == "__main__":
    main()