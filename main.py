import sys
import os

# 현재 폴더를 파이썬 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from datetime import datetime
from src.scraper import fetch_api_data
from src.parser import parse_xml_to_list

# 설정
SERVICE_KEY = os.environ.get('SERVICE_KEY')
TARGET_REGIONS = ["11680", "11110", "11710"]

def main():
    if not SERVICE_KEY:
        print("❌ SERVICE_KEY가 설정되지 않았습니다.")
        return

    deal_ymd = datetime.now().strftime("%Y%m")
    all_data = []

    for lawd_cd in TARGET_REGIONS:
        print(f"🚀 {lawd_cd} 지역 데이터 수집 중...")
        xml_res = fetch_api_data(SERVICE_KEY, lawd_cd, deal_ymd)
        parsed_items = parse_xml_to_list(xml_res)
        if parsed_items:
            all_data.extend(parsed_items)

    if all_data:
        df = pd.DataFrame(all_data)
        os.makedirs('data', exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"data/realty_combined_{timestamp}.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ 저장 완료: {filename} ({len(df)}건)")
    else:
        print("❌ 수집된 데이터가 없습니다.")

if __name__ == "__main__":
    main()