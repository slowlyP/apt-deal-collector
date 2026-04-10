import xml.etree.ElementTree as ET

def parse_xml_to_list(xml_data):
    try:
        root = ET.fromstring(xml_data)
        items = root.findall('.//item')
        parsed_list = []

        for item in items:
            # API가 주는 한글 태그명을 우리가 설정한 영어 컬럼명으로 매핑
            record = {
                'apartment': item.findtext('aptNm', '').strip(),  # 아파트 이름
                'price': item.findtext('dealAmount', '').strip().replace(',', ''), # 거래금액 (쉼표 제거)
                'floor': item.findtext('floor', '').strip(),      # 층수
                'area': item.findtext('excluArea', '').strip(),   # 전용면적
                'dong': item.findtext('umdNm', '').strip(),       # 법정동 (umdNm 또는 dong)
                'deal_date': f"{item.findtext('dealYear')}-{item.findtext('dealMonth').zfill(2)}-{item.findtext('dealDay').zfill(2)}"
            }
            
            # 데이터가 비어있지 않은 경우만 리스트에 추가
            if record['apartment']:
                parsed_list.append(record)

        return parsed_list
    except Exception as e:
        print(f"❌ 파싱 에러 발생: {e}")
        return []