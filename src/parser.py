import xml.etree.ElementTree as ET
import pandas as pd
import logging

def parse_xml_to_list(xml_data):
    if not xml_data:
        return []

    try:
        root = ET.fromstring(xml_data)
        items = root.findall('.//item')
        
        result_list = []
        for item in items:
            # 헬퍼 함수: XML 태그가 없을 경우 대비
            def get_text(tag):
                node = item.find(tag)
                return node.text.strip() if node is not None and node.text else ""

            # 금액의 콤마 제거 및 정수형 변환
            raw_price = get_text('거래금액').replace(',', '')
            
            data = {
                'apartment': get_text('아파트'),
                'price': int(raw_price) if raw_price.isdigit() else 0,
                'floor': get_text('층'),
                'area': get_text('전용면적'),
                'dong': get_text('법정동'),
                'deal_date': f"{get_text('년')}-{get_text('월').zfill(2)}-{get_text('일').zfill(2)}"
            }
            result_list.append(data)
        return result_list
    except Exception as e:
        logging.error(f"Parsing error: {e}")
        return []