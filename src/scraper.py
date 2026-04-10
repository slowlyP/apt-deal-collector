import requests
import logging
from urllib.parse import unquote  # 추가

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_api_data(service_key, lawd_cd, deal_ym):
    # 1. 혹시 모를 중복 인코딩 방지를 위해 키를 한 번 디코딩합니다.
    decoded_key = unquote(service_key)
    
    # 2. 주소 설정
    url = "https://apis.data.go.kr/1613000/RTMSOBJSvc/getRTMSDataSvcAptTradeDev"
    
    # 3. 인증키는 URL에 직접 붙이고, 나머지만 params로 보냅니다.
    request_url = f"{url}?serviceKey={decoded_key}"
    
    params = {
        'pageNo': '1',
        'numOfRows': '100',
        'LAWD_CD': lawd_cd,
        'DEAL_YMD': deal_ym
    }

    try:
        # params 대신 request_url을 사용합니다.
        response = requests.get(request_url, params=params, timeout=30, verify=True)
        
        if response.status_code == 200:
            if "<resultCode>00" in response.text:
                return response.text
            else:
                logging.error(f"❌ API 인증/로직 에러 (키를 확인하세요): {response.text}")
                return None
        else:
            # 여기서 500이 뜬다면 거의 100% 인증키 활성화 대기 문제입니다.
            logging.error(f"❌ 서버 응답 에러 (코드: {response.status_code})")
            return None
            
    except requests.exceptions.RequestException as e:
        logging.error(f"⚠️ 연결 실패: {e}")
        return None