import requests
import logging
from urllib.parse import unquote

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_api_data(service_key, lawd_cd, deal_ym):
    # 1. 키를 깨끗하게 한 번 디코딩 (중복 인코딩 방지)
    decoded_key = unquote(service_key)
    
    # 2. 브라우저 주소창과 100% 똑같은 URL을 수동으로 조립합니다.
    # params 옵션을 쓰지 않고 URL 문자열에 직접 다 때려 넣습니다.
    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    request_url = (
        f"{url}?serviceKey={decoded_key}"
        f"&pageNo=1&numOfRows=100&LAWD_CD={lawd_cd}&DEAL_YMD={deal_ym}"
    )

    try:
        # 3. 브라우저인 척 위장하기 (User-Agent 추가)
        # 서버가 프로그램(봇)의 접근을 거부할 때 쓰는 필살기입니다.
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Accept': 'application/xml,text/xml,*/*'
        }
        
        # params 없이 조립된 주소(request_url)로 직접 요청을 쏩니다.
        response = requests.get(request_url, headers=headers, timeout=30)
        print("요청 URL:", request_url)
        print("응답 내용:", response.text[:500])
        
        # 서버가 준 실제 주소를 로그로 확인 (디버깅용)
        # print(f"DEBUG URL: {response.url}") 

        if response.status_code == 200:
            if "<resultCode>00" in response.text:
                return response.text
            else:
                logging.error(f"❌ API 인증/로직 에러: {response.text}")
                return None
        else:
            logging.error(f"❌ 서버 응답 에러 (코드: {response.status_code})")
            return None
            
    except requests.exceptions.RequestException as e:
        logging.error(f"⚠️ 연결 실패: {e}")
        return None