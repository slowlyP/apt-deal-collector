import requests
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_api_data(service_key, lawd_cd, deal_ym):
    # [변경 포인트] http -> https 및 최신 도메인 적용
    url = "https://apis.data.go.kr/1613000/RTMSOBJSvc/getRTMSDataSvcAptTradeDev"
    
    params = {
        'serviceKey': service_key,
        'pageNo': '1',
        'numOfRows': '100',
        'LAWD_CD': lawd_cd,
        'DEAL_YMD': deal_ym
    }

    try:
        # 넉넉한 타임아웃 설정
        response = requests.get(url, params=params, timeout=30, verify=True)
        
        if response.status_code == 200:
            # API 내부 오류 코드 확인
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