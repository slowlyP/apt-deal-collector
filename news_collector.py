import os
import requests
import pandas as pd
import html
from dotenv import load_dotenv

load_dotenv()

def get_realtime_news(query="부동산 급매"):
    """
    네이버 실시간 뉴스를 수집하고 예외 상황을 안전하게 처리하는 함수
    """
    # [예외 1] API 키 누락 방어
    client_id = os.getenv('NAVER_CLIENT_ID')
    client_secret = os.getenv('NAVER_CLIENT_SECRET')
    
    if not client_id or not client_secret:
        print("🚨 [치명적 오류] .env 파일에 네이버 API 키가 없습니다. 수집을 중단합니다.")
        return pd.DataFrame() # 에러 대신 빈 데이터프레임 반환하여 메인 로직 붕괴 방지

    url = "https://openapi.naver.com/v1/search/news.json"
    params = {
        "query": query,
        "display": 50, # 더 많은 데이터 수집을 위해 50개로 증가
        "sort": "sim"
    }
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret
    }
    
    try:
        # [예외 2] 네트워크 무한 대기(Timeout) 방어
        response = requests.get(url, headers=headers, params=params, timeout=5)
        
        # [예외 3] HTTP 상태 코드 검증 (401 인증 실패, 429 호출 한도 초과 등)
        response.raise_for_status() 

    except requests.exceptions.Timeout:
        print("⚠️ [경고] 네이버 API 서버 응답이 너무 늦습니다. (Timeout)")
        return pd.DataFrame()
    except requests.exceptions.HTTPError as err:
        print(f"⚠️ [경고] API 호출 에러 발생 (권한/한도 초과 등): {err}")
        return pd.DataFrame()
    except requests.exceptions.RequestException as err:
        print(f"⚠️ [경고] 네트워크 연결 끊김: {err}")
        return pd.DataFrame()

    # [예외 4] 응답 데이터 구조 오류 방어
    try:
        data = response.json()
        items = data.get('items', [])
    except ValueError:
        print("⚠️ [경고] 네이버에서 받은 데이터를 JSON으로 변환할 수 없습니다.")
        return pd.DataFrame()

    if not items:
        print(f"ℹ️ '{query}'에 대한 최신 뉴스 결과가 없습니다.")
        return pd.DataFrame()

    # 타겟 지역 리스트 (필요에 따라 추가/수정하세요)
    regions = ['강남', '서초', '송파', '여의도', '반포', '잠실', '성수', '용산', '마포', '노원']
    
    issue_data = []
    for item in items:
        try:
            # [예외 5] 텍스트 정제 (HTML 특수문자 및 태그 깨짐 방어)
            raw_title = item.get('title', '')
            # html.unescape로 &quot; 같은 특수문자를 진짜 따옴표로 변환
            clean_title = html.unescape(raw_title).replace('<b>', '').replace('</b>', '')
            link = item.get('link', '')
            
            # 지역명 매칭 로직
            found_region = next((r for r in regions if r in clean_title), None)
            
            if found_region:
                issue_data.append({
                    'region': found_region,
                    'title': clean_title,
                    'link': link
                })
        except Exception as e:
            # 한 개의 뉴스가 에러나도 전체 루프가 멈추지 않도록 처리
            print(f"🔍 개별 뉴스 파싱 건너뜀 (원인: {e})")
            continue

    # 조건에 맞는 이슈가 없을 경우 빈 데이터프레임 반환
    if not issue_data:
        print("ℹ️ 뉴스는 수집되었으나, 지정된 '타겟 지역'이 포함된 헤드라인이 현재 없습니다.")
        return pd.DataFrame()

    return pd.DataFrame(issue_data)

if __name__ == "__main__":
    print("🚀 실시간 뉴스 수집 엔진 가동 중...")
    
    # 테스트 1: 기본 키워드 (부동산 급매)
    df = get_realtime_news()
    
    # 테스트 2: 다른 키워드로 수집하고 싶을 때 (예: 아래 주석 해제)
    # df = get_realtime_news(query="아파트 신고가")

    if not df.empty:
        print(f"\n🚨 총 {len(df)}건의 실시간 지역 이슈 추출 성공 🚨")
        print(df)
    else:
        print("\n✅ 현재 수집된 지역 이슈가 없습니다. (정상 대기 상태)")