import os
import logging
from flask import Flask, jsonify, render_template
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pandas as pd
from dotenv import load_dotenv

# 기존 수집 로직이 담긴 main.py에서 run_collector 함수를 가져옵니다.
try:
    from main import run_collector
except ImportError:
    print("🚨 main.py 파일을 찾을 수 없습니다. 수집기 로직이 필요합니다.")

# 1. 환경 설정 및 로깅 세팅
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        # encoding='utf-8'을 추가하여 파일 저장 시 이모지 깨짐 방지
        logging.FileHandler("app.log", encoding='utf-8'), 
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 2. DB 엔진 설정 (서버 시작 시 1회 생성)
DB_URL = os.getenv('DB_URL')
if not DB_URL:
    logger.critical("🚨 DB_URL이 .env에 설정되지 않았습니다. 서버를 종료합니다.")
    exit(1)

try:
    engine = create_engine(DB_URL, pool_recycle=3600, pool_pre_ping=True)
    # pool_pre_ping: DB 연결이 끊어졌는지 미리 체크하는 기능 (실무 필수)
except Exception as e:
    logger.critical(f"🚨 DB 엔진 생성 실패: {e}")
    exit(1)

# --- [API 엔드포인트] ---

@app.route('/')
def health_check():
    return jsonify({"status": "healthy", "message": "부동산 API 서버 가동 중"}), 200

@app.route('/api/map-data')
def get_map_data():
    """지도에 표시할 최신 실거래 데이터를 반환 (취소 매물 제외)"""
    try:
        # 1. 쿼리 작성 (is_cancelled 필드가 DB에 추가되어 있어야 합니다)
        # 만약 아직 DB에 is_cancelled를 안 만드셨다면 우선 빼고 테스트하세요.
        query = "SELECT * FROM apt_deals WHERE is_cancelled = FALSE ORDER BY deal_year DESC, deal_month DESC LIMIT 200"
        
        # 2. 데이터 읽기
        df = pd.read_sql(query, con=engine)
        
        # 3. JSON 반환
        return jsonify(df.to_dict(orient='records')), 200
    except Exception as e:
        logger.error(f"❌ 지도 데이터 로드 실패: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/deals/search', methods=['GET'])
def search_deals():
    from flask import request
    
    # URL 파라미터 읽기 (예: /api/deals/search?city=역삼동&max_price=100000)
    target_city = request.args.get('city')
    max_price = request.args.get('max_price')

    try:
        query = "SELECT * FROM apt_deals WHERE 1=1"
        params = {}
        
        if target_city:
            query += " AND city = :city"
            params['city'] = target_city
        if max_price:
            query += " AND deal_amount <= :max_price"
            params['max_price'] = max_price
            
        query += " ORDER BY deal_date DESC LIMIT 100"
        
        df = pd.read_sql(text(query), con=engine, params=params)
        return jsonify(df.to_dict(orient='records')), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- [자동화 스케줄러] ---

def scheduled_collector_job():
    """정기 수집 작업 (예외 처리 및 로깅 강화)"""
    logger.info("⏰ [Scheduler] 정기 수집 작업을 시작합니다.")
    try:
        # 수집기 실행
        run_collector()
        logger.info("✅ [Scheduler] 수집 및 DB 적재 성공")
    except Exception as e:
        logger.error(f"❌ [Scheduler] 수집 도중 치명적 오류: {e}")

# 스케줄러 초기화 및 시작
# use_reloader=False를 위해 수동으로 중복 방지 로직을 넣거나 팩토리 패턴을 쓰지만,
# 여기서는 가장 직관적인 BackgroundScheduler를 사용합니다.
scheduler = BackgroundScheduler(daemon=True)

# 매일 새벽 3시 30분에 실행 (데이터 포털 갱신 지연 대비)
scheduler.add_job(
    func=scheduled_collector_job,
    trigger=CronTrigger(hour=3, minute=30),
    id='daily_collect_job',
    name='Daily Apartment Deal Collection',
    replace_existing=True
)

@app.route('/map')
def view_map():
    # 이 코드가 있어야 /map 주소로 접속했을 때 
    # templates/realty_map.html 파일을 보여줍니다.
    return render_template('realty_map.html', kakao_key=os.getenv('KAKAO_JS_KEY'))
# 서버 시작 시 즉시 한 번 실행하고 싶다면 아래 주석 해제
# scheduler.add_job(func=scheduled_collector_job, trigger='date')

scheduler.start()

if __name__ == "__main__":
    try:
        logger.info("🚀 Flask 서버 가동 (Port: 5000)")
        # use_reloader=False: 스케줄러 중복 실행을 막기 위한 실무 필수 설정
        app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
    except (KeyboardInterrupt, SystemExit):
        logger.info("👋 서버를 종료합니다.")
        scheduler.shutdown()