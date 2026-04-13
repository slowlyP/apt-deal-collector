# 🏙️ 실시간 부동산 이슈 및 실거래가 시각화 시스템

본 프로젝트는 공공데이터 포털의 실거래가 API와 네이버 뉴스 API를 결합하여, 지역별 부동산 시장 트렌드를 지도 상에 실시간으로 시각화하는 자동화 대시보드입니다.

## ✨ 핵심 기능
```
- **실시간 뉴스 이슈 매핑**: 네이버 검색 API를 통해 수집된 최신 부동산 뉴스를 자연어 처리하여, 해당 지역에 실시간 이슈 마커(⚡)를 표시합니다.
- **전국 실거래가 데이터 수집**: 국토교통부 실거래가 오픈 API를 연동하여 전국 단위의 아파트 거래 데이터를 자동 수집 및 정제합니다.
- **Interactive Map**: Folium 기반의 동적 지도를 통해 가격 변동성 및 주요 이슈 지역을 한눈에 파악할 수 있습니다.
```
## 🛠️ 자동화 아키텍처 (CI/CD)
본 시스템은 운영 효율성과 데이터의 최신성을 위해 **GitHub Actions**를 이용한 자동화 파이프라인을 구축하였습니다.
```
- **Schedule**: 매 2시간 주기(Bi-hourly)로 가상 서버(Ubuntu)가 자동 기동됩니다.
- **Workflow**: 
  1. API 환경 변수 및 의존성 라이브러리 로드
  2. 데이터 수집 및 전처리 스크립트 실행
  3. 시각화 엔진을 통한 `realty_map.html` 갱신
  4. 갱신된 결과물을 레포지토리에 자동 Push
```
## ⚙️ Tech Stack
```
- **Language**: Python 3.9
- **Framework**: Flask
- **Data**: Pandas, SQLAlchemy, PyMySQL
- **Visualization**: Folium
- **Automation**: GitHub Actions
```
## 📂 폴더 구조
```
- `src/`: 데이터 처리 및 크롤링 핵심 로직
- `templates/`: 최종 생성된 시각화 HTML 지도
- `data/`: 수집된 지역별 실거래가 CSV 데이터
- `.github/workflows/`: 자동화 스케줄링 설정 파일
```
