#!/usr/bin/env python3
"""
12-31 h24(연말 자정) 누락 데이터 패치 스크립트.

sync_kma.py의 연말 자정 누락 버그(ac5b164 수정 전)로 저장되지 않은
year-12-31의 h24(24번 컬럼) 값을 보완합니다.

각 지점/연도당 KMA API 1회 호출(다음해 1월 fetch)로만 처리하므로
전체 재동기화 대비 약 1/13의 API 호출로 완료됩니다.

환경변수:
  KMA_API_KEY          기상청 공공데이터 API 키 (인코딩 여부 무관)
  SUPABASE_URL         Supabase 프로젝트 URL
  SUPABASE_SERVICE_KEY Supabase service_role 키 (RLS 우회)
  START_YEAR           패치 시작 연도 (기본: 1954)
  END_YEAR             패치 종료 연도 (기본: 전년도)
  MAX_CALLS            최대 KMA API 호출 횟수 (기본: 5000)
  STATION_IDS          처리할 지점 번호, 쉼표 구분 (기본: 전체)
"""

import os
import sys
import time
import logging
import calendar
from datetime import date, timedelta
from urllib.parse import unquote

import requests
from supabase import create_client, Client

# ─── 로깅 ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

# ─── KMA ASOS 전체 지점 목록 (95개) ──────────────────────────────────────────
ALL_STATIONS = [
    # 서울특별시
    {'id': 108, 'name': '서울'},
    # 부산광역시
    {'id': 159, 'name': '부산'},    {'id': 296, 'name': '북부산'},
    # 대구광역시
    {'id': 143, 'name': '대구'},
    # 인천광역시
    {'id': 112, 'name': '인천'},    {'id': 102, 'name': '백령도'}, {'id': 201, 'name': '강화'},
    # 광주광역시
    {'id': 156, 'name': '광주'},
    # 대전광역시
    {'id': 133, 'name': '대전'},
    # 울산광역시
    {'id': 152, 'name': '울산'},
    # 세종특별자치시
    {'id': 239, 'name': '세종'},
    # 경기도
    {'id': 98,  'name': '동두천'},  {'id': 99,  'name': '파주'},
    {'id': 119, 'name': '수원'},    {'id': 202, 'name': '양평'},   {'id': 203, 'name': '이천'},
    # 강원도
    {'id': 90,  'name': '속초'},    {'id': 95,  'name': '철원'},   {'id': 100, 'name': '대관령'},
    {'id': 101, 'name': '춘천'},    {'id': 104, 'name': '북강릉'}, {'id': 105, 'name': '강릉'},
    {'id': 106, 'name': '동해'},    {'id': 114, 'name': '원주'},   {'id': 121, 'name': '영월'},
    {'id': 211, 'name': '인제'},    {'id': 212, 'name': '홍천'},   {'id': 216, 'name': '태백'},
    {'id': 217, 'name': '정선'},
    # 충청북도
    {'id': 127, 'name': '충주'},    {'id': 131, 'name': '청주'},   {'id': 135, 'name': '추풍령'},
    {'id': 221, 'name': '제천'},    {'id': 226, 'name': '보은'},
    # 충청남도
    {'id': 129, 'name': '서산'},    {'id': 177, 'name': '홍성'},   {'id': 232, 'name': '천안'},
    {'id': 235, 'name': '보령'},    {'id': 236, 'name': '부여'},   {'id': 238, 'name': '금산'},
    # 전라북도
    {'id': 140, 'name': '군산'},    {'id': 146, 'name': '전주'},   {'id': 172, 'name': '고창'},
    {'id': 243, 'name': '부안'},    {'id': 244, 'name': '임실'},   {'id': 245, 'name': '정읍'},
    {'id': 247, 'name': '남원'},    {'id': 248, 'name': '장수'},   {'id': 251, 'name': '고창군'},
    {'id': 254, 'name': '순창군'},
    # 전라남도
    {'id': 165, 'name': '목포'},    {'id': 168, 'name': '여수'},   {'id': 169, 'name': '흑산도'},
    {'id': 170, 'name': '완도'},    {'id': 174, 'name': '순천'},   {'id': 252, 'name': '영광군'},
    {'id': 258, 'name': '보성군'},  {'id': 259, 'name': '강진군'}, {'id': 260, 'name': '장흥'},
    {'id': 261, 'name': '해남'},    {'id': 262, 'name': '고흥'},   {'id': 266, 'name': '광양시'},
    {'id': 268, 'name': '진도군'},
    # 경상북도
    {'id': 115, 'name': '울릉도'},  {'id': 130, 'name': '울진'},   {'id': 136, 'name': '안동'},
    {'id': 137, 'name': '상주'},    {'id': 138, 'name': '포항'},   {'id': 271, 'name': '봉화'},
    {'id': 272, 'name': '영주'},    {'id': 273, 'name': '문경'},   {'id': 276, 'name': '청송군'},
    {'id': 277, 'name': '영덕'},    {'id': 278, 'name': '의성'},   {'id': 279, 'name': '구미'},
    {'id': 281, 'name': '영천'},    {'id': 283, 'name': '경주시'},
    # 경상남도
    {'id': 155, 'name': '창원'},    {'id': 162, 'name': '통영'},   {'id': 192, 'name': '진주'},
    {'id': 253, 'name': '김해시'},  {'id': 255, 'name': '북창원'}, {'id': 257, 'name': '양산시'},
    {'id': 263, 'name': '의령군'},  {'id': 264, 'name': '함양군'}, {'id': 284, 'name': '거창'},
    {'id': 285, 'name': '합천'},    {'id': 288, 'name': '밀양'},   {'id': 289, 'name': '산청'},
    {'id': 294, 'name': '거제'},    {'id': 295, 'name': '남해'},
    # 제주특별자치도
    {'id': 184, 'name': '제주'},    {'id': 185, 'name': '고산'},
    {'id': 188, 'name': '성산'},    {'id': 189, 'name': '서귀포'},
]

KMA_ENDPOINT = 'https://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList'
TABLE_NAME   = 'rainfall_hourly'


# ─── 환경변수 ─────────────────────────────────────────────────────────────────
def get_env(name: str, default: str = None, required: bool = True) -> str:
    val = os.environ.get(name, default)
    if required and not val:
        log.error(f"필수 환경변수 미설정: {name}")
        sys.exit(1)
    return val or ''


# ─── KMA API 조회 ─────────────────────────────────────────────────────────────
def fetch_kma_month(api_key: str, station_id: int, year: int, month: int) -> dict:
    """
    KMA API에서 1개월 데이터를 가져와 daily_map을 반환합니다.
    daily_map 형식: { "YYYY-MM-DD": { hour(1~24): rainfall_int } }
    00:00 레코드는 전날의 24시로 변환합니다.
    """
    last_day = calendar.monthrange(year, month)[1]
    params = {
        'serviceKey': api_key,
        'pageNo':     1,
        'numOfRows':  750,
        'dataType':   'JSON',
        'dataCd':     'ASOS',
        'dateCd':     'HR',
        'startDt':    f"{year}{month:02d}01",
        'startHh':    '00',
        'endDt':      f"{year}{month:02d}{last_day:02d}",
        'endHh':      '23',
        'stnIds':     station_id,
    }

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(KMA_ENDPOINT, params=params, timeout=30)
            resp.raise_for_status()
            data   = resp.json()
            header = data['response']['header']

            if header['resultCode'] != '00':
                return {}

            items = data['response']['body']['items']['item']
            if not isinstance(items, list):
                items = [items]

            daily_map: dict = {}
            for item in items:
                tm = item.get('tm', '')
                if len(tm) < 16:
                    continue
                date_str = tm[:10]
                hour     = int(tm[11:13])
                try:
                    rn_val = float(item.get('rn', 0) or 0)
                    rn = round(rn_val * 10) if rn_val > 0 else 0
                except (ValueError, TypeError):
                    rn = 0
                if hour == 0:
                    d = date.fromisoformat(date_str) - timedelta(days=1)
                    date_str = d.isoformat()
                    hour = 24
                daily_map.setdefault(date_str, {})[hour] = rn

            return daily_map

        except requests.exceptions.Timeout:
            log.warning(f"    타임아웃 (시도 {attempt}/{max_retries})")
        except Exception as e:
            log.warning(f"    오류: {e} (시도 {attempt}/{max_retries})")

        if attempt < max_retries:
            wait = 2 ** attempt
            time.sleep(wait)

    log.error(f"    {max_retries}회 재시도 모두 실패")
    return {}


# ─── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    api_key      = unquote(get_env('KMA_API_KEY'))
    supabase_url = get_env('SUPABASE_URL')
    supabase_key = get_env('SUPABASE_SERVICE_KEY')

    today      = date.today()
    start_year = int(get_env('START_YEAR', '1954', required=False) or '1954')
    # 현재 연도는 sync_kma.py가 매일 처리하므로 기본값은 전년도까지
    end_year   = int(get_env('END_YEAR', str(today.year - 1), required=False) or str(today.year - 1))
    max_calls  = int(get_env('MAX_CALLS', '5000', required=False) or '5000')

    station_ids_env = get_env('STATION_IDS', '', required=False)
    if station_ids_env.strip():
        filter_ids = {int(x.strip()) for x in station_ids_env.split(',') if x.strip()}
        stations   = [s for s in ALL_STATIONS if s['id'] in filter_ids]
    else:
        stations = ALL_STATIONS

    log.info("=" * 55)
    log.info("  12-31 h24 누락 패치 시작")
    log.info(f"  대상: {len(stations)}개 지점 | 기간: {start_year}~{end_year}")
    log.info(f"  최대 API 호출: {max_calls}회")
    log.info("=" * 55)

    sb = create_client(supabase_url, supabase_key)
    log.info("Supabase 연결 완료 (service_role)")

    total_calls   = 0
    patched_count = 0   # 실제로 non-zero 값이 업데이트된 건수
    skipped_zero  = 0   # KMA에서도 0이라 건너뛴 건수
    missing_row   = 0   # Supabase에 12-31 행 자체가 없는 경우

    for station in stations:
        if total_calls >= max_calls:
            log.info(f"\n⚠️  API 호출 한도 도달 ({max_calls}회). 중단합니다.")
            break

        stn_id   = station['id']
        stn_name = station['name']
        stn_patched = 0

        for year in range(start_year, end_year + 1):
            if total_calls >= max_calls:
                log.info(f"  ⚠️  호출 한도 도달. {stn_name} {year}년부터 중단.")
                break

            dec31_key = f"{year}-12-31"

            # 다음해 1월 fetch → year-12-31의 h24 추출
            jan_data = fetch_kma_month(api_key, stn_id, year + 1, 1)
            total_calls += 1
            time.sleep(0.3)

            h24 = jan_data.get(dec31_key, {}).get(24, 0)

            if h24 == 0:
                skipped_zero += 1
                continue

            # Supabase에서 해당 행 업데이트 (h24 컬럼 = "24" 만 변경)
            try:
                resp = (
                    sb.table(TABLE_NAME)
                    .update({"24": h24})
                    .eq("Station", stn_id)
                    .eq("Year",    year)
                    .eq("Month",   12)
                    .eq("Day",     31)
                    .execute()
                )
                if resp.data:
                    log.info(f"  ✅ {stn_name}({stn_id}) {year}-12-31 h24={h24} 패치")
                    patched_count += 1
                    stn_patched   += 1
                else:
                    log.warning(f"  ⚠️  {stn_name}({stn_id}) {year}-12-31 행 없음 (h24={h24} 미적용)")
                    missing_row += 1
            except Exception as e:
                log.error(f"  ❌ {stn_name}({stn_id}) {year}-12-31 업데이트 실패: {e}")

        if stn_patched > 0:
            log.info(f"  → {stn_name}: {stn_patched}개 연도 패치 완료")

    log.info("\n" + "=" * 55)
    log.info("  패치 완료 요약")
    log.info(f"  API 호출:     {total_calls}회 / {max_calls}회 한도")
    log.info(f"  패치 성공:    {patched_count}건 (non-zero h24 업데이트)")
    log.info(f"  스킵(h24=0): {skipped_zero}건")
    if missing_row:
        log.warning(f"  행 없음:      {missing_row}건 (12-31 행이 Supabase에 없음)")
    log.info("=" * 55)


if __name__ == '__main__':
    main()
