#!/usr/bin/env python3
"""
KMA ASOS 시간 강우 데이터 → Supabase 자동 동기화
GitHub Actions에서 매일 실행, Supabase에 없는 연도만 보완합니다.

환경변수:
  KMA_API_KEY          기상청 공공데이터 API 키 (인코딩 여부 무관)
  SUPABASE_URL         Supabase 프로젝트 URL
  SUPABASE_SERVICE_KEY Supabase service_role 키 (RLS 우회)
  START_YEAR           동기화 시작 연도 (기본: 2001)
  END_YEAR             동기화 종료 연도 (기본: 실행 연도)
  MAX_CALLS            최대 KMA API 호출 횟수 (기본: 1000)
  STATION_IDS          처리할 지점 번호, 쉼표 구분 (예: 90,95 / 기본: 전체)
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


# ─── Supabase 쿼리 ────────────────────────────────────────────────────────────
def get_present_years(sb: Client, station_id: int, start_year: int, end_year: int) -> set:
    """
    Supabase에 이미 저장된 연도 집합을 반환합니다.
    month=1, day=1 레코드만 확인하는 경량 쿼리 (연도당 최대 1행 조회).
    """
    try:
        resp = (
            sb.table(TABLE_NAME)
            .select('Year')
            .eq('Station', station_id)
            .eq('Month', 1)
            .eq('Day', 1)
            .gte('Year', start_year)
            .lte('Year', end_year)
            .execute()
        )
        return {row['Year'] for row in (resp.data or [])}
    except Exception as e:
        log.warning(f"  보유 연도 조회 실패 (지점 {station_id}): {e}")
        return set()


def upsert_records(sb: Client, records: list, batch_size: int = 500) -> int:
    """
    Supabase에 레코드를 배치 upsert합니다.
    Station+Year+Month+Day 복합키 충돌 시 덮어씁니다.
    """
    total = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            sb.table(TABLE_NAME).upsert(
                batch,
                on_conflict='Station,Year,Month,Day'
            ).execute()
            total += len(batch)
        except Exception as e:
            log.error(f"  upsert 오류 (배치 {i}~{i+len(batch)}): {e}")
    return total


# ─── KMA API 조회 ─────────────────────────────────────────────────────────────
def fetch_kma_month(api_key: str, station_id: int, year: int, month: int) -> dict:
    """
    KMA API에서 1개월 데이터를 가져와 daily_map을 반환합니다.
    daily_map 형식: { "YYYY-MM-DD": { hour(1~24): rainfall_str } }

    00:00 레코드는 전날의 24시로 변환합니다.
    강우량은 mm(소수) → 0.1mm 정수 문자열로 변환, 0 또는 공란은 '' 처리합니다.
    """
    last_day  = calendar.monthrange(year, month)[1]
    start_dt  = f"{year}{month:02d}01"
    end_dt    = f"{year}{month:02d}{last_day:02d}"

    params = {
        'serviceKey': api_key,
        'pageNo':     1,
        'numOfRows':  750,
        'dataType':   'JSON',
        'dataCd':     'ASOS',
        'dateCd':     'HR',
        'startDt':    start_dt,
        'startHh':    '00',
        'endDt':      end_dt,
        'endHh':      '23',
        'stnIds':     station_id,
    }

    try:
        resp = requests.get(KMA_ENDPOINT, params=params, timeout=30)
        resp.raise_for_status()
        data   = resp.json()
        header = data['response']['header']

        if header['resultCode'] != '00':
            log.debug(f"    {year}-{month:02d}: 결과 없음 ({header['resultMsg']})")
            return {}

        items = data['response']['body']['items']['item']
        if not isinstance(items, list):
            items = [items]

        daily_map: dict = {}

        for item in items:
            tm = item.get('tm', '')
            if len(tm) < 16:
                continue

            date_str = tm[:10]          # "2023-07-01"
            hour     = int(tm[11:13])   # 0 ~ 23

            # 강우량 변환
            try:
                rn_val = float(item.get('rn', 0) or 0)
                rn = str(round(rn_val * 10)) if rn_val > 0 else ''
            except (ValueError, TypeError):
                rn = ''

            # 00:00 → 전날 24시로 이동
            if hour == 0:
                d = date.fromisoformat(date_str) - timedelta(days=1)
                date_str = d.isoformat()
                hour = 24

            daily_map.setdefault(date_str, {})[hour] = rn

        return daily_map

    except requests.exceptions.Timeout:
        log.warning(f"    {year}-{month:02d}: 요청 타임아웃")
        return {}
    except Exception as e:
        log.warning(f"    {year}-{month:02d}: 오류 - {e}")
        return {}


def build_year_records(station_id: int, year: int, daily_map: dict) -> list:
    """daily_map에서 해당 연도의 레코드만 추출해 Supabase 형식으로 변환합니다."""
    records = []
    for date_str, hours in sorted(daily_map.items()):
        y, m, d = map(int, date_str.split('-'))
        if y != year:
            continue
        record = {'Station': station_id, 'Year': y, 'Month': m, 'Day': d}
        for h in range(1, 25):
            record[str(h)] = hours.get(h, '')
        records.append(record)
    return records


# ─── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    # 환경변수 로드
    api_key_raw  = get_env('KMA_API_KEY')
    supabase_url = get_env('SUPABASE_URL')
    supabase_key = get_env('SUPABASE_SERVICE_KEY')

    # URL 인코딩된 API 키 자동 디코딩 (%2F → / 등)
    api_key = unquote(api_key_raw)

    start_year = int(get_env('START_YEAR', '2001', required=False) or '2001')
    end_year   = int(get_env('END_YEAR',   str(date.today().year), required=False) or str(date.today().year))
    max_calls  = int(get_env('MAX_CALLS',  '1000', required=False) or '1000')

    # 특정 지점 필터 (STATION_IDS=90,95,100)
    station_ids_env = get_env('STATION_IDS', '', required=False)
    if station_ids_env.strip():
        filter_ids = {int(x.strip()) for x in station_ids_env.split(',') if x.strip()}
        stations   = [s for s in ALL_STATIONS if s['id'] in filter_ids]
    else:
        stations = ALL_STATIONS

    log.info("=" * 55)
    log.info("  KMA ASOS → Supabase 동기화 시작")
    log.info(f"  대상: {len(stations)}개 지점 | 기간: {start_year}~{end_year}")
    log.info(f"  최대 API 호출: {max_calls}회")
    log.info("=" * 55)

    # Supabase 연결
    sb = create_client(supabase_url, supabase_key)
    log.info("Supabase 연결 완료 (service_role)")

    total_calls = 0
    total_rows  = 0
    done_count  = 0

    for station in stations:
        if total_calls >= max_calls:
            log.info(f"\n⚠️  API 호출 한도 도달 ({max_calls}회). 내일 계속합니다.")
            break

        stn_id   = station['id']
        stn_name = station['name']
        log.info(f"\n[{done_count + 1}/{len(stations)}] {stn_name} (#{stn_id})")

        # 보유 연도 확인
        present = get_present_years(sb, stn_id, start_year, end_year)
        missing = [y for y in range(start_year, end_year + 1) if y not in present]

        if not missing:
            log.info(f"  ✅ 전체 보유 ({start_year}~{end_year}) — 건너뜀")
            done_count += 1
            continue

        preview = ', '.join(map(str, missing[:5])) + ('...' if len(missing) > 5 else '')
        log.info(f"  누락 연도 {len(missing)}개: {preview}")

        for year in missing:
            if total_calls >= max_calls:
                log.info(f"  ⚠️  호출 한도 도달. {stn_name} {year}년부터 중단.")
                break

            year_map: dict = {}
            year_calls = 0

            for month in range(1, 13):
                if total_calls >= max_calls:
                    break

                monthly = fetch_kma_month(api_key, stn_id, year, month)
                year_map.update(monthly)
                total_calls += 1
                year_calls  += 1

                # KMA 서버 부하 방지 (0.3초 대기)
                time.sleep(0.3)

            records = build_year_records(stn_id, year, year_map)
            if records:
                saved = upsert_records(sb, records)
                total_rows += saved
                log.info(f"  {year}년: API {year_calls}회 → {saved}행 저장")
            else:
                log.warning(f"  {year}년: 저장할 데이터 없음")

        done_count += 1

    # 최종 요약
    log.info("\n" + "=" * 55)
    log.info("  동기화 완료 요약")
    log.info(f"  처리 지점: {done_count}개")
    log.info(f"  API 호출:  {total_calls}회 / {max_calls}회 한도")
    log.info(f"  저장 행수: {total_rows:,}행")
    log.info("=" * 55)


if __name__ == '__main__':
    main()
