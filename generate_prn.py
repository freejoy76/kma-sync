#!/usr/bin/env python3
"""
Supabase → PRN 파일 생성 스크립트.

Supabase rainfall_hourly 테이블에서 데이터를 읽어
PRN 형식(pipe 구분)으로 출력합니다.

PRN 포맷: station|year|month|day|h1|h2|...|h24|
  - 강우량 0 또는 미관측 → 빈 문자열
  - 강우량 비0 → 정수 (0.1mm 단위)

환경변수:
  SUPABASE_URL         Supabase 프로젝트 URL
  SUPABASE_SERVICE_KEY Supabase service_role 키
  STATION_IDS          생성할 지점 번호, 쉼표 구분 (예: 216,211,121)
  START_YEAR           시작 연도 (기본: 1954)
  END_YEAR             종료 연도 (기본: 현재 연도)
  OUTPUT_DIR           PRN 파일 저장 경로 (기본: ./prn_output)
"""

import os
import sys
import logging
from datetime import date
from pathlib import Path
from supabase import create_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

TABLE_NAME = 'rainfall_hourly'
HOURS = list(range(1, 25))

# 지점번호 → 지점명 매핑
STATION_NAMES = {
    108: '서울',   159: '부산',   296: '북부산', 143: '대구',
    112: '인천',   102: '백령도', 201: '강화',   156: '광주',
    133: '대전',   152: '울산',   239: '세종',   98:  '동두천',
    99:  '파주',   119: '수원',   202: '양평',   203: '이천',
    90:  '속초',   95:  '철원',   100: '대관령', 101: '춘천',
    104: '북강릉', 105: '강릉',   106: '동해',   114: '원주',
    121: '영월',   211: '인제',   212: '홍천',   216: '태백',
    217: '정선',   127: '충주',   131: '청주',   135: '추풍령',
    221: '제천',   226: '보은',   129: '서산',   177: '홍성',
    232: '천안',   235: '보령',   236: '부여',   238: '금산',
    140: '군산',   146: '전주',   172: '고창',   243: '부안',
    244: '임실',   245: '정읍',   247: '남원',   248: '장수',
    251: '고창군', 254: '순창군', 165: '목포',   168: '여수',
    169: '흑산도', 170: '완도',   174: '순천',   252: '영광군',
    258: '보성군', 259: '강진군', 260: '장흥',   261: '해남',
    262: '고흥',   266: '광양시', 268: '진도군', 115: '울릉도',
    130: '울진',   136: '안동',   137: '상주',   138: '포항',
    271: '봉화',   272: '영주',   273: '문경',   276: '청송군',
    277: '영덕',   278: '의성',   279: '구미',   281: '영천',
    283: '경주시', 155: '창원',   162: '통영',   192: '진주',
    253: '김해시', 255: '북창원', 257: '양산시', 263: '의령군',
    264: '함양군', 284: '거창',   285: '합천',   288: '밀양',
    289: '산청',   294: '거제',   295: '남해',   184: '제주',
    185: '고산',   188: '성산',   189: '서귀포',
}


def fetch_station_years(sb, station_id: int, start_year: int, end_year: int) -> list:
    """Supabase에서 지점의 보유 연도 목록 반환."""
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
        return sorted({row['Year'] for row in (resp.data or [])})
    except Exception as e:
        log.error(f"  연도 조회 실패 (지점 {station_id}): {e}")
        return []


def fetch_year_data(sb, station_id: int, year: int) -> list:
    """Supabase에서 지점/연도 전체 데이터를 행 리스트로 반환 (월 순서 정렬)."""
    try:
        resp = (
            sb.table(TABLE_NAME)
            .select('*')
            .eq('Station', station_id)
            .eq('Year', year)
            .order('Month')
            .order('Day')
            .execute()
        )
        return resp.data or []
    except Exception as e:
        log.error(f"  데이터 조회 실패 ({station_id}/{year}): {e}")
        return []


def row_to_prn(row: dict) -> str:
    """
    Supabase 행 → PRN 형식 문자열 변환.
    포맷: station|year|month|day|h1|...|h24|
    0값은 빈 문자열, 비0값은 정수 문자열.
    """
    stn   = row['Station']
    year  = row['Year']
    month = row['Month']
    day   = row['Day']
    h_vals = []
    for h in HOURS:
        v = row.get(str(h), 0) or 0
        h_vals.append(str(v) if v else '')
    return f"{stn}|{year}|{month}|{day}|{'|'.join(h_vals)}|"


def generate_prn(sb, station_id: int, start_year: int, end_year: int, output_dir: Path):
    """지점 데이터를 Supabase에서 읽어 PRN 파일로 저장."""
    stn_name = STATION_NAMES.get(station_id, str(station_id))

    years = fetch_station_years(sb, station_id, start_year, end_year)
    if not years:
        log.warning(f"  {stn_name}({station_id}): Supabase에 데이터 없음 — 건너뜀")
        return

    year_range = f"{years[0]}~{years[-1]}"
    filename   = f"강우카드({stn_name})_{year_range}.prn"
    out_path   = output_dir / filename

    log.info(f"  생성: {filename} ({len(years)}개 연도)")

    total_rows = 0
    with open(out_path, 'w', encoding='utf-8') as f:
        for year in years:
            rows = fetch_year_data(sb, station_id, year)
            for row in rows:
                f.write(row_to_prn(row) + '\n')
                total_rows += 1

    log.info(f"  → {out_path.name}: {total_rows:,}행 저장")


def main():
    supabase_url = os.environ.get('SUPABASE_URL', '')
    supabase_key = os.environ.get('SUPABASE_SERVICE_KEY', '')

    if not supabase_url or not supabase_key:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY 환경변수 필요")
        sys.exit(1)

    today      = date.today()
    start_year = int(os.environ.get('START_YEAR', '1954') or '1954')
    end_year   = int(os.environ.get('END_YEAR', str(today.year)) or str(today.year))

    station_ids_env = os.environ.get('STATION_IDS', '')
    if station_ids_env.strip():
        station_ids = [int(x.strip()) for x in station_ids_env.split(',') if x.strip()]
    else:
        station_ids = list(STATION_NAMES.keys())

    output_dir = Path(os.environ.get('OUTPUT_DIR', 'prn_output'))
    output_dir.mkdir(parents=True, exist_ok=True)

    sb = create_client(supabase_url, supabase_key)
    log.info("Supabase 연결 완료")
    log.info(f"대상: {len(station_ids)}개 지점 | 기간: {start_year}~{end_year}")
    log.info(f"출력: {output_dir.resolve()}")

    for stn_id in station_ids:
        generate_prn(sb, stn_id, start_year, end_year, output_dir)

    log.info("✅ 전체 PRN 생성 완료")


if __name__ == '__main__':
    main()
