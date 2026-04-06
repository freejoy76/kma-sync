#!/usr/bin/env python3
"""
PRN 파일 ↔ Supabase 데이터 검증 스크립트.

PRN 파일의 h1~h24 값과 Supabase rainfall_hourly 테이블을 비교해
불일치 행을 출력합니다.

환경변수:
  SUPABASE_URL         Supabase 프로젝트 URL
  SUPABASE_SERVICE_KEY Supabase service_role 키
  PRN_DIR              PRN 파일이 있는 디렉토리 경로
"""

import os
import sys
import logging
from pathlib import Path
from supabase import create_client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

TABLE_NAME = 'rainfall_hourly'
HOURS = list(range(1, 25))   # h1 ~ h24


# ─── PRN 파싱 ─────────────────────────────────────────────────────────────────
def parse_prn(path: Path) -> dict:
    """
    PRN 파일을 파싱해 { (station, year, month, day): {h: val} } 반환.
    PRN 포맷: station|year|month|day|h1|h2|...|h24| (파이프 구분, 빈칸=0)
    """
    data = {}
    with open(path, encoding='utf-8', errors='replace') as f:
        for lineno, line in enumerate(f, 1):
            line = line.rstrip('\n')
            if not line:
                continue
            fields = line.split('|')
            if len(fields) < 28:
                log.warning(f"  {path.name} line {lineno}: 컬럼 부족 ({len(fields)}개) — 건너뜀")
                continue
            try:
                stn   = int(fields[0])
                year  = int(fields[1])
                month = int(fields[2])
                day   = int(fields[3])
            except ValueError:
                continue

            hours = {}
            for h in HOURS:
                raw = fields[3 + h].strip()          # field[4]=h1 ... field[27]=h24
                hours[h] = int(raw) if raw else 0

            data[(stn, year, month, day)] = hours
    return data


# ─── Supabase 조회 ────────────────────────────────────────────────────────────
def fetch_sb_year(sb, station_id: int, year: int) -> dict:
    """
    Supabase에서 특정 지점/연도 전체 데이터를 조회.
    반환: { (station, year, month, day): {h: val} }
    """
    try:
        resp = (
            sb.table(TABLE_NAME)
            .select('*')
            .eq('Station', station_id)
            .eq('Year', year)
            .execute()
        )
        result = {}
        for row in (resp.data or []):
            key = (row['Station'], row['Year'], row['Month'], row['Day'])
            result[key] = {h: row.get(str(h), 0) or 0 for h in HOURS}
        return result
    except Exception as e:
        log.error(f"  Supabase 조회 실패 (지점 {station_id}, {year}년): {e}")
        return {}


# ─── 검증 ────────────────────────────────────────────────────────────────────
def verify(prn_data: dict, sb: object, station_id: int, label: str):
    """PRN과 Supabase를 비교해 불일치를 출력합니다."""

    # PRN에서 해당 지점 연도 목록 추출
    years = sorted({y for (s, y, m, d) in prn_data if s == station_id})
    if not years:
        log.warning(f"  {label}: PRN에 지점 {station_id} 데이터 없음")
        return

    log.info(f"\n{'='*55}")
    log.info(f"  검증: {label} (지점 #{station_id})")
    log.info(f"  PRN 연도 범위: {years[0]}~{years[-1]} ({len(years)}년)")
    log.info(f"{'='*55}")

    total_rows    = 0
    mismatch_rows = 0
    missing_in_sb = 0

    for year in years:
        prn_year = {k: v for k, v in prn_data.items()
                    if k[0] == station_id and k[1] == year}
        sb_year  = fetch_sb_year(sb, station_id, year)

        for key in sorted(prn_year):
            total_rows += 1
            _, y, m, d = key

            if key not in sb_year:
                missing_in_sb += 1
                log.warning(f"  ❌ {y}-{m:02d}-{d:02d}: Supabase에 행 없음")
                continue

            prn_h = prn_year[key]
            sb_h  = sb_year[key]

            diffs = []
            for h in HOURS:
                pv = prn_h.get(h, 0)
                sv = sb_h.get(h, 0)
                if pv != sv:
                    diffs.append(f"h{h}: PRN={pv} SB={sv}")

            if diffs:
                mismatch_rows += 1
                log.warning(f"  ❌ {y}-{m:02d}-{d:02d}: {', '.join(diffs)}")

    match_rows = total_rows - mismatch_rows - missing_in_sb
    pct = match_rows / total_rows * 100 if total_rows else 0

    log.info(f"\n  결과 요약 [{label}]")
    log.info(f"  전체 행:    {total_rows:,}")
    log.info(f"  일치:       {match_rows:,} ({pct:.1f}%)")
    log.info(f"  불일치:     {mismatch_rows:,}")
    log.info(f"  SB 행 없음: {missing_in_sb:,}")


# ─── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    supabase_url = os.environ.get('SUPABASE_URL', '')
    supabase_key = os.environ.get('SUPABASE_SERVICE_KEY', '')
    prn_dir      = Path(os.environ.get('PRN_DIR', '.'))

    if not supabase_url or not supabase_key:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY 환경변수 필요")
        sys.exit(1)

    sb = create_client(supabase_url, supabase_key)
    log.info("Supabase 연결 완료")

    # PRN 파일 목록
    prn_files = sorted(prn_dir.glob('*.prn'))
    if not prn_files:
        log.error(f"PRN 파일 없음: {prn_dir}")
        sys.exit(1)

    log.info(f"PRN 파일 {len(prn_files)}개 발견: {[f.name for f in prn_files]}")

    # 파일명에서 지점번호 추론 (파일 첫 행의 station 컬럼 사용)
    for prn_path in prn_files:
        log.info(f"\n파싱 중: {prn_path.name}")
        prn_data = parse_prn(prn_path)

        if not prn_data:
            log.warning(f"  데이터 없음 — 건너뜀")
            continue

        # 지점번호는 PRN 데이터에서 추출
        stations = sorted({s for (s, y, m, d) in prn_data})
        for stn_id in stations:
            verify(prn_data, sb, stn_id, prn_path.stem)

    log.info("\n\n✅ 전체 검증 완료")


if __name__ == '__main__':
    main()
