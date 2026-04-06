#!/usr/bin/env python3
"""
Supabase rainfall_hourly 테이블 데이터 무결성 검사 스크립트.

검사 항목:
  1. 날짜 완결성 검사  - (지점, 연도)별 행 수 vs 실제 일수 비교 (누락/중복)
  2. 이상값 검사       - h1~h24 음수/극단값(>1000) 탐지
  3. h24 연속성 검사   - N일 h24 == N+1일 h1 인 버그 잔재 탐지
  4. PRN 신뢰 구간 대조 - 태백(216) 1986~2018, 인제(211) 1973~2018

환경변수:
  SUPABASE_URL         Supabase 프로젝트 URL
  SUPABASE_SERVICE_KEY Supabase service_role 키
  PRN_DIR              PRN 파일 디렉토리 (검사 4용, 기본: ./prn_data)
  STATION_IDS          검사 1~3 대상 지점 (쉼표 구분, 기본: 전체 95개)
  START_YEAR           검사 시작 연도 (기본: 1954)
  END_YEAR             검사 종료 연도 (기본: 현재 연도)
"""

import os
import sys
import calendar
import logging
from datetime import date
from pathlib import Path

from supabase import create_client

# ─── 로깅 ──────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

TABLE_NAME = 'rainfall_hourly'
HOURS = list(range(1, 25))  # h1 ~ h24

# ─── 전체 95개 지점 목록 ────────────────────────────────────────────────────────
ALL_STATIONS = [
    {'id': 108, 'name': '서울'},
    {'id': 159, 'name': '부산'},    {'id': 296, 'name': '북부산'},
    {'id': 143, 'name': '대구'},
    {'id': 112, 'name': '인천'},    {'id': 102, 'name': '백령도'}, {'id': 201, 'name': '강화'},
    {'id': 156, 'name': '광주'},
    {'id': 133, 'name': '대전'},
    {'id': 152, 'name': '울산'},
    {'id': 239, 'name': '세종'},
    {'id': 98,  'name': '동두천'},  {'id': 99,  'name': '파주'},
    {'id': 119, 'name': '수원'},    {'id': 202, 'name': '양평'},   {'id': 203, 'name': '이천'},
    {'id': 90,  'name': '속초'},    {'id': 95,  'name': '철원'},   {'id': 100, 'name': '대관령'},
    {'id': 101, 'name': '춘천'},    {'id': 104, 'name': '북강릉'}, {'id': 105, 'name': '강릉'},
    {'id': 106, 'name': '동해'},    {'id': 114, 'name': '원주'},   {'id': 121, 'name': '영월'},
    {'id': 211, 'name': '인제'},    {'id': 212, 'name': '홍천'},   {'id': 216, 'name': '태백'},
    {'id': 217, 'name': '정선'},
    {'id': 127, 'name': '충주'},    {'id': 131, 'name': '청주'},   {'id': 135, 'name': '추풍령'},
    {'id': 221, 'name': '제천'},    {'id': 226, 'name': '보은'},
    {'id': 129, 'name': '서산'},    {'id': 177, 'name': '홍성'},   {'id': 232, 'name': '천안'},
    {'id': 235, 'name': '보령'},    {'id': 236, 'name': '부여'},   {'id': 238, 'name': '금산'},
    {'id': 140, 'name': '군산'},    {'id': 146, 'name': '전주'},   {'id': 172, 'name': '고창'},
    {'id': 243, 'name': '부안'},    {'id': 244, 'name': '임실'},   {'id': 245, 'name': '정읍'},
    {'id': 247, 'name': '남원'},    {'id': 248, 'name': '장수'},   {'id': 251, 'name': '고창군'},
    {'id': 254, 'name': '순창군'},
    {'id': 165, 'name': '목포'},    {'id': 168, 'name': '여수'},   {'id': 169, 'name': '흑산도'},
    {'id': 170, 'name': '완도'},    {'id': 174, 'name': '순천'},   {'id': 252, 'name': '영광군'},
    {'id': 258, 'name': '보성군'},  {'id': 259, 'name': '강진군'}, {'id': 260, 'name': '장흥'},
    {'id': 261, 'name': '해남'},    {'id': 262, 'name': '고흥'},   {'id': 266, 'name': '광양시'},
    {'id': 268, 'name': '진도군'},
    {'id': 115, 'name': '울릉도'},  {'id': 130, 'name': '울진'},   {'id': 136, 'name': '안동'},
    {'id': 137, 'name': '상주'},    {'id': 138, 'name': '포항'},   {'id': 271, 'name': '봉화'},
    {'id': 272, 'name': '영주'},    {'id': 273, 'name': '문경'},   {'id': 276, 'name': '청송군'},
    {'id': 277, 'name': '영덕'},    {'id': 278, 'name': '의성'},   {'id': 279, 'name': '구미'},
    {'id': 281, 'name': '영천'},    {'id': 283, 'name': '경주시'},
    {'id': 155, 'name': '창원'},    {'id': 162, 'name': '통영'},   {'id': 192, 'name': '진주'},
    {'id': 253, 'name': '김해시'},  {'id': 255, 'name': '북창원'}, {'id': 257, 'name': '양산시'},
    {'id': 263, 'name': '의령군'},  {'id': 264, 'name': '함양군'}, {'id': 284, 'name': '거창'},
    {'id': 285, 'name': '합천'},    {'id': 288, 'name': '밀양'},   {'id': 289, 'name': '산청'},
    {'id': 294, 'name': '거제'},    {'id': 295, 'name': '남해'},
    {'id': 184, 'name': '제주'},    {'id': 185, 'name': '고산'},
    {'id': 188, 'name': '성산'},    {'id': 189, 'name': '서귀포'},
]

# ─── PRN 신뢰 구간 설정 (검사 4) ──────────────────────────────────────────────
PRN_TRUST = {
    216: {'name': '태백', 'file_pattern': '강우카드(태백)*.prn', 'start': 1986, 'end': 2018},
    211: {'name': '인제', 'file_pattern': '강우카드(인제)*.prn', 'start': 1973, 'end': 2018},
}


# ─── 헬퍼 ────────────────────────────────────────────────────────────────────
def fetch_year_data(sb, station_id: int, year: int) -> list:
    """지점/연도의 모든 행을 조회 (행 리스트 반환)."""
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
        log.error(f"  Supabase 조회 실패 (지점 {station_id}, {year}년): {e}")
        return []


def days_in_year(year: int) -> int:
    """해당 연도의 실제 일수 반환 (윤년 포함)."""
    return 366 if calendar.isleap(year) else 365


# ─── 검사 1: 날짜 완결성 ──────────────────────────────────────────────────────
def check_date_completeness(sb, stations: list, start_year: int, end_year: int) -> dict:
    """
    각 (지점, 연도)에서 저장된 행 수 vs 실제 일수를 비교.
    returns: { 'missing': [(station, year, actual_days, stored_rows)], 'duplicate': [...] }
    """
    log.info("\n" + "=" * 60)
    log.info("  [검사 1] 날짜 완결성 검사")
    log.info(f"  대상: {len(stations)}개 지점, {start_year}~{end_year}년")
    log.info("=" * 60)

    results = {'missing': [], 'duplicate': [], 'ok': 0}
    total_checks = 0

    for stn in stations:
        stn_id = stn['id']
        stn_name = stn['name']

        for year in range(start_year, end_year + 1):
            rows = fetch_year_data(sb, stn_id, year)
            if not rows:
                # 데이터 자체 없음 (동기화 전 기간 등) - 0행은 스킵
                continue

            stored = len(rows)
            expected = days_in_year(year)
            total_checks += 1

            if stored < expected:
                results['missing'].append((stn_id, stn_name, year, expected, stored))
                log.warning(f"  [누락] {stn_name}({stn_id}) {year}년: "
                            f"예상 {expected}일, 실제 {stored}행 ({expected - stored}개 부족)")
            elif stored > expected:
                results['duplicate'].append((stn_id, stn_name, year, expected, stored))
                log.warning(f"  [중복] {stn_name}({stn_id}) {year}년: "
                            f"예상 {expected}일, 실제 {stored}행 ({stored - expected}개 초과)")
            else:
                results['ok'] += 1

    log.info(f"\n  [검사 1 요약]")
    log.info(f"  검사한 (지점,연도) 쌍: {total_checks:,}개")
    log.info(f"  정상: {results['ok']:,}개")
    log.info(f"  날짜 누락: {len(results['missing']):,}개")
    log.info(f"  중복 행: {len(results['duplicate']):,}개")
    return results


# ─── 검사 2: 이상값 ──────────────────────────────────────────────────────────
def check_outliers(sb, stations: list, start_year: int, end_year: int) -> dict:
    """
    h1~h24 컬럼에서 음수 또는 1000 초과(= 100mm/hr 초과) 값 탐지.
    값 단위: 0.1mm (정수), 1000 = 100.0mm
    """
    log.info("\n" + "=" * 60)
    log.info("  [검사 2] 이상값 검사 (음수 / 시간당 100mm 초과)")
    log.info(f"  대상: {len(stations)}개 지점, {start_year}~{end_year}년")
    log.info("=" * 60)

    results = {'negative': [], 'extreme': [], 'ok_rows': 0, 'total_rows': 0}

    for stn in stations:
        stn_id = stn['id']
        stn_name = stn['name']

        for year in range(start_year, end_year + 1):
            rows = fetch_year_data(sb, stn_id, year)
            if not rows:
                continue

            for row in rows:
                results['total_rows'] += 1
                row_bad = False

                negatives = []
                extremes = []
                for h in HOURS:
                    val = row.get(str(h), 0) or 0
                    if val < 0:
                        negatives.append(f"h{h}={val}")
                        row_bad = True
                    elif val > 1000:
                        extremes.append(f"h{h}={val}")
                        row_bad = True

                m = row['Month']
                d = row['Day']

                if negatives:
                    entry = (stn_id, stn_name, year, m, d, negatives)
                    results['negative'].append(entry)
                    log.warning(f"  [음수] {stn_name}({stn_id}) "
                                f"{year}-{m:02d}-{d:02d}: {', '.join(negatives)}")
                if extremes:
                    entry = (stn_id, stn_name, year, m, d, extremes)
                    results['extreme'].append(entry)
                    log.warning(f"  [극단] {stn_name}({stn_id}) "
                                f"{year}-{m:02d}-{d:02d}: {', '.join(extremes)}")

                if not row_bad:
                    results['ok_rows'] += 1

    log.info(f"\n  [검사 2 요약]")
    log.info(f"  검사한 행 수: {results['total_rows']:,}개")
    log.info(f"  정상: {results['ok_rows']:,}개")
    log.info(f"  음수 값 행: {len(results['negative']):,}개")
    log.info(f"  극단값 행 (>100mm/hr): {len(results['extreme']):,}개")
    return results


# ─── 검사 3: h24 연속성 ──────────────────────────────────────────────────────
def check_h24_continuity(sb, stations: list, start_year: int, end_year: int) -> dict:
    """
    N일 h24 값 == N+1일 h1 값이면서 둘 다 0이 아닌 경우를 버그 잔재로 탐지.
    (정상적으로는 h24 강우가 h1에 중복되지 않아야 함)
    """
    log.info("\n" + "=" * 60)
    log.info("  [검사 3] h24 연속성 검사 (버그 잔재: N일 h24 == N+1일 h1)")
    log.info(f"  대상: {len(stations)}개 지점, {start_year}~{end_year}년")
    log.info("=" * 60)

    results = {'bugs': [], 'ok': 0, 'total_pairs': 0}

    for stn in stations:
        stn_id = stn['id']
        stn_name = stn['name']

        for year in range(start_year, end_year + 1):
            rows = fetch_year_data(sb, stn_id, year)
            if len(rows) < 2:
                continue

            # (month, day) → row 매핑
            day_map = {}
            for row in rows:
                key = (row['Month'], row['Day'])
                day_map[key] = row

            # 연속된 날 쌍 검사
            for row in rows:
                m, d = row['Month'], row['Day']
                # 다음 날 계산
                try:
                    cur_date = date(year, m, d)
                except ValueError:
                    continue
                from datetime import timedelta
                next_date = cur_date + timedelta(days=1)

                # 연말 → 다음 연도 첫날은 별도 조회 필요하므로 스킵
                if next_date.year != year:
                    continue

                next_key = (next_date.month, next_date.day)
                if next_key not in day_map:
                    continue

                results['total_pairs'] += 1

                h24_val = row.get('24', 0) or 0
                h1_next = day_map[next_key].get('1', 0) or 0

                # 둘 다 0이 아닌데 값이 같으면 버그 잔재
                if h24_val != 0 and h24_val == h1_next:
                    bug = (stn_id, stn_name, year, m, d, h24_val,
                           next_date.month, next_date.day)
                    results['bugs'].append(bug)
                    log.warning(
                        f"  [버그] {stn_name}({stn_id}) "
                        f"{year}-{m:02d}-{d:02d} h24={h24_val} == "
                        f"{year}-{next_date.month:02d}-{next_date.day:02d} h1={h1_next}"
                    )
                else:
                    results['ok'] += 1

    log.info(f"\n  [검사 3 요약]")
    log.info(f"  검사한 날짜 쌍: {results['total_pairs']:,}개")
    log.info(f"  정상: {results['ok']:,}개")
    log.info(f"  h24 연속성 버그 의심: {len(results['bugs']):,}개")
    return results


# ─── PRN 파싱 ────────────────────────────────────────────────────────────────
def parse_prn(path: Path, start_year: int, end_year: int) -> dict:
    """
    PRN 파일 파싱 (신뢰 기간만).
    포맷: station|year|month|day|h1|...|h24|
    반환: { (station, year, month, day): {h: val} }
    """
    data = {}
    with open(path, encoding='utf-8', errors='replace') as f:
        for lineno, line in enumerate(f, 1):
            line = line.rstrip('\n')
            if not line:
                continue
            fields = line.split('|')
            if len(fields) < 28:
                continue
            try:
                stn   = int(fields[0])
                year  = int(fields[1])
                month = int(fields[2])
                day   = int(fields[3])
            except ValueError:
                continue

            if not (start_year <= year <= end_year):
                continue

            hours = {}
            for h in HOURS:
                raw = fields[3 + h].strip()  # fields[4]=h1 ... fields[27]=h24
                hours[h] = int(raw) if raw else 0

            data[(stn, year, month, day)] = hours
    return data


# ─── 검사 4: PRN 신뢰 구간 대조 ──────────────────────────────────────────────
def check_prn_trust(sb, prn_dir: Path) -> dict:
    """
    태백(216) 1986~2018, 인제(211) 1973~2018 기간의
    PRN vs Supabase 값 대조.
    """
    log.info("\n" + "=" * 60)
    log.info("  [검사 4] PRN 신뢰 구간 대조")
    log.info("  태백(216): 1986~2018 / 인제(211): 1973~2018")
    log.info("=" * 60)

    all_results = {}

    for stn_id, cfg in PRN_TRUST.items():
        stn_name = cfg['name']
        trust_start = cfg['start']
        trust_end = cfg['end']

        # PRN 파일 탐색
        prn_files = sorted(prn_dir.glob(cfg['file_pattern']))
        if not prn_files:
            log.warning(f"  {stn_name}({stn_id}): PRN 파일 없음 ({cfg['file_pattern']})")
            all_results[stn_id] = None
            continue

        prn_path = prn_files[0]
        log.info(f"\n  파싱: {prn_path.name} ({trust_start}~{trust_end}년만)")

        prn_data = parse_prn(prn_path, trust_start, trust_end)
        if not prn_data:
            log.warning(f"  {stn_name}({stn_id}): PRN 신뢰 구간 데이터 없음")
            all_results[stn_id] = None
            continue

        prn_years = sorted({y for (s, y, m, d) in prn_data if s == stn_id})
        log.info(f"  PRN 데이터: {len(prn_data):,}행, {prn_years[0]}~{prn_years[-1]}년")

        total_rows = 0
        mismatch_rows = 0
        missing_in_sb = 0

        for year in prn_years:
            prn_year = {k: v for k, v in prn_data.items()
                        if k[0] == stn_id and k[1] == year}

            # Supabase에서 해당 연도 조회
            sb_rows = fetch_year_data(sb, stn_id, year)
            sb_year = {}
            for row in sb_rows:
                key = (row['Station'], row['Year'], row['Month'], row['Day'])
                sb_year[key] = {h: row.get(str(h), 0) or 0 for h in HOURS}

            for key in sorted(prn_year):
                total_rows += 1
                _, y, m, d = key

                if key not in sb_year:
                    missing_in_sb += 1
                    log.warning(f"  [SB없음] {stn_name}({stn_id}) "
                                f"{y}-{m:02d}-{d:02d}: Supabase 행 없음")
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
                    log.warning(f"  [불일치] {stn_name}({stn_id}) "
                                f"{y}-{m:02d}-{d:02d}: {', '.join(diffs)}")

        ok_rows = total_rows - mismatch_rows - missing_in_sb
        pct = ok_rows / total_rows * 100 if total_rows else 0

        log.info(f"\n  [{stn_name} PRN 대조 결과]")
        log.info(f"  전체 행: {total_rows:,}개")
        log.info(f"  일치:    {ok_rows:,}개 ({pct:.2f}%)")
        log.info(f"  불일치:  {mismatch_rows:,}개")
        log.info(f"  SB없음:  {missing_in_sb:,}개")

        all_results[stn_id] = {
            'total': total_rows, 'ok': ok_rows,
            'mismatch': mismatch_rows, 'missing': missing_in_sb
        }

    return all_results


# ─── 메인 ────────────────────────────────────────────────────────────────────
def main():
    supabase_url = os.environ.get('SUPABASE_URL', '')
    supabase_key = os.environ.get('SUPABASE_SERVICE_KEY', '')
    prn_dir      = Path(os.environ.get('PRN_DIR', './prn_data'))

    if not supabase_url or not supabase_key:
        log.error("SUPABASE_URL / SUPABASE_SERVICE_KEY 환경변수가 필요합니다.")
        sys.exit(1)

    start_year = int(os.environ.get('START_YEAR', '1954') or '1954')
    end_year   = int(os.environ.get('END_YEAR', str(date.today().year)) or str(date.today().year))

    # 지점 필터
    station_ids_env = os.environ.get('STATION_IDS', '').strip()
    if station_ids_env:
        filter_ids = {int(x.strip()) for x in station_ids_env.split(',') if x.strip()}
        stations = [s for s in ALL_STATIONS if s['id'] in filter_ids]
    else:
        stations = ALL_STATIONS

    log.info("=" * 60)
    log.info("  rainfall_hourly 데이터 무결성 검사 시작")
    log.info(f"  대상 지점: {len(stations)}개")
    log.info(f"  기간: {start_year}~{end_year}년")
    log.info(f"  PRN 디렉토리: {prn_dir}")
    log.info("=" * 60)

    sb = create_client(supabase_url, supabase_key)
    log.info("Supabase 연결 완료 (service_role)")

    # ── 검사 실행 ──────────────────────────────────────────────────────────────
    r1 = check_date_completeness(sb, stations, start_year, end_year)
    r2 = check_outliers(sb, stations, start_year, end_year)
    r3 = check_h24_continuity(sb, stations, start_year, end_year)
    r4 = check_prn_trust(sb, prn_dir)

    # ── 최종 종합 요약 ─────────────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("  최종 무결성 검사 종합 요약")
    log.info("=" * 60)

    c1_issues = len(r1['missing']) + len(r1['duplicate'])
    c2_issues = len(r2['negative']) + len(r2['extreme'])
    c3_issues = len(r3['bugs'])
    c4_issues = sum(
        (v['mismatch'] + v['missing']) for v in r4.values() if v is not None
    )

    log.info(f"  [검사 1] 날짜 완결성 - 이상 {c1_issues:,}건 "
             f"(누락 {len(r1['missing'])}, 중복 {len(r1['duplicate'])})")
    log.info(f"  [검사 2] 이상값      - 이상 {c2_issues:,}건 "
             f"(음수 {len(r2['negative'])}, 극단 {len(r2['extreme'])})")
    log.info(f"  [검사 3] h24 연속성  - 버그 의심 {c3_issues:,}건")
    log.info(f"  [검사 4] PRN 대조    - 불일치 {c4_issues:,}건")

    total_issues = c1_issues + c2_issues + c3_issues + c4_issues
    if total_issues == 0:
        log.info("\n  결과: 모든 검사 통과 (이상 없음)")
    else:
        log.info(f"\n  결과: 총 {total_issues:,}건 이상 탐지")

    log.info("=" * 60)

    # 이슈가 있으면 exit code 1 반환 (CI에서 실패로 처리)
    sys.exit(1 if total_issues > 0 else 0)


if __name__ == '__main__':
    main()
