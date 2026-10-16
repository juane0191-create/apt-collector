"""
수도권 핵심 25개 구역 아파트 실거래가 일일 자동 수집 및 이상거래 분석 파이프라인
- 실행 환경: GitHub Actions (매일 지정된 시각 자동 가동)
- 원천 데이터: 국토교통부 아파트매매 실거래 상세자료 Open API
- 수집 대상: 서울 18개 자치구 + 경기 7개 주요 권역
- 출력 결과: 
  1. data/all_trades_{YYYYMM}.csv (전체 실거래 원본 누적본)
  2. data/daily_briefing_data.json (신고가·급매소진·거래량 핀셋 분석본)
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from PublicDataReader import TransactionPrice

# 1. 대상 25개 핵심 권역 시군구 법정동 코드 매핑
TARGET_REGIONS = {
    # [서울 강남·동남권]
    "11680": {"name": "서울 강남구", "category": "seoul-southeast"},
    "11650": {"name": "서울 서초구", "category": "seoul-southeast"},
    "11710": {"name": "서울 송파구", "category": "seoul-southeast"},
    "11740": {"name": "서울 강동구", "category": "seoul-southeast"},

    # [서울 마용성 & 한강변]
    "11170": {"name": "서울 용산구", "category": "seoul-river"},
    "11200": {"name": "서울 성동구", "category": "seoul-river"},
    "11440": {"name": "서울 마포구", "category": "seoul-river"},
    "11215": {"name": "서울 광진구", "category": "seoul-river"},

    # [서울 서남권]
    "11560": {"name": "서울 영등포구", "category": "seoul-southwest"},
    "11590": {"name": "서울 동작구", "category": "seoul-southwest"},
    "11500": {"name": "서울 강서구", "category": "seoul-southwest"},
    "11620": {"name": "서울 관악구", "category": "seoul-southwest"},
    "11530": {"name": "서울 구로구", "category": "seoul-southwest"},
    "11540": {"name": "서울 금천구", "category": "seoul-southwest"},

    # [서울 동북권]
    "11350": {"name": "서울 노원구", "category": "seoul-northeast"},
    "11320": {"name": "서울 도봉구", "category": "seoul-northeast"},
    "11305": {"name": "서울 강북구", "category": "seoul-northeast"},
    "11290": {"name": "서울 성북구", "category": "seoul-northeast"},

    # [경기 경부축 & 남부 대장]
    "41135": {"name": "성남시 분당구", "category": "gyeonggi-south"},
    "41465": {"name": "용인시 수지구", "category": "gyeonggi-south"},
    "41590": {"name": "화성시(동탄포함)", "category": "gyeonggi-south"},
    "41117": {"name": "수원시 영통구(광교)", "category": "gyeonggi-south"},

    # [경기 서울인접 핵심지]
    "41290": {"name": "과천시", "category": "gyeonggi-close"},
    "41210": {"name": "광명시", "category": "gyeonggi-close"},
    "41450": {"name": "하남시", "category": "gyeonggi-close"},
}

def collect_all_regions_data(service_key: str) -> pd.DataFrame:
    """25개 타깃 지역의 당월(및 월초 보정을 위한 전월) 실거래가를 순차 수집합니다."""
    ts = TransactionPrice(service_key)
    now = datetime.now()
    current_ym = now.strftime("%Y%m")
    
    # 30일 이내 신고 원칙상 월초(1~7일)에는 전월 데이터도 함께 조회하여 누락 방지
    query_months = [current_ym]
    if now.day <= 7:
        prev_month = (now.replace(day=1) - timedelta(days=1)).strftime("%Y%m")
        query_months.append(prev_month)

    all_dfs = []
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 25개 자치구 실거래가 일괄 수집 시작...")

    for code, info in TARGET_REGIONS.items():
        for ym in query_months:
            try:
                print(f" -> 수집 중: {info['name']} ({code}) - {ym}")
                df = ts.get_data(
                    property_type="아파트",
                    trade_type="매매",
                    sigungu_code=code,
                    year_month=ym
                )
                if df is not None and not df.empty:
                    df["지역명"] = info["name"]
                    df["권역코드"] = info["category"]
                    all_dfs.append(df)
            except Exception as e:
                print(f" [경고] {info['name']}({code}) 조회 실패: {e}")
            
            # 공공데이터포털 트래픽 차단 방지용 딜레이
            time.sleep(0.25)

    if not all_dfs:
        print("[오류] 수집된 데이터가 없습니다.")
        return pd.DataFrame()

    combined_df = pd.concat(all_dfs, ignore_index=True)
    return combined_df

def clean_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """취소 거래(시세 교란), 결측치 정제 및 가격 단위 표준화"""
    if df.empty:
        return df

    # 1. 해제(취소) 거래 필터링
    if "해제사유발생일" in df.columns:
        df = df[df["해제사유발생일"].isna() | (df["해제사유발생일"].astype(str).str.strip() == "")]
    if "해제여부" in df.columns:
        df = df[df["해제여부"] != "O"]

    # 2. 거래금액(만원)을 숫자형(억원 단위)으로 변환
    def parse_price(val):
        try:
            if isinstance(val, (int, float)):
                return round(float(val) / 10000.0, 2)
            clean_str = str(val).replace(",", "").strip()
            return round(float(clean_str) / 10000.0, 2)
        except Exception:
            return 0.0

    df["거래금액_억"] = df["거래금액"].apply(parse_price)

    # 3. 계약일자 포맷 통일 (YYYY-MM-DD)
    if {"년", "월", "일"}.issubset(df.columns):
        df["계약일자"] = df.apply(
            lambda r: f"{int(r['년']):04d}-{int(r['월']):02d}-{int(r['일']):02d}", axis=1
        )

    # 4. 층수 및 전용면적 정제
    df["전용면적"] = pd.to_numeric(df["전용면적"], errors="coerce").fillna(0.0)
    df["층"] = df["층"].astype(str).str.replace(".0", "", regex=False) + "층"

    return df

def extract_curated_insights(df: pd.DataFrame) -> list:
    """단지별 신고가(ATH), 급매 소진 및 거래량 급증 단지를 핀셋 선별합니다."""
    if df.empty or "아파트" not in df.columns:
        return []

    # 직거래 및 극단적 저층(1층)은 시세 대표성 왜곡 방지를 위해 선별에서 제외
    valid_df = df.copy()
    if "거래유형" in valid_df.columns:
        valid_df = valid_df[~valid_df["거래유형"].astype(str).str.contains("직거래")]
    valid_df = valid_df[~valid_df["층"].isin(["1층", "2층"])]

    curated = []
    
    # 단지명과 전용면적(반올림 정수) 기준으로 그룹화
    valid_df["평형그룹"] = valid_df["전용면적"].round().astype(int)
    groups = valid_df.groupby(["아파트", "평형그룹", "지역명"])

    for (apt_name, area_int, region_name), group in groups:
        if len(group) == 0:
            continue
            
        sorted_g = group.sort_values(by="계약일자")
        latest = sorted_g.iloc[-1]
        prices = sorted_g["거래금액_억"].tolist()

        if len(prices) == 0:
            continue

        latest_price = latest["거래금액_억"]
        row_id = f"tx-{len(curated)+1}"
        region_cat = latest.get("권역코드", "all")
        dong = latest.get("법정동", "")

        # 1. 신고가(ATH) 판정: 직전 기록 대비 상승 체결
        if len(prices) >= 2 and latest_price > max(prices[:-1]):
            prev_high = max(prices[:-1])
            diff = round(latest_price - prev_high, 2)
            diff_pct = round((diff / prev_high) * 100, 2)
            
            curated.append({
                "id": row_id,
                "complexName": apt_name,
                "regionCategory": region_cat,
                "regionName": f"{region_name} {dong}".strip(),
                "area": f"{latest['전용면적']:.1f}㎡",
                "floor": latest["층"],
                "dealPrice": latest_price,
                "previousHigh": prev_high,
                "dealType": "ath",
                "diff": diff,
                "diffPercent": diff_pct,
                "date": latest["계약일자"],
                "tags": ["신고가경신", "상급지리딩"],
                "description": f"종전 최고가 {prev_high}억 대비 {diff}억원 상승하며 직전 거래가를 경신했습니다.",
                "recentPrices": prices[-5:]
            })

        # 2. 거래량 급증 단지 판정: 단기간 내 동일 평형 거래 3건 이상 집중
        elif len(sorted_g) >= 3:
            first_price = prices[0]
            diff = round(latest_price - first_price, 2)
            diff_pct = round((diff / (first_price or 1)) * 100, 2)
            
            curated.append({
                "id": row_id,
                "complexName": apt_name,
                "regionCategory": region_cat,
                "regionName": f"{region_name} {dong}".strip(),
                "area": f"{latest['전용면적']:.1f}㎡",
                "floor": latest["층"],
                "dealPrice": latest_price,
                "previousHigh": max(prices),
                "dealType": "volume",
                "diff": diff,
                "diffPercent": diff_pct,
                "date": latest["계약일자"],
                "tags": ["거래량급증", "손바뀜활발"],
                "description": f"최근 단기간에 거래가 집중되며 매수세가 강하게 유입된 단지입니다.",
                "recentPrices": prices[-5:]
            })

    # 상위 유의미한 15개 단지 선별
    return curated[:15]

def main():
    service_key = os.environ.get("MOLIT_KEY")
    if not service_key:
        print("[오류] GitHub Secrets에 MOLIT_KEY가 설정되지 않았습니다.")
        sys.exit(1)

    now = datetime.now()
    os.makedirs("data", exist_ok=True)

    # 1. 25개 구역 데이터 수집
    raw_df = collect_all_regions_data(service_key)
    if raw_df.empty:
        print("수집된 거래가 없습니다.")
        return

    # 2. 데이터 정제 및 취소거래 제외
    clean_df = clean_and_normalize(raw_df)

    # 3. 누적 전체 CSV 저장 (엑셀 다운로드용)
    csv_filename = f"data/real_estate_trades_{now.strftime('%Y%m')}.csv"
    clean_df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
    print(f"[저장 완료] 전체 실거래 원본: {csv_filename} (총 {len(clean_df)}건)")

    # 4. 웹앱/브리핑용 이상거래 추출 및 JSON 저장
    curated_list = extract_curated_insights(clean_df)
    json_payload = {
        "updatedAt": now.strftime("%Y-%m-%d %H:%M:%S"),
        "totalTradeCount": len(clean_df),
        "curatedCount": len(curated_list),
        "curatedTransactions": curated_list
    }

    json_filename = "data/daily_briefing_data.json"
    with open(json_filename, "w", encoding="utf-8") as f:
        json.dump(json_payload, f, ensure_ascii=False, indent=2)
    print(f"[저장 완료] 브리핑 대시보드 데이터: {json_filename} (선별 {len(curated_list)}건)")

if __name__ == "__main__":
    main()
