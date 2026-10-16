"""
수도권 핵심 25개 구역 아파트 실거래가 고속 자동 수집 파이프라인
- 주요 기능:
  1. 병렬 멀티스레딩(ThreadPool)으로 25개 구역 초고속 수집 (1~2분 소요)
  2. 실시간 콘솔 출력 (flush=True)으로 진행 상태 실시간 확인
  3. 최근 2개월치(당월+전월) 조회를 통한 실거래 누락 방지
  4. 단지별 신고가(ATH) 및 거래량 급증 특이거래 자동 큐레이션
  5. data/all_trades_{YYYYMM}.csv 및 data/daily_briefing_data.json 자동 출력
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
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

def fetch_single_region(ts, code, info, query_months):
    """단일 구역 데이터를 안전하게 수집하는 작업 단위 (스레드에서 실행)"""
    results = []
    for ym in query_months:
        try:
            df = ts.get_data(
                property_type="아파트",
                trade_type="매매",
                sigungu_code=code,
                year_month=ym
            )
            if df is not None and not df.empty:
                df["지역명"] = info["name"]
                df["권역코드"] = info["category"]
                results.append(df)
        except Exception as e:
            print(f"⚠️ [{info['name']}] 조회 중 예외 발생: {e}", flush=True)

    count = sum(len(d) for d in results)
    return results, count, info["name"]

def collect_all_regions_parallel(service_key: str) -> pd.DataFrame:
    """25개 권역을 멀티스레드로 병렬 수집합니다."""
    ts = TransactionPrice(service_key)
    now = datetime.now()
    current_ym = now.strftime("%Y%m")

    # 신고 기한(30일)을 감안하여 항상 당월 + 전월 데이터를 함께 조회
    prev_month = (now.replace(day=1) - timedelta(days=1)).strftime("%Y%m")
    query_months = [prev_month, current_ym]

    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] 🚀 25개 구역 실거래 수집 시작 (조회 대상: {query_months})...", flush=True)

    all_dfs = []
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(fetch_single_region, ts, code, info, query_months)
            for code, info in TARGET_REGIONS.items()
        ]

        for idx, future in enumerate(as_completed(futures), 1):
            try:
                dfs, count, region_name = future.result()
                all_dfs.extend(dfs)
                print(f"  [{idx}/25] ✅ {region_name} 수집 완료 ({count}건)", flush=True)
            except Exception as e:
                print(f"❌ 작업 실패: {e}", flush=True)

    if not all_dfs:
        print("⚠️ 수집된 데이터가 없습니다.", flush=True)
        return pd.DataFrame()

    combined_df = pd.concat(all_dfs, ignore_index=True)
    return combined_df

def clean_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """취소 거래 필터링 및 가격 단위 표준화 (구/신버전 컬럼명 호환)"""
    if df.empty:
        return df

    # 컬럼명 표준화 (신버전 -> 공통 컬럼명)
    col_map = {
        "단지명": "아파트",
        "단지": "아파트",
        "거래금액(만원)": "거래금액",
        "계약년도": "년",
        "계약월": "월",
        "계약일": "일",
        "전용면적(㎡)": "전용면적"
    }
    for old_col, new_col in col_map.items():
        if old_col in df.columns and new_col not in df.columns:
            df[new_col] = df[old_col]

    # 1. 취소(해제) 거래 필터링
    if "해제사유발생일" in df.columns:
        df = df[df["해제사유발생일"].isna() | (df["해제사유발생일"].astype(str).str.strip().isin(["", "nan", "None"]))]
    if "해제여부" in df.columns:
        df = df[df["해제여부"] != "O"]

    # 2. 거래금액(만원) -> 억원 단위 변환
    def parse_price(val):
        try:
            if pd.isna(val):
                return 0.0
            clean = str(val).replace(",", "").strip()
            return round(float(clean) / 10000.0, 2)
        except Exception:
            return 0.0

    if "거래금액" in df.columns:
        df["거래금액_억"] = df["거래금액"].apply(parse_price)
    else:
        df["거래금액_억"] = 0.0

    # 3. 계약일자 포맷 통일 (YYYY-MM-DD)
    def format_date(r):
        try:
            y = int(float(r.get("년", 0)))
            m = int(float(r.get("월", 0)))
            d = int(float(r.get("일", 0)))
            return f"{y:04d}-{m:02d}-{d:02d}"
        except Exception:
            return ""

    if {"년", "월", "일"}.issubset(df.columns):
        df["계약일자"] = df.apply(format_date, axis=1)
    else:
        df["계약일자"] = datetime.now().strftime("%Y-%m-%d")

    # 4. 층수 및 전용면적 정제
    if "전용면적" in df.columns:
        df["전용면적"] = pd.to_numeric(df["전용면적"], errors="coerce").fillna(0.0)
    else:
        df["전용면적"] = 84.0

    if "층" in df.columns:
        df["층"] = df["층"].astype(str).str.replace(".0", "", regex=False).str.strip() + "층"
    else:
        df["층"] = "고층"

    # 중복 계약 제거
    dedup_cols = [c for c in ["아파트", "전용면적", "층", "거래금액_억", "계약일자"] if c in df.columns]
    if dedup_cols:
        df = df.drop_duplicates(subset=dedup_cols)

    return df

def extract_curated_insights(df: pd.DataFrame) -> list:
    """단지별 신고가(ATH) 및 거래량 집중 핀셋 선별"""
    if df.empty or "아파트" not in df.columns:
        return []

    # 직거래 및 극단적 저층(1층) 배제 (시세 왜곡 방지)
    valid_df = df.copy()
    if "거래유형" in valid_df.columns:
        valid_df = valid_df[~valid_df["거래유형"].astype(str).str.contains("직거래", na=False)]
    if "층" in valid_df.columns:
        valid_df = valid_df[~valid_df["층"].isin(["1층", "2층"])]

    curated = []
    valid_df["평형그룹"] = valid_df["전용면적"].round().astype(int)
    groups = valid_df.groupby(["아파트", "평형그룹", "지역명"])

    for (apt_name, area_int, region_name), group in groups:
        if len(group) == 0:
            continue

        sorted_g = group.sort_values(by="계약일자")
        latest = sorted_g.iloc[-1]
        prices = [p for p in sorted_g["거래금액_억"].tolist() if p > 0]

        if not prices:
            continue

        latest_price = latest["거래금액_억"]
        row_id = f"tx-{len(curated)+1}"
        region_cat = latest.get("권역코드", "all")
        dong = latest.get("법정동", "")

        # 1. 신고가 (ATH)
        if len(prices) >= 2 and latest_price > max(prices[:-1]):
            prev_high = max(prices[:-1])
            diff = round(latest_price - prev_high, 2)
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
                "diffPercent": round((diff / (prev_high or 1)) * 100, 2),
                "date": latest.get("계약일자", ""),
                "tags": ["신고가경신", "상급지리딩"],
                "description": f"종전 최고가 {prev_high}억 대비 {diff}억원 상승하며 최고가를 경신했습니다.",
                "recentPrices": prices[-5:]
            })

        # 2. 거래량 집중 단지
        elif len(sorted_g) >= 2:
            first_price = prices[0]
            diff = round(latest_price - first_price, 2)
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
                "diffPercent": round((diff / (first_price or 1)) * 100, 2),
                "date": latest.get("계약일자", ""),
                "tags": ["거래량급증", "손바뀜활발"],
                "description": f"최근 단기간에 거래가 집중되며 매수세가 유입된 단지입니다.",
                "recentPrices": prices[-5:]
            })

    # 선별 건수가 적을 경우, 최신 정상 거래 상위 건으로 자동 보충
    if len(curated) < 12 and not valid_df.empty:
        latest_sorted = valid_df.sort_values(by="계약일자", ascending=False).head(30)
        for _, row in latest_sorted.iterrows():
            if any(c["complexName"] == row["아파트"] for c in curated):
                continue
            curated.append({
                "id": f"tx-{len(curated)+1}",
                "complexName": row["아파트"],
                "regionCategory": row.get("권역코드", "all"),
                "regionName": f"{row.get('지역명', '')} {row.get('법정동', '')}".strip(),
                "area": f"{row['전용면적']:.1f}㎡",
                "floor": row["층"],
                "dealPrice": row["거래금액_억"],
                "previousHigh": row["거래금액_억"],
                "dealType": "volume",
                "diff": 0.0,
                "diffPercent": 0.0,
                "date": row.get("계약일자", ""),
                "tags": ["실거래체결", "관심단지"],
                "description": f"{row.get('계약일자', '')} 체결된 최신 정상 실거래건입니다.",
                "recentPrices": [row["거래금액_억"]]
            })

    # 최신 체결일 기준 정렬
    curated = sorted(curated, key=lambda x: x["date"], reverse=True)
    return curated[:24]

def main():
    service_key = os.environ.get("MOLIT_KEY")
    if not service_key:
        print("❌ [오류] GitHub Secrets에 MOLIT_KEY가 설정되지 않았습니다.", flush=True)
        sys.exit(1)

    now = datetime.now()
    os.makedirs("data", exist_ok=True)

    # 1. 25개 구역 데이터 수집
    raw_df = collect_all_regions_parallel(service_key)
    if raw_df.empty:
        print("⚠️ 수집된 거래 데이터가 없습니다.", flush=True)
        sys.exit(0)

    print(f"📊 수집된 원시 데이터: 총 {len(raw_df)}건", flush=True)

    # 2. 데이터 정제
    clean_df = clean_and_normalize(raw_df)

    # 3. CSV 원본 저장
    csv_filename = f"data/real_estate_trades_{now.strftime('%Y%m')}.csv"
    clean_df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
    print(f"💾 [저장 완료] 전체 실거래 원본: {csv_filename} ({len(clean_df)}건)", flush=True)

    # 4. JSON 선별 데이터 저장
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
    print(f"✨ [저장 완료] 웹 대시보드 데이터: {json_filename} (선별 {len(curated_list)}건)", flush=True)

if __name__ == "__main__":
    main()
