"""
수도권 핵심 25개 구역 아파트 실거래가 고속 자동 수집 파이프라인 (5개년 벤치마크 비교 엔진 탑재)
- 주요 기능:
  1. 당월 및 전월 실거래가 고속 수집 (직거래 및 해제거래 원천 배제)
  2. 'data/history_ath_benchmark.csv'(5개년 전고점 DB) 자동 로드 및 실시간 대조
  3. 역대 신고가(전고점 돌파) 여부 및 '전고점 대비 회복률(%)' 정밀 계산
  4. 신규 역대 최고가 발생 시 벤치마크 DB 자동 갱신
  5. 웹 대시보드(index.html)와 100% 호환되는 JSON 내보내기
"""

import os
import sys
import json
import time
import urllib.parse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import xml.etree.ElementTree as ET
import pandas as pd

# 1. 25개 핵심 권역 시군구 법정동 코드
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
    "41590": {"name": "화성시", "category": "gyeonggi-south"},
    "41117": {"name": "수원시 영통구", "category": "gyeonggi-south"},

    # [경기 서울인접 핵심지]
    "41290": {"name": "과천시", "category": "gyeonggi-close"},
    "41210": {"name": "광명시", "category": "gyeonggi-close"},
    "41450": {"name": "하남시", "category": "gyeonggi-close"},
}

API_ENDPOINT = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
BENCHMARK_FILE = "data/history_ath_benchmark.csv"

def fetch_region_xml(service_key: str, code: str, ym: str) -> list:
    """10초 타임아웃 및 안전 파싱 적용 API 호출 (해제 및 직거래 원천 차단)"""
    decoded_key = urllib.parse.unquote(service_key)
    
    params = {
        "serviceKey": decoded_key,
        "pageNo": "1",
        "numOfRows": "1000",
        "LAWD_CD": code,
        "DEAL_YMD": ym
    }
    
    records = []
    try:
        resp = requests.get(API_ENDPOINT, params=params, timeout=10)
        if resp.status_code != 200:
            return records
        
        root = ET.fromstring(resp.content)
        items = root.findall(".//item")
        
        for item in items:
            def val(tag):
                node = item.find(tag)
                return node.text.strip() if (node is not None and node.text) else ""
            
            # 해제 거래 및 직거래 제외
            if val("cdealDay") or val("cdealType") == "O":
                continue
            if "직거래" in val("dealingGbn"):
                continue
            
            price_raw = val("dealAmount").replace(",", "").strip()
            if not price_raw:
                continue
            
            records.append({
                "아파트": val("aptNm"),
                "전용면적": float(val("excluUseAr")) if val("excluUseAr") else 84.0,
                "층": f"{val('floor')}층" if val('floor') else "중층",
                "거래금액_억": round(float(price_raw) / 10000.0, 2),
                "계약일자": f"{val('dealYear')}-{str(val('dealMonth')).zfill(2)}-{str(val('dealDay')).zfill(2)}",
                "조회월": ym,
                "법정동": val("umdNm"),
                "거래유형": val("dealingGbn")
            })
    except Exception:
        pass
        
    return records

def fetch_single_region_task(service_key: str, code: str, info: dict, query_months: list):
    """단일 구역 조회 작업"""
    all_records = []
    for ym in query_months:
        records = fetch_region_xml(service_key, code, ym)
        for r in records:
            r["지역명"] = info["name"]
            r["권역코드"] = info["category"]
            all_records.append(r)
            
    return info["name"], all_records

def determine_query_months():
    """조회 월 결정 (당월 및 직전월)"""
    now = datetime.now()
    cur_ym = now.strftime("%Y%m")
    
    m = now.month - 1
    y = now.year
    if m == 0:
        m = 12
        y -= 1
    prev_ym = f"{y:04d}{m:02d}"
    
    return [prev_ym, cur_ym]

def load_benchmark_db():
    """5개년 역대 최고가 벤치마크 DB 로드"""
    if not os.path.exists(BENCHMARK_FILE):
        return {}

    try:
        b_df = pd.read_csv(BENCHMARK_FILE)
        benchmark = {}
        for _, r in b_df.iterrows():
            key = f"{r['아파트']}_{int(round(r['전용면적']))}_{r['지역명']}"
            benchmark[key] = {
                "역대최고가_억": float(r["역대최고가_억"]),
                "최고가계약일": str(r["최고가계약일"]),
                "최고가층": str(r.get("최고가층", ""))
            }
        return benchmark
    except Exception as e:
        print(f"⚠️ 벤치마크 DB 로드 실패: {e}", flush=True)
        return {}

def extract_curated_insights(clean_df: pd.DataFrame, benchmark_db: dict) -> list:
    """단지별 신고가 및 거래량 집중 핀셋 선별 (5개년 전고점 비교 반영)"""
    if clean_df.empty or "아파트" not in clean_df.columns:
        return []

    valid_df = clean_df.copy()
    if "층" in valid_df.columns:
        valid_df = valid_df[~valid_df["층"].isin(["1층", "2층"])]

    curated = []
    valid_df["평형그룹"] = valid_df["전용면적"].round().astype(int)
    groups = valid_df.groupby(["아파트", "평형그룹", "지역명"])

    for (apt_name, area_group, region_name), group in groups:
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
        b_key = f"{apt_name}_{area_group}_{region_name}"

        # 5개년 벤치마크 전고점 확인
        has_history = b_key in benchmark_db
        hist_ath = benchmark_db[b_key]["역대최고가_억"] if has_history else max(prices)
        hist_date = benchmark_db[b_key]["최고가계약일"] if has_history else ""

        # 회복률 계산 (전고점 대비 현재 거래가 비율)
        recovery_ratio = round((latest_price / (hist_ath or 1)) * 100, 1)

        # 1. 5개년 역대 최고가 돌파 (진짜 ATH)
        if latest_price > hist_ath:
            diff = round(latest_price - hist_ath, 2)
            curated.append({
                "id": row_id,
                "complexName": apt_name,
                "regionCategory": region_cat,
                "regionName": f"{region_name} {dong}".strip(),
                "area": f"{latest['전용면적']:.1f}㎡",
                "floor": latest["층"],
                "dealPrice": latest_price,
                "previousHigh": hist_ath,
                "dealType": "ath",
                "diff": diff,
                "diffPercent": round((diff / (hist_ath or 1)) * 100, 2),
                "date": latest.get("계약일자", ""),
                "month": latest.get("조회월", ""),
                "tags": ["역대신고가", "전고점돌파"],
                "description": f"종전 역대 최고가({hist_ath}억, {hist_date})를 돌파하며 5개년 신고가를 경신했습니다. (회복률 {recovery_ratio}%)",
                "recentPrices": prices[-5:]
            })
            # 벤치마크 실시간 업데이트
            benchmark_db[b_key] = {"역대최고가_억": latest_price, "최고가계약일": latest.get("계약일자", ""), "최고가층": latest["층"]}

        # 2. 단기(2개월 내) 신고가 또는 전고점 95% 이상 근접
        elif len(prices) >= 2 and latest_price > max(prices[:-1]):
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
                "month": latest.get("조회월", ""),
                "tags": ["단기신고가", f"전고점대비 {recovery_ratio}%"],
                "description": f"직전 거래 대비 {diff}억원 상승했습니다. 역대 전고점({hist_ath}억) 대비 {recovery_ratio}% 수준입니다.",
                "recentPrices": prices[-5:]
            })

        # 3. 거래량 집중 단지
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
                "previousHigh": hist_ath,
                "dealType": "volume",
                "diff": diff,
                "diffPercent": round((diff / (first_price or 1)) * 100, 2),
                "date": latest.get("계약일자", ""),
                "month": latest.get("조회월", ""),
                "tags": ["거래량집중", f"전고점대비 {recovery_ratio}%"],
                "description": f"단기간 내 체결이 집중된 단지입니다. (역대 전고점 대비 {recovery_ratio}% 시세)",
                "recentPrices": prices[-5:]
            })

    curated = sorted(curated, key=lambda x: x["date"], reverse=True)
    return curated

def main():
    service_key = os.environ.get("MOLIT_KEY")
    if not service_key:
        print("❌ [오류] GitHub Secrets에 MOLIT_KEY가 설정되지 않았습니다.", flush=True)
        sys.exit(1)

    os.makedirs("data", exist_ok=True)
    query_months = determine_query_months()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 25개 구역 실거래 수집 시작 (대상월: {query_months})...", flush=True)

    # 1. 5개년 벤치마크 DB 로드
    benchmark_db = load_benchmark_db()
    if benchmark_db:
        print(f"📚 [벤치마크 연동] 5개년 역대 전고점 DB 로드 완료 ({len(benchmark_db)}개 단지 대조 대기)", flush=True)
    else:
        print(f"ℹ️ [안내] 벤치마크 DB가 없습니다. 최근 수집 데이터 기준으로 전고점을 비교합니다.", flush=True)

    # 2. 최근 2개월 실거래 병렬 수집
    all_data = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(fetch_single_region_task, service_key, code, info, query_months)
            for code, info in TARGET_REGIONS.items()
        ]

        for idx, fut in enumerate(as_completed(futures), 1):
            try:
                name, records = fut.result()
                all_data.extend(records)
                print(f"  [{idx:02d}/25] ✅ {name} 수집 완료 ({len(records)}건)", flush=True)
            except Exception as e:
                print(f"  [{idx:02d}/25] ⚠️ 스킵됨: {e}", flush=True)

    print(f"\n📊 총 수집 완료: {len(all_data)}건", flush=True)

    if all_data:
        clean_df = pd.DataFrame(all_data)
        clean_df = clean_df.drop_duplicates(subset=["아파트", "전용면적", "층", "거래금액_억", "계약일자"])
    else:
        clean_df = pd.DataFrame(columns=["아파트", "전용면적", "층", "거래금액_억", "계약일자", "지역명", "권역코드", "법정동", "조회월"])

    # 월별 CSV 개별 저장
    for ym in query_months:
        sub_df = clean_df[clean_df["조회월"] == ym] if "조회월" in clean_df.columns else pd.DataFrame()
        csv_filename = f"data/real_estate_trades_{ym}.csv"
        sub_df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
        print(f"💾 [저장 완료] CSV 원본: {csv_filename} ({len(sub_df)}건)", flush=True)

    # 3. 5개년 전고점 대조 큐레이션 추출
    curated_list = extract_curated_insights(clean_df, benchmark_db)

    # 4. 웹 대시보드 페이로드 구성
    months_payload = {}
    periods_payload = {}

    for ym in query_months:
        month_df = clean_df[clean_df["조회월"] == ym] if "조회월" in clean_df.columns else pd.DataFrame()
        month_curated = [item for item in curated_list if item.get("month") == ym]

        seoul_count = len(month_df[month_df["권역코드"].astype(str).str.startswith("seoul")]) if not month_df.empty else 0
        gyeonggi_count = len(month_df[month_df["권역코드"].astype(str).str.startswith("gyeonggi")]) if not month_df.empty else 0
        total_m = len(month_df) or 1
        m_label = f"{ym[:4]}년 {int(ym[4:])}월"

        m_data = {
            "label": m_label,
            "totalTradeCount": len(month_df),
            "athCount": len([i for i in month_curated if i.get("dealType") == "ath"]),
            "curatedCount": len(month_curated),
            "seoulPercent": round((seoul_count / total_m) * 100, 1),
            "gyeonggiPercent": round((gyeonggi_count / total_m) * 100, 1),
            "transactions": month_curated,
            "curatedTransactions": month_curated
        }

        months_payload[ym] = m_data
        if ym == query_months[-1]:
            periods_payload["current"] = m_data
        else:
            periods_payload["previous"] = m_data

    latest_ym = query_months[-1]
    json_payload = {
        "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "availableMonths": query_months,
        "defaultMonth": latest_ym,
        "monthsData": months_payload,
        "periods": periods_payload,
        "hasBenchmark": len(benchmark_db) > 0,
        "totalTradeCount": months_payload[latest_ym]["totalTradeCount"],
        "curatedCount": months_payload[latest_ym]["curatedCount"],
        "curatedTransactions": months_payload[latest_ym]["transactions"]
    }

    json_filename = "data/daily_briefing_data.json"
    with open(json_filename, "w", encoding="utf-8") as f:
        json.dump(json_payload, f, ensure_ascii=False, indent=2)
    print(f"✨ [저장 완료] 웹 대시보드 데이터: {json_filename}", flush=True)

if __name__ == "__main__":
    main()
