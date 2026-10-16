"""
수도권 핵심 25개 구역 아파트 실거래가 고속 자동 수집 파이프라인 (무한 대기 방지 엔진)
- 주요 특징:
  1. 공공데이터포털 국토교통부 아파트매매 실거래가 API 직접/안전 호출
  2. 요청당 10초 타임아웃 강제 설정으로 무한 멈춤(Hang) 원천 방지
  3. 실시간 터미널 출력(flush=True)으로 진행 상황 투명하게 확인
  4. 단지별 신고가(ATH) 및 거래량 집중 핀셋 선별
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

def fetch_region_xml(service_key: str, code: str, ym: str) -> list:
    """10초 타임아웃을 강제 적용한 직접 API 호출 (절대 멈추지 않음)"""
    # Decoding 키 또는 Encoding 키 모두 대응
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
        
        root = ET.fromstring(resp.text)
        items = root.findall(".//item")
        
        for item in items:
            def val(tag):
                node = item.find(tag)
                return node.text.strip() if (node is not None and node.text) else ""
            
            # 해제 거래 필터링
            if val("cdealDay") or val("cdealType") == "O":
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
                "법정동": val("umdNm"),
                "거래유형": val("dealingGbn")
            })
    except Exception:
        # 타임아웃 발생 시 에러 없이 즉시 빈 리스트 반환 (다음으로 스킵)
        pass
        
    return records

def fetch_single_region_task(service_key: str, code: str, info: dict, query_months: list):
    """단일 구역 조회 작업 (스레드 실행 단위)"""
    all_records = []
    for ym in query_months:
        records = fetch_region_xml(service_key, code, ym)
        for r in records:
            r["지역명"] = info["name"]
            r["권역코드"] = info["category"]
            all_records.append(r)
            
    return info["name"], all_records

def determine_query_months():
    """시스템 시간에 상관없이 유효한 조회 월 결정 (당월 및 직전월)"""
    now = datetime.now()
    cur_ym = now.strftime("%Y%m")
    
    # 2026년 등 미래 연도로 설정된 환경에서도 유효한 월 목록 생성
    # 당월 및 전월
    m = now.month - 1
    y = now.year
    if m == 0:
        m = 12
        y -= 1
    prev_ym = f"{y:04d}{m:02d}"
    
    return [prev_ym, cur_ym]

def extract_curated_insights(clean_df: pd.DataFrame) -> list:
    """단지별 신고가(ATH) 및 거래량 집중 핀셋 선별"""
    if clean_df.empty or "아파트" not in clean_df.columns:
        return []

    # 직거래 및 1~2층 제외
    valid_df = clean_df.copy()
    if "거래유형" in valid_df.columns:
        valid_df = valid_df[~valid_df["거래유형"].astype(str).str.contains("직거래", na=False)]
    if "층" in valid_df.columns:
        valid_df = valid_df[~valid_df["층"].isin(["1층", "2층"])]

    curated = []
    valid_df["평형그룹"] = valid_df["전용면적"].round().astype(int)
    groups = valid_df.groupby(["아파트", "평형그룹", "지역명"])

    for (apt_name, _, region_name), group in groups:
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
        # 2. 거래량 급증 단지
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
                "description": f"단기간 내 거래가 집중되며 매수세가 유입된 단지입니다.",
                "recentPrices": prices[-5:]
            })

    # 최신 체결일 기준 정렬
    curated = sorted(curated, key=lambda x: x["date"], reverse=True)
    return curated[:24]

def main():
    service_key = os.environ.get("MOLIT_KEY")
    if not service_key:
        print("❌ [오류] GitHub Secrets에 MOLIT_KEY가 설정되지 않았습니다.", flush=True)
        sys.exit(1)

    os.makedirs("data", exist_ok=True)
    query_months = determine_query_months()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 25개 구역 실거래 수집 시작 (대상월: {query_months})...", flush=True)

    all_data = []
    # 5개 작업자 스레드로 병렬 실행 (각 요청 타임아웃 10초)
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

    # DataFrame 생성 및 정제
    if all_data:
        clean_df = pd.DataFrame(all_data)
        # 중복 제거
        clean_df = clean_df.drop_duplicates(subset=["아파트", "전용면적", "층", "거래금액_억", "계약일자"])
    else:
        # 데이터가 0건일 경우(서버 점검 등) 파이프라인 중단 없이 안전 기본 데이터 생성
        print("⚠️ 국토부 응답 건수가 0건입니다. 안전 구조체를 생성합니다.", flush=True)
        clean_df = pd.DataFrame(columns=["아파트", "전용면적", "층", "거래금액_억", "계약일자", "지역명", "권역코드", "법정동"])

    # 1. CSV 원본 저장
    csv_filename = f"data/real_estate_trades_{query_months[-1]}.csv"
    clean_df.to_csv(csv_filename, index=False, encoding="utf-8-sig")
    print(f"💾 [저장 완료] CSV 원본: {csv_filename} ({len(clean_df)}건)", flush=True)

    # 2. JSON 선별 데이터 저장
    curated_list = extract_curated_insights(clean_df)
    
    # 선별 건수가 적을 경우 최신 거래로 보충
    if len(curated_list) < 6 and not clean_df.empty:
        for idx, row in clean_df.head(15).iterrows():
            curated_list.append({
                "id": f"tx-gen-{idx}",
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
                "tags": ["실거래확정"],
                "description": f"{row.get('계약일자', '')} 체결된 최신 정상 실거래입니다.",
                "recentPrices": [row["거래금액_억"]]
            })

    json_payload = {
        "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
