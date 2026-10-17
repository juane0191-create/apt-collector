"""
수도권 25개 구역 실거래가 수집 및 2020~2026 전고점 대조 엔진
- 개선사항:
  1. 2020~2022 연도 강제 삭제 필터 원천 제거 (2020~2026 전고점 100% 정상 로드)
  2. 25개 자치구 핵심 시그니처 정규화 (동 이름 유무 무관 100% 매칭)
  3. 버림/반올림 이중 매칭(Dual Fallback) 지원
"""

import os
import re
import sys
import json
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests

TARGET_REGIONS = {
    # [서울 강남·동남권]
    "11680": {"name": "서울 강남구", "core": "강남구", "category": "seoul-southeast"},
    "11650": {"name": "서울 서초구", "core": "서초구", "category": "seoul-southeast"},
    "11710": {"name": "서울 송파구", "core": "송파구", "category": "seoul-southeast"},
    "11740": {"name": "서울 강동구", "core": "강동구", "category": "seoul-southeast"},

    # [서울 마용성 & 한강변]
    "11170": {"name": "서울 용산구", "core": "용산구", "category": "seoul-river"},
    "11200": {"name": "서울 성동구", "core": "성동구", "category": "seoul-river"},
    "11440": {"name": "서울 마포구", "core": "마포구", "category": "seoul-river"},
    "11215": {"name": "서울 광진구", "core": "광진구", "category": "seoul-river"},

    # [서울 서남권]
    "11560": {"name": "서울 영등포구", "core": "영등포구", "category": "seoul-southwest"},
    "11590": {"name": "서울 동작구", "core": "동작구", "category": "seoul-southwest"},
    "11500": {"name": "서울 강서구", "core": "강서구", "category": "seoul-southwest"},
    "11620": {"name": "서울 관악구", "core": "관악구", "category": "seoul-southwest"},
    "11530": {"name": "서울 구로구", "core": "구로구", "category": "seoul-southwest"},
    "11540": {"name": "서울 금천구", "core": "금천구", "category": "seoul-southwest"},

    # [서울 동북권]
    "11350": {"name": "서울 노원구", "core": "노원구", "category": "seoul-northeast"},
    "11320": {"name": "서울 도봉구", "core": "도봉구", "category": "seoul-northeast"},
    "11305": {"name": "서울 강북구", "core": "강북구", "category": "seoul-northeast"},
    "11290": {"name": "서울 성북구", "core": "성북구", "category": "seoul-northeast"},

    # [경기 경부축 & 남부 대장]
    "41135": {"name": "성남시 분당구", "core": "분당구", "category": "gyeonggi-south"},
    "41465": {"name": "용인시 수지구", "core": "수지구", "category": "gyeonggi-south"},
    "41590": {"name": "화성시", "core": "화성시", "category": "gyeonggi-south"},
    "41117": {"name": "수원시 영통구", "core": "영통구", "category": "gyeonggi-south"},

    # [경기 서울인접 핵심지]
    "41290": {"name": "과천시", "core": "과천시", "category": "gyeonggi-close"},
    "41210": {"name": "광명시", "core": "광명시", "category": "gyeonggi-close"},
    "41450": {"name": "하남시", "core": "하남시", "category": "gyeonggi-close"},
}

API_ENDPOINT = "http://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
BENCHMARK_FILE = "data/history_ath_benchmark.csv"
OUTPUT_JSON = "data/daily_briefing_data.json"

def normalize_apt_name(name: str) -> str:
    """단지명 띄어쓰기 및 특수문자 제거 후 소문자 변환"""
    if not name:
        return ""
    return re.sub(r'[\s\(\)\-\_\,\.]', '', name).lower()

def extract_region_core(region_str: str) -> str:
    """지역명에서 25개 핵심 자치구 식별자 추출 ('서울 강남구 대치동' -> '강남구')"""
    if not region_str:
        return ""
    for info in TARGET_REGIONS.values():
        if info["core"] in region_str:
            return info["core"]
    return re.sub(r'[\s\(\)\-\_\,\.]', '', region_str)

def get_integer_area(area) -> int:
    """전용면적 소수점 버림 처리"""
    try:
        return int(float(area))
    except (ValueError, TypeError):
        return 84

def make_key(apt_name: str, area, region_name: str) -> str:
    """완벽히 정규화된 고유 키 생성"""
    norm_apt = normalize_apt_name(apt_name)
    int_area = get_integer_area(area)
    core_region = extract_region_core(region_name)
    return f"{norm_apt}_{int_area}_{core_region}"

def load_benchmark_map() -> dict:
    """2020~2026 장기 벤치마크 맵 로드 (연도 제한 없이 100% 온전하게 로드)"""
    bench_map = {}
    if not os.path.exists(BENCHMARK_FILE):
        return bench_map

    try:
        df = pd.read_csv(BENCHMARK_FILE)
        for _, row in df.iterrows():
            apt = str(row.get("아파트", ""))
            area = row.get("전용면적", 84)
            region = str(row.get("지역명", ""))
            price = float(row.get("역대최고가_억", 0))
            deal_date = str(row.get("최고가계약일", ""))
            floor = str(row.get("최고가층", "중층"))

            key = make_key(apt, area, region)
            # 최고가 기준 등록
            if key not in bench_map or price > bench_map[key]["price"]:
                bench_map[key] = {
                    "price": price,
                    "date": deal_date,
                    "floor": floor
                }

            # 반올림 면적 키도 Fallback으로 함께 등록 (구버전 호환)
            try:
                round_key = f"{normalize_apt_name(apt)}_{round(float(area))}_{extract_region_core(region)}"
                if round_key not in bench_map or price > bench_map[round_key]["price"]:
                    bench_map[round_key] = {
                        "price": price,
                        "date": deal_date,
                        "floor": floor
                    }
            except Exception:
                pass

    except Exception as e:
        print(f"⚠️ 벤치마크 로드 중 주의: {e}", flush=True)

    return bench_map

def fetch_region_xml(service_key: str, code: str, ym: str, region_info: dict) -> list:
    """단일 구역/단일 월 국토부 API 수집"""
    decoded_key = urllib.parse.unquote(service_key)
    params = {
        "serviceKey": decoded_key,
        "pageNo": "1",
        "numOfRows": "2000",
        "LAWD_CD": code,
        "DEAL_YMD": ym
    }
    records = []
    try:
        resp = requests.get(API_ENDPOINT, params=params, timeout=8)
        if resp.status_code != 200:
            return records

        root = ET.fromstring(resp.content)
        for item in root.findall(".//item"):
            def val(tag):
                node = item.find(tag)
                return node.text.strip() if (node is not None and node.text) else ""

            # 해제거래 및 직거래 원천 차단
            if val("cdealDay") or val("cdealType") == "O":
                continue
            if "직거래" in val("dealingGbn"):
                continue

            price_raw = val("dealAmount").replace(",", "").strip()
            if not price_raw:
                continue

            records.append({
                "aptName": val("aptNm"),
                "area": float(val("excluUseAr")) if val("excluUseAr") else 84.0,
                "floor": f"{val('floor')}층" if val('floor') else "중층",
                "price": round(float(price_raw) / 10000.0, 2),
                "tradeDate": f"{val('dealYear')}-{str(val('dealMonth')).zfill(2)}-{str(val('dealDay')).zfill(2)}",
                "regionName": region_info["name"],
                "category": region_info["category"]
            })
    except Exception:
        pass
    return records

def run_pipeline():
    service_key = os.environ.get("MOLIT_KEY")
    if not service_key:
        print("❌ [오류] MOLIT_KEY 환경변수가 설정되지 않았습니다.", flush=True)
        sys.exit(1)

    os.makedirs("data", exist_ok=True)
    bench_map = load_benchmark_map()
    print(f"📦 [벤치마크 로드 완료] 총 {len(bench_map)}개 단지 전고점 기준표 등록 완료", flush=True)

    now = datetime.now()
    prev_month = now.replace(day=1) - timedelta(days=1)
    target_months = [now.strftime("%Y%m"), prev_month.strftime("%Y%m")]

    output_payload = {
        "lastUpdated": now.strftime("%Y-%m-%d %H:%M:%S"),
        "availableMonths": target_months,
        "monthsData": {}
    }

    with ThreadPoolExecutor(max_workers=4) as executor:
        for ym in target_months:
            futures = [
                executor.submit(fetch_region_xml, service_key, code, ym, info)
                for code, info in TARGET_REGIONS.items()
            ]

            month_records = []
            for f in as_completed(futures):
                recs = f.result()
                if recs:
                    month_records.extend(recs)

            seoul_count = sum(1 for r in month_records if "서울" in r["regionName"])
            gyeonggi_count = len(month_records) - seoul_count
            total_count = len(month_records)

            seoul_pct = round((seoul_count / total_count * 100), 1) if total_count > 0 else 50
            gyeonggi_pct = round((gyeonggi_count / total_count * 100), 1) if total_count > 0 else 50

            curated_list = []
            matched_count = 0

            for r in month_records:
                key = make_key(r["aptName"], r["area"], r["regionName"])
                bench = bench_map.get(key)

                # Fallback: 반올림 키 조회
                if not bench:
                    round_key = f"{normalize_apt_name(r['aptName'])}_{round(float(r['area']))}_{extract_region_core(r['regionName'])}"
                    bench = bench_map.get(round_key, {})

                ath_price = bench.get("price", 0)
                ath_date = bench.get("date", "2020~2026")

                if ath_price > 0:
                    matched_count += 1
                    recovery_ratio = round((r["price"] / ath_price * 100), 1)
                else:
                    recovery_ratio = 0

                curated_list.append({
                    "aptName": r["aptName"],
                    "area": r["area"],
                    "intArea": get_integer_area(r["area"]),
                    "floor": r["floor"],
                    "price": r["price"],
                    "tradeDate": r["tradeDate"],
                    "regionName": r["regionName"],
                    "category": r["category"],
                    "previousHigh": ath_price,
                    "previousHighDate": ath_date,
                    "recoveryRatio": recovery_ratio
                })

            # 실거래 가격 높은 순 정렬
            curated_list.sort(key=lambda x: x["price"], reverse=True)

            output_payload["monthsData"][ym] = {
                "totalTradeCount": total_count,
                "seoulPercent": seoul_pct,
                "gyeonggiPercent": gyeonggi_pct,
                "curatedTransactions": curated_list
            }

            print(f"✅ [{ym}] 수집 완료: 총 {total_count}건 체결 중 {matched_count}건 전고점 매칭 성공!", flush=True)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, ensure_ascii=False, indent=2)

    print(f"\n🎉 [최종 저장 완료] {OUTPUT_JSON} 동기화가 성공적으로 끝났습니다.", flush=True)

if __name__ == "__main__":
    run_pipeline()
