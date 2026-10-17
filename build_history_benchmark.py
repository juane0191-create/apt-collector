"""
수도권 25개 구역 아파트 실거래가 장기 벤치마크 데이터베이스 구축기
- 변경사항:
  1. 단지명 띄어쓰기/특수문자 제거 정규화 (normalize_apt_name)
  2. 전용면적 소수점 버림 처리 (get_integer_area)로 키값 통일
  3. 실시간 로깅(flush=True) 및 타임아웃 6초 적용
"""

import os
import re
import sys
import time
import argparse
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests

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

def normalize_apt_name(name: str) -> str:
    """단지명 띄어쓰기 및 특수문자 제거 후 소문자 변환"""
    if not name:
        return ""
    return re.sub(r'[\s\(\)\-\_\,\.]', '', name).lower()

def get_integer_area(area) -> int:
    """전용면적 소수점 버림 처리"""
    try:
        val = float(area)
        return int(val)
    except (ValueError, TypeError):
        return 84

def make_key(apt_name: str, area, region_name: str) -> str:
    """정규화된 고유 비교 키 생성"""
    norm_apt = normalize_apt_name(apt_name)
    int_area = get_integer_area(area)
    norm_region = normalize_apt_name(region_name)
    return f"{norm_apt}_{int_area}_{norm_region}"

def fetch_region_month(service_key: str, code: str, ym: str, region_name: str, category: str) -> list:
    """단일 구역 단일 월 XML 조회 (타임아웃 6초)"""
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
        resp = requests.get(API_ENDPOINT, params=params, timeout=6)
        if resp.status_code != 200:
            return records

        root = ET.fromstring(resp.content)
        for item in root.findall(".//item"):
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
                "지역명": region_name,
                "권역코드": category
            })
    except Exception:
        pass
    return records

def generate_target_months(start_ym: str, end_ym: str) -> list:
    """시작월부터 종료월까지 월별 리스트 생성"""
    start_y, start_m = int(start_ym[:4]), int(start_ym[4:])
    end_y, end_m = int(end_ym[:4]), int(end_ym[4:])
    months = []

    y, m = start_y, start_m
    while (y < end_y) or (y == end_y and m <= end_m):
        months.append(f"{y:04d}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months

def save_benchmark_to_csv(benchmark_dict: dict):
    """벤치마크 데이터를 파일로 즉시 저장"""
    if not benchmark_dict:
        return
    res_df = pd.DataFrame(list(benchmark_dict.values()))
    res_df = res_df.sort_values(by="역대최고가_억", ascending=False)
    res_df.to_csv(BENCHMARK_FILE, index=False, encoding="utf-8-sig")

def build_benchmark(start_ym="202001", end_ym="202112"):
    service_key = os.environ.get("MOLIT_KEY")
    if not service_key:
        print("❌ [오류] MOLIT_KEY 환경변수가 설정되지 않았습니다.", flush=True)
        sys.exit(1)

    os.makedirs("data", exist_ok=True)
    months = generate_target_months(start_ym, end_ym)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 수집 시작: {start_ym} ~ {end_ym} (총 {len(months)}개월)", flush=True)

    existing_benchmark = {}
    if os.path.exists(BENCHMARK_FILE):
        try:
            prev_df = pd.read_csv(BENCHMARK_FILE)
            for _, r in prev_df.iterrows():
                apt_str = str(r["아파트"])
                area_val = r["전용면적"]
                region_str = str(r["지역명"])

                key = make_key(apt_str, area_val, region_str)
                existing_benchmark[key] = {
                    "아파트": apt_str,
                    "전용면적": float(area_val),
                    "평형그룹": get_integer_area(area_val),
                    "지역명": region_str,
                    "권역코드": r.get("권역코드", "all"),
                    "역대최고가_억": float(r["역대최고가_억"]),
                    "최고가계약일": str(r["최고가계약일"]),
                    "최고가층": str(r.get("최고가층", "중층")),
                    "누적거래건수": int(r.get("누적거래건수", 1))
                }
            print(f"📦 [기존 벤치마크 로드] 기존 {len(existing_benchmark)}개 단지 전고점 데이터 병합 준비 완료.", flush=True)
        except Exception as e:
            print(f"⚠️ 기존 파일 파싱 주의: {e}", flush=True)

    with ThreadPoolExecutor(max_workers=4) as executor:
        for ym_idx, ym in enumerate(months, 1):
            print(f"⏳ [{ym_idx:02d}/{len(months):02d}] {ym} 데이터 수집 중...", end="", flush=True)

            futures = [
                executor.submit(fetch_region_month, service_key, code, ym, info["name"], info["category"])
                for code, info in TARGET_REGIONS.items()
            ]

            month_records = []
            for f in as_completed(futures):
                try:
                    recs = f.result()
                    if recs:
                        month_records.extend(recs)
                except Exception:
                    pass

            updated_count = 0
            new_count = 0
            if month_records:
                for row in month_records:
                    key = make_key(row["아파트"], row["전용면적"], row["지역명"])
                    deal_price = row["거래금액_억"]
                    deal_date = row["계약일자"]
                    floor = row["층"]
                    int_area = get_integer_area(row["전용면적"])

                    if key not in existing_benchmark:
                        new_count += 1
                        existing_benchmark[key] = {
                            "아파트": row["아파트"],
                            "전용면적": row["전용면적"],
                            "평형그룹": int_area,
                            "지역명": row["지역명"],
                            "권역코드": row["권역코드"],
                            "역대최고가_억": deal_price,
                            "최고가계약일": deal_date,
                            "최고가층": floor,
                            "누적거래건수": 1
                        }
                    else:
                        existing_benchmark[key]["누적거래건수"] += 1
                        if deal_price > existing_benchmark[key]["역대최고가_억"]:
                            existing_benchmark[key]["역대최고가_억"] = deal_price
                            existing_benchmark[key]["최고가계약일"] = deal_date
                            existing_benchmark[key]["최고가층"] = floor
                            updated_count += 1

            print(f" 완료! (체결 {len(month_records)}건 | 신규 {new_count}개 | 최고가갱신 {updated_count}건)", flush=True)

            if ym_idx % 4 == 0:
                save_benchmark_to_csv(existing_benchmark)

            time.sleep(0.3)

    save_benchmark_to_csv(existing_benchmark)
    print(f"\n🎉 [완료] 총 {len(existing_benchmark)}개 단지 전고점 벤치마크 병합 완료: {BENCHMARK_FILE}", flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="실거래가 벤치마크 수집기")
    parser.add_argument("--start", default="202001", help="시작 년월 (예: 202001)")
    parser.add_argument("--end", default="202112", help="종료 년월 (예: 202112)")
    args = parser.parse_args()

    build_benchmark(start_ym=args.start, end_ym=args.end)
