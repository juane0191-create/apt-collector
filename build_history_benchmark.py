"""
2022년~2026년 5개년 수도권 25개 구역 아파트 실거래가 장기 벤치마크 데이터베이스 구축기
- 역할:
  1. 2022.01부터 현재까지의 25개 구역 전체 실거래 데이터를 안전하게 순회 수집
  2. 수십만 건의 대용량 데이터를 그대로 보관하지 않고, 
     [단지명 + 평형(전용면적) + 권역]별 '역대 최고가(ATH)', '달성일', '최고가 층수'만 핀셋 요약
  3. 경량화된 초고속 기준표 'data/history_ath_benchmark.csv' 파일로 저장
  4. 일일 트래픽(1000회) 초과 방지를 위한 분할 수집(체크포인트) 지원
"""

import os
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

def fetch_region_month(service_key: str, code: str, ym: str) -> list:
    """단일 구역 단일 월 국토부 XML 안전 조회"""
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
        resp = requests.get(API_ENDPOINT, params=params, timeout=12)
        if resp.status_code != 200:
            return records
        
        root = ET.fromstring(resp.content)
        items = root.findall(".//item")
        for item in items:
            def val(tag):
                node = item.find(tag)
                return node.text.strip() if (node is not None and node.text) else ""

            # 해제 거래 및 직거래는 대표 시세 왜곡 방지를 위해 제외
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
                "법정동": val("umdNm")
            })
    except Exception:
        pass
    return records

def generate_target_months(start_ym: str, end_ym: str) -> list:
    """시작월(YYYYMM)부터 종료월까지 월별 리스트 생성"""
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

def build_benchmark(start_ym="202201", end_ym="202610"):
    service_key = os.environ.get("MOLIT_KEY")
    if not service_key:
        print("❌ [오류] MOLIT_KEY 환경변수가 설정되지 않았습니다.", flush=True)
        sys.exit(1)

    os.makedirs("data", exist_ok=True)
    months = generate_target_months(start_ym, end_ym)
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 장기 벤치마크 수집 시작: {start_ym} ~ {end_ym} (총 {len(months)}개월 x 25개 구역 = {len(months)*25}회 요청)", flush=True)

    # 기존에 구축된 벤치마크가 있다면 로드 후 병합 준비
    existing_benchmark = {}
    if os.path.exists(BENCHMARK_FILE):
        try:
            prev_df = pd.read_csv(BENCHMARK_FILE)
            for _, r in prev_df.iterrows():
                key = f"{r['아파트']}_{int(round(r['전용면적']))}_{r['지역명']}"
                existing_benchmark[key] = {
                    "아파트": r["아파트"],
                    "전용면적": float(r["전용면적"]),
                    "평형그룹": int(round(r["전용면적"])),
                    "지역명": r["지역명"],
                    "권역코드": r.get("권역코드", "all"),
                    "역대최고가_억": float(r["역대최고가_억"]),
                    "최고가계약일": str(r["최고가계약일"]),
                    "최고가층": str(r.get("최고가층", "중층")),
                    "누적거래건수": int(r.get("누적거래건수", 1))
                }
            print(f"📦 [기존 벤치마크 로드] 기존 {len(existing_benchmark)}개 단지 전고점 데이터와 병합합니다.", flush=True)
        except Exception as e:
            print(f"⚠️ 기존 파일 로드 실패, 새로 생성합니다: {e}", flush=True)

    # 월별 순회 수집 (서버 과부하 및 차단 방지를 위해 max_workers=4)
    total_calls = 0
    with ThreadPoolExecutor(max_workers=4) as executor:
        for ym_idx, ym in enumerate(months, 1):
            tasks = []
            for code, info in TARGET_REGIONS.items():
                tasks.append((executor.submit(fetch_region_month, service_key, code, ym), info))
                total_calls += 1

            month_records = []
            for future, info in tasks:
                try:
                    recs = future.result()
                    for r in recs:
                        r["지역명"] = info["name"]
                        r["권역코드"] = info["category"]
                        month_records.append(r)
                except Exception:
                    pass

            # 실시간으로 최고가 갱신
            if month_records:
                m_df = pd.DataFrame(month_records)
                m_df["평형그룹"] = m_df["전용면적"].round().astype(int)

                for _, row in m_df.iterrows():
                    key = f"{row['아파트']}_{row['평형그룹']}_{row['지역명']}"
                    deal_price = row["거래금액_억"]
                    deal_date = row["계약일자"]
                    floor = row["층"]

                    if key not in existing_benchmark:
                        existing_benchmark[key] = {
                            "아파트": row["아파트"],
                            "전용면적": row["전용면적"],
                            "평형그룹": row["평형그룹"],
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

            print(f"  [{ym_idx:02d}/{len(months):02d}] 📅 {ym} 수집 완료 ({len(month_records)}건 체결, 누적 벤치마크 {len(existing_benchmark)}개 단지)", flush=True)
            time.sleep(0.5)  # 공공데이터포털 트래픽 보호용

    # 최종 벤치마크 파일 저장
    if existing_benchmark:
        result_df = pd.DataFrame(list(existing_benchmark.values()))
        result_df = result_df.sort_values(by="역대최고가_억", ascending=False)
        result_df.to_csv(BENCHMARK_FILE, index=False, encoding="utf-8-sig")
        print(f"\n🎉 [벤치마크 DB 완성] 총 {len(result_df)}개 단지·평형별 역대 최고가 기준표 저장 완료: {BENCHMARK_FILE}", flush=True)
        print(f"   (예: 송파 잠실엘스 84㎡ 역대 최고가, 분당 파크뷰 84㎡ 역대 최고가 등)", flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="5개년 실거래가 벤치마크 수집기")
    parser.add_argument("--start", default="202201", help="시작 년월 (예: 202201)")
    parser.add_argument("--end", default="202610", help="종료 년월 (예: 202610)")
    args = parser.parse_args()

    build_benchmark(start_ym=args.start, end_ym=args.end)
