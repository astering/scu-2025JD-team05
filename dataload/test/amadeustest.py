# -*- coding: utf-8 -*-
import requests
from datetime import date, timedelta

BASE_URL = "https://test.api.amadeus.com"
API_KEY = "HbS6Xv3LTuySlybVUw0A7oxLxoDaMjn8"
API_SECRET = "Dms0KwztqxmGxc3E"

# ✅ 常见“城市码/你项目码” -> Amadeus更容易接受的机场码
CODE_MAP = {
    "BJS": "PEK",  # 北京：先用首都（也可试 PKX）
    "SHA": "PVG",  # 上海：很多情况下用 PVG 更稳（也可试 SHA）
}

def get_token():
    url = f"{BASE_URL}/v1/security/oauth2/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": API_KEY,
        "client_secret": API_SECRET,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    r = requests.post(url, data=data, headers=headers, timeout=15)
    print("[token] status =", r.status_code)
    r.raise_for_status()
    j = r.json()
    print("[token] keys =", list(j.keys()))
    return j["access_token"]

def _print_error_response(r):
    print("[offers] status =", r.status_code)
    print("[offers] final_url =", r.url)
    ct = (r.headers.get("Content-Type") or "").lower()

    # 优先打印 JSON errors
    try:
        j = r.json()
        print("[offers] error json =", j)
    except Exception:
        # 非 JSON 就截断打印
        txt = r.text or ""
        print("[offers] error text =", txt[:1500])

def search_offers(token, origin="BJS", dest="SHA", depart_date=None, adults=1, max_results=10, auto_map=True):
    url = f"{BASE_URL}/v2/shopping/flight-offers"

    if depart_date is None:
        # ✅ 默认用未来 30 天内，避免超远日期导致的问题
        depart_date = (date.today() + timedelta(days=30)).strftime("%Y-%m-%d")

    if auto_map:
        origin = CODE_MAP.get(origin, origin)
        dest = CODE_MAP.get(dest, dest)

    params = {
        "originLocationCode": origin,
        "destinationLocationCode": dest,
        "departureDate": depart_date,
        "adults": int(adults),
        "currencyCode": "CNY",
        "max": int(max_results),
    }
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(url, params=params, headers=headers, timeout=20)

    if r.status_code >= 400:
        _print_error_response(r)
        # 这里不 raise，让你能看到错误详情后再决定怎么处理
        raise RuntimeError("Amadeus offers request failed, see error json/text above.")

    print("[offers] status =", r.status_code)
    print("[offers] final_url =", r.url)
    return r.json()

def pretty_print_first_offers(resp_json, limit=3):
    data = resp_json.get("data") or []
    print("[offers] count =", len(data))

    for i, offer in enumerate(data[:limit], start=1):
        price = offer.get("price", {}) or {}
        total = price.get("total")
        currency = price.get("currency")

        itineraries = offer.get("itineraries") or []
        traveler_pricings = offer.get("travelerPricings") or []

        print(f"\n--- OFFER {i} --- total={total} {currency} itineraries={len(itineraries)} travelerPricings={len(traveler_pricings)}")

        # segments 基本信息（带 segment id）
        seg_id_map = {}
        for it_idx, it in enumerate(itineraries):
            segs = it.get("segments") or []
            dur = it.get("duration")
            print(f"  itinerary[{it_idx}] duration={dur} segments={len(segs)}")
            for s_idx, s in enumerate(segs):
                seg_id = s.get("id")
                carrier = s.get("carrierCode", "")
                num = s.get("number", "")
                flight_no = f"{carrier}{num}" if carrier and num else ""

                dep = (s.get("departure") or {}).get("iataCode", "")
                arr = (s.get("arrival") or {}).get("iataCode", "")
                dep_at = (s.get("departure") or {}).get("at", "")
                arr_at = (s.get("arrival") or {}).get("at", "")
                equipment = (s.get("aircraft") or {}).get("code", "")
                print(f"    seg{s_idx}: id={seg_id} {flight_no} {dep}->{arr} {dep_at} -> {arr_at} equipment={equipment}")

                if seg_id:
                    seg_id_map[str(seg_id)] = f"{flight_no} {dep}->{arr}"

        # cabin/fare details
        if not traveler_pricings:
            print("  ⚠ travelerPricings missing (no cabin/fare details in this response).")
            continue

        for tp in traveler_pricings[:1]:  # 只打印第一位旅客，避免太长
            fds = tp.get("fareDetailsBySegment") or []
            print(f"  fareDetailsBySegment={len(fds)}")
            for fd in fds:
                seg_id = str(fd.get("segmentId") or "")
                cabin = fd.get("cabin")
                booking_class = fd.get("class")
                fare_basis = fd.get("fareBasis")
                branded = fd.get("brandedFare")
                seg_hint = seg_id_map.get(seg_id, "")
                print(f"    segmentId={seg_id} [{seg_hint}] cabin={cabin} class={booking_class} fareBasis={fare_basis} brandedFare={branded}")

if __name__ == "__main__":
    token = get_token()

    # ✅ 建议先用机场码（PEK/PVG）+ 未来 30 天
    resp = search_offers(
        token,
        origin="BJS",          # auto_map=True 会映射成 PEK
        dest="SHA",            # auto_map=True 会映射成 PVG
        depart_date=None,      # 默认未来30天
        adults=1,
        max_results=10,
        auto_map=True
    )

    pretty_print_first_offers(resp, limit=3)
