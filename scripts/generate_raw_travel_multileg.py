#!/usr/bin/env python3
# scripts/generate_raw_travel_multileg.py
# Synthetic travel/ticketing generator (RAW) with realistic multi-leg itineraries.
# - No anomaly flags/weights/scores
# - Each PNR has 1..4 legs; end-to-end O&D and JSON "legs" given
# - All rows include the same set of columns; empty values as ""

import os, random, string, hashlib, json
from datetime import datetime, timedelta, timezone
import pandas as pd

UTC = timezone.utc
rnd = random.Random(42)

# -------------------------------
# Basic universes (users/devices/etc.)
# -------------------------------
N_USERS = 800
N_ACCOUNTS = 240
N_DEVICES = 600

users    = [f"user_{i:05d}"   for i in range(N_USERS)]
accounts = [f"acct_{i:05d}"   for i in range(N_ACCOUNTS)]
devices  = [f"dev_{i:05d}"    for i in range(N_DEVICES)]

N_IP_BLOCKS = [
    ("FR", "85.168.", 180),
    ("ES", "83.44.", 120),
    ("DE", "88.130.", 100),
    ("GB", "92.40.", 100),
    ("US", "66.102.", 150),
    ("AE", "94.200.", 80),
    ("TR", "85.105.", 80),
    ("QA", "78.100.", 60),
]

def _random_ip(block):
    _, prefix, _ = block
    return f"{prefix}{rnd.randint(0,255)}.{rnd.randint(0,255)}"

def _hash_card(seed: str) -> str:
    return hashlib.sha1(("card|" + seed).encode()).hexdigest()[:16]

# -------------------------------
# Travel / ticketing domain
# -------------------------------

AIRPORTS = {
    "CDG": ("FR", "Paris CDG", 49.0097, 2.5479),
    "ORY": ("FR", "Paris ORY", 48.7262, 2.3652),
    "NCE": ("FR", "Nice", 43.6653, 7.2150),
    "LHR": ("GB", "London LHR", 51.4700, -0.4543),
    "LGW": ("GB", "London LGW", 51.1537, -0.1821),
    "FRA": ("DE", "Frankfurt", 50.0379, 8.5622),
    "MUC": ("DE", "Munich", 48.3538, 11.7861),
    "MAD": ("ES", "Madrid", 40.4719, -3.5626),
    "BCN": ("ES", "Barcelona", 41.2974, 2.0833),
    "JFK": ("US", "New York JFK", 40.6413, -73.7781),
    "EWR": ("US", "Newark", 40.6895, -74.1745),
    "DXB": ("AE", "Dubai", 25.2532, 55.3657),
    "DOH": ("QA", "Doha", 25.2731, 51.6081),
    "IST": ("TR", "Istanbul", 41.2753, 28.7519),
    "SIN": ("SG", "Singapore", 1.3644, 103.9915),
    "HND": ("JP", "Tokyo Haneda", 35.5494, 139.7798),
    "CWA": ("US", "Central Wisconsin", 44.7776, -89.6668),
}

CARRIERS = ["AF","KL","BA","LH","LX","UA","DL","AA","EK","QR","TK","IB","AZ","SN"]
CHANNELS = ["web","mobile","api","agency"]

OFFICES = [f"PARAE{217 + i:03d}V{c}" for i in range(10) for c in "xyz"] + \
          [f"ISTT{10000 + i}" for i in range(10)] + \
          [f"RAKMO{2000 + i}" for i in range(10)] + \
          [f"CKY{260000 + i}" for i in range(10)]
SIGNS = [f"{rnd.randint(1000,9999)}{rnd.choice(['AE','DE','EG','SA','TR','QA','US','FR','GB'])}" for _ in range(120)]

def _pnr() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=6))

def _ticket_number(carrier: str) -> str:
    prefix = {
        "AF":"057","KL":"074","BA":"125","LH":"220","LX":"724",
        "UA":"016","DL":"006","AA":"001","EK":"176","QR":"157","TK":"235",
        "IB":"075","AZ":"055","SN":"082"
    }.get(carrier, "999")
    return f"{prefix}-{random.randint(1000000, 9999999)}"

HUBS = ["CDG","LHR","FRA","MAD","DXB","DOH","IST","JFK","EWR","SIN","HND"]

def _pick_multileg_route() -> list[str]:
    """Return a contiguous list of IATAs: [leg0_origin, ..., final_dest], size 2..5 (1..4 legs)."""
    n_legs = rnd.choice([1,1,2,2,3,4])  # bias toward 1–2 legs
    path = [rnd.choice(HUBS)]
    for _ in range(n_legs):
        # Pick a next node different from current; avoid immediate repeats
        nxt = rnd.choice([a for a in HUBS if a != path[-1]])
        path.append(nxt)
    return path  # e.g.["CDG","FRA","JFK"]

def _airport(iata: str):
    return AIRPORTS[iata]

def _flight_no(carrier: str) -> str:
    return f"{carrier}{rnd.randint(1, 9999):04d}"

# -------------------------------
# Main generation
# -------------------------------

def main(out="data"):
    os.makedirs(out, exist_ok=True)
    now = datetime.now(UTC)

    travel_rows = []
    N_PNR = 1300

    for _ in range(N_PNR):
        # actors
        carrier = rnd.choice(CARRIERS)
        user = rnd.choice(users)
        acct = rnd.choice(accounts)
        dev  = rnd.choice(devices)
        office_id = rnd.choice(OFFICES)
        user_sign = rnd.choice(SIGNS)
        organization = rnd.choice(["TA_GLOBAL","TA_LOCAL","OTA","Consolidator"])
        ip_block = rnd.choice(N_IP_BLOCKS)
        ip = _random_ip(ip_block)

        # itinerary with 1..4 legs
        path = _pick_multileg_route()
        origin_iata, dest_iata = path[0], path[-1]
        ori_cty, ori_city, ori_lat, ori_lon = _airport(origin_iata)
        des_cty, des_city, des_lat, des_lon = _airport(dest_iata)

        # times
        advance_h = rnd.choice([6, 8, 10, 12, 24, 48, 72, 120, 240, 360])
        first_dep = now + timedelta(hours=rnd.randint(4, 240))
        booked_at = first_dep - timedelta(hours=advance_h)

        # per-leg schedule (durations 1.2–10.5h, layover 1–5h)
        legs = []
        dep = first_dep
        total_amount = 0.0
        for i in range(len(path)-1):
            a = path[i]
            b = path[i+1]
            a_cty, a_city, a_lat, a_lon = _airport(a)
            b_cty, b_city, b_lat, b_lon = _airport(b)
            duration_h = rnd.uniform(1.2, 10.5)
            arr = dep + timedelta(hours=duration_h)
            flight = _flight_no(carrier)
            fare = rnd.uniform(60, 900)  # distribute cost over legs
            total_amount += fare
            legs.append({
                "segment": i+1,
                "carrier": carrier,
                "flight_no": flight,
                "origin": a,
                "dest": b,
                "dep_time": dep.isoformat().replace("+00:00","Z"),
                "arr_time": arr.isoformat().replace("+00:00","Z"),
            })
            # next dep = arr + layover (for next leg if any)
            dep = arr + timedelta(hours=rnd.uniform(1.0, 5.0))

        # final arrival time for display
        final_arr = legs[-1]["arr_time"]

        pnr = _pnr()
        tkt = _ticket_number(carrier)
        amount = round(total_amount, 2)

        pos_country   = ori_cty
        issue_country = ori_cty
        card_country  = rnd.choice([ori_cty, des_cty, "DE","GB","AE","US","TR","QA"])
        stay_nights   = rnd.choice([0,1,2,3,5,7,10])
        card_hash     = _hash_card(user + acct + str(rnd.randint(0,50)))

        # Booking event (raw)
        travel_rows.append({
            "ts": booked_at.isoformat().replace("+00:00","Z"),
            "event_type": "booking",
            "user_id": user, "account_id": acct, "device_id": dev, "ip": ip,
            "geo_lat": ori_lat, "geo_lon": ori_lon, "country": ori_cty, "city": ori_city,
            "amount": 0.0,
            "office_id": office_id, "user_sign": user_sign, "organization": organization,
            "pnr": pnr, "carrier": carrier, "tkt_number": "",
            "origin": origin_iata, "dest": dest_iata,
            "dep_time": legs[0]["dep_time"],
            "arr_time": final_arr,
            "pos_country": pos_country, "issue_country": "", "card_country": card_country,
            "advance_hours": advance_h, "stay_nights": stay_nights,
            "card_hash": "",
            "legs": json.dumps(legs, ensure_ascii=False),
        })

        # Ticket issuance (raw)
        ticketed_at = booked_at + timedelta(minutes=rnd.randint(2, 90))
        travel_rows.append({
            "ts": ticketed_at.isoformat().replace("+00:00","Z"),
            "event_type": "tkt_issued",
            "user_id": user, "account_id": acct, "device_id": dev, "ip": ip,
            "geo_lat": ori_lat, "geo_lon": ori_lon, "country": ori_cty, "city": ori_city,
            "amount": amount,
            "office_id": office_id, "user_sign": user_sign, "organization": organization,
            "pnr": pnr, "carrier": carrier, "tkt_number": tkt,
            "origin": origin_iata, "dest": dest_iata,
            "dep_time": legs[0]["dep_time"],
            "arr_time": final_arr,
            "pos_country": pos_country, "issue_country": issue_country, "card_country": card_country,
            "advance_hours": advance_h, "stay_nights": stay_nights,
            "card_hash": card_hash,
            "legs": json.dumps(legs, ensure_ascii=False),
        })

        # Payment (raw)
        paid_at = booked_at + timedelta(minutes=rnd.randint(1, 120))
        travel_rows.append({
            "ts": paid_at.isoformat().replace("+00:00","Z"),
            "event_type": "payment",
            "user_id": user, "account_id": acct, "device_id": dev, "ip": ip,
            "geo_lat": ori_lat, "geo_lon": ori_lon, "country": ori_cty, "city": ori_city,
            "amount": amount,
            "office_id": office_id, "user_sign": user_sign, "organization": organization,
            "pnr": pnr, "carrier": carrier, "tkt_number": tkt,
            "origin": origin_iata, "dest": dest_iata,
            "dep_time": legs[0]["dep_time"],
            "arr_time": final_arr,
            "pos_country": pos_country, "issue_country": issue_country, "card_country": card_country,
            "advance_hours": advance_h, "stay_nights": stay_nights,
            "card_hash": card_hash,
            "legs": json.dumps(legs, ensure_ascii=False),
        })

    # Always write the same columns and fill empty as ""
    all_cols = [
        "ts", "event_type", "user_id", "account_id", "device_id", "ip",
        "geo_lat", "geo_lon", "country", "city",
        "amount",
        "office_id", "user_sign", "organization",
        "pnr", "carrier", "tkt_number",
        "origin", "dest",
        "dep_time", "arr_time",
        "pos_country", "issue_country", "card_country",
        "advance_hours", "stay_nights",
        "card_hash",
        "legs"
    ]
    df = pd.DataFrame(travel_rows, columns=all_cols)
    df.fillna("", inplace=True)
    out_path = os.path.join(out, "travel_events.csv")
    df.to_csv(out_path, index=False)
    print(f"[OK] wrote {out_path} rows={len(df)}")

if __name__ == "__main__":
    main()