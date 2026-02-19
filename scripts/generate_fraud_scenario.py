#!/usr/bin/env python3
# scripts/generate_fraud_scenario.py
"""
Comprehensive fraud investigation scenario generator for Lizard.

Generates realistic data for:
1.Authentication logs (success/failure with VPN detection during fraud)
2.Ticket issuance events (OK/KO with routes, currencies, FoP)
3.Spike patterns in auth failures and ticketing
4.Geographic distribution (real location vs VPN during fraud)
5.Time-to-departure analysis
6.Multiple currencies (EUR, USD, GBP, etc.)
7.Form of Payment (FoP) diversity

Scenario:  A fraud ring operating from multiple locations using VPNs,
with a legitimate agent whose credentials are compromised.
"""

import os
import random
import string
import hashlib
import json
import argparse
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
import numpy as np

UTC = timezone.utc

# Set seed for reproducibility
SEED = 42
rnd = random.Random(SEED)
np.random.seed(SEED)


# ============================================================
# Configuration
# ============================================================

class Config:
    """Scenario configuration."""
    # Time range
    SCENARIO_DAYS = 14  # 2 weeks of data

    # Volume
    N_USERS = 150  # Regular users
    N_FRAUDSTERS = 8  # Fraud actors
    N_COMPROMISED_AGENTS = 3  # Legitimate agents whose credentials are stolen
    N_ACCOUNTS = 80
    N_DEVICES = 200
    N_OFFICES = 25

    # Event volumes
    BASE_AUTH_PER_DAY = 800
    BASE_TICKETS_PER_DAY = 200

    # Spike configuration
    SPIKE_DAYS = [3, 7, 11]  # Days with fraud spikes
    SPIKE_MULTIPLIER = 4.
    5  # How much higher during spikes
    AUTH_FAILURE_SPIKE_MULT = 8.0  # Auth failures spike even higher

    # Fraud patterns
    FRAUD_START_DAY = 2  # Fraud begins on day 2
    FRAUD_PEAK_DAYS = [3, 7, 11]  # Peak fraud activity
    VPN_USAGE_DURING_FRAUD = 0.85  # 85% of fraud uses VPN

    # Time-to-departure thresholds (hours)
    NORMAL_MIN_ADVANCE = 24
    NORMAL_MAX_ADVANCE = 720  # 30 days
    FRAUD_MIN_ADVANCE = 2  # Very short advance
    FRAUD_MAX_ADVANCE = 72  # 3 days max


# ============================================================
# Geographic Data
# ============================================================

AIRPORTS = {
    # Europe
    "CDG": ("FR", "Paris CDG", 49.0097, 2.5479),
    "ORY": ("FR", "Paris ORY", 48.7262, 2.3652),
    "NCE": ("FR", "Nice", 43.6653, 7.2150),
    "LHR": ("GB", "London LHR", 51.4700, -0.4543),
    "LGW": ("GB", "London LGW", 51.1537, -0.1821),
    "FRA": ("DE", "Frankfurt", 50.0379, 8.5622),
    "MUC": ("DE", "Munich", 48.3538, 11.7861),
    "MAD": ("ES", "Madrid", 40.4719, -3.5626),
    "BCN": ("ES", "Barcelona", 41.2974, 2.0833),
    "FCO": ("IT", "Rome", 41.8003, 12.2389),
    "MXP": ("IT", "Milan", 45.6306, 8.7281),
    "AMS": ("NL", "Amsterdam", 52.3105, 4.7683),
    "ZRH": ("CH", "Zurich", 47.4647, 8.5492),
    "VIE": ("AT", "Vienna", 48.1103, 16.5697),
    "CPH": ("DK", "Copenhagen", 55.6180, 12.6560),

    # Middle East
    "DXB": ("AE", "Dubai", 25.2532, 55.3657),
    "DOH": ("QA", "Doha", 25.2731, 51.6081),
    "IST": ("TR", "Istanbul", 41.2753, 28.7519),
    "SAW": ("TR", "Istanbul Sabiha", 40.8986, 29.3092),
    "JED": ("SA", "Jeddah", 21.6796, 39.1565),
    "RUH": ("SA", "Riyadh", 24.9578, 46.6989),
    "CAI": ("EG", "Cairo", 30.1219, 31.4056),
    "CMN": ("MA", "Casablanca", 33.3675, -7.5898),
    "TUN": ("TN", "Tunis", 36.8510, 10.2272),

    # Americas
    "JFK": ("US", "New York JFK", 40.6413, -73.7781),
    "EWR": ("US", "Newark", 40.6895, -74.1745),
    "LAX": ("US", "Los Angeles", 33.9416, -118.4085),
    "MIA": ("US", "Miami", 25.7959, -80.2870),
    "ORD": ("US", "Chicago", 41.9742, -87.9073),
    "YYZ": ("CA", "Toronto", 43.6777, -79.6248),
    "GRU": ("BR", "Sao Paulo", -23.4356, -46.4731),

    # Asia Pacific
    "SIN": ("SG", "Singapore", 1.3644, 103.9915),
    "HKG": ("HK", "Hong Kong", 22.3080, 113.9185),
    "NRT": ("JP", "Tokyo Narita", 35.7720, 140.3929),
    "HND": ("JP", "Tokyo Haneda", 35.5494, 139.7798),
    "BKK": ("TH", "Bangkok", 13.6900, 100.7501),
    "DEL": ("IN", "Delhi", 28.5562, 77.1000),
    "BOM": ("IN", "Mumbai", 19.0896, 72.8656),

    # Africa
    "JNB": ("ZA", "Johannesburg", -26.1367, 28.2411),
    "CPT": ("ZA", "Cape Town", -33.9715, 18.6021),
    "NBO": ("KE", "Nairobi", -1.3192, 36.9278),
    "LOS": ("NG", "Lagos", 6.5774, 3.3212),
    "ACC": ("GH", "Accra", 5.6052, -0.1668),
    "CKY": ("GN", "Conakry", 9.5769, -13.6120),
}

# VPN exit nodes (fake locations during fraud)
VPN_LOCATIONS = {
    "vpn_nl": ("NL", "Amsterdam VPN", 52.37, 4.89),
    "vpn_de": ("DE", "Frankfurt VPN", 50.11, 8.68),
    "vpn_us": ("US", "New York VPN", 40.71, -74.00),
    "vpn_sg": ("SG", "Singapore VPN", 1.35, 103.82),
    "vpn_gb": ("GB", "London VPN", 51.51, -0.13),
    "vpn_ch": ("CH", "Zurich VPN", 47.37, 8.54),
}

# Real fraudster locations
FRAUDSTER_REAL_LOCATIONS = {
    "fraud_loc_1": ("NG", "Lagos", 6.45, 3.39),
    "fraud_loc_2": ("GH", "Accra", 5.56, -0.20),
    "fraud_loc_3": ("GN", "Conakry", 9.64, -13.58),
    "fraud_loc_4": ("SN", "Dakar", 14.69, -17.44),
    "fraud_loc_5": ("CI", "Abidjan", 5.35, -4.00),
}

# Legitimate agent locations
AGENT_LOCATIONS = {
    "office_paris": ("FR", "Paris", 48.86, 2.35),
    "office_madrid": ("ES", "Madrid", 40.42, -3.70),
    "office_london": ("GB", "London", 51.51, -0.13),
    "office_dubai": ("AE", "Dubai", 25.20, 55.27),
    "office_istanbul": ("TR", "Istanbul", 41.01, 28.98),
}

# IP ranges by country/type
IP_RANGES = {
    "FR": ["85.168.", "92.154.", "90.83."],
    "ES": ["83.44.", "88.3.", "95.17."],
    "DE": ["88.130.", "91.64.", "84.157."],
    "GB": ["92.40.", "86.8.", "81.174."],
    "US": ["66.102.", "72.14.", "74.125."],
    "AE": ["94.200.", "185.73.", "195.229."],
    "TR": ["85.105.", "88.233.", "94.54."],
    "NL": ["145.131.", "82.94.", "213.46."],  # VPN common
    "SG": ["118.189.", "175.156.", "203.116."],
    "NG": ["41.58.", "105.112.", "197.210."],  # Fraud origin
    "GH": ["41.74.", "154.160.", "197.251."],  # Fraud origin
    "GN": ["41.223.", "197.149."],  # Fraud origin
}

CARRIERS = ["AF", "KL", "BA", "LH", "LX", "UA", "DL", "AA", "EK", "QR", "TK", "IB", "AZ", "SN", "OS", "SK"]

CURRENCIES = ["EUR", "USD", "GBP", "CHF", "AED", "QAR", "TRY", "SAR", "EGP", "MAD"]

# Form of Payment types
FOP_TYPES = [
    {"code": "CC", "name": "Credit Card", "subtype": ["VI", "MC", "AX", "DC"]},
    {"code": "CA", "name": "Cash", "subtype": None},
    {"code": "CK", "name": "Check", "subtype": None},
    {"code": "MS", "name": "Miscellaneous", "subtype": ["BSP", "ARC", "INVOICE"]},
    {"code": "WI", "name": "Wire Transfer", "subtype": None},
    {"code": "AG", "name": "Agency Credit", "subtype": None},
]

# Card BINs for different card types
CARD_BINS = {
    "VI": ["4", "4111", "4532", "4916"],
    "MC": ["5", "5100", "5200", "5300", "5400", "5500"],
    "AX": ["34", "37"],
    "DC": ["30", "36", "38"],
}


# ============================================================
# Entity Generators
# ============================================================

def generate_users(config: Config) -> Dict[str, Dict]:
    """Generate user profiles."""
    users = {}

    # Regular legitimate users
    for i in range(config.N_USERS):
        user_id = f"agent_{i:04d}"
        location = rnd.choice(list(AGENT_LOCATIONS.keys()))
        country, city, lat, lon = AGENT_LOCATIONS[location]
        users[user_id] = {
            "type": "legitimate",
            "office_location": location,
            "country": country,
            "city": city,
            "lat": lat,
            "lon": lon,
            "risk_level": rnd.choice(["low", "low", "low", "medium"]),
            "sign": f"{rnd.randint(1000, 9999)}{country}",
        }

    # Compromised agent accounts (legitimate agents whose credentials are stolen)
    for i in range(config.N_COMPROMISED_AGENTS):
        user_id = f"agent_compromised_{i:02d}"
        location = rnd.choice(list(AGENT_LOCATIONS.keys()))
        country, city, lat, lon = AGENT_LOCATIONS[location]
        users[user_id] = {
            "type": "compromised",
            "office_location": location,
            "country": country,
            "city": city,
            "lat": lat,
            "lon": lon,
            "risk_level": "high",
            "sign": f"{rnd.randint(1000, 9999)}{country}",
            "compromised_day": rnd.randint(config.FRAUD_START_DAY - 1, config.FRAUD_START_DAY + 1),
        }

    # Fraudster accounts
    for i in range(config.N_FRAUDSTERS):
        user_id = f"fraudster_{i:02d}"
        real_loc = rnd.choice(list(FRAUDSTER_REAL_LOCATIONS.keys()))
        country, city, lat, lon = FRAUDSTER_REAL_LOCATIONS[real_loc]
        users[user_id] = {
            "type": "fraudster",
            "real_location": real_loc,
            "country": country,
            "city": city,
            "lat": lat,
            "lon": lon,
            "risk_level": "critical",
            "sign": f"{rnd.randint(1000, 9999)}XX",  # Suspicious sign format
            "preferred_vpn": rnd.choice(list(VPN_LOCATIONS.keys())),
        }

    return users


def generate_devices(config: Config) -> List[str]:
    """Generate device IDs."""
    return [f"dev_{i:05d}" for i in range(config.N_DEVICES)]


def generate_offices(config: Config) -> List[Dict]:
    """Generate office/agency data."""
    offices = []
    office_patterns = [
        ("PARAE", "V", 217, "FR"),
        ("LONBA", "T", 100, "GB"),
        ("MADIB", "K", 50, "ES"),
        ("ISTTK", "", 10000, "TR"),
        ("DXBEK", "M", 500, "AE"),
        ("CKYAA", "", 260000, "GN"),  # Suspicious office pattern
        ("LOSAA", "", 270000, "NG"),  # Suspicious office pattern
        ("ACCGH", "", 280000, "GH"),  # Suspicious office pattern
    ]

    for pattern, suffix_chars, start_num, country in office_patterns:
        for i in range(config.N_OFFICES // len(office_patterns) + 1):
            if suffix_chars:
                suffix = rnd.choice(list(suffix_chars)) if len(suffix_chars) == 1 else rnd.choice(list(suffix_chars))
                office_id = f"{pattern}{start_num + i:03d}{suffix}"
            else:
                office_id = f"{pattern}{start_num + i}"
            offices.append({
                "office_id": office_id,
                "country": country,
                "risk_score": 0.8 if country in ["GN", "NG", "GH"] else 0.1,
            })

    return offices


# ============================================================
# Helper Functions
# ============================================================

def random_ip(country: str) -> str:
    """Generate random IP for a country."""
    prefixes = IP_RANGES.get(country, IP_RANGES["US"])
    prefix = rnd.choice(prefixes)
    return f"{prefix}{rnd.randint(1, 254)}.{rnd.randint(1, 254)}"


def hash_card(seed: str) -> str:
    """Generate hashed card number."""
    return hashlib.sha256(f"card|{seed}".encode()).hexdigest()[:16]


def generate_pnr() -> str:
    """Generate PNR/booking reference."""
    return "".join(rnd.choices(string.ascii_uppercase + string.digits, k=6))


def generate_ticket_number(carrier: str) -> str:
    """Generate ticket number."""
    prefix_map = {
        "AF": "057", "KL": "074", "BA": "125", "LH": "220", "LX": "724",
        "UA": "016", "DL": "006", "AA": "001", "EK": "176", "QR": "157",
        "TK": "235", "IB": "075", "AZ": "055", "SN": "082", "OS": "257", "SK": "117"
    }
    prefix = prefix_map.get(carrier, "999")
    return f"{prefix}-{rnd.randint(1000000, 9999999)}"


def generate_fop() -> Dict[str, Any]:
    """Generate Form of Payment."""
    fop = rnd.choice(FOP_TYPES)
    result = {"fop_type": fop["code"], "fop_name": fop["name"]}

    if fop["subtype"]:
        subtype = rnd.choice(fop["subtype"])
        result["fop_subtype"] = subtype

        if fop["code"] == "CC":
            # Generate card details
            bins = CARD_BINS.get(subtype, ["4"])
            bin_prefix = rnd.choice(bins)
            card_num = bin_prefix + "".join([str(rnd.randint(0, 9)) for _ in range(16 - len(bin_prefix))])
            result["card_last4"] = card_num[-4:]
            result["card_bin"] = card_num[: 6]

    return result


def generate_route(is_fraud: bool = False) -> Tuple[str, str, List[Dict]]:
    """Generate flight route with legs."""
    # Fraud tends to use specific high-value routes
    if is_fraud:
        fraud_routes = [
            (["LOS", "CDG", "JFK"], "High-value transatlantic"),
            (["ACC", "LHR", "MIA"], "Suspicious routing"),
            (["CKY", "IST", "DXB"], "Middle East connection"),
            (["LOS", "FRA", "LAX"], "Premium transatlantic"),
            (["ACC", "AMS", "JFK"], "VPN country routing"),
        ]
        route, _ = rnd.choice(fraud_routes)
    else:
        # Normal routes
        hubs = ["CDG", "LHR", "FRA", "AMS", "MAD", "FCO", "DXB", "IST", "JFK", "SIN"]
        n_legs = rnd.choices([1, 2, 3], weights=[0.5, 0.35, 0.15])[0]
        route = [rnd.choice(hubs)]
        for _ in range(n_legs):
            next_hub = rnd.choice([h for h in hubs if h != route[-1]])
            route.append(next_hub)

    origin = route[0]
    dest = route[-1]

    # Build legs
    legs = []
    carrier = rnd.choice(CARRIERS)
    dep_time = datetime.now(UTC) + timedelta(hours=rnd.randint(24, 720))

    for i in range(len(route) - 1):
        leg_origin = route[i]
        leg_dest = route[i + 1]
        duration_h = rnd.uniform(1.5, 12.0)
        arr_time = dep_time + timedelta(hours=duration_h)

        legs.append({
            "segment": i + 1,
            "carrier": carrier,
            "flight_no": f"{carrier}{rnd.randint(100, 9999):04d}",
            "origin": leg_origin,
            "dest": leg_dest,
            "dep_time": dep_time.isoformat().replace("+00:00", "Z"),
            "arr_time": arr_time.isoformat().replace("+00:00", "Z"),
        })

        # Layover for next leg
        dep_time = arr_time + timedelta(hours=rnd.uniform(1.0, 4.0))

    return origin, dest, legs


def calculate_advance_hours(dep_time_str: str, booking_time: datetime) -> float:
    """Calculate hours between booking and departure."""
    try:
        dep_time = datetime.fromisoformat(dep_time_str.replace("Z", "+00:00"))
        delta = dep_time - booking_time
        return max(0, delta.total_seconds() / 3600)
    except:
        return 0


# ============================================================
# Event Generators
# ============================================================

def generate_auth_event(
        ts: datetime,
        user: Dict,
        user_id: str,
        devices: List[str],
        is_fraud: bool = False,
        is_failure: bool = False,
        use_vpn: bool = False,
) -> Dict[str, Any]:
    """Generate an authentication event."""

    # Determine location
    if is_fraud and use_vpn:
        vpn_loc = user.get("preferred_vpn", rnd.choice(list(VPN_LOCATIONS.keys())))
        country, city, lat, lon = VPN_LOCATIONS[vpn_loc]
        ip = random_ip("NL" if "nl" in vpn_loc else "DE")  # VPN IPs
        is_vpn = True
    elif is_fraud:
        # Real fraudster location (rare - when VPN fails)
        country = user.get("country", "NG")
        city = user.get("city", "Lagos")
        lat = user.get("lat", 6.45)
        lon = user.get("lon", 3.39)
        ip = random_ip(country)
        is_vpn = False
    else:
        # Legitimate user
        country = user.get("country", "FR")
        city = user.get("city", "Paris")
        lat = user.get("lat", 48.86) + rnd.uniform(-0.1, 0.1)
        lon = user.get("lon", 2.35) + rnd.uniform(-0.1, 0.1)
        ip = random_ip(country)
        is_vpn = False

    device_id = rnd.choice(devices)

    # Event type
    if is_failure:
        event_type = rnd.choice(["auth_failure", "auth_failure", "auth_locked", "auth_invalid_token"])
        failure_reason = rnd.choice([
            "invalid_password", "invalid_password", "invalid_otp",
            "account_locked", "expired_session", "invalid_token"
        ])
    else:
        event_type = "auth_success"
        failure_reason = None

    event = {
        "ts": ts.isoformat().replace("+00:00", "Z"),
        "event_type": event_type,
        "user_id": user_id,
        "device_id": device_id,
        "ip": ip,
        "geo_lat": round(lat, 4),
        "geo_lon": round(lon, 4),
        "country": country,
        "city": city,
        "user_agent": rnd.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) Firefox/121.0",
            "Amadeus/API/2.1",
        ]),
        "session_id": f"sess_{hashlib.md5(f'{user_id}{ts}'.encode()).hexdigest()[:12]}",
        "is_vpn": is_vpn,
        "is_fraud_indicator": is_fraud,
    }

    if failure_reason:
        event["failure_reason"] = failure_reason

    return event


def generate_ticket_event(
        ts: datetime,
        user: Dict,
        user_id: str,
        devices: List[str],
        offices: List[Dict],
        is_fraud: bool = False,
        is_failure: bool = False,
        use_vpn: bool = False,
        config: Config = Config(),
) -> List[Dict[str, Any]]:
    """Generate ticket-related events (booking, ticketing, payment)."""

    events = []

    # Determine location
    if is_fraud and use_vpn:
        vpn_loc = user.get("preferred_vpn", rnd.choice(list(VPN_LOCATIONS.keys())))
        country, city, lat, lon = VPN_LOCATIONS[vpn_loc]
        ip = random_ip("NL" if "nl" in vpn_loc else "DE")
    elif is_fraud:
        country = user.get("country", "NG")
        city = user.get("city", "Lagos")
        lat = user.get("lat", 6.45)
        lon = user.get("lon", 3.39)
        ip = random_ip(country)
    else:
        country = user.get("country", "FR")
        city = user.get("city", "Paris")
        lat = user.get("lat", 48.86) + rnd.uniform(-0.05, 0.05)
        lon = user.get("lon", 2.35) + rnd.uniform(-0.05, 0.05)
        ip = random_ip(country)

    device_id = rnd.choice(devices)

    # Select office (fraud uses suspicious offices)
    if is_fraud:
        suspicious_offices = [o for o in offices if o["risk_score"] > 0.5]
        office = rnd.choice(suspicious_offices) if suspicious_offices else rnd.choice(offices)
    else:
        normal_offices = [o for o in offices if o["risk_score"] < 0.5]
        office = rnd.choice(normal_offices) if normal_offices else rnd.choice(offices)

    office_id = office["office_id"]
    user_sign = user.get("sign", f"{rnd.randint(1000, 9999)}XX")

    # Generate route
    origin, dest, legs = generate_route(is_fraud)
    origin_data = AIRPORTS.get(origin, ("XX", "Unknown", 0, 0))

    # Carrier and ticket
    carrier = legs[0]["carrier"] if legs else rnd.choice(CARRIERS)
    pnr = generate_pnr()
    tkt_number = generate_ticket_number(carrier)

    # Amount and currency (fraud = higher amounts, specific currencies)
    if is_fraud:
        amount = round(rnd.uniform(1500, 8000), 2)
        currency = rnd.choice(["EUR", "USD", "GBP"])  # High-value currencies
    else:
        amount = round(rnd.uniform(150, 2500), 2)
        currency = rnd.choice(CURRENCIES)

    # FoP
    fop = generate_fop()

    # Time to departure
    if legs:
        first_dep = legs[0]["dep_time"]
        advance_hours = calculate_advance_hours(first_dep, ts)

        # Fraud has very short advance booking
        if is_fraud:
            # Override with short advance
            new_dep = ts + timedelta(hours=rnd.uniform(config.FRAUD_MIN_ADVANCE, config.FRAUD_MAX_ADVANCE))
            legs[0]["dep_time"] = new_dep.isoformat().replace("+00:00", "Z")
            advance_hours = rnd.uniform(config.FRAUD_MIN_ADVANCE, config.FRAUD_MAX_ADVANCE)
    else:
        advance_hours = rnd.uniform(24, 720)

    # Card details
    card_country = rnd.choice(["NG", "GH", "GN"]) if is_fraud else origin_data[0]
    card_hash = hash_card(f"{user_id}_{rnd.randint(0, 100)}")

    # Common fields
    base_event = {
        "user_id": user_id,
        "device_id": device_id,
        "ip": ip,
        "geo_lat": round(lat, 4),
        "geo_lon": round(lon, 4),
        "country": country,
        "city": city,
        "office_id": office_id,
        "user_sign": user_sign,
        "organization": rnd.choice(["TA_GLOBAL", "TA_LOCAL", "OTA", "Consolidator", "Corporate"]),
        "pnr": pnr,
        "carrier": carrier,
        "origin": origin,
        "dest": dest,
        "pos_country": country,
        "card_country": card_country,
        "card_hash": card_hash,
        "advance_hours": round(advance_hours, 1),
        "stay_nights": rnd.choice([0, 1, 2, 3, 5, 7, 10, 14]),
        "legs": json.dumps(legs, ensure_ascii=False),
        "amount": amount,
        "currency": currency,
        "fop_type": fop["fop_type"],
        "fop_name": fop["fop_name"],
        "fop_subtype": fop.get("fop_subtype", ""),
        "is_fraud_indicator": is_fraud,
    }

    if fop.get("card_last4"):
        base_event["card_last4"] = fop["card_last4"]
    if fop.get("card_bin"):
        base_event["card_bin"] = fop["card_bin"]

    # 1.Booking event
    booking_ts = ts
    events.append({
        **base_event,
        "ts": booking_ts.isoformat().replace("+00:00", "Z"),
        "event_type": "booking",
        "tkt_number": "",
        "status": "confirmed",
    })

    # 2.Ticketing event (may fail)
    ticket_ts = booking_ts + timedelta(minutes=rnd.randint(2, 60))
    if is_failure:
        events.append({
            **base_event,
            "ts": ticket_ts.isoformat().replace("+00:00", "Z"),
            "event_type": "tkt_issue_failed",
            "tkt_number": "",
            "status": "failed",
            "failure_reason": rnd.choice([
                "payment_declined", "card_expired", "insufficient_funds",
                "fraud_check_failed", "system_error", "invalid_card"
            ]),
        })
    else:
        events.append({
            **base_event,
            "ts": ticket_ts.isoformat().replace("+00:00", "Z"),
            "event_type": "tkt_issued",
            "tkt_number": tkt_number,
            "status": "issued",
        })

        # 3.Payment event (only if ticketing succeeded)
        payment_ts = ticket_ts + timedelta(minutes=rnd.randint(1, 30))
        events.append({
            **base_event,
            "ts": payment_ts.isoformat().replace("+00:00", "Z"),
            "event_type": "payment",
            "tkt_number": tkt_number,
            "status": "completed",
        })

    return events


# ============================================================
# Main Scenario Generator
# ============================================================

def generate_scenario(config: Config, output_dir: str = "data"):
    """Generate the complete fraud investigation scenario."""

    os.makedirs(output_dir, exist_ok=True)

    print("=" * 60)
    print("LIZARD Fraud Investigation Scenario Generator")
    print("=" * 60)

    # Generate entities
    print("\n[1/4] Generating entities...")
    users = generate_users(config)
    devices = generate_devices(config)
    offices = generate_offices(config)

    print(
        f"  - Users: {len(users)} (legitimate: {config.N_USERS}, compromised: {config.N_COMPROMISED_AGENTS}, fraudsters: {config.N_FRAUDSTERS})")
    print(f"  - Devices: {len(devices)}")
    print(f"  - Offices: {len(offices)}")

    # Time range
    start_time = datetime.now(UTC) - timedelta(days=config.SCENARIO_DAYS)
    end_time = datetime.now(UTC)

    print(f"\n[2/4] Generating events from {start_time.date()} to {end_time.date()}...")

    auth_events = []
    ticket_events = []

    current_day = start_time
    day_num = 0

    while current_day < end_time:
        day_num += 1
        is_spike_day = day_num in config.SPIKE_DAYS
        is_fraud_active = day_num >= config.FRAUD_START_DAY
        is_fraud_peak = day_num in config.FRAUD_PEAK_DAYS

        # Calculate volumes for this day
        auth_volume = config.BASE_AUTH_PER_DAY
        ticket_volume = config.BASE_TICKETS_PER_DAY

        if is_spike_day:
            auth_volume = int(auth_volume * config.SPIKE_MULTIPLIER)
            ticket_volume = int(ticket_volume * config.SPIKE_MULTIPLIER)

        # Add extra auth failures during spikes
        auth_failure_volume = int(auth_volume * 0.05)  # Base 5% failure
        if is_spike_day:
            auth_failure_volume = int(auth_failure_volume * config.AUTH_FAILURE_SPIKE_MULT)

        # Generate authentication events for this day
        for _ in range(auth_volume):
            # Randomly pick time within the day
            ts = current_day + timedelta(
                hours=rnd.randint(0, 23),
                minutes=rnd.randint(0, 59),
                seconds=rnd.randint(0, 59)
            )

            # Determine if this is a fraud event
            is_fraud = False
            user_id = None
            user_data = None

            if is_fraud_active and rnd.random() < (0.15 if is_fraud_peak else 0.05):
                # Fraud event
                is_fraud = True
                fraud_type = rnd.choice(["fraudster", "compromised"])
                if fraud_type == "fraudster":
                    user_id = rnd.choice([u for u, d in users.items() if d["type"] == "fraudster"])
                else:
                    user_id = rnd.choice([u for u, d in users.items() if d["type"] == "compromised"])
                user_data = users[user_id]
                use_vpn = rnd.random() < config.VPN_USAGE_DURING_FRAUD
            else:
                # Legitimate event
                user_id = rnd.choice([u for u, d in users.items() if d["type"] == "legitimate"])
                user_data = users[user_id]
                use_vpn = False

            event = generate_auth_event(
                ts=ts,
                user=user_data,
                user_id=user_id,
                devices=devices,
                is_fraud=is_fraud,
                is_failure=False,
                use_vpn=use_vpn,
            )
            auth_events.append(event)

        # Generate auth failures (concentrated during fraud)
        for _ in range(auth_failure_volume):
            ts = current_day + timedelta(
                hours=rnd.randint(0, 23),
                minutes=rnd.randint(0, 59),
                seconds=rnd.randint(0, 59)
            )

            is_fraud = is_fraud_active and rnd.random() < 0.6  # 60% of failures during fraud are fraud-related

            if is_fraud:
                user_id = rnd.choice([u for u, d in users.items() if d["type"] in ["fraudster", "compromised"]])
                user_data = users[user_id]
                use_vpn = rnd.random() < config.VPN_USAGE_DURING_FRAUD
            else:
                user_id = rnd.choice([u for u, d in users.items() if d["type"] == "legitimate"])
                user_data = users[user_id]
                use_vpn = False

            event = generate_auth_event(
                ts=ts,
                user=user_data,
                user_id=user_id,
                devices=devices,
                is_fraud=is_fraud,
                is_failure=True,
                use_vpn=use_vpn,
            )
            auth_events.append(event)

        # Generate ticket events for this day
        ticket_failure_volume = int(ticket_volume * 0.08)  # Base 8% failure
        if is_spike_day:
            ticket_failure_volume = int(ticket_failure_volume * 3)

        for _ in range(ticket_volume):
            ts = current_day + timedelta(
                hours=rnd.randint(6, 22),  # Tickets mostly during business hours
                minutes=rnd.randint(0, 59),
                seconds=rnd.randint(0, 59)
            )

            is_fraud = False
            if is_fraud_active and rnd.random() < (0.20 if is_fraud_peak else 0.08):
                is_fraud = True
                user_id = rnd.choice([u for u, d in users.items() if d["type"] in ["fraudster", "compromised"]])
                user_data = users[user_id]
                use_vpn = rnd.random() < config.VPN_USAGE_DURING_FRAUD
            else:
                user_id = rnd.choice([u for u, d in users.items() if d["type"] == "legitimate"])
                user_data = users[user_id]
                use_vpn = False

            events = generate_ticket_event(
                ts=ts,
                user=user_data,
                user_id=user_id,
                devices=devices,
                offices=offices,
                is_fraud=is_fraud,
                is_failure=False,
                use_vpn=use_vpn,
                config=config,
            )
            ticket_events.extend(events)

        # Failed tickets
        for _ in range(ticket_failure_volume):
            ts = current_day + timedelta(
                hours=rnd.randint(6, 22),
                minutes=rnd.randint(0, 59),
                seconds=rnd.randint(0, 59)
            )

            is_fraud = is_fraud_active and rnd.random() < 0.7

            if is_fraud:
                user_id = rnd.choice([u for u, d in users.items() if d["type"] in ["fraudster", "compromised"]])
                user_data = users[user_id]
                use_vpn = rnd.random() < config.VPN_USAGE_DURING_FRAUD
            else:
                user_id = rnd.choice([u for u, d in users.items() if d["type"] == "legitimate"])
                user_data = users[user_id]
                use_vpn = False

            events = generate_ticket_event(
                ts=ts,
                user=user_data,
                user_id=user_id,
                devices=devices,
                offices=offices,
                is_fraud=is_fraud,
                is_failure=True,
                use_vpn=use_vpn,
                config=config,
            )
            ticket_events.extend(events)

        current_day += timedelta(days=1)

    print(f"  - Auth events: {len(auth_events)}")
    print(f"  - Ticket events: {len(ticket_events)}")

    # Convert to DataFrames
    print("\n[3/4] Saving to CSV files...")

    # Auth events
    auth_df = pd.DataFrame(auth_events)
    auth_df = auth_df.sort_values("ts").reset_index(drop=True)
    auth_path = os.path.join(output_dir, "auth_events.csv")
    auth_df.to_csv(auth_path, index=False)
    print(f"  - Saved:  {auth_path} ({len(auth_df)} rows)")

    # Ticket events
    ticket_df = pd.DataFrame(ticket_events)
    ticket_df = ticket_df.sort_values("ts").reset_index(drop=True)
    ticket_path = os.path.join(output_dir, "ticket_events.csv")
    ticket_df.to_csv(ticket_path, index=False)
    print(f"  - Saved: {ticket_path} ({len(ticket_df)} rows)")

    # Combined events (for single-file import)
    combined_events = auth_events + ticket_events
    combined_df = pd.DataFrame(combined_events)
    combined_df = combined_df.sort_values("ts").reset_index(drop=True)
    combined_path = os.path.join(output_dir, "all_events.csv")
    combined_df.to_csv(combined_path, index=False)
    print(f"  - Saved: {combined_path} ({len(combined_df)} rows)")

    # Generate summary statistics
    print("\n[4/4] Generating summary report...")

    summary = {
        "scenario": {
            "days": config.SCENARIO_DAYS,
            "start_date": start_time.isoformat(),
            "end_date": end_time.isoformat(),
            "spike_days": config.SPIKE_DAYS,
            "fraud_start_day": config.FRAUD_START_DAY,
        },
        "entities": {
            "users": len(users),
            "legitimate_users": config.N_USERS,
            "compromised_agents": config.N_COMPROMISED_AGENTS,
            "fraudsters": config.N_FRAUDSTERS,
            "devices": len(devices),
            "offices": len(offices),
        },
        "events": {
            "auth_total": len(auth_df),
            "auth_success": len(auth_df[auth_df["event_type"] == "auth_success"]),
            "auth_failure": len(auth_df[auth_df["event_type"] != "auth_success"]),
            "ticket_total": len(ticket_df),
            "ticket_issued": len(ticket_df[ticket_df["event_type"] == "tkt_issued"]),
            "ticket_failed": len(ticket_df[ticket_df["event_type"] == "tkt_issue_failed"]),
            "bookings": len(ticket_df[ticket_df["event_type"] == "booking"]),
            "payments": len(ticket_df[ticket_df["event_type"] == "payment"]),
        },
        "fraud_indicators": {
            "auth_with_vpn": len(auth_df[auth_df["is_vpn"] == True]),
            "auth_fraud_events": len(auth_df[auth_df["is_fraud_indicator"] == True]),
            "ticket_fraud_events": len(ticket_df[ticket_df["is_fraud_indicator"] == True]),
        },
        "currencies": ticket_df["currency"].value_counts().to_dict() if "currency" in ticket_df.columns else {},
        "fop_types": ticket_df["fop_type"].value_counts().to_dict() if "fop_type" in ticket_df.columns else {},
    }

    summary_path = os.path.join(output_dir, "scenario_summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"  - Saved:  {summary_path}")

    # Print summary
    print("\n" + "=" * 60)
    print("SCENARIO SUMMARY")
    print("=" * 60)
    print(f"\n📅 Time Range: {config.SCENARIO_DAYS} days")
    print(f"   Spike Days: {config.SPIKE_DAYS}")
    print(f"   Fraud Active: Day {config.FRAUD_START_DAY}+")
    print(f"\n👥 Entities:")
    print(f"   - Legitimate Users: {config.N_USERS}")
    print(f"   - Compromised Agents: {config.N_COMPROMISED_AGENTS}")
    print(f"   - Fraudsters: {config.N_FRAUDSTERS}")
    print(f"\n📊 Events Generated:")
    print(f"   - Auth Events: {len(auth_df):,}")
    print(f"     • Success: {summary['events']['auth_success']:,}")
    print(f"     • Failure: {summary['events']['auth_failure']:,}")
    print(f"   - Ticket Events: {len(ticket_df):,}")
    print(f"     • Bookings: {summary['events']['bookings']:,}")
    print(f"     • Issued: {summary['events']['ticket_issued']:,}")
    print(f"     • Failed: {summary['events']['ticket_failed']: ,}")
    print(f"     • Payments: {summary['events']['payments']: ,}")
    print(f"\n🚨 Fraud Indicators:")
    print(f"   - VPN Usage: {summary['fraud_indicators']['auth_with_vpn']:,}")
    print(f"   - Auth Fraud Events: {summary['fraud_indicators']['auth_fraud_events']:,}")
    print(f"   - Ticket Fraud Events: {summary['fraud_indicators']['ticket_fraud_events']: ,}")
    print(f"\n💰 Currencies:  {', '.join(summary['currencies'].keys())}")
    print(f"💳 FoP Types: {', '.join(summary['fop_types'].keys())}")
    print("\n" + "=" * 60)

    return summary


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate fraud investigation scenario data for Lizard",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples: 
  # Generate with default settings (14 days)
  python generate_fraud_scenario.py

  # Generate 30 days of data
  python generate_fraud_scenario.py --days 30

  # Generate with custom output directory
  python generate_fraud_scenario.py --output ./my_data

  # Generate larger dataset
  python generate_fraud_scenario.py --days 30 --users 500 --tickets-per-day 500
        """
    )

    parser.add_argument("--output", "-o", default="data", help="Output directory (default: data)")
    parser.add_argument("--days", type=int, default=14, help="Number of days to generate (default: 14)")
    parser.add_argument("--users", type=int, default=150, help="Number of legitimate users (default: 150)")
    parser.add_argument("--fraudsters", type=int, default=8, help="Number of fraudsters (default: 8)")
    parser.add_argument("--auth-per-day", type=int, default=800, help="Base auth events per day (default: 800)")
    parser.add_argument("--tickets-per-day", type=int, default=200, help="Base tickets per day (default: 200)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")

    args = parser.parse_args()

    # Update config
    config = Config()
    config.SCENARIO_DAYS = args.days
    config.N_USERS = args.users
    config.N_FRAUDSTERS = args.fraudsters
    config.BASE_AUTH_PER_DAY = args.auth_per_day
    config.BASE_TICKETS_PER_DAY = args.tickets_per_day

    # Update spike days based on duration
    if args.days > 14:
        config.SPIKE_DAYS = [3, 7, 11, 15, 22, 28][: args.days // 4]
        config.FRAUD_PEAK_DAYS = config.SPIKE_DAYS

    # Set seed
    global rnd
    rnd = random.Random(args.seed)
    np.random.seed(args.seed)

    # Generate
    generate_scenario(config, args.output)

    print(f"\n✅ Data generation complete!")
    print(f"   Files are in: {os.path.abspath(args.output)}/")


if __name__ == "__main__":
    main()