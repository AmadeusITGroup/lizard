#!/usr/bin/env python3
# scripts/generate_demo_data.py
"""
LIZARD Demo Data Generator
==========================
Generates realistic, correlated auth and ticketing events for fraud investigation demos.

This script generates data that matches the built-in mapping templates:
- "Fraud Scenario - Auth Events" 
- "Fraud Scenario - Ticket Events"

Key Design Principles:
1.Auth events correlate with ticket events (login 5-45 min before booking)
2.Users have consistent behavior patterns (devices, locations, travel habits)
3.Anomalies are subtle - detectable with analytics, not obvious labels
4.No "fraudster" or "compromised" visible in the output data

Anomaly Scenarios (subtle, realistic):
1.Account Takeover - New device/location, password reset, high-value booking
2.Credential Stuffing - Multiple failed logins from different IPs, then success
3.Velocity Abuse - Many bookings in short time (unusual for that user)
4.Impossible Travel - Login from distant locations within short timeframe
5.Payment Mismatch - Card country doesn't match user's usual country

Usage:
    python -m scripts.generate_demo_data --out ./data --users 50 --days 30
"""

from __future__ import annotations
import argparse
import hashlib
import json
import os
import random
import string
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

UTC = timezone.utc

# ============================================================
# Configuration
# ============================================================

RANDOM_SEED = 42
DEFAULT_DAYS = 30
DEFAULT_USERS = 50

# ============================================================
# Geographic Data (matching generate_fraud_scenario.py)
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
    # Middle East
    "DXB": ("AE", "Dubai", 25.2532, 55.3657),
    "DOH": ("QA", "Doha", 25.2731, 51.6081),
    "IST": ("TR", "Istanbul", 41.2753, 28.7519),
    # Americas
    "JFK": ("US", "New York JFK", 40.6413, -73.7781),
    "LAX": ("US", "Los Angeles", 33.9416, -118.4085),
    "MIA": ("US", "Miami", 25.7959, -80.2870),
    "ORD": ("US", "Chicago", 41.9742, -87.9073),
    "YYZ": ("CA", "Toronto", 43.6777, -79.6248),
    "GRU": ("BR", "Sao Paulo", -23.4356, -46.4731),
    # Asia Pacific
    "SIN": ("SG", "Singapore", 1.3644, 103.9915),
    "HKG": ("HK", "Hong Kong", 22.3080, 113.9185),
    "NRT": ("JP", "Tokyo Narita", 35.7720, 140.3929),
    "BKK": ("TH", "Bangkok", 13.6900, 100.7501),
}

# Agent office locations
AGENT_LOCATIONS = {
    "office_paris": ("FR", "Paris", 48.86, 2.35),
    "office_london": ("GB", "London", 51.51, -0.13),
    "office_madrid": ("ES", "Madrid", 40.42, -3.70),
    "office_frankfurt": ("DE", "Frankfurt", 50.11, 8.68),
    "office_amsterdam": ("NL", "Amsterdam", 52.37, 4.90),
    "office_dubai": ("AE", "Dubai", 25.20, 55.27),
    "office_istanbul": ("TR", "Istanbul", 41.01, 28.98),
    "office_nyc": ("US", "New York", 40.71, -74.01),
    "office_singapore": ("SG", "Singapore", 1.35, 103.82),
}

# VPN exit nodes (used during anomalous sessions)
VPN_LOCATIONS = {
    "vpn_nl": ("NL", "Amsterdam VPN", 52.37, 4.89),
    "vpn_de": ("DE", "Frankfurt VPN", 50.11, 8.68),
    "vpn_us": ("US", "New York VPN", 40.71, -74.00),
    "vpn_sg": ("SG", "Singapore VPN", 1.35, 103.82),
}

# IP ranges by country
IP_RANGES = {
    "FR": ["85.168.", "92.154.", "90.83."],
    "ES": ["83.44.", "88.3.", "95.17."],
    "DE": ["88.130.", "91.64.", "84.157."],
    "GB": ["92.40.", "86.8.", "81.174."],
    "US": ["66.102.", "72.14.", "74.125."],
    "AE": ["94.200.", "185.73.", "195.229."],
    "TR": ["85.105.", "88.233.", "94.54."],
    "NL": ["145.131.", "82.94.", "213.46."],
    "SG": ["118.189.", "175.156.", "203.116."],
    "CA": ["24.48.", "70.26.", "99.224."],
    "JP": ["126.72.", "153.120.", "210.136."],
    "IT": ["79.0.", "82.48.", "93.34."],
    "CH": ["178.197.", "185.104.", "193.247."],
    "AT": ["77.116.", "84.112.", "213.47."],
    "QA": ["37.186.", "78.100.", "82.148."],
    "HK": ["14.0.", "42.2.", "58.64."],
    "TH": ["49.228.", "110.164.", "171.96."],
    "BR": ["138.36.", "177.0.", "189.0."],
}

CARRIERS = ["AF", "KL", "BA", "LH", "LX", "UA", "DL", "AA", "EK", "QR", "TK", "IB", "AZ", "SN", "OS", "SK"]

CURRENCIES = ["EUR", "USD", "GBP", "CHF", "AED", "QAR", "TRY"]

FOP_TYPES = [
    {"code": "CC", "name": "Credit Card", "subtype": ["VI", "MC", "AX"]},
    {"code": "CA", "name": "Cash", "subtype": None},
    {"code": "MS", "name": "Miscellaneous", "subtype": ["BSP", "INVOICE"]},
]

CARD_BINS = {
    "VI": ["4111", "4532", "4916"],
    "MC": ["5100", "5200", "5300"],
    "AX": ["3400", "3700"],
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/120.0",
    "Amadeus/API/2.1",
]

OFFICE_PATTERNS = [
    ("PARAF", "V", 217, "FR"),
    ("LONBA", "T", 100, "GB"),
    ("MADIB", "K", 50, "ES"),
    ("FRAXX", "L", 300, "DE"),
    ("AMSXX", "N", 400, "NL"),
    ("DXBEK", "M", 500, "AE"),
    ("ISTTK", "A", 600, "TR"),
    ("NYCUA", "J", 700, "US"),
    ("SINXX", "S", 800, "SG"),
]


# ============================================================
# Data Classes
# ============================================================

@dataclass
class UserProfile:
    """User profile with consistent behavior patterns."""
    user_id: str
    user_sign: str
    home_location: str
    country: str
    city: str
    lat: float
    lon: float
    preferred_device: str
    preferred_browser: str
    card_country: str
    card_bin: str
    card_last4: str
    typical_booking_amount: float
    booking_frequency: float  # per week
    work_hours: Tuple[int, int]
    usual_destinations: List[str] = field(default_factory=list)
    office_id: str = ""


@dataclass
class AnomalyScenario:
    """Anomaly scenario for reference."""
    name: str
    user_id: str
    date: str
    description: str


# ============================================================
# Generator Class
# ============================================================

class DemoDataGenerator:
    """Generates correlated auth and ticket events for demos."""

    def __init__(self, num_users: int = 50, num_days: int = 30, seed: int = RANDOM_SEED):
        random.seed(seed)
        np.random.seed(seed)
        self.rnd = random.Random(seed)

        self.num_users = num_users
        self.num_days = num_days
        self.end_date = datetime.now(UTC).replace(hour=23, minute=59, second=59, microsecond=0)
        self.start_date = self.end_date - timedelta(days=num_days)

        self.users: List[UserProfile] = []
        self.offices: List[Dict] = []
        self.devices: List[str] = []
        self.auth_events: List[Dict] = []
        self.ticket_events: List[Dict] = []
        self.anomaly_scenarios: List[AnomalyScenario] = []

    def generate_all(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Generate all data."""
        print(f"🎲 Generating demo data:  {self.num_users} users, {self.num_days} days...")

        # Step 1: Create entities
        self._create_offices()
        self._create_devices()
        self._create_users()
        print(f"  ✓ Created {len(self.users)} users, {len(self.offices)} offices, {len(self.devices)} devices")

        # Step 2: Generate normal behavior
        self._generate_normal_behavior()
        print(f"  ✓ Generated {len(self.auth_events)} auth events, {len(self.ticket_events)} ticket events")

        # Step 3: Inject anomalies (last 2 weeks)
        self._inject_anomalies()
        print(f"  ✓ Injected {len(self.anomaly_scenarios)} anomaly scenarios")

        # Convert to DataFrames
        auth_df = pd.DataFrame(self.auth_events).sort_values('ts').reset_index(drop=True)
        ticket_df = pd.DataFrame(self.ticket_events).sort_values('ts').reset_index(drop=True)

        return auth_df, ticket_df

    def _create_offices(self):
        """Create office entities."""
        for pattern, suffix, start_num, country in OFFICE_PATTERNS:
            for i in range(3):  # 3 offices per pattern
                office_id = f"{pattern}{start_num + i: 03d}{suffix}"
                self.offices.append({
                    "office_id": office_id,
                    "country": country,
                })

    def _create_devices(self):
        """Create device IDs."""
        self.devices = [f"dev_{i: 05d}" for i in range(self.num_users * 2)]

    def _create_users(self):
        """Create user profiles with consistent characteristics."""
        for i in range(self.num_users):
            # Pick a home location
            loc_key = self.rnd.choice(list(AGENT_LOCATIONS.keys()))
            country, city, lat, lon = AGENT_LOCATIONS[loc_key]

            # Assign office
            country_offices = [o for o in self.offices if o["country"] == country]
            office = self.rnd.choice(country_offices) if country_offices else self.rnd.choice(self.offices)

            # Device
            device_id = f"dev_{i:05d}"

            # Card info
            card_type = self.rnd.choice(["VI", "MC", "AX"])
            card_bin = self.rnd.choice(CARD_BINS[card_type])
            card_last4 = f"{self.rnd.randint(1000, 9999)}"

            # Usual destinations (2-4 airports)
            home_airports = [k for k, v in AIRPORTS.items() if v[0] == country]
            other_airports = [k for k in AIRPORTS.keys() if k not in home_airports]
            usual_dests = self.rnd.sample(other_airports, min(4, len(other_airports)))

            user = UserProfile(
                user_id=f"agent_{i:04d}",
                user_sign=f"{self.rnd.randint(1000, 9999)}{country}",
                home_location=loc_key,
                country=country,
                city=city,
                lat=lat,
                lon=lon,
                preferred_device=device_id,
                preferred_browser=self.rnd.choice(USER_AGENTS),
                card_country=country,
                card_bin=card_bin,
                card_last4=card_last4,
                typical_booking_amount=self.rnd.gauss(600, 200),
                booking_frequency=self.rnd.uniform(0.3, 1.5),  # 0.3 to 1.5 per week
                work_hours=(8, 18),
                usual_destinations=usual_dests,
                office_id=office["office_id"],
            )
            self.users.append(user)

    def _random_ip(self, country: str, is_vpn: bool = False) -> str:
        """Generate IP address."""
        if is_vpn:
            # VPN typically exits through NL or DE
            prefixes = IP_RANGES.get("NL", ["145.131."])
        else:
            prefixes = IP_RANGES.get(country, IP_RANGES["US"])
        prefix = self.rnd.choice(prefixes)
        return f"{prefix}{self.rnd.randint(1, 254)}.{self.rnd.randint(1, 254)}"

    def _generate_pnr(self) -> str:
        """Generate PNR."""
        return "".join(self.rnd.choices(string.ascii_uppercase + string.digits, k=6))

    def _generate_ticket_number(self, carrier: str) -> str:
        """Generate ticket number."""
        prefix_map = {
            "AF": "057", "KL": "074", "BA": "125", "LH": "220", "LX": "724",
            "UA": "016", "DL": "006", "AA": "001", "EK": "176", "QR": "157",
            "TK": "235", "IB": "075", "AZ": "055", "SN": "082", "OS": "257", "SK": "117"
        }
        prefix = prefix_map.get(carrier, "999")
        return f"{prefix}-{self.rnd.randint(1000000, 9999999)}"

    def _generate_fop(self, user: UserProfile) -> Dict[str, Any]:
        """Generate Form of Payment."""
        fop = self.rnd.choice(FOP_TYPES)
        result = {"fop_type": fop["code"], "fop_name": fop["name"], "fop_subtype": ""}

        if fop["subtype"]:
            subtype = self.rnd.choice(fop["subtype"])
            result["fop_subtype"] = subtype

            if fop["code"] == "CC":
                result["card_last4"] = user.card_last4
                result["card_bin"] = user.card_bin

        return result

    def _generate_route(self, user: UserProfile) -> Tuple[str, str, List[Dict]]:
        """Generate flight route."""
        # Pick origin from user's country
        home_airports = [k for k, v in AIRPORTS.items() if v[0] == user.country]
        origin = self.rnd.choice(home_airports) if home_airports else self.rnd.choice(list(AIRPORTS.keys()))

        # Pick destination from usual destinations or random
        if user.usual_destinations and self.rnd.random() < 0.8:
            dest = self.rnd.choice(user.usual_destinations)
        else:
            dest = self.rnd.choice([k for k in AIRPORTS.keys() if k != origin])

        carrier = self.rnd.choice(CARRIERS)
        dep_time = datetime.now(UTC) + timedelta(hours=self.rnd.randint(24, 720))

        legs = [{
            "segment": 1,
            "carrier": carrier,
            "flight_no": f"{carrier}{self.rnd.randint(100, 9999):04d}",
            "origin": origin,
            "dest": dest,
            "dep_time": dep_time.isoformat().replace("+00:00", "Z"),
            "arr_time": (dep_time + timedelta(hours=self.rnd.uniform(1.5, 12))).isoformat().replace("+00:00", "Z"),
        }]

        return origin, dest, legs

    def _generate_normal_behavior(self):
        """Generate normal user behavior patterns."""
        current = self.start_date

        while current < self.end_date:
            is_weekday = current.weekday() < 5

            for user in self.users:
                # Activity probability
                activity_prob = 0.6 if is_weekday else 0.2
                if self.rnd.random() > activity_prob:
                    continue

                # Generate auth events for the day
                self._generate_user_auth_day(user, current)

                # Maybe generate a booking
                weekly_prob = user.booking_frequency / 7
                if self.rnd.random() < weekly_prob:
                    self._generate_user_booking_session(user, current)

            current += timedelta(days=1)

    def _generate_user_auth_day(self, user: UserProfile, date: datetime):
        """Generate auth events for a user on a given day."""
        start_hour, end_hour = user.work_hours

        # Morning login
        login_hour = self.rnd.randint(start_hour, min(start_hour + 2, 12))
        login_time = date.replace(hour=login_hour, minute=self.rnd.randint(0, 59), second=self.rnd.randint(0, 59),
                                  tzinfo=UTC)

        session_id = f"sess_{hashlib.md5(f'{user.user_id}{login_time}'.encode()).hexdigest()[:12]}"

        # Add small geo variation
        lat_var = self.rnd.gauss(0, 0.01)
        lon_var = self.rnd.gauss(0, 0.01)

        self.auth_events.append({
            "ts": login_time.isoformat().replace("+00:00", "Z"),
            "event_type": "auth_success",
            "user_id": user.user_id,
            "device_id": user.preferred_device,
            "ip": self._random_ip(user.country),
            "geo_lat": round(user.lat + lat_var, 4),
            "geo_lon": round(user.lon + lon_var, 4),
            "country": user.country,
            "city": user.city,
            "user_agent": user.preferred_browser,
            "session_id": session_id,
            "is_vpn": False,
            "is_fraud_indicator": False,
            "failure_reason": "",
        })

        # Session refreshes during the day
        for _ in range(self.rnd.randint(0, 2)):
            refresh_hour = self.rnd.randint(login_hour + 1, end_hour)
            refresh_time = date.replace(hour=refresh_hour, minute=self.rnd.randint(0, 59), tzinfo=UTC)

            self.auth_events.append({
                "ts": refresh_time.isoformat().replace("+00:00", "Z"),
                "event_type": "auth_success",
                "user_id": user.user_id,
                "device_id": user.preferred_device,
                "ip": self._random_ip(user.country),
                "geo_lat": round(user.lat + self.rnd.gauss(0, 0.01), 4),
                "geo_lon": round(user.lon + self.rnd.gauss(0, 0.01), 4),
                "country": user.country,
                "city": user.city,
                "user_agent": user.preferred_browser,
                "session_id": session_id,
                "is_vpn": False,
                "is_fraud_indicator": False,
                "failure_reason": "",
            })

    def _generate_user_booking_session(self, user: UserProfile, date: datetime,
                                       is_anomaly: bool = False,
                                       anomaly_location: Optional[Dict] = None,
                                       anomaly_amount: Optional[float] = None):
        """Generate a correlated auth + ticket booking session."""
        start_hour, end_hour = user.work_hours
        booking_hour = self.rnd.randint(start_hour + 1, end_hour - 1)
        booking_time = date.replace(hour=booking_hour, minute=self.rnd.randint(0, 59), second=self.rnd.randint(0, 59),
                                    tzinfo=UTC)

        # Auth event 5-45 minutes BEFORE booking
        auth_offset = self.rnd.randint(5, 45)
        auth_time = booking_time - timedelta(minutes=auth_offset)

        # Determine location
        if is_anomaly and anomaly_location:
            country = anomaly_location["country"]
            city = anomaly_location["city"]
            lat = anomaly_location["lat"]
            lon = anomaly_location["lon"]
            device_id = f"dev_{self.rnd.randint(90000, 99999)}"  # New device
            is_vpn = anomaly_location.get("is_vpn", False)
            user_agent = self.rnd.choice(USER_AGENTS)
        else:
            country = user.country
            city = user.city
            lat = user.lat + self.rnd.gauss(0, 0.01)
            lon = user.lon + self.rnd.gauss(0, 0.01)
            device_id = user.preferred_device
            is_vpn = False
            user_agent = user.preferred_browser

        ip = self._random_ip(country, is_vpn)
        session_id = f"sess_{hashlib.md5(f'{user.user_id}{auth_time}'.encode()).hexdigest()[:12]}"

        # Auth event
        self.auth_events.append({
            "ts": auth_time.isoformat().replace("+00:00", "Z"),
            "event_type": "auth_success",
            "user_id": user.user_id,
            "device_id": device_id,
            "ip": ip,
            "geo_lat": round(lat, 4),
            "geo_lon": round(lon, 4),
            "country": country,
            "city": city,
            "user_agent": user_agent,
            "session_id": session_id,
            "is_vpn": is_vpn,
            "is_fraud_indicator": is_anomaly,
            "failure_reason": "",
        })

        # Generate ticket event
        origin, dest, legs = self._generate_route(user)
        carrier = legs[0]["carrier"] if legs else self.rnd.choice(CARRIERS)
        pnr = self._generate_pnr()
        tkt_number = self._generate_ticket_number(carrier)

        # Amount
        base_amount = anomaly_amount if anomaly_amount else user.typical_booking_amount
        amount = max(100, base_amount + self.rnd.gauss(0, base_amount * 0.15))

        # FoP
        fop = self._generate_fop(user)

        # Advance hours
        advance_hours = self.rnd.uniform(24, 720)
        if is_anomaly:
            advance_hours = self.rnd.uniform(4, 72)  # Short advance for anomalies

        # Card country (might mismatch for anomaly)
        card_country = user.card_country
        if is_anomaly and self.rnd.random() < 0.3:
            # Mismatched card country
            other_countries = [c for c in ["US", "GB", "DE", "FR"] if c != user.card_country]
            card_country = self.rnd.choice(other_countries)

        # Booking event
        self.ticket_events.append({
            "ts": booking_time.isoformat().replace("+00:00", "Z"),
            "event_type": "booking",
            "user_id": user.user_id,
            "device_id": device_id,
            "ip": ip,
            "geo_lat": round(lat, 4),
            "geo_lon": round(lon, 4),
            "country": country,
            "city": city,
            "office_id": user.office_id,
            "user_sign": user.user_sign,
            "organization": self.rnd.choice(["TA_GLOBAL", "TA_LOCAL", "OTA", "Corporate"]),
            "pnr": pnr,
            "carrier": carrier,
            "origin": origin,
            "dest": dest,
            "tkt_number": "",
            "status": "confirmed",
            "pos_country": country,
            "card_country": card_country,
            "card_hash": hashlib.sha256(f"card|{user.user_id}".encode()).hexdigest()[:16],
            "advance_hours": round(advance_hours, 1),
            "stay_nights": self.rnd.choice([0, 1, 2, 3, 5, 7, 14]),
            "legs": json.dumps(legs, ensure_ascii=False),
            "amount": round(amount, 2),
            "currency": self.rnd.choice(CURRENCIES),
            "fop_type": fop["fop_type"],
            "fop_name": fop["fop_name"],
            "fop_subtype": fop.get("fop_subtype", ""),
            "card_last4": fop.get("card_last4", ""),
            "card_bin": fop.get("card_bin", ""),
            "is_fraud_indicator": is_anomaly,
            "failure_reason": "",
        })

        # Ticket issuance (2-30 min after booking)
        issue_time = booking_time + timedelta(minutes=self.rnd.randint(2, 30))
        self.ticket_events.append({
            "ts": issue_time.isoformat().replace("+00:00", "Z"),
            "event_type": "tkt_issued",
            "user_id": user.user_id,
            "device_id": device_id,
            "ip": ip,
            "geo_lat": round(lat, 4),
            "geo_lon": round(lon, 4),
            "country": country,
            "city": city,
            "office_id": user.office_id,
            "user_sign": user.user_sign,
            "organization": self.rnd.choice(["TA_GLOBAL", "TA_LOCAL", "OTA", "Corporate"]),
            "pnr": pnr,
            "carrier": carrier,
            "origin": origin,
            "dest": dest,
            "tkt_number": tkt_number,
            "status": "issued",
            "pos_country": country,
            "card_country": card_country,
            "card_hash": hashlib.sha256(f"card|{user.user_id}".encode()).hexdigest()[:16],
            "advance_hours": round(advance_hours, 1),
            "stay_nights": self.rnd.choice([0, 1, 2, 3, 5, 7, 14]),
            "legs": json.dumps(legs, ensure_ascii=False),
            "amount": round(amount, 2),
            "currency": self.rnd.choice(CURRENCIES),
            "fop_type": fop["fop_type"],
            "fop_name": fop["fop_name"],
            "fop_subtype": fop.get("fop_subtype", ""),
            "card_last4": fop.get("card_last4", ""),
            "card_bin": fop.get("card_bin", ""),
            "is_fraud_indicator": is_anomaly,
            "failure_reason": "",
        })

    def _inject_anomalies(self):
        """Inject anomaly scenarios."""
        # Select users for anomalies (5-7 users)
        num_anomalous = min(self.rnd.randint(5, 7), len(self.users))
        anomalous_users = self.rnd.sample(self.users, num_anomalous)

        # Anomalies in last 2 weeks
        anomaly_window_start = self.end_date - timedelta(days=14)

        for i, user in enumerate(anomalous_users):
            scenario_type = i % 5
            anomaly_day = anomaly_window_start + timedelta(days=self.rnd.randint(0, 13))

            if scenario_type == 0:
                self._inject_account_takeover(user, anomaly_day)
            elif scenario_type == 1:
                self._inject_credential_stuffing(user, anomaly_day)
            elif scenario_type == 2:
                self._inject_velocity_abuse(user, anomaly_day)
            elif scenario_type == 3:
                self._inject_impossible_travel(user, anomaly_day)
            else:
                self._inject_payment_mismatch(user, anomaly_day)

    def _inject_account_takeover(self, user: UserProfile, date: datetime):
        """Account takeover:  new device, new location, password reset, high-value booking."""
        # Pick foreign location
        foreign_locs = [v for k, v in AGENT_LOCATIONS.items() if v[0] != user.country]
        if not foreign_locs:
            foreign_locs = [("NL", "Amsterdam", 52.37, 4.89)]
        foreign = self.rnd.choice(foreign_locs)
        attack_loc = {"country": foreign[0], "city": foreign[1], "lat": foreign[2], "lon": foreign[3], "is_vpn": True}

        new_device = f"dev_{self.rnd.randint(90000, 99999)}"

        # Failed login attempts
        for attempt in range(self.rnd.randint(2, 4)):
            fail_time = date.replace(hour=3, minute=self.rnd.randint(0, 20) + attempt * 5, tzinfo=UTC)
            self.auth_events.append({
                "ts": fail_time.isoformat().replace("+00:00", "Z"),
                "event_type": "auth_failure",
                "user_id": user.user_id,
                "device_id": new_device,
                "ip": self._random_ip(attack_loc["country"], is_vpn=True),
                "geo_lat": round(attack_loc["lat"] + self.rnd.gauss(0, 0.01), 4),
                "geo_lon": round(attack_loc["lon"] + self.rnd.gauss(0, 0.01), 4),
                "country": attack_loc["country"],
                "city": attack_loc["city"],
                "user_agent": self.rnd.choice(USER_AGENTS),
                "session_id": "",
                "is_vpn": True,
                "is_fraud_indicator": True,
                "failure_reason": "invalid_password",
            })

        # Password reset
        reset_time = date.replace(hour=3, minute=40, tzinfo=UTC)
        self.auth_events.append({
            "ts": reset_time.isoformat().replace("+00:00", "Z"),
            "event_type": "password_reset",
            "user_id": user.user_id,
            "device_id": new_device,
            "ip": self._random_ip(attack_loc["country"], is_vpn=True),
            "geo_lat": round(attack_loc["lat"], 4),
            "geo_lon": round(attack_loc["lon"], 4),
            "country": attack_loc["country"],
            "city": attack_loc["city"],
            "user_agent": self.rnd.choice(USER_AGENTS),
            "session_id": "",
            "is_vpn": True,
            "is_fraud_indicator": True,
            "failure_reason": "",
        })

        # High-value booking
        self._generate_user_booking_session(
            user,
            date.replace(hour=4, tzinfo=UTC),
            is_anomaly=True,
            anomaly_location=attack_loc,
            anomaly_amount=user.typical_booking_amount * 4
        )

        self.anomaly_scenarios.append(AnomalyScenario(
            name="Account Takeover",
            user_id=user.user_id,
            date=date.strftime("%Y-%m-%d"),
            description=f"Failed logins from {attack_loc['city']}, password reset, high-value booking"
        ))

    def _inject_credential_stuffing(self, user: UserProfile, date: datetime):
        """Credential stuffing: multiple failed logins from different IPs."""
        locations = self.rnd.sample(list(AGENT_LOCATIONS.values()), 4)

        for i, loc in enumerate(locations):
            fail_time = date.replace(hour=2, minute=i * 3, second=self.rnd.randint(0, 30), tzinfo=UTC)
            self.auth_events.append({
                "ts": fail_time.isoformat().replace("+00:00", "Z"),
                "event_type": "auth_failure",
                "user_id": user.user_id,
                "device_id": f"dev_bot_{self.rnd.randint(10000, 99999)}",
                "ip": self._random_ip(loc[0], is_vpn=True),
                "geo_lat": round(loc[2], 4),
                "geo_lon": round(loc[3], 4),
                "country": loc[0],
                "city": loc[1],
                "user_agent": self.rnd.choice(USER_AGENTS),
                "session_id": "",
                "is_vpn": True,
                "is_fraud_indicator": True,
                "failure_reason": "invalid_password",
            })

        # One succeeds
        success_loc = locations[-1]
        attack_loc = {"country": success_loc[0], "city": success_loc[1], "lat": success_loc[2], "lon": success_loc[3],
                      "is_vpn": True}

        self._generate_user_booking_session(
            user,
            date.replace(hour=2, minute=20, tzinfo=UTC),
            is_anomaly=True,
            anomaly_location=attack_loc,
            anomaly_amount=user.typical_booking_amount * 2.5
        )

        self.anomaly_scenarios.append(AnomalyScenario(
            name="Credential Stuffing",
            user_id=user.user_id,
            date=date.strftime("%Y-%m-%d"),
            description=f"Multiple failed logins from different IPs, then success"
        ))

    def _inject_velocity_abuse(self, user: UserProfile, date: datetime):
        """Velocity abuse: many bookings in short time."""
        num_bookings = self.rnd.randint(6, 10)

        # Start time for the velocity abuse
        start_time = date.replace(hour=10, minute=5, tzinfo=UTC)

        for i in range(num_bookings):
            # Use timedelta to properly handle time increments
            booking_time = start_time + timedelta(minutes=i * 10)
            self._generate_user_booking_session(
                user,
                booking_time,
                is_anomaly=True,
                anomaly_amount=self.rnd.uniform(300, 800)
            )

        self.anomaly_scenarios.append(AnomalyScenario(
            name="Velocity Abuse",
            user_id=user.user_id,
            date=date.strftime("%Y-%m-%d"),
            description=f"{num_bookings} bookings in ~{num_bookings * 10} minutes"
        ))

    def _inject_impossible_travel(self, user: UserProfile, date: datetime):
        """Impossible travel: login from distant locations in short time."""
        # First login from home
        home_time = date.replace(hour=14, minute=0, tzinfo=UTC)
        session_id = f"sess_{hashlib.md5(f'{user.user_id}{home_time}'.encode()).hexdigest()[:12]}"

        self.auth_events.append({
            "ts": home_time.isoformat().replace("+00:00", "Z"),
            "event_type": "auth_success",
            "user_id": user.user_id,
            "device_id": user.preferred_device,
            "ip": self._random_ip(user.country),
            "geo_lat": round(user.lat, 4),
            "geo_lon": round(user.lon, 4),
            "country": user.country,
            "city": user.city,
            "user_agent": user.preferred_browser,
            "session_id": session_id,
            "is_vpn": False,
            "is_fraud_indicator": True,
            "failure_reason": "",
        })

        # Second login from distant location 30 minutes later
        distant_locs = [v for k, v in AGENT_LOCATIONS.items() if v[0] != user.country]
        distant = self.rnd.choice(distant_locs)
        distant_loc = {"country": distant[0], "city": distant[1], "lat": distant[2], "lon": distant[3], "is_vpn": False}

        self._generate_user_booking_session(
            user,
            date.replace(hour=14, minute=30, tzinfo=UTC),
            is_anomaly=True,
            anomaly_location=distant_loc,
            anomaly_amount=user.typical_booking_amount * 2
        )

        self.anomaly_scenarios.append(AnomalyScenario(
            name="Impossible Travel",
            user_id=user.user_id,
            date=date.strftime("%Y-%m-%d"),
            description=f"Login from {user.city} then {distant_loc['city']} 30 min later"
        ))

    def _inject_payment_mismatch(self, user: UserProfile, date: datetime):
        """Payment mismatch: card country doesn't match."""
        # Normal location but mismatched card
        self._generate_user_booking_session(
            user,
            date.replace(hour=11, tzinfo=UTC),
            is_anomaly=True,
            anomaly_amount=user.typical_booking_amount * 3
        )

        # Override the last ticket event's card_country
        if self.ticket_events:
            mismatched = [c for c in ["US", "GB", "DE", "BR", "SG"] if c != user.card_country]
            self.ticket_events[-1]["card_country"] = self.rnd.choice(mismatched)
            self.ticket_events[-2]["card_country"] = self.ticket_events[-1]["card_country"]

        self.anomaly_scenarios.append(AnomalyScenario(
            name="Payment Mismatch",
            user_id=user.user_id,
            date=date.strftime("%Y-%m-%d"),
            description=f"Card country mismatch with user's usual country ({user.card_country})"
        ))


def save_scenario_summary(scenarios: List[AnomalyScenario], output_dir: str):
    """Save anomaly scenarios for reference."""
    summary = {
        "generated_at": datetime.now(UTC).isoformat(),
        "total_scenarios": len(scenarios),
        "scenarios": [
            {
                "name": s.name,
                "user_id": s.user_id,
                "date": s.date,
                "description": s.description,
            }
            for s in scenarios
        ]
    }

    summary_path = os.path.join(output_dir, "anomaly_scenarios.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n📋 Anomaly scenarios saved to:  {summary_path}")
    print("\n🎯 DEMO ANOMALY SCENARIOS (use these in your demo):")
    print("=" * 65)
    for s in scenarios:
        print(f"  • [{s.name}] {s.user_id} on {s.date}")
        print(f"    → {s.description}")
    print("=" * 65)


def main():
    parser = argparse.ArgumentParser(description="Generate LIZARD demo data")
    parser.add_argument("--out", type=str, default="./data", help="Output directory")
    parser.add_argument("--users", type=int, default=DEFAULT_USERS, help="Number of users")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Number of days")
    parser.add_argument("--seed", type=int, default=RANDOM_SEED, help="Random seed")
    args = parser.parse_args()

    output_dir = args.out
    os.makedirs(output_dir, exist_ok=True)

    # Generate data
    generator = DemoDataGenerator(
        num_users=args.users,
        num_days=args.days,
        seed=args.seed
    )
    auth_df, ticket_df = generator.generate_all()

    # Save CSVs
    auth_path = os.path.join(output_dir, "auth_events.csv")
    ticket_path = os.path.join(output_dir, "ticket_events.csv")

    auth_df.to_csv(auth_path, index=False)
    ticket_df.to_csv(ticket_path, index=False)

    print(f"\n✅ Data saved:")
    print(f"   • {auth_path} ({len(auth_df)} events)")
    print(f"   • {ticket_path} ({len(ticket_df)} events)")

    # Save scenario summary
    save_scenario_summary(generator.anomaly_scenarios, output_dir)

    # Print demo instructions
    print("\n" + "=" * 65)
    print("📖 DEMO WORKFLOW")
    print("=" * 65)
    print("""
1.START APPLICATION:
   Terminal 1: make api
   Terminal 2: make ui
   Open:  http://localhost:5173

2.UPLOAD DATA (Mapping Page):
   • Upload auth_events.csv → Select "Fraud Scenario - Auth Events" template
   • Upload ticket_events.csv → Select "Fraud Scenario - Ticket Events" template

3.CREATE JOINED VIEW (Workbench Page):
   Join auth and ticket events by user_id within 1-hour window: 

   • Base:  auth_events
   • Join: ticket_events  
   • Condition: user_id match AND ticket.ts within 1 hour after auth.ts
   • Save as: "user_sessions"

4.INVESTIGATE (Dashboard):
   • Timeline:  Look for anomaly spikes (use Advanced IForest analytics)
   • Map: Find impossible travel patterns  
   • Pie Chart: Group by user_id, color by anomaly
   • Bar Chart: Compare by country or office_id
   • Scatter:  Plot amount vs advance_hours

5.DRILL DOWN: 
   • Click anomalous points to open detail drawer
   • Use "Drill Down" button to filter to specific user
   • Follow breadcrumb navigation back up
""")


if __name__ == "__main__":
    main()