import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

np.random.seed(42)
random.seed(42)

def random_ip():
    return ".".join(str(random.randint(1, 254)) for _ in range(4))

def random_device():
    return "dev-" + "".join(random.choices("abcdef0123456789", k=8))

def random_city():
    # (lat, lon, city)
    cities = [
        (48.8566, 2.3522, "Paris"),
        (51.5074, -0.1278, "London"),
        (40.7128, -74.0060, "New York"),
        (35.6895, 139.6917, "Tokyo"),
        (34.0522, -118.2437, "Los Angeles"),
        (55.7558, 37.6173, "Moscow"),
        (19.4326, -99.1332, "Mexico City"),
        (1.3521, 103.8198, "Singapore"),
        (52.52, 13.405, "Berlin"),
        (41.9028, 12.4964, "Rome"),
    ]
    return random.choice(cities)

def random_hour(user_id):
    # Most users log in 8-18h, but some have rare hours
    if user_id.startswith("user_rare"):
        return random.choice([0, 1, 2, 3, 4, 23])
    return random.randint(8, 18)

def generate_events(n_users=100, days=30, events_per_user=200):
    rows = []
    base_time = datetime.now() - timedelta(days=days)
    for u in range(n_users):
        user_id = f"user_{u}"
        # Some users will have rare login hours
        if u % 10 == 0:
            user_id = f"user_rare_{u}"
        # Some users will have a "fraud burst" day
        fraud_day = base_time + timedelta(days=random.randint(0, days-1))
        for e in range(events_per_user):
            ts = base_time + timedelta(days=random.randint(0, days-1), hours=random_hour(user_id), minutes=random.randint(0,59))
            event_type = "login_success"
            anomaly_types = []
            # 5% of events are failures (spikes)
            if random.random() < 0.05:
                event_type = "login_fail"
                if random.random() < 0.5:
                    anomaly_types.append("spike_failure")
            # Normal device/IP
            device_id = random_device()
            ip = random_ip()
            # 10% of users get a new device burst
            if u % 15 == 0 and random.random() < 0.2:
                device_id = f"fraud_dev_{random.randint(1,5)}"
                anomaly_types.append("new_device")
            # 10% of users get a new IP burst
            if u % 12 == 0 and random.random() < 0.2:
                ip = f"fraud_ip_{random.randint(1,5)}"
                anomaly_types.append("new_ip")
            # 5% of events are from rare devices/IPs globally
            if random.random() < 0.05:
                device_id = f"rare_dev_{random.randint(100,999)}"
                ip = f"rare_ip_{random.randint(100,999)}"
                anomaly_types.append("rare_device_ip")
            # Impossible travel: same user, far cities in short time
            if u % 20 == 0 and random.random() < 0.1:
                lat1, lon1, city1 = random_city()
                lat2, lon2, city2 = random_city()
                geo_lat, geo_lon, city = (lat2, lon2, city2)
                anomaly_types.append("impossible_travel")
            else:
                geo_lat, geo_lon, city = random_city()
            # Clustered fraud: multiple users, same device/IP, same hour
            if e % 50 == 0 and random.random() < 0.5:
                device_id = "cluster_dev"
                ip = "cluster_ip"
                anomaly_types.append("clustered_fraud")
            # Unusual hour
            hour = ts.hour
            if user_id.startswith("user_rare") and hour in [0,1,2,3,4,23]:
                anomaly_types.append("unusual_hour")
            # Compose row
            rows.append(dict(
                ts=ts,
                user_id=user_id,
                event_type=event_type,
                device_id=device_id,
                ip=ip,
                geo_lat=geo_lat,
                geo_lon=geo_lon,
                city=city,
                anomaly_types=";".join(anomaly_types)
            ))
    df = pd.DataFrame(rows)
    df = df.sort_values("ts")
    return df

if __name__ == "__main__":
    df = generate_events(n_users=200, days=60, events_per_user=300)
    df.to_csv("synthetic_events.csv", index=False)
    print("Generated", len(df), "events.")
    print("Sample anomalies:")
    print(df[df.anomaly_types != ""].sample(10))