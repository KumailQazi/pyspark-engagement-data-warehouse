"""
data_generator.py
Generate synthetic data for demonstrating the pipeline.
Run: python data_generator.py --date 2024-01-15 --events 100000
"""

import argparse
import json
import random
from datetime import datetime, timedelta
from faker import Faker

random.seed(42)
Faker.seed(42)
fake = Faker()

def generate_playback_events(date_str, num_events):
    """Generate synthetic playback events."""
    base_time = datetime.strptime(date_str, "%Y-%m-%d")
    events = []
    
    for i in range(num_events):
        event_ts = base_time + timedelta(seconds=random.randint(0, 86399))
        server_ts = event_ts + timedelta(seconds=random.randint(0, 3600))  # Up to 1h delay
        
        event = {
            "event_id": f"evt_{fake.uuid4()}",
            "user_id": f"usr_{random.randint(1, 10000)}" if random.random() > 0.15 else None,  # 15% anonymous
            "device_id": f"dev_{random.randint(1, 5000)}",
            "session_id": f"sess_{random.randint(1, 50000)}",
            "content_id": f"cnt_{random.randint(1, 500)}",
            "event_type": random.choice(["play_start", "heartbeat", "pause", "seek", "play_end", "error"]),
            "event_ts": event_ts.isoformat(),
            "server_ts": server_ts.isoformat(),
            "position_sec": random.randint(0, 7200),
            "platform": random.choice(["ios", "android", "web", "tv", "firetv"]),
            "country_code": random.choice(["PK", "US", "GB", "IN", "AE"])
        }
        events.append(event)
    
    return events

def write_events_to_json(events, date_str):
    """Write events to partitioned JSON directory."""
    import os
    output_dir = f"raw/playback_events/event_date={date_str}"
    os.makedirs(output_dir, exist_ok=True)
    
    with open(f"{output_dir}/events.json", "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    
    print(f"Wrote {len(events)} events to {output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Event date (YYYY-MM-DD)")
    parser.add_argument("--events", type=int, default=100000, help="Number of events")
    args = parser.parse_args()
    
    events = generate_playback_events(args.date, args.events)
    write_events_to_json(events, args.date)
