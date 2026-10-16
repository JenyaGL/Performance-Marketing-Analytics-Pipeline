import json
import os
import uuid
import random
from datetime import datetime, timedelta

# Configuration
NUM_CAMPAIGNS = 5
NUM_SESSIONS = 1000
DUPLICATE_RATE = 0.05
OUTPUT_DIR = os.environ.get("DATA_OUTPUT_DIR", "./")  # Set to your local path or Airflow staging directory
# Comma-separated faults to rehearse broken pulls; empty = clean output.
FAULTS = {f.strip() for f in os.environ.get("FAULTS", "").split(",") if f.strip()}
KNOWN_FAULTS = {"malformed_lines", "missing_key", "schema_drift", "empty_file"}

def generate_mock_pipeline_data():
    traffic_records = []
    event_records = []
    conversion_records = []

    # 1. Generate Ad Traffic (Top of Funnel)
    campaigns = [f"cmp_search_{i}" for i in range(NUM_CAMPAIGNS)]
    partners = ["google_ads", "meta_ads", "tiktok_ads", "affiliate_network"]
    
    # Pre-generate traffic clicks to map sessions against
    traffic_pool = []
    for _ in range(NUM_SESSIONS):
        traffic_id = f"trf_{uuid.uuid4().hex[:8]}"
        traffic_pool.append(traffic_id)
        
        traffic_records.append({
            "traffic_id": traffic_id,
            "partner_id": random.choice(partners),
            "campaign_id": random.choice(campaigns),
            "vertical": "auto_insurance",
            "traffic_source": random.choice(["search", "social", "display"]),
            "clicks": 1,
            "cost_usd": round(random.uniform(0.50, 5.00), 2),
            "timestamp": (datetime.utcnow() - timedelta(days=random.randint(0, 5))).isoformat() + "Z"
        })

    # 2. Generate Funnel Events (Mid-Funnel) & Conversions (Bottom-Funnel)
    for traffic_id in traffic_pool:
        session_id = f"sess_{uuid.uuid4().hex[:8]}"
        user_pseudo_id = f"usr_{random.randint(10000, 99999)}"
        variant = "treatment_v2" if random.random() > 0.5 else "control"
        
        base_time = datetime.utcnow() - timedelta(days=random.randint(0, 5))
        
        # Step 1: Landing View (100% of sessions)
        event_records.append({
            "event_id": f"evt_{uuid.uuid4().hex[:8]}",
            "session_id": session_id,
            "traffic_id": traffic_id,
            "user_pseudo_id": user_pseudo_id,
            "step_name": "landing_view",
            "step_number": 1,
            "variant_id": variant,
            "timestamp": base_time.isoformat() + "Z"
        })
        
        # Step 2: Form Start (65% progression)
        if random.random() < 0.65:
            t1 = base_time + timedelta(seconds=random.randint(5, 45))
            event_records.append({
                "event_id": f"evt_{uuid.uuid4().hex[:8]}",
                "session_id": session_id,
                "traffic_id": traffic_id,
                "user_pseudo_id": user_pseudo_id,
                "step_name": "form_start",
                "step_number": 2,
                "variant_id": variant,
                "timestamp": t1.isoformat() + "Z"
            })
            
            # Step 3: Form Submit (Treatment converts better)
            conv_rate = 0.55 if variant == "treatment_v2" else 0.40
            if random.random() < conv_rate:
                t2 = t1 + timedelta(seconds=random.randint(30, 120))
                event_records.append({
                    "event_id": f"evt_{uuid.uuid4().hex[:8]}",
                    "session_id": session_id,
                    "traffic_id": traffic_id,
                    "user_pseudo_id": user_pseudo_id,
                    "step_name": "form_submit",
                    "step_number": 3,
                    "variant_id": variant,
                    "timestamp": t2.isoformat() + "Z"
                })
                
                # Generate Conversion Record
                lead_status = random.choices(["accepted", "rejected", "qualified"], weights=[0.7, 0.2, 0.1])[0]
                
                # Simulate late-arriving conversions (10% arrive up to 48 hours later)
                if random.random() < 0.10:
                    conv_time = t2 + timedelta(hours=random.randint(12, 48))
                else:
                    conv_time = t2 + timedelta(seconds=random.randint(2, 10))

                conversion_records.append({
                    "conversion_id": f"cnv_{uuid.uuid4().hex[:8]}",
                    "session_id": session_id,
                    "lead_status": lead_status,
                    "buyer_id": random.choice(["buyer_A", "buyer_B", "buyer_C"]),
                    "revenue_usd": round(random.uniform(20.0, 85.0), 2) if lead_status == "accepted" else 0.0,
                    "partner_payout_usd": round(random.uniform(5.0, 15.0), 2),
                    "timestamp": conv_time.isoformat() + "Z"
                })

    # 3. Inject Edge Cases (Exact Duplicates to simulate Airflow retries)
    def inject_duplicates(records, rate):
        dupes = random.sample(records, int(len(records) * rate))
        return records + dupes

    traffic_records = inject_duplicates(traffic_records, DUPLICATE_RATE)
    event_records = inject_duplicates(event_records, DUPLICATE_RATE)
    conversion_records = inject_duplicates(conversion_records, DUPLICATE_RATE)

    # 4. Optional fault injection (FAULTS env var) to rehearse broken pulls
    unknown = FAULTS - KNOWN_FAULTS
    if unknown:
        raise SystemExit(f"Unknown FAULTS: {', '.join(sorted(unknown))}. Known: {', '.join(sorted(KNOWN_FAULTS))}")
    if "malformed_lines" in FAULTS:
        # A truncated line (cut-off API response) and a wrong-typed value: a small reject rate, batch still loads.
        traffic_records = traffic_records + [
            json.dumps(traffic_records[0])[:-15],
            {**traffic_records[1], "traffic_id": f"trf_{uuid.uuid4().hex[:8]}", "clicks": "abc"},
        ]
    if "missing_key" in FAULTS:
        broken = dict(event_records[0])
        broken.pop("event_id")
        event_records = event_records + [broken]
    if "schema_drift" in FAULTS:
        # Upstream added a field without notice: every line is rejected, so the load fails.
        conversion_records = [{**r, "device_type": "mobile"} for r in conversion_records]
    if "empty_file" in FAULTS:
        # The pull "succeeded" but returned nothing: the load must fail, not silently load 0 rows.
        conversion_records = []

    # 5. Write to NDJSON Files (raw strings are written as-is, to simulate malformed lines)
    def write_ndjson(filename, data):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(os.path.join(OUTPUT_DIR, f"{filename}.ndjson"), "w") as f:
            for record in data:
                f.write((record if isinstance(record, str) else json.dumps(record)) + "\n")
        print(f"Generated {len(data)} records for {filename}.ndjson")

    write_ndjson("ad_traffic", traffic_records)
    write_ndjson("funnel_events", event_records)
    write_ndjson("conversions", conversion_records)

if __name__ == "__main__":
    generate_mock_pipeline_data()