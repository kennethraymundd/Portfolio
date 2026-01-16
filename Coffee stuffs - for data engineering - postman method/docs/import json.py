import json
from datetime import datetime

# ===== INPUT & OUTPUT FILES =====
input_file = r"C:\Users\kenne\Documents\Business Stuffs\Coffee stuffs - for data engineering - postman method\docs\response.json"
output_file = r"C:\Users\kenne\Documents\Business Stuffs\Coffee stuffs - for data engineering - postman method\docs\response2.ndjson"

# ===== Helper: Normalize dates =====
def normalize_date(d):
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%MZ", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(d, fmt).strftime("%Y-%m-%d")
        except:
            continue
    return "1970-01-01"

# ===== Optional mapping for string quantities =====
word_to_number = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5
}

# ===== Read JSON (array or NDJSON) =====
with open(input_file, "r") as f_in, open(output_file, "w") as f_out:
    try:
        data = json.load(f_in)  # JSON array
    except json.JSONDecodeError:
        f_in.seek(0)
        data = [json.loads(line) for line in f_in]  # NDJSON

    for obj in data:
        # ---- FIELD MAPPING (client → BigQuery schema) ----
        field_mapping = {
            "item_name": "item"
            # add more if needed:
            # "cust_id": "customer_id"
        }

        for src, dest in field_mapping.items():
            if src in obj:
                obj[dest] = obj.pop(src)

        # ---- Normalize & combine date + time ----
        date_norm = normalize_date(obj.get("date", "1970-01-01"))
        time_part = obj.get("time", "00:00:00")

        # BigQuery TIMESTAMP / DATETIME column
        obj["time"] = f"{date_norm} {time_part}"

        # ---- Clean quantity ----
        q = obj.get("quantity", 0)
        if isinstance(q, str):
            obj["quantity"] = word_to_number.get(q.lower(), 0)
        else:
            try:
                obj["quantity"] = int(q)
            except:
                obj["quantity"] = 0

        # ---- Clean price ----
        p = obj.get("price", 0)
        if p is None or (isinstance(p, (int, float)) and p < 0):
            obj["price"] = 0.0
        else:
            try:
                obj["price"] = float(p)
            except:
                obj["price"] = 0.0

        # ---- Remove only old date field ----
        obj.pop("date", None)

        # ---- Write NDJSON ----
        f_out.write(json.dumps(obj) + "\n")

print(f"✅ All done! BigQuery-ready NDJSON saved as: {output_file}")