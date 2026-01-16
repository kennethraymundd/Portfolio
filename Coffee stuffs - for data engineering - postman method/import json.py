import json

#Read your JSON file (array format)
with open("response.json", "r") as f:
    data = json.load(f)  # this can be a single object or a list of objects

#Write NDJSON file
with open(r"C:\Users\kenne\Documents\Business Stuffs\Coffee stuffs - for data engineering - postman method\docs\response.ndjson", "w") as f:
    if isinstance(data, list):
        for item in data:
            f.write(json.dumps(item) + "\n")  # one object per line
    else:
        f.write(json.dumps(data) + "\n")  # in case it's a single object

print("✅ Converted to NDJSON: response.ndjson")

