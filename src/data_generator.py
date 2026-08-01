"""Synthetic data generator.

Simulates 3 independent source systems describing overlapping customers:
  CRM    - clean-ish CSV, the system of record
  WEB    - JSON extract, noisy free-text entry, nicknames, format drift
  LEGACY - pipe-delimited mainframe-style extract, upper case, stale data

Planted issues (each is deliberately traceable so the pipeline's behaviour
can be validated against known ground truth):
  - the same person present in 2-3 systems with format variations (dedup targets)
  - mixed date formats, mixed phone formats (standardization targets)
  - missing mandatory fields, invalid emails, future DOBs (DQ targets)
  - conflicting attribute values across systems (survivorship targets)
  - a duplicate primary key within one source (DQ duplicate-key target)
"""
import csv
import json
import os
import random

random.seed(42)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LANDING = os.path.join(ROOT, "data", "landing")

# ---- ground-truth people. Each spawns 1-3 source records with variations ----
PEOPLE = [
    dict(name="Rahul Sharma",   email="rahul.sharma@gmail.com",  phone="9876543210", addr="12 MG Rd", city="Gurgaon",   state="Haryana",     dob="1990-04-12"),
    dict(name="Priya Nair",     email="priya.nair@yahoo.com",    phone="9812345678", addr="4 Palm St", city="Bangalore", state="Karnataka",   dob="1988-11-02"),
    dict(name="Amit Kumar Verma", email="amit.verma@outlook.com", phone="9900112233", addr="88 Sec 21", city="Noida",    state="Uttar Pradesh", dob="1985-06-30"),
    dict(name="Sneha Iyer",     email="sneha.iyer@gmail.com",    phone="9765432109", addr="7 Lake Rd", city="Bombay",    state="Maharashtra", dob="1992-01-25"),
    dict(name="Vikram Singh",   email="vikram.s@gmail.com",      phone="9654321098", addr="3 Park Ave", city="Delhi",    state="Delhi",       dob="1979-09-15"),
    dict(name="Ananya Das",     email="ananya.das@gmail.com",    phone="9543210987", addr="21 Hill Rd", city="Calcutta",  state="West Bengal", dob="1995-03-08"),
    dict(name="Karan Mehta",    email="karan.mehta@gmail.com",   phone="9432109876", addr="55 Ring Rd", city="Gurgaon",  state="Haryana",     dob="1991-12-19"),
    dict(name="Divya Reddy",    email="divya.reddy@gmail.com",   phone="9321098765", addr="9 Rose Apt", city="Hyderabad", state="Telangana",   dob="1993-07-04"),
    # FP-guard scenario: TWO DIFFERENT real people named Rohit Sharma in the
    # same city. Similar enough to become a candidate pair; conflicting DOB
    # must trigger REJECTED_FP_GUARD so they are never merged.
    dict(name="Rohit Sharma",   email="rohit.sharma88@gmail.com", phone="9888777666", addr="5 MG Rd",  city="Gurgaon",   state="Haryana",     dob="1988-05-10"),
]

NICKNAME = {"Rahul": "Rahul", "Amit Kumar": "Amit", "Vikram": "Vicky"}


def crm_rows():
    rows = []
    for i, p in enumerate(PEOPLE):
        rows.append({
            "cust_id": f"C{i+1:03d}",
            "full_name": p["name"],
            "email_addr": p["email"],
            "phone": "+91-" + p["phone"][:5] + "-" + p["phone"][5:],
            "addr_line": p["addr"],
            "city": p["city"],
            "state": p["state"],
            "country": "India",
            "dob": p["dob"],                       # ISO
            "updated_at": "2026-06-01",
        })
    # planted issues
    rows[3]["email_addr"] = "sneha.iyer[at]gmail.com"      # invalid email (DQ003)
    rows[5]["dob"] = "2031-03-08"                          # future DOB (DQ006)
    rows.append(dict(rows[6], updated_at="2026-06-15"))    # duplicate PK C007 (DQ007)
    return rows


def web_rows():
    rows = []
    picks = [0, 1, 2, 4, 6, 7]                             # overlap with CRM (dedup targets)
    for j, i in enumerate(picks):
        p = PEOPLE[i]
        first = p["name"].split()[0]
        name = p["name"].replace(first, NICKNAME.get(first, first)).lower()
        rows.append({
            "user_id": f"U{j+1:04d}",
            "name": " " + name + "  ",                      # messy whitespace + lowercase
            "contact_email": p["email"].upper(),
            "mobile": p["phone"],                           # bare 10-digit
            "street": p["addr"].replace("Rd", "Road"),
            "city_name": p["city"],
            "region": p["state"],
            "country_code": "IN",
            "birth_date": "/".join(reversed(p["dob"].split("-"))),   # dd/MM/yyyy
            "modified_ts": "2026-07-10",
        })
    # newer conflicting values (survivorship: recency should win for contact info)
    rows[0]["street"] = "121 Golf Course Road"              # Rahul moved
    rows[0]["city_name"] = "Gurgaon"
    rows[3]["mobile"] = "9111222333"                        # Vikram changed phone
    # Divya changed BOTH email and phone -> no deterministic key survives;
    # only probabilistic (name+address+dob) can link her -> lands in REVIEW queue
    rows[5]["contact_email"] = "divya.r@hotmail.com"
    rows[5]["mobile"] = "9000000001"
    # net-new person only in WEB
    rows.append({
        "user_id": "U9999", "name": "meera joshi", "contact_email": "meera.j@gmail.com",
        "mobile": "9222333444", "street": "2 Cross St", "city_name": "Pune",
        "region": "Maharashtra", "country_code": "IN", "birth_date": "14/02/1994",
        "modified_ts": "2026-07-12",
    })
    # a DIFFERENT Rohit Sharma (namesake, same city, different DOB/contacts):
    # candidate pair via name block, but DOB conflict -> must be FP-guard rejected
    rows.append({
        "user_id": "U9995", "name": "rohit sharma", "contact_email": "rohit.s.trader@gmail.com",
        "mobile": "9777666555", "street": "17 Mall Rd", "city_name": "Gurgaon",
        "region": "Haryana", "country_code": "IN", "birth_date": "22/09/1996",
        "modified_ts": "2026-07-11",
    })
    # missing mandatory name (DQ001) and no contact identifiers (DQ002)
    rows.append({"user_id": "U9998", "name": None, "contact_email": "ghost@web.com",
                 "mobile": None, "street": None, "city_name": None, "region": None,
                 "country_code": "IN", "birth_date": None, "modified_ts": "2026-07-12"})
    rows.append({"user_id": "U9997", "name": "no contact person", "contact_email": None,
                 "mobile": None, "street": "1 Nowhere Ln", "city_name": "Delhi", "region": "Delhi",
                 "country_code": "IN", "birth_date": "01/01/1990", "modified_ts": "2026-07-12"})
    return rows


def legacy_rows():
    rows = []
    picks = [0, 2, 3, 5]                                    # overlap subset
    for j, i in enumerate(picks):
        p = PEOPLE[i]
        rows.append({
            "CUST_NO": f"L{j+1:05d}",
            "CUST_NAME": p["name"].upper(),
            "EMAIL": "" if j == 2 else p["email"],
            "TEL": "0" + p["phone"],                        # trunk-prefixed
            "ADDR": p["addr"].upper(),
            "CITY": p["city"].upper(),
            "ST": p["state"].upper(),
            "CTRY": "IND",
            "BIRTH_DT": p["dob"].replace("-", ""),          # yyyyMMdd
            "LAST_UPD": "2024-01-20",                       # stale (timeliness WARN)
        })
    return rows


def main():
    for sub in ["crm", "web", "legacy"]:
        os.makedirs(os.path.join(LANDING, sub), exist_ok=True)

    crm = crm_rows()
    with open(os.path.join(LANDING, "crm", "crm_extract_20260715.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=crm[0].keys())
        w.writeheader()
        w.writerows(crm)

    with open(os.path.join(LANDING, "web", "web_extract_20260715.json"), "w") as f:
        for r in web_rows():
            f.write(json.dumps(r) + "\n")

    leg = legacy_rows()
    with open(os.path.join(LANDING, "legacy", "legacy_extract_20260715.dat"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=leg[0].keys(), delimiter="|")
        w.writeheader()
        w.writerows(leg)

    print(f"Generated: CRM={len(crm)}  WEB={len(web_rows())}  LEGACY={len(leg)} records")


if __name__ == "__main__":
    main()
