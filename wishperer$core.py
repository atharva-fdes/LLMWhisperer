import re
import pandas as pd
import time
import os
# ---------------------------------------------------
# 1. Parse ASCII table into raw rows
# ---------------------------------------------------

from unstract.llmwhisperer import LLMWhispererClientV2
from unstract.llmwhisperer.client_v2 import LLMWhispererClientException

# ==============================
# CONFIG
# ==============================
# LLMWHISPERER_API_KEY = os.environ.get("LLMWhisperer_API_Key_Abu") #abu
LLMWHISPERER_API_KEY = os.environ.get("LLMWhisperer_API_Key_Ath") #atharva

PDF_PATH = r"dataset\ICICI_1.pdf"
OUTPUT_EXCEL = r"output Core\ICICI_1_updated.xlsx"
print("Starting LLMWhisperer extraction...")

whisper_client = LLMWhispererClientV2(
    base_url="https://llmwhisperer-api.us-central.unstract.com/api/v2",
    api_key=LLMWHISPERER_API_KEY
)

result = whisper_client.whisper(
    file_path=PDF_PATH,
    mode="table",
    output_mode="layout_preserving"
)

while True:
    status = whisper_client.whisper_status(
        whisper_hash=result["whisper_hash"]
    )
    if status["status"] == "processed":
        resultx = whisper_client.whisper_retrieve(
            whisper_hash=result["whisper_hash"]
        )
        break
    time.sleep(5)

extracted_text = resultx["extraction"]["result_text"]
print("Text extraction completed.")




def parse_ascii_table(text):
    rows = []

    for line in text.splitlines():
        line = line.rstrip()

        # Skip borders
        if not line.startswith("|"):
            continue
        if set(line.replace("|", "").strip()) in [{"-"}, {"="}, set()]:
            continue

        cols = [c.strip() for c in line.split("|")[1:-1]]
        rows.append(cols)

    return rows


# ---------------------------------------------------
# 2. Identify header row dynamically
# ---------------------------------------------------

def detect_header(rows):
    for i, row in enumerate(rows):
        joined = " ".join(row).lower()
        if "date" in joined and ("balance" in joined or "amount" in joined):
            return i, row
    raise ValueError("Header row not found")


# ---------------------------------------------------
# 3. Identify important column indices
# ---------------------------------------------------

def map_columns(headers):
    col_map = {}

    for i, h in enumerate(headers):
        hl = h.lower()
        if "date" in hl and "tran" not in hl:
            col_map["date"] = i
        elif any(k in hl for k in ["narration", "description", "details", "particular"]):
            col_map["desc"] = i
        elif "debit" in hl:
            col_map["debit"] = i
        elif "credit" in hl:
            col_map["credit"] = i
        elif "balance" in hl:
            col_map["balance"] = i

    return col_map


# ---------------------------------------------------
# 4. Merge split rows
# ---------------------------------------------------

def merge_split_rows(rows, col_map):
    merged = []
    prev = None

    for row in rows:
        row = row + [""] * (max(col_map.values()) + 1 - len(row))

        date = row[col_map.get("date", -1)].strip()
        bal = row[col_map.get("balance", -1)].strip()
        desc = row[col_map.get("desc", -1)].strip()

        # Continuation row logic
        # if prev and (not date or not bal) and desc:
        #     prev[col_map["desc"]] += " " + desc
        #     continue

        # Skip junk rows
        # junk = desc.lower()
        # if any(k in junk for k in ["b/f", "brought forward", "carry forward", "total"]):
        #     continue

        row_text = " ".join(row).lower()

        if prev and (not date or not bal) and desc and "total" not in row_text:
            prev[col_map["desc"]] += " " + desc
            continue

        if any(k in row_text for k in [
            "total",
            "subtotal",
            "grand total",
            "b/f",
            "brought forward",
            "c/f",
            "carry forward"
        ]):
            continue

        prev = row
        merged.append(row)

    return merged


# ---------------------------------------------------
# 5. Clean + normalize rows
# ---------------------------------------------------

def clean_transactions(rows, headers):
    clean = []

    for r in rows:
        # 🔑 FINAL DEFENSIVE FILTER (ADD HERE)
        full_text = " ".join(r).lower()
        if any(k in full_text for k in ["total", "subtotal", "grand total", "b/f", "c/f", "carry forward"]):
            continue

        if not any(r):
            continue

        # Remove rows without any amount
        amt_present = any(re.search(r"\d", r[i]) for i in range(len(r)))
        if not amt_present:
            continue

        clean.append(dict(zip(headers, r)))

    return pd.DataFrame(clean)


# ---------------------------------------------------
# 6. MAIN FUNCTION
# ---------------------------------------------------

def extract_transactions_no_gpt(extracted_text, output_excel):
    raw_rows = parse_ascii_table(extracted_text)

    header_idx, headers = detect_header(raw_rows)
    data_rows = raw_rows[header_idx + 1 :]

    col_map = map_columns(headers)

    merged_rows = merge_split_rows(data_rows, col_map)

    df = clean_transactions(merged_rows, headers)

    df.to_excel(output_excel, index=False)
    print(f"Saved: {output_excel}")
    print(df.head())


# ---------------------------------------------------
# 7. USAGE
# ---------------------------------------------------

# extracted_text = resultx['extraction']['result_text']
extract_transactions_no_gpt(
    extracted_text,
    OUTPUT_EXCEL
)
