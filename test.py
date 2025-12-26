import re
import pandas as pd
import time
# -----------------------------
# CONFIG
# -----------------------------
from unstract.llmwhisperer import LLMWhispererClientV2
from unstract.llmwhisperer.client_v2 import LLMWhispererClientException

# ==============================
# CONFIG
# ==============================
# LLMWHISPERER_API_KEY = "cr69D4qNSWsn6OcruJg8EP1YcKE4cexq69AqdS3kei4"
LLMWHISPERER_API_KEY = "hd5LRgi8ssWTyKHNPYh4jtIR-x3X6uNt2KOU5qCrKos" #atharva

PDF_PATH = r"dataset\ICICI_1.pdf"
OUTPUT_EXCEL = r"output Core\ICICI_1_updated2.xlsx"
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



REMOVE_KEYWORDS = [
    "TOTAL", "B/F", "Brought Forward", "Carry Forward",
    "SUMMARY", "OPENING BALANCE", "CLOSING BALANCE",
    "INTEREST", "TDS", "MICR", "ACCOUNT", "RTGS-REAL"
]

DATE_REGEX = re.compile(r"\d{2}[-/]\d{2}[-/]\d{2,4}")

# -----------------------------
# STEP 1: Parse ASCII table
# -----------------------------
def parse_ascii_table(text):
    rows = []
    for line in text.splitlines():
        line = line.rstrip()
        if not line.startswith("|"):
            continue
        if line.strip().startswith("+"):
            continue

        cols = [c.strip() for c in line.split("|")[1:-1]]
        rows.append(cols)
    return rows


# -----------------------------
# STEP 2: Detect header row
# -----------------------------
def detect_header(rows):
    for i, row in enumerate(rows):
        joined = " ".join(row).lower()
        if "date" in joined and ("balance" in joined or "amount" in joined):
            return i, row
    raise ValueError("Header row not found")


# -----------------------------
# STEP 3: Remove TOTAL / B/F rows
# -----------------------------
def is_removal_row(row):
    text = " ".join(row).upper()
    return any(keyword.upper() in text for keyword in REMOVE_KEYWORDS)


# -----------------------------
# STEP 4: Merge continuation rows
# -----------------------------
def merge_continuation_rows(rows, headers):
    merged = []
    prev = None

    date_idx = next(i for i, h in enumerate(headers) if "date" in h.lower())
    balance_idx = next(i for i, h in enumerate(headers) if "balance" in h.lower())

    for row in rows:
        # Skip TOTAL / SUMMARY rows completely
        if is_removal_row(row):
            continue

        date_val = row[date_idx].strip() if date_idx < len(row) else ""
        balance_val = row[balance_idx].strip() if balance_idx < len(row) else ""

        is_continuation = (
            prev is not None
            and not date_val
            and not balance_val
        )

        if is_continuation:
            # Append narration ONLY
            for i in range(len(row)):
                if row[i] and not prev[i]:
                    prev[i] = row[i]
                elif row[i] and i != date_idx and i != balance_idx:
                    prev[i] += " " + row[i]
        else:
            merged.append(row)
            prev = row

    return merged


# -----------------------------
# STEP 5: Clean & Normalize
# -----------------------------
def clean_table(headers, rows):
    clean_rows = []
    for row in rows:
        if len(row) < len(headers):
            row += [""] * (len(headers) - len(row))
        clean_rows.append(row[:len(headers)])
    return pd.DataFrame(clean_rows, columns=headers)


# -----------------------------
# MAIN PIPELINE
# -----------------------------
def extract_transactions_to_excel(extracted_text, output_file):
    rows = parse_ascii_table(extracted_text)

    header_idx, headers = detect_header(rows)
    data_rows = rows[header_idx + 1:]

    # Remove TOTAL/BF rows first
    data_rows = [r for r in data_rows if not is_removal_row(r)]

    # Merge continuation rows
    merged_rows = merge_continuation_rows(data_rows, headers)

    df = clean_table(headers, merged_rows)

    # Remove rows without date (extra safety)
    date_col = next(c for c in df.columns if "date" in c.lower())
    df = df[df[date_col].str.contains(DATE_REGEX, na=False)]

    df.to_excel(output_file, index=False)
    print(f"✅ Saved clean transactions to {output_file}")
    return df

df = extract_transactions_to_excel(
    extracted_text,
    "clean_bank_transactions.xlsx"
)
print(df.head())
