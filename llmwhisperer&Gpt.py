import time
import json
import re
import pandas as pd
from unstract.llmwhisperer import LLMWhispererClientV2
from unstract.llmwhisperer.client_v2 import LLMWhispererClientException
from openai import OpenAI
import os

# ==============================
# CONFIG
# ==============================
LLMWHISPERER_API_KEY = os.environ.get("LLMWhisperer_API_Key_Ath")
OPENAI_API_KEY = os.environ.get("OpenAI_API_Key")
PDF_PATH = r"dataset\IndianBank_1.pdf"
OUTPUT_EXCEL = r"output\IndianBank_1_updated.xlsx"

# ==============================
# STEP 1: Extract text using LLMWhisperer
# ==============================
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
# print(extracted_text)

# ==============================
# STEP 2: Build LLM Prompt
# ==============================
SYSTEM_PROMPT = """

You are a bank statement transaction table extractor.

The document contains ASCII tables using '|' characters.
Some transaction rows are split across multiple physical lines due to long narration text.

IMPORTANT MERGE RULE:
- If a row has text ONLY in narration/description-related columns
  AND other columns like Date, Amount, Balance are empty,
  THEN this row is a CONTINUATION of the previous transaction.
- Such rows MUST be merged into the previous row by appending the text
  to the narration/description column with a space.

Your task:
1. Locate the MAIN transaction table.
2. Identify the HEADER ROW exactly as written.
3. Extract ONLY real transaction rows.
4. Merge split/continued rows into their correct parent transaction.
5. Merge tables if they continue across pages.

Strictly ignore:
- Account summary sections
- Opening balance / Closing balance
- B/F, C/F, Balance Forward, Carry Forward
- Total, Subtotal, Grand Total

Rules:
- Do NOT invent or rename headers.
- Preserve column order.
- Each final row must represent exactly ONE transaction.
- Output STRICT VALID JSON only.
- No explanations, no markdown.

"""


def build_prompt(text):
    return f"""
Below is text extracted from a bank statement.

Important:
- The transaction table uses '|' separated rows.
- Some transactions have narration split into multiple rows.
- A continuation row has empty Date / Amount / Balance columns.
- Such rows must be merged into the previous transaction.

Instructions:
- Find the MAIN transaction table.
- Identify the header row.
- Use that header to extract transactions.
- Merge continuation rows into the previous row.
- Skip balance forward, totals, summaries.

Return JSON EXACTLY in this format:

{{
  "headers": [...],
  "rows": [
    [...],
    [...]
  ]
}}

Text:
{text}
"""


def extract_json_from_llm(text):
    """
    Removes markdown code fences if present and returns raw JSON string
    """
    text = text.strip()

    # Remove ```json or ``` if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text)
        text = re.sub(r"```$", "", text)

    return text.strip()

# ==============================
# STEP 3: Call GPT-4o
# ==============================
print("Sending text to GPT-4o...")

openai_client = OpenAI(api_key=OPENAI_API_KEY)

response = openai_client.chat.completions.create(
    model="gpt-4o",
    temperature=0,
    messages=[
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": build_prompt(extracted_text)}
    ]
)

content = response.choices[0].message.content.strip()

# ==============================
# STEP 4: Parse JSON safely
# ==============================
try:
    clean_content = extract_json_from_llm(content)
    llm_output = json.loads(clean_content)
except json.JSONDecodeError:
    raise ValueError("GPT output is not valid JSON:\n" + content)

headers = llm_output.get("headers")
rows = llm_output.get("rows")
if not headers:
    raise RuntimeError("LLM could not identify a header row.")

if not rows:
    raise RuntimeError("Header found but no transaction rows extracted.")

# ==============================
# STEP 5: Convert to Excel
# ==============================
df = pd.DataFrame(rows, columns=headers)
df.to_excel(OUTPUT_EXCEL, index=False)

print(f"✅ Saved clean transaction table to: {OUTPUT_EXCEL}")
print(df.head())