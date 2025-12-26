import os


api = os.environ.get("LLMWhisperer_API_Key")

# import os
# import time
# from unstract.llmwhisper import LLMWhispererClientV2


# client = LLMWhispererClientV2(
#     base_url='https://llmwhisperer-api.us-central.unstract.com/api/v2',
#     api_key=api
# )

# result = client.whisper(file_path=r'dataset\UCO_1.pdf')

# while True:
#     status = client.whisper_status(whisper_hash=result['whisper_hash'])
#     if status['status'] == 'processed':
#         resultx = client.whisper_retrieve(whisper_hash=result['whisper_hash'])
#         break
#     time.sleep(5)

# extracted_text = resultx['extraction']['result_text']

# print(extracted_text)


import time
from unstract.llmwhisperer import LLMWhispererClientV2
from unstract.llmwhisperer.client_v2 import LLMWhispererClientException
print('hi')
# Provide the base URL and API key explicitly
client = LLMWhispererClientV2(base_url="https://llmwhisperer-api.us-central.unstract.com/api/v2", api_key=api)

result = client.whisper(file_path=r'dataset\AllahabadBank_1.pdf',mode="table",                  # Focus on form/table extraction
    output_mode="layout_preserving")

while True:
    status = client.whisper_status(whisper_hash=result['whisper_hash'])
    if status['status'] == 'processed':
        resultx = client.whisper_retrieve(whisper_hash=result['whisper_hash'])
        break
    time.sleep(5)

extracted_text = resultx['extraction']['result_text']

# print(extracted_text)


import pandas as pd
import re

def extract_transaction_table(text):
    lines = text.splitlines()

    transactions = []
    capture = False

    for line in lines:
        line = line.rstrip()

        # Start capturing after header
        if re.search(r"\|\s*Date\s*\|\s*Transaction Details", line):
            capture = True
            continue

        # Stop if section ends
        if capture and line.strip().startswith("ACCOUNT"):
            break

        # Skip borders and empty lines
        if not capture:
            continue
        if line.strip().startswith("+") or line.strip() == "":
            continue

        # Must be a table row
        if line.startswith("|"):
            cols = [c.strip() for c in line.split("|")[1:-1]]

            if len(cols) == 5:
                date, details, debit, credit, balance = cols

                transactions.append({
                    "Date": date,
                    "Transaction Details": details,
                    "Debit": debit,
                    "Credit": credit,
                    "Balance": balance
                })

    return pd.DataFrame(transactions)



# -------------------------------
# Your extracted text
# -------------------------------
df = extract_transaction_table(extracted_text)

# Save to Excel
df.to_excel("alhabad_bank_llmwhishperer.xlsx", index=False)

print("alhabad_bank_llmwhishperer.xlsx")
print(df.head())
