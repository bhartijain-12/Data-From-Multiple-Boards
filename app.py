import os, requests, json
from dotenv import load_dotenv
import pdfplumber  # pip install pdfplumber
from threading import Thread

load_dotenv()
API_KEY           = os.getenv("MONDAY_API_KEY")
BOARD_ID          = int(os.getenv("BOARD_ID"))
ITEM_ID           = int(os.getenv("ITEM_ID"))
COLUMN_FILE_ID    = os.getenv("COLUMN_FILE_ID")      # e.g. "file_column"
COLUMN_TARGET_IDS = json.loads(os.getenv("TARGET_COLUMNS_JSON"))  
# e.g.: {"order_id":"order_col", "units": "units_col", "price":"price_col", "feedback":"feedback_col"}

HEADERS = {
    "Authorization": API_KEY,
    "Content-Type": "application/json"
}
GQL_ENDPOINT = "https://api.monday.com/v2"

def get_pdf_public_url():
    q = f"""
    query {{
      items(ids: {ITEM_ID}) {{
        assets {{
          id
          public_url
          name
        }}
      }}
    }}
    """
    resp = requests.post(GQL_ENDPOINT, headers=HEADERS, json={"query": q})
    resp.raise_for_status()
    url = resp.json()["data"]["items"][0]["assets"][0]["public_url"]
    return url

def download_pdf(url: str) -> bytes:
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.content

def extract_table(pdf_bytes: bytes):
    rows = []
    import io
    pdf = io.BytesIO(pdf_bytes)
    with pdfplumber.open(pdf) as doc:
        for p in doc.pages:
            tbls = p.extract_tables()
            for t in tbls:
                if len(t) <= 1: continue
                header = t[0]
                for row in t[1:]:
                    rows.append(dict(zip(header, row)))
    return rows

def update_columns(rows):
    col_updates = {}
    # example: take first row and slice values into target columns
    if rows:
        rec = rows[0]
        for target_k, col_id in COLUMN_TARGET_IDS.items():
            val = rec.get(target_k, "")
            col_updates[col_id] = str(val)
    mutation = f"""
    mutation ($vals: JSON!) {{
      change_multiple_column_values(board_id: {BOARD_ID}, item_id:{ITEM_ID}, column_values: $vals) {{
        id
      }}
    }}
    """
    resp = requests.post(GQL_ENDPOINT, headers=HEADERS,
                         json={"query":mutation, "variables":{"vals":col_updates}})
    resp.raise_for_status()

def run_workflow():
    url = get_pdf_public_url()
    content = download_pdf(url)
    rows = extract_table(content)
    if not rows:
        print("❌ No table found in PDF.")
        return
    print("Extracted rows:", rows[:3])
    update_columns(rows)

if __name__ == "__main__":
    run_workflow()
