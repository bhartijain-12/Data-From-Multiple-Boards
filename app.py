import os
import json
import requests
from dotenv import load_dotenv
from flask import Flask, request ,jsonify
from threading import Thread
from fpdf import FPDF
import textwrap
import re
from datetime import datetime
import time

# Load environment variables
load_dotenv()

API_KEY = os.getenv("MONDAY_API_KEY")
BOARD_IDS = os.getenv("SOURCE_BOARD_IDS", "").split(",")
TARGET_BOARD_ID = os.getenv("TARGET_BOARD_ID")
TARGET_ITEM_ID = "2052301917"
TARGET_COLUMN_ID = "text_mktd466v"
board_id = 2052340887  
item_id = 2052855842
columns = ['long_text_mktf36f7', 'long_text_mktf4sss']
non_formatted_files_column_id = 'file_mktf24g0'
asia_pacific_sales_item_id = 2052855846


API_URL = "https://api.monday.com/v2"
HEADERS = {
    "Authorization": API_KEY,
    "Content-Type": "application/json"
}

app = Flask(__name__)

board_id_north = "2052884263"
board_id = 2052340887  
item_id = 2052855842
columns = ['long_text_mktf36f7', 'long_text_mktf4sss']


def fetch_board_data(board_id_north):
    print('inside this north america',flush=True)
    query = f"""
        query {{
          boards(ids: {board_id_north}) {{
            name
            columns {{
              id
              title
            }}
            items_page(limit: 20) {{
              items {{
                id
                name
               column_values {{
                                id
                                text
                                value
                                column {{
                                    title
                                    type
                                }}
                                
                }}
              }}
            }}
          }}
        }}
        """

    response = requests.post(API_URL, json={"query": query}, headers=HEADERS)
    response.raise_for_status()
    data = response.json()
    print('data-north',data,flush=True)
    
    if "errors" in data:
        raise Exception(f"Error from API: {data['errors']}")

    board = data["data"]["boards"][0]
    print('board---->',board,flush=True)

    fetch_monday_board_data(board_id,item_id,columns)

    parse_json = parse_monday_board_data(board)
    print('parse_json',parse_json,flush=True)

    file_path = create_pdf_from_json(parse_json)

    clear_file_column(board_id,item_id,non_formatted_files_column_id)

    upload_file(asia_pacific_sales_item_id,file_path,non_formatted_files_column_id)
    
    return board

def parse_monday_board_data(board_data):
    print('inside this parse monday data',flush=True)
    print('board-data----->',board_data,flush=True)
    parsed_items = []

    # Create dynamic column title to ID mapping from the fetched board data
    column_title_to_id = {col['title']: col['id'] for col in board_data['columns']}
    print('Available columns from board:', column_title_to_id, flush=True)

    for item in board_data["items_page"]["items"]:
        item_data = {"Order_ID": item["name"]}

        # Create column_id to value mapping
        column_values = {
            col_val["id"]: col_val.get("text", None)
            for col_val in item["column_values"]
        }

        # Process each column dynamically based on what's available in the board
        for col_title, col_id in column_title_to_id.items():
            # Skip the 'name' column since we're using item["name"] as "Order_ID"
            if col_id == 'name' or col_title == 'Product Name' or col_title == 'Status' or col_title == 'Target Date' or col_title == 'Lead Type' or col_title == 'Sales Price' or col_title == 'Selling Price' or col_title == 'Lead Score' or col_title == 'Lead Owner' or col_title == 'Manager':    
             continue

            value = column_values.get(col_id, None)
            
            # Apply formatting based on column title patterns
            # if any(date_keyword in col_title.lower() for date_keyword in ['date', 'created', 'close']):
            #     if value:
            #         try:
            #             value = value.strip('"')
            #             date_obj = datetime.strptime(value, "%Y-%m-%d")
            #             value = date_obj.strftime("%d-%m-%Y") if 'close' in col_title.lower() else date_obj.toordinal()
            #         except Exception as e:
            #             print(f"Date parsing failed for {col_title}: {value}, error: {e}", flush=True)
            #             value = None
            
            # elif any(numeric_keyword in col_title.lower() for numeric_keyword in ['age', 'sold', 'unit', 'price', 'cost', 'discount', 'revenue', 'score']):
            #     try:
            #         value = float(value) if value else 0
            #     except:
            #         value = 0
            
            # Use column title as the key in item_data
            item_data[col_title] = value

        parsed_items.append(item_data)
        print('parsed_items-->',parsed_items,flush=True)
    
    return parsed_items


def fetch_monday_board_data(board_id, item_id, column_ids=None):
    
    url = "https://api.monday.com/v2"
    headers = {
        "Authorization": API_KEY,
        "Content-Type": "application/json"
    }

    column_ids = column_ids or []
    print('column ids --->',column_ids,flush=True)
    column_id_string = ', '.join(f'"{cid}"' for cid in column_ids)
    print('ccolumn_id_string --->',column_id_string,flush=True)

    query = f"""
    query {{
        items(ids: {item_id}) {{
            name
            column_values(ids: [{column_id_string}]) {{
                id
                text
                column {{
                    title 
                }}
            }}
        }}
    }}
    """
    

    response = requests.post(url, headers=headers, json={'query': query})
    print('response0000000 --->',response,flush=True)
    
    if response.status_code == 200:
        data = response.json()
        print('data- insight--->',data,flush=True)
        if "errors" in data:
            print("GraphQL Errors:", data["errors"],flush=True)
            return None
        return data["data"]["items"][0]  # Only one item returned
    else:
        print(f"Request failed with status {response.status_code}: {response.text}",flush=True)
        return None

def upload_file(item_id, file_path, column_id):
    print('inside this upload column--->',flush=True)
    url = "https://api.monday.com/v2/file"
    headers = {
        "Authorization": API_KEY,
        "API-version": "2024-04"
    }
    
    query = """
    mutation add_file($file: File!, $itemId: ID!, $columnId: String!) {
      add_file_to_column (item_id: $itemId, column_id: $columnId, file: $file) {
        id
      }
    }
    """

    # Check file exists
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}",flush=True)
        return

    # Prepare file upload map and variables
    files = {
        '1': (os.path.basename(file_path), open(file_path, 'rb'), 'application/pdf')
    }

    payload = {
        "query": query,
        "variables": json.dumps({
            "file": None,
            "itemId": str(item_id),
            "columnId": column_id
        }),
        "map": json.dumps({
            "1": ["variables.file"]
        })
    }

    print("Uploading PDF...", flush=True)
    response = requests.post(url, headers=headers, data=payload, files=files)

    try:
        resp_json = response.json()
        print("Response-JSON:", json.dumps(resp_json, indent=2), flush=True)
        if "errors" in resp_json:
            print("GraphQL-Errors:", resp_json["errors"], flush=True)
        else:
            print("PDF uploaded successfully.", flush=True)
    except Exception as e:
        print("Failed to parse response:", str(e), flush=True)
        print("Raw response:", response.text, flush=True)


def clear_file_column(board_id, item_id, column_id):
    print("Clearing file column using clear_all...", flush=True)

    mutation = """
    mutation ($boardId: Int!, $itemId: Int!, $columnId: String!, $value: JSON!) {
      change_column_value(
        board_id: $boardId,
        item_id: $itemId,
        column_id: $columnId,
        value: $value
      ) {
        id
      }
    }
    """

    variables = {
        "boardId": board_id,
        "itemId": item_id,
        "columnId": column_id,
        "value": json.dumps({"clear_all": True})
    }

    response = requests.post(API_URL, json={"query": mutation, "variables": variables}, headers=HEADERS)
    response.raise_for_status()
`   print('response-clear-file->',response,flush=True)
    print(f"Cleared file column '{column_id}' on item {item_id}", flush=True)


def create_pdf_from_json(json_data, filename="output.pdf"):
    print('json_data_pdf----->',json_data,flush=True)
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=10)
    pdf.add_page()
    pdf.set_font("Courier", size=8)

    # One-line JSON string (not pretty printed)
    json_str = json.dumps(json_data)
    print('json_str------>',json_str,flush=True)
    
    # Print entire JSON string as one line (like in the image)
    pdf.multi_cell(0, 5, json_str)

    pdf.output(filename)
    print(f"PDF saved to: {filename}", flush=True)
    return filename

def fetch_data_with_columns():
    print('inside this column--->',flush=True)
    all_data = []

    for board_id in BOARD_IDS:
        board_id = board_id.strip()
        query = f"""
        query {{
          boards(ids: {board_id}) {{
            name
            columns {{
              id
              title
            }}
            items_page(limit: 100) {{
              items {{
                id
                name
                column_values {{
                  id
                  text
                  value
                  type
                }}
              }}
            }}
          }}
        }}
        """
        try:
            response = requests.post(API_URL, json={"query": query}, headers=HEADERS)
            response.raise_for_status()
            data = response.json()
            print('data34---->',data,flush=True)
                
            if "errors" in data:
                print(f"Error fetching board {board_id}:", data["errors"],flush=True)
                continue

            # board = data["data"]["boards"][0]
            boards = data["data"].get("boards", [])
            if not boards:
                print(f"⚠️ No board found with ID: {board_id}",flush=True)
                continue
            board = boards[0]

            board_name = board["name"]
            print(f"\nBoard: {board_name} (ID: {board_id})",flush=True)

            board_data = {
                "board": board_name,
                "items": []
            }

            for item in board["items_page"]["items"]:
                item_info = {
                    "id": item["id"],
                    "name": item["name"],
                    "columns": {}
                }

                for col in item["column_values"]:
                    item_info["columns"][col["id"]] = col["text"] or ""

                print(f"  • {item_info['name']}",flush=True)
                board_data["items"].append(item_info)

            all_data.append(board_data)
            print('all_data',all_data,flush=True)
        except Exception as e:
            print(f"Exception fetching board {board_id}: {e}",flush=True)
            continue

    return all_data


def update_target_item(data):
    json_text = json.dumps(data, ensure_ascii=False)
    escaped_json = json_text.replace('"', '\\"')

    print("\nJSON to update:",flush=True)
    print(json_text,flush=True)

    mutation = f"""
    mutation {{
      change_simple_column_value(
        board_id: {TARGET_BOARD_ID},
        item_id: {TARGET_ITEM_ID},
        column_id: "{TARGET_COLUMN_ID}",
        value: "{escaped_json}"
      ) {{
        id
      }}
    }}
    """
    try:
        response = requests.post(API_URL, json={"query": mutation}, headers=HEADERS)
        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            print("Failed to update item:", data["errors"],flush=True)
        else:
            print(f"Successfully updated item {TARGET_ITEM_ID}.",flush=True)
    except Exception as e:
        print(" Exception during item update:", e,flush=True)


def handle_webhook_trigger():
    print(" Webhook triggered — Fetching & updating...",flush=True)
    fetch_board_data(board_id_north)
    structured_data = fetch_data_with_columns()
    update_target_item(structured_data)


# @app.route("/webhook", methods=["POST"])
# def webhook():
#     payload = request.json
#     print(" Webhook received:", json.dumps(payload, indent=2))

#     # Run update logic in a background thread (non-blocking)
#     Thread(target=handle_webhook_trigger).start()
#     return {"status": "ok"}, 200

@app.route("/webhook", methods=["POST"])
# @app.route("/webhook", methods=["POST"])
def webhook():
    payload = request.json
    print("📬 Webhook received",flush=True)
    print("Headers:", json.dumps(dict(request.headers), indent=2),flush=True)
    print("Body:", json.dumps(payload, indent=2),flush=True)

    # ✅ Handle Monday's webhook verification challenge
    if "challenge" in payload:
        print("✅ Responding to webhook challenge",flush=True)
        return jsonify({"challenge": payload["challenge"]}), 200

    # ✅ Run update logic in background thread
    Thread(target=handle_webhook_trigger).start()

    # ✅ Always return a valid response
    return jsonify({"status": "ok"}), 200


@app.route("/", methods=["GET"])
def health():
    return " Webhook server is running!", 200
