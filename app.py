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



API_URL = "https://api.monday.com/v2"
HEADERS = {
    "Authorization": API_KEY,
    "Content-Type": "application/json"
}

app = Flask(__name__)

board_id_north = "2052330963"
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
            items_page(limit: 5) {{
              items {{
                id
                name
                column_values {{
                  text
                  value
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

    upload_file_to_supplier_manifest_column(2052855846,file_path,"file_mktf24g0")
    
    # return board
    return board
# def parse_monday_board_data(board_data):
#     print('inside this parse monday data',flush=True)
#     print('board-data----->',board_data,flush=True)
#     parsed_items = []

#     # Map column titles to IDs for easier access
#     column_id_map = {
#         "Lead_Creation_Date": "date_mktearzs",  
#         "Close_Date": "date_mktezc1y", 
#         "Country": "text_mktebys0",
#         "City": "text_mktemekh",
#         "Customer_ID": "text_mkteca06",
#         "Area": "text_mktg5jgn",
#         "Age": "numeric_mktgk91w",
#         "Customer_Segment": "text_mktenj5e",
#         "Lead Owner": "person",
#         "Manager": "multiple_person_mktenvjm",
#         "Product_Name": "text_mktexk7m",
#         "SKU": "text_mktekj0c",
#         "Units_Sold": "numeric_mkteyfx2",
#         "Price_Per_Unit ($)": "numeric_mktebsrg",
#         "Cost_Per_Unit ($)": "numeric_mktedry",
#         "Discount_Applied (%)": "numeric_mktezn44",
#         "Total_Revenue ($)": "numeric_mkteyhgs",
#         "Sales_Channel": "text_mkterm44",
#         "NPS_Score (0-10)": "text_mktefcba",
#         "Feedback_Summary": "text_mktevjd9"
#     }

#     # Get list of column IDs in order
#     column_id_order = [col['id'] for col in board_data['columns']]
#     print('column_id_order-->',column_id_order,flush=True)

#     for item in board_data["items_page"]["items"]:
#         item_data = {
#             "Order_ID": item["name"]             # Placeholder
#         }

#         # Rebuild {column_id: text} for current item
#         column_values_raw = item["column_values"]
#         column_values = {
#             column_id_order[i]: column_values_raw[i].get("text", None)
#             for i in range(min(len(column_id_order), len(column_values_raw)))
#         }

#         # Extract and format each required field
#         for key, col_id in column_id_map.items():
#             value = column_values.get(col_id, None)

#             if key in ["Lead_Creation_Date", "Close_Date"] and value:
#                 try:
#                     date_obj = datetime.strptime(value, "%Y-%m-%d")
#                     value = date_obj.strftime("%d-%m-%Y") if key == "Close_Date" else date_obj.toordinal()
#                 except Exception:
#                     value = None

#             elif key in [
#                 "Units_Sold", "Price_Per_Unit ($)", "Cost_Per_Unit ($)", "Discount_Applied (%)",
#                 "Total_Revenue ($)", "NPS_Score (0-10)"
#             ]:
#                 try:
#                     value = float(value)
#                 except:
#                     value = 0

#             item_data[key] = value

#         parsed_items.append(item_data)
#         print('parsed_items-->',parsed_items,flush=True)
#     return parsed_items

def parse_monday_board_data(board_data):
    print('inside this parse monday data',flush=True)
    parsed_items = []

    # Create column title to ID mapping from the board data
    column_title_to_id = {}
    for col in board_data['columns']:
        column_title_to_id[col['title']] = col['id']
    
    print('Available columns:', column_title_to_id, flush=True)

    for item in board_data["items_page"]["items"]:
        item_data = {"Order_ID": item["name"]}

        # Create column_id to value mapping
        column_values = {}
        for i, col_value in enumerate(item["column_values"]):
            if i < len(board_data['columns']):
                col_id = board_data['columns'][i]['id']
                column_values[col_id] = col_value.get("text", None)

        # Map to your desired output format using column titles
        field_mappings = {
            "Lead_Creation_Date": "Lead Creation Date",  # Use actual column titles
            "Close_Date": "Close Date", 
            "Country": "Country",
            "City": "City",
            "Customer_ID": "Customer ID",
            "Area": "Area",
            "Customer Age": "Customer Age",  # Note: was "Age" 
            "Customer_Segment": "Customer Segment",
            "Lead Owner": "Lead Owner",
            "Manager": "Manager",
            "Product_Name": "Product Name",
            "SKU": "SKU",
            "Units_Sold": "Units Sold",
            "Price_Per_Unit ($)": "Price Per Unit ($)",
            "Cost_Per_Unit ($)": "Cost Per Unit ($)",
            "Discount_Applied (%)": "Discount Applied (%)",
            "Total_Revenue ($)": "Total Revenue ($)",
            "Sales_Channel": "Sales Channel",
            "NPS_Score (0-10)": "NPS Score (0-10)",
            "Feedback_Summary": "Feedback Summary"
        }

        for output_key, column_title in field_mappings.items():
            col_id = column_title_to_id.get(column_title)
            value = column_values.get(col_id, None) if col_id else None

            # Format dates and numbers as needed
            if output_key in ["Lead_Creation_Date", "Close_Date"] and value:
                try:
                    date_obj = datetime.strptime(value, "%Y-%m-%d")
                    value = date_obj.strftime("%d-%m-%Y") if output_key == "Close_Date" else date_obj.toordinal()
                except:
                    value = None
            elif output_key in ["Units_Sold", "Price_Per_Unit ($)", "Cost_Per_Unit ($)", "Discount_Applied (%)", "Total_Revenue ($)", "NPS_Score (0-10)"]:
                try:
                    value = float(value) if value else 0
                except:
                    value = 0

            item_data[output_key] = value

        parsed_items.append(item_data)
    
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

    # query = f"""
    # query {{
    #     items(ids: {item_id}) {{
    #         name
    #         column_values(ids: {column_id_string}) {{
    #             id
    #             text
    #             }}
    #             column{{
    #             title 
    #         }}
    #     }}
    # }}
    # """

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



def upload_file_to_supplier_manifest_column(item_id, file_path, column_id):
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
            print("✅ PDF uploaded successfully.", flush=True)
    except Exception as e:
        print("❌ Failed to parse response:", str(e), flush=True)
        print("Raw response:", response.text, flush=True)

# def create_pdf_with_json_content(json_data, filename="output.pdf"):
#     print('inside create file',flush=True)
#     pdf = FPDF()
#     pdf.add_page()
#     pdf.set_font("Courier", size=10)

#     # Convert dict/list to pretty-printed JSON string
#     json_str = json.dumps(json_data, indent=4)

#     # Add line by line to the PDF
#     for line in json_str.split("\n"):
#         pdf.multi_cell(0, 5, line)

#     pdf.output(filename)
#     print(f"PDF saved: {filename}",flush=True)
#     return filename

# def create_pdf_with_json_content(json_data, filename="output.pdf"):
#     print('Creating PDF with one-line JSON...', flush=True)
#     print('Creating PDF with asia data...',json_data, flush=True)
    

#     # Convert JSON to a compact string (no indent)
#     json_str = json.dumps(json_data)

#     pdf = FPDF()
#     pdf.set_auto_page_break(auto=True, margin=10)
#     pdf.add_page()
#     pdf.set_font("Courier", size=9)

#     # Write the full JSON string (will wrap naturally)
#     pdf.multi_cell(0, 5, json_str)

#     pdf.output(filename)
#     print(f"PDF saved as: {filename}", flush=True)
#     return filename


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

