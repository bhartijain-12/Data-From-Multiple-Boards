import os
import json
import requests
from dotenv import load_dotenv
from flask import Flask, request ,jsonify
from threading import Thread

# Load environment variables
load_dotenv()

API_KEY = os.getenv("MONDAY_API_KEY")
BOARD_IDS = os.getenv("SOURCE_BOARD_IDS", "").split(",")
TARGET_BOARD_ID = os.getenv("TARGET_BOARD_ID")
TARGET_ITEM_ID = "2052800979"
TARGET_COLUMN_ID = "file_mkteeyg6"

API_URL = "https://api.monday.com/v2/file"
HEADERS = {
    "Authorization": API_KEY,
    "Content-Type": "application/json"
}

app = Flask(__name__)

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

