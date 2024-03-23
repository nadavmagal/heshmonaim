from flask import Flask, render_template, send_from_directory
from fizikal_api import FizikalAPI
import toml
import os
from datetime import datetime

app = Flask(__name__)


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


def __init_config(config_file: str = "config.toml"):
    global config
    global fizikal_config
    global phone_number
    global persistent_storage
    if not config_file:
        print("No config file specified")
        exit(1)
    elif not config_file.endswith(".toml"):
        print("Config file must be a .toml file")
        exit(1)
    elif not os.path.exists(config_file):
        print("Config file does not exist")
        exit(1)
    config = toml.load(config_file)

    persistent_storage = config.get("persistent_storage", "./persist")
    if not os.path.exists(persistent_storage):
        os.makedirs(persistent_storage, exist_ok=True)

    google_sheets_config = config.get("google_sheets", {})
    fizikal_config = config.get("fizikal", {})

    phone_number = fizikal_config.get("phone_number", None)
    if not phone_number:
        print("Phone number not specified in config file")
        exit(1)


def list_of_dicts_to_html_table(list_of_dicts: list):
    headers = list_of_dicts[0].keys()
    html = "<tr>"
    for i, header in enumerate(headers):
        html += f'<th  onclick="sortTable({i})">{header}</th>'
    html += f"<th>Register</th>"
    html += "</tr>"
    for i, row in enumerate(list_of_dicts):
        class_id = row.get("id", f"unknown_id-{i}")
        class_date = row.get("dateRequest", "")
        html += f"<tr id=row-{class_id}>"
        for header in headers:
            html += f"<td>{row[header]}</td>"
        html += f"<td class='registration-status' onclick=\"registerClass('{class_id}','{class_date}')\">Register</td>"
        html += "</tr>"
    return html


@app.route("/classes")
def get_classes() -> str:
    """
    Expected classes:
            [
                {
                    "id": 3100,
                    "day": "שישי",
                    "date": "01/12",
                    "startTime": "07:00",
                    "endTime": "07:45",
                    "description": "Body Shape",
                    "instructorName": "ליליה קרנדל",
                    "maxParticipants": 24,
                    "totalParticipants": 14,
                    "locationName": "אולם תנועה",
                    "action": {
                    "text": "הרשמה",
                    "name": "AddRegistration"
                    }
                },
            ...
            ]
    """
    classes = api.get_classes()
    return list_of_dicts_to_html_table(classes)


@app.route("/register/<class_id>/<class_date>")
def register_class(class_id: str, class_date: str) -> str:
    """
    Expected output:
    {
        "status": "success",
        "message": "הרשמתך נקלטה בהצלחה"
    }
    """
    print(f"Registering class {class_id} on {class_date}")
    response = api.register_class(class_id=class_id, class_date=class_date)
    return response


def main():
    global api
    __init_config(config_file="config.toml")
    api = FizikalAPI(
        fizikal_config=fizikal_config, persistent_storage=persistent_storage, mock=True
    )
    app.run()


if __name__ == "__main__":
    main()
