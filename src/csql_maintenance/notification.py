import os
import datetime
from json import dumps
import json
from dotenv import load_dotenv
from httplib2 import Http


load_dotenv()


def get_oncall(opsgenie_url, opsgenie_token):
    opsgenie_api = opsgenie_url
    token_api = opsgenie_token
    msg_headers = {
        "Authorization": token_api,
    }
    http_obj = Http()
    response, content = http_obj.request(opsgenie_api, "GET", headers=msg_headers)

    if response.status == 200:
        data = json.loads(content.decode("utf-8"))

        on_call_recipients = data.get("data", {}).get("onCallRecipients", [])
        return on_call_recipients

        # print("On Call Recipients:", on_call_recipients)
    else:
        return "Failed to retrieve data"


def get_userid(email):
    base_path = os.path.dirname(os.path.abspath(__file__))
    with open(f"{base_path}/gchat_userid.json", "r") as file:
        users = json.load(file)
    for user in users:
        if user["email"] == email:
            return str(user["user_id"])
    return None


def create_message(status,version,project_name,instance_name):
    now = datetime.datetime.now().strftime("%A, %d %b %Y %I:%M %p")
    msg = ""
    msg_concat = ""
    footer = ""
    base_path = os.path.dirname(os.path.abspath(__file__))
    gcp_console_url = os.environ.get("BASE_PATH_GCP_CONSOLE").format(instance_name=instance_name,project_name=project_name)
    with open(f"{base_path}/template.json", "r") as file:
        space_data = json.load(file)

    # Filter hanya untuk status yang cocok
    filtered_data = [lspace for lspace in space_data if lspace["msg_type"] == status]


    if not filtered_data:
        return "No matching message type found."

    for lspace in filtered_data:
        # Set hyperlink footer_message
        footer_message = lspace['footer_message'].format(link=gcp_console_url)
        # Template pesan berdasarkan status
        msg = (f"📢 *{lspace['header_message']} {instance_name}*\n"\
                        f"_{now}_\n"
                    f"{lspace['body_message']}\n"\
                        f"📝 *More Detail* \n"
                        f"* Project Name: *{project_name}*\n"
                        f"* Instance: *{instance_name}*\n"
                        f"* Version: *{version}*\n"
                        f"_{footer_message}_\n"
                )
        
        # Set footer berdasarkan status
        if status == 'failed':
            footer = f"⚠️⚠️⚠️ <users/all> ⚠️⚠️⚠️"
        elif status == 'success':
            footer = f"⚠️⚠️⚠️ <users/all> ⚠️⚠️⚠️"
        elif status == 'wip':
            footer = f"⏲️"
        
        msg_concat += msg + footer

        # Mengirim pesan ke Google Chat
        url = lspace["space_url"]
        app_message = {
            "text": msg_concat
        }
        message_headers = {"Content-Type": "application/json; charset=UTF-8"}
        http_obj = Http()
        response = http_obj.request(
            uri=url,
            method="POST",
            headers=message_headers,
            body=dumps(app_message),
        )

 
    # if mention_all == str.lower(now) or mention_all == "everyday":
    #     h_message = f"{header_message} <users/all>"
    #     msg_concat += h_message + body_message
    # else:
    #     msg_concat += header_message + body_message
    # return msg_concat
    return msg_concat


if __name__ == "__main__":
    create_message("success","SQLSERVER_2019_STANDARD_CU28.R20240916.00_01","treasury-prd","bank-tre-p-cloudsql-ase2-sql")