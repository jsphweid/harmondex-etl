import requests
import base64
import json


def transcribe(in_path: str, out_path: str, is_piano: bool):
    route = "transcribe-piano" if is_piano else "transcribe-anything"
    with open(in_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode('ascii')
    data = json.dumps({"data": b64})
    headers = {"Content-Type": "application/json"}
    res = requests.post(f"http://127.0.0.1:5000/{route}", data=data, headers=headers)
    b64 = base64.b64decode(res.json()["data"])
    with open (out_path, "wb") as f:
        f.write(b64)
