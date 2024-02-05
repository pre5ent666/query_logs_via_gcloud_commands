import subprocess
import json
import time
import numpy as np

# cmd = 'gcloud logging read "jsonPayload.type=vehicle-state" AND jsonPayload.device="5efc568967354e8d" --limit 5 --format=json'

def query_gcp(device_id, event_type, limit=5):
    query =f'jsonPayload.type="{event_type}" AND jsonPayload.device="{device_id}" AND timestamp>="2024-01-23T00:00:00Z" AND timestamp<="2024-01-25T00:00:00Z"'
    try:
        byteOutput = subprocess.check_output(["gcloud", "logging", "read", query, "--limit", str(limit), "--format", "json"], timeout=10)
        rlts = json.loads(byteOutput.decode('UTF-8').rstrip())
        return rlts
    except subprocess.CalledProcessError as e:
        print("[Error]", e.output)
        return None
    
def query_gcp_ts(device_id, event_type, ts1, ts2, limit=5):
    query =f'jsonPayload.type="{event_type}" AND jsonPayload.device="{device_id}" AND timestamp>="{ts1}" AND timestamp<="{ts2}"'
    cmd = ["gcloud", "logging", "read", query, "--limit", str(limit), "--format", "json"]
    try:
        byteOutput = subprocess.check_output(cmd, timeout=10)
        print(f"Execute the following command\n{' '.join(cmd)}")
        rlts = json.loads(byteOutput.decode('UTF-8').rstrip())
        return rlts
    except subprocess.CalledProcessError as e:
        print("[Error]", e.output)
        return None

if __name__ == "__main__":
    x = 1800
    START_TIME = time.time_ns()
    ts2 = np.datetime_as_string(np.datetime64(int(START_TIME), "ns"), timezone="UTC")
    # print(ts2)
    ts1 = np.datetime_as_string(np.datetime64(int(START_TIME - x*1e9), "ns"), timezone="UTC")
    # print(ts1)

    # time.sleep(500)

    device_id = "5efc568967354e8d"
    # event_type = "watchdog-report-summary"
    event_type = "vehicle-state"
    # j = query_gcp(device_id, event_type, 1)
    # print(j[0]['jsonPayload']['parameters']['imei'])
    # print(j[0]['jsonPayload']['parameters']['data']['current_vehicle_state'])
    # print(j[0]['jsonPayload']['parameters']['version'])

    j2 = query_gcp_ts(device_id, event_type, ts1, ts2)
    for msg in j2:
        print(msg['jsonPayload']['message_ts'])



