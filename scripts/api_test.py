import json
import time
import traceback
import requests
from signalrcore.hub_connection_builder import HubConnectionBuilder

# eventHubUrl = "http://10.42.0.1:33001/hubs/eventHub"  # URL of event hub SignalR
eventHubUrl = "http://192.168.50.87:33001/hubs/eventHub"  # URL of event hub SignalR
# apiUrl = "http://10.42.0.1:5000/v3/"  # URL of the EventHub API
apiUrl = "http://192.168.50.87:5000/v3/"  # URL of the EventHub API

AUTH_ID = "Admin"
AUTH_SECRET = "ZeroKey_Admin1"


def log_info(message):
    print(f"[INFO] {message}")


def log_warn(message):
    print(f"[WARNING] {message}")


def log_error(message):
    print(f"[ERROR] {message}")


def pretty_print_json(data, prefix="[DATA]"):
    try:
        print(f"{prefix} {json.dumps(data, indent=2, sort_keys=True)}")
    except Exception:
        print(f"{prefix} {data}")


def request_with_logging(method, url, headers=None, body=None, timeout=10):
    log_info(f"Sending {method.upper()} request to: {url}")

    if headers:
        pretty_print_json(headers, prefix="[REQUEST HEADERS]")

    if body is not None:
        try:
            parsed = json.loads(body) if isinstance(body, str) else body
            pretty_print_json(parsed, prefix="[REQUEST BODY]")
        except Exception:
            print(f"[REQUEST BODY] {body}")

    try:
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            data=body,
            timeout=timeout
        )

        log_info(f"Received HTTP {response.status_code} from {url}")

        if not response.ok:
            log_warn(f"Request to {url} returned a non-success status code.")
            print("[RESPONSE TEXT]")
            print(response.text)

        return response

    except requests.exceptions.RequestException as exc:
        log_error(f"HTTP request failed: {exc}")
        raise


def on_open():
    log_info("Connection opened and SignalR handshake completed.")


def on_close():
    log_warn("Connection closed.")


def on_error(error):
    log_error(f"SignalR error: {error}")


def on_event_received(data):
    log_info("Received event data from hub.")

    if data is None:
        log_warn("Received event payload is None.")
        return

    if data == "" or data == [] or data == {}:
        log_warn("Received event payload is empty.")
        print(f"[RAW EVENT] {data}")
        return

    print("[RAW EVENT RECEIVED]")
    print(data)

    try:
        parsed_data = data

        # SignalR payload often arrives as a list with one JSON string inside
        if isinstance(parsed_data, list):
            if len(parsed_data) == 0:
                log_warn("Received an empty list payload.")
                return

            if len(parsed_data) == 1:
                parsed_data = parsed_data[0]
            else:
                log_warn(f"Received list payload with {len(parsed_data)} entries.")

        # If the payload is a JSON string, decode it
        if isinstance(parsed_data, str):
            parsed_data = json.loads(parsed_data)

        print("[PARSED EVENT]")
        print(json.dumps(parsed_data, indent=2, sort_keys=True))

        # Optional: print important fields separately
        if isinstance(parsed_data, dict):
            category = parsed_data.get("Category")
            event_type = parsed_data.get("Type")
            timestamp = parsed_data.get("Timestamp")
            local_timestamp = parsed_data.get("LocalTimestamp")
            content = parsed_data.get("Content", {})
            source = parsed_data.get("Source", {})

            print("[EVENT SUMMARY]")
            print(f"  Category       : {category}")
            print(f"  Type           : {event_type}")
            print(f"  Timestamp      : {timestamp}")
            print(f"  LocalTimestamp : {local_timestamp}")
            print(f"  Position       : {content.get('Position')}")
            print(f"  Velocity       : {content.get('Velocity')}")
            print(f"  Sequence       : {content.get('Sequence')}")
            print(f"  MAC            : {source.get('MAC')}")
            print(f"  GatewayURI     : {source.get('GatewayURI')}")

    except json.JSONDecodeError as exc:
        log_warn(f"Failed to decode JSON payload: {exc}")
        print(f"[UNPARSED PAYLOAD] {data}")
    except Exception as exc:
        log_warn(f"Failed to process received event: {exc}")
        traceback.print_exc()


def authenticateConnection():
    log_info("Starting authentication flow.")

    # Step 1: Get bearer token
    token_headers = {
        "Content-Type": "application/json"
    }
    token_body = json.dumps({
        "grant_type": "client_credentials",
        "auth_id": AUTH_ID,
        "auth_secret": AUTH_SECRET
    })

    try:
        authTokenResponse = request_with_logging(
            method="post",
            url=apiUrl + "auth/token",
            headers=token_headers,
            body=token_body
        )

        if not authTokenResponse.ok:
            log_error("Authentication failed. Could not retrieve bearer token.")
            raise RuntimeError("Authentication request failed.")

        auth_json = authTokenResponse.json()
        if "access_token" not in auth_json:
            log_error("Authentication response does not contain 'access_token'.")
            pretty_print_json(auth_json, prefix="[AUTH RESPONSE]")
            raise RuntimeError("Missing access_token in authentication response.")

        authToken = auth_json["access_token"]
        log_info("Bearer token retrieved successfully.")

    except Exception as exc:
        log_error(f"Failed during token retrieval: {exc}")
        raise

    # Step 2: Initiate connection details to obtain EndpointID
    endpoint_headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + authToken
    }
    endpoint_body = json.dumps({
        "QualityOfService": {
            "MaxUpdateRate": 20,
            "MaxThroughput": 10240,
            "AutotuneConnectionParameters": False
        },
        "Mode": "read",
        "Filters": [{"FilterTemplate": "position_events"}]
    })

    try:
        endpointIDResponse = request_with_logging(
            method="post",
            url=apiUrl + "events/connections",
            headers=endpoint_headers,
            body=endpoint_body
        )

        if not endpointIDResponse.ok:
            log_error("Failed to create event connection / obtain EndpointID.")
            raise RuntimeError("Endpoint connection request failed.")

        endpoint_json = endpointIDResponse.json()
        if "EndpointID" not in endpoint_json:
            log_error("Endpoint response does not contain 'EndpointID'.")
            pretty_print_json(endpoint_json, prefix="[ENDPOINT RESPONSE]")
            raise RuntimeError("Missing EndpointID in endpoint response.")

        endpointID = endpoint_json["EndpointID"]
        log_info(f"EndpointID retrieved successfully: {endpointID}")

        return endpointID

    except Exception as exc:
        log_error(f"Failed during endpoint creation: {exc}")
        raise


def main():
    log_info("Building SignalR hub connection.")

    try:
        hub_connection = (
            HubConnectionBuilder()
            .with_url(
                eventHubUrl,
                options={
                    "access_token_factory": authenticateConnection
                }
            )
            .build()
        )
    except Exception as exc:
        log_error(f"Failed to build SignalR hub connection: {exc}")
        traceback.print_exc()
        return

    hub_connection.on_open(on_open)
    hub_connection.on_close(on_close)

    # Some versions of signalrcore support on_error, some may not.
    try:
        hub_connection.on_error(on_error)
    except Exception:
        log_warn("This signalrcore version does not support 'on_error' callback registration.")

    hub_connection.on("Event", on_event_received)

    try:
        log_info(f"Starting connection to hub: {eventHubUrl}")
        hub_connection.start()
        log_info("Hub connection start requested.")
    except Exception as exc:
        log_error(f"Failed to start hub connection: {exc}")
        traceback.print_exc()
        return

    log_info("Listening for events. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log_warn("Keyboard interrupt received. Stopping connection.")
    except Exception as exc:
        log_error(f"Unexpected runtime error: {exc}")
        traceback.print_exc()
    finally:
        try:
            hub_connection.stop()
            log_info("Hub connection stopped cleanly.")
        except Exception as exc:
            log_warn(f"Failed to stop hub connection cleanly: {exc}")


if __name__ == "__main__":
    main()