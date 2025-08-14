import time
import requests

def call_api_with_backoff(url, max_retries=5, backoff_factor=1):
    """
    Call the given API URL with exponential backoff and respect 'Retry-After' header.
    Stops immediately if a successful response (status 200) is received.

    Args:
        url (str): The API endpoint to call.
        max_retries (int): Maximum number of retry attempts.
        backoff_factor (int/float): Base delay in seconds before retries (multiplied exponentially).

    Returns:
        dict or None: JSON response if successful, None otherwise.
    """

    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries}...")

            # Example GET request
            response = requests.get(url, timeout=10)

            # ✅ Success
            if response.status_code == 200:
                print("✅ Successful response received!")
                return response.json()

            # ⚠️ Retryable conditions: 429 or 5xx
            elif response.status_code == 429 or (500 <= response.status_code < 600):
                # Check for Retry-After header
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_time = int(retry_after)
                        print(f"⚠️ Server requested wait of {wait_time} seconds (Retry-After header).")
                    except ValueError:
                        # In case of malformed header — fallback to exponential
                        wait_time = backoff_factor * (2 ** attempt)
                        print(f"⚠️ Malformed Retry-After header; using {wait_time} seconds instead.")
                else:
                    # Standard exponential backoff
                    wait_time = backoff_factor * (2 ** attempt)
                    print(f"⚠️ No Retry-After header. Waiting {wait_time} seconds before retry.")

                time.sleep(wait_time)

            # ❌ Non-retryable error
            else:
                print(f"❌ Non-retryable HTTP error: {response.status_code}")
                break

        except requests.exceptions.RequestException as e:
            # Retry network-related errors with exponential backoff
            wait_time = backoff_factor * (2 ** attempt)
            print(f"⚠️ Request failed ({e}), retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    print("❌ Max retries reached, exiting.")
    return None


# ----------------------- USAGE EXAMPLE -----------------------
if __name__ == "__main__":
    # This uses a test API for demo, replace with your real endpoint
    API_URL = "https://jsonplaceholder.typicode.com/posts/1"

    result = call_api_with_backoff(API_URL)

    if result:
        print("📄 Response JSON:", result)
