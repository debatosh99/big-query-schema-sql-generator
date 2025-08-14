import time
import requests

def call_api_with_backoff(url, max_retries=5, backoff_factor=1):
    """
    Call the given API URL with exponential backoff.
    Stops immediately if a successful response (status 200) is received.

    Args:
        url (str): The API endpoint to call.
        max_retries (int): Maximum number of retry attempts.
        backoff_factor (int/float): Base delay in seconds before retries (will be multiplied exponentially).

    Returns:
        dict or None: JSON response if successful, None otherwise.
    """

    for attempt in range(max_retries):
        try:
            print(f"Attempt {attempt + 1}/{max_retries}...")

            # Example GET request
            response = requests.get(url, timeout=10)

            # Success case
            if response.status_code == 200:
                print("✅ Successful response received!")
                return response.json()

            # Retryable error codes: 5xx or 429
            elif response.status_code in [429] or (500 <= response.status_code < 600):
                wait_time = backoff_factor * (2 ** attempt)
                print(f"⚠️ Received {response.status_code}, retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"❌ Non-retryable error: {response.status_code}")
                break  # Permanent failure — stop retrying

        except requests.exceptions.RequestException as e:
            # Connection errors, timeouts, etc.
            wait_time = backoff_factor * (2 ** attempt)
            print(f"⚠️ Request failed ({e}), retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    print("❌ Max retries reached, exiting.")
    return None


# ----------------------- USAGE EXAMPLE -----------------------
if __name__ == "__main__":
    API_URL = "https://jsonplaceholder.typicode.com/posts/1"  # Example test API

    result = call_api_with_backoff(API_URL)

    if result:
        print("📄 Response JSON:", result)
