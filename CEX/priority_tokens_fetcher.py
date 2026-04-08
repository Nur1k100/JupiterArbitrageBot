import requests
import time
from datetime import datetime

# --- Configuration ---
# The URL to fetch the JSON data from
API_URL = "https://tokens.jup.ag/tokens?tags=strict,lst,verified"

# The name of the file to save the symbols to
OUTPUT_FILE = "priority_token_symbols.txt"

# The interval in seconds to wait before fetching again
FETCH_INTERVAL_SECONDS = 60


def fetch_and_save_symbols():
    """
    Fetches token data from the Jupiter API, extracts the symbols,
    and saves them to a text file, overwriting the previous content.
    """
    try:
        # 1. Fetch the data from the URL
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Fetching token data from {API_URL}...")
        response = requests.get(API_URL, timeout=10)  # 10-second timeout

        # Raise an exception if the request failed (e.g., 404, 500)
        response.raise_for_status()

        # 2. Parse the JSON response into a Python list of dictionaries
        tokens = response.json()

        # 3. Extract the "symbol" from each token dictionary
        # We use a list comprehension for a clean and efficient way to do this.
        # We also filter out any potential entries that might be missing a symbol.
        symbols = [token['symbol'] for token in tokens if 'symbol' in token]

        if not symbols:
            print("Warning: No symbols were found in the API response.")
            return

        # 4. Save the symbols to the output file, overwriting it
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            # Join all symbols with a newline character for a clean, multi-line file
            f.write('\n'.join(symbols))
            # Add a final newline for good measure
            f.write('\n')

        print(f"Successfully saved {len(symbols)} symbols to '{OUTPUT_FILE}'.")

    except requests.exceptions.RequestException as e:
        print(f"Error: Could not fetch data from the URL. {e}")
    except KeyError as e:
        print(f"Error: A 'symbol' key was not found in one of the token entries. {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    print("--- Jupiter Token Symbol Fetcher ---")
    print(f"This script will update '{OUTPUT_FILE}' every {FETCH_INTERVAL_SECONDS} seconds.")
    print("Press Ctrl+C to stop the script.")

    # Run the function in an infinite loop
    while True:
        fetch_and_save_symbols()
        print(f"Waiting for {FETCH_INTERVAL_SECONDS} seconds...")
        time.sleep(FETCH_INTERVAL_SECONDS)
