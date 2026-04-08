#!/usr/bin/env python3
import sys
import json


def count_coins_in_json(filename):
    """
    Loads an exchanges JSON file and counts the number of coin tickers
    listed under each top-level exchange.
    """
    try:
        with open(filename, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: The file '{filename}' is not a valid JSON file.")
        sys.exit(1)

    # Dictionary to hold the final counts, e.g., {'lbank': 50, 'coinex': 120}
    exchange_counts = {}

    # The top-level keys in the JSON are the exchange names.
    # The value for each exchange is another dictionary where the keys are coin tickers.
    for exchange_name, coins_on_exchange in data.items():
        # The number of coins is simply the number of keys in the inner dictionary.
        # We check if coins_on_exchange is actually a dictionary to avoid errors.
        if isinstance(coins_on_exchange, dict):
            num_coins = len(coins_on_exchange)
            exchange_counts[exchange_name] = num_coins
        else:
            # This handles cases where an exchange might have a non-dict value, e.g., null
            exchange_counts[exchange_name] = 0

    # --- Print the summary ---
    if not exchange_counts:
        print("No exchanges found in the JSON file.")
        return

    print("Total Coin Count per Exchange:")
    print("------------------------------")

    # Sort by exchange name for a clean, consistent report
    for exchange in sorted(exchange_counts.keys()):
        count = exchange_counts[exchange]
        # Print with nice formatting
        print(f"{exchange:<15}: {count} coins")


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 count_coins_from_json.py <path_to_json_file>")
        print("Example: python3 count_coins_from_json.py all_fees.json")
        sys.exit(1)

    json_file = sys.argv[1]
    count_coins_in_json(json_file)


if __name__ == "__main__":
    main()