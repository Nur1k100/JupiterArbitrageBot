#!/usr/bin/env python3
import sys
import json


def find_coin_in_exchanges(filename, coin_to_find):
    """
    Searches a JSON file for a specific coin ticker and prints the
    exchange name and the full data block for that coin.
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

    # Convert the coin to find to uppercase, as tickers usually are.
    coin_to_find = coin_to_find.upper()
    found_once = False

    # The top-level keys are the exchange names (e.g., "lbank", "coinex")
    for exchange_name, coins_on_exchange in data.items():
        # Check if our target coin is a key in this exchange's dictionary
        if coin_to_find in coins_on_exchange:
            found_once = True

            # Prepare the output object for this specific find
            output_data = {
                exchange_name: {
                    coin_to_find: coins_on_exchange[coin_to_find]
                }
            }

            # Print the result, nicely formatted as JSON
            print(json.dumps(output_data, indent=4))
            print("-" * 20)  # Add a separator for clarity if found on multiple exchanges

    if not found_once:
        print(f"Coin '{coin_to_find}' was not found on any exchange in the file.")


def main():
    # Check if we have the right number of command-line arguments
    if len(sys.argv) != 3:
        print("Usage: python3 find_coin.py <json_file> <coin_ticker>")
        print("Example: python3 find_coin.py all_fees.json BUM")
        sys.exit(1)

    filename = sys.argv[1]
    coin_ticker = sys.argv[2]

    find_coin_in_exchanges(filename, coin_ticker)


if __name__ == "__main__":
    main()