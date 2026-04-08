import json

PATH_TO_ASK = 'DEX/DEX_json/x_eff_y_eff_ask.json'
PATH_TO_BID = 'DEX/DEX_json/x_eff_y_eff_bids.json'

def __load_data(path) -> dict or None:
    try:
        with open(path) as json_file:
            data = json.load(json_file)

        return data
    except FileNotFoundError:
        return None


def __get_token():
    module = input('Enter module token | ask (a) | bid (b): ')
    symbol = input('Enter symbol: ')

    if module == 'a':
        load_data = __load_data(PATH_TO_ASK)
        if load_data is None:
            print('No data for your search')
            return
        data_about_token = load_data.get(symbol)
        print(data_about_token)
    elif module == 'b':
        load_data = __load_data(PATH_TO_BID)
        if load_data is None:
            print('No data for your search')
            return
        data_about_token = load_data.get(symbol)
        print(data_about_token)
    else:
        print('Invalid module')

def main():
    """
    easy fucking debug
    :return:
    """

    func = input('Enter function | __get_token (gt): ')

    if func == 'gt':
        __get_token()
    else:
        print('Invalid function')

if __name__ == '__main__':
    main()