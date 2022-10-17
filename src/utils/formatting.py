def format_price_to_million_tenge(price):
    return str(round(price / 1e6, 2)) + 'M₸'


def format_prices_to_million_tenge(prices):
    return prices.apply(lambda x: format_price_to_million_tenge(x))
