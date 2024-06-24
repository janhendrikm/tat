import requests
import json
import time
import hmac
import hashlib
from decimal import Decimal, getcontext

# Set decimal precision
getcontext().prec = 10

class BitvavoAPIError(Exception):
    pass

class BitvavoTrader:
    def __init__(self, api_key, api_secret, trading_fee=Decimal('0.002')):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = 'https://api.bitvavo.com/v2'
        self.headers = {
            'Content-Type': 'application/json',
            'Bitvavo-Access-Key': self.api_key,
            'Bitvavo-Access-Window': '10000'
        }
        self.trading_fee = trading_fee
        self.amount_dict = self.get_account_balance()
        self.market_data = self.get_symbol_details()
        self.inc_list = {x['market']: x['baseIncrement'] for x in self.market_data}
        self.qinc_list = {x['market']: x['quoteIncrement'] for x in self.market_data}

    def generate_signature(self, timestamp, method, endpoint, body=''):
        message = timestamp + method + endpoint + body
        signature = hmac.new(self.api_secret.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).hexdigest()
        return signature

    def bitvavo_request(self, endpoint, method='GET', params=None):
        timestamp = str(int(time.time() * 1000))
        signature = self.generate_signature(timestamp, method, endpoint)
        self.headers['Bitvavo-Access-Signature'] = signature
        self.headers['Bitvavo-Access-Timestamp'] = timestamp

        url = f"{self.base_url}{endpoint}"
        if params:
            url += '?' + '&'.join([f"{key}={requests.utils.quote(value)}" for key, value in params.items()])

        try:
            response = requests.request(method, url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            raise BitvavoAPIError(f"API request failed: {e}")

    def get_account_balance(self):
        endpoint = '/balance'
        response = self.bitvavo_request(endpoint)
        return {item['symbol']: Decimal(item['available']) for item in response if Decimal(item['available']) > 0}

    def get_market_data(self):
        endpoint = '/ticker/24h'
        return self.bitvavo_request(endpoint)

    def get_symbol_details(self):
        endpoint = '/markets'
        return self.bitvavo_request(endpoint)

    def get_orderbook(self, symbol):
        endpoint = f'/orderbook/{symbol}'
        return self.bitvavo_request(endpoint)

    def collect_tradeables(self, json_obj):
        return [coin['market'] for coin in json_obj]

    def structure_triangular_pairs(self, coin_list):
        triangular_pairs_list = []
        remove_duplicates_list = []
        pairs_list = coin_list[:]

        for pair_a in pairs_list:
            pair_a_split = pair_a.split('-')
            a_base, a_quote = pair_a_split

            a_pair_box = [a_base, a_quote]

            for pair_b in pairs_list:
                pair_b_split = pair_b.split('-')
                b_base, b_quote = pair_b_split

                if pair_b != pair_a:
                    if b_base in a_pair_box or b_quote in a_pair_box:

                        for pair_c in pairs_list:
                            pair_c_split = pair_c.split('-')
                            c_base, c_quote = pair_c_split

                            if pair_c != pair_a and pair_c != pair_b:
                                combine_all = [pair_a, pair_b, pair_c]
                                pair_box = [a_base, a_quote, b_base, b_quote, c_base, c_quote]
                                counts_c_base = pair_box.count(c_base)
                                counts_c_quote = pair_box.count(c_quote)

                                if counts_c_base == 2 and counts_c_quote == 2 and c_base != c_quote:
                                    combined = f"{pair_a},{pair_b},{pair_c}"
                                    unique_item = ''.join(sorted(combine_all))
                                    if unique_item not in remove_duplicates_list:
                                        match_dict = {
                                            "a_base": a_base,
                                            "b_base": b_base,
                                            "c_base": c_base,
                                            "a_quote": a_quote,
                                            "b_quote": b_quote,
                                            "c_quote": c_quote,
                                            "pair_a": pair_a,
                                            "pair_b": pair_b,
                                            "pair_c": pair_c,
                                            "combined": combined
                                        }
                                        triangular_pairs_list.append(match_dict)
                                        remove_duplicates_list.append(unique_item)
        return triangular_pairs_list

    def get_price_for_t_pair(self, t_pair, prices_json):
        pair_a = t_pair['pair_a']
        pair_b = t_pair['pair_b']
        pair_c = t_pair['pair_c']

        price_dict = {}
        for x in prices_json:
            if x['market'] in [pair_a, pair_b, pair_c]:
                price_dict[x['market'] + '_ask'] = Decimal(x['ask'])
                price_dict[x['market'] + '_bid'] = Decimal(x['bid'])

        return price_dict

    def safe_divide(self, numerator, denominator):
        return numerator / denominator if denominator != 0 else Decimal('Infinity')

    def determine_swap_details(self, base, quote, ask, bid, direction):
        if direction == "forward":
            return base, quote, self.safe_divide(Decimal('1'), ask), "base_to_quote"
        else:
            return quote, base, bid, "quote_to_base"

    def cal_triangular_arb_surface_rate(self, t_pair, prices_dict):
        starting_amount = Decimal('1')
        surface_dict = {}

        for direction in ['forward', 'reverse']:
            a_base, a_quote = t_pair['a_base'], t_pair['a_quote']
            b_base, b_quote = t_pair['b_base'], t_pair['b_quote']
            c_base, c_quote = t_pair['c_base'], t_pair['c_quote']

            # First swap
            swap_1, swap_2, swap_1_rate, direction_trade_1 = self.determine_swap_details(
                a_base, a_quote, 
                prices_dict.get(t_pair['pair_a'] + '_ask', Decimal('Infinity')), 
                prices_dict.get(t_pair['pair_a'] + '_bid', Decimal('0')), 
                direction
            )

            acquired_coin_t1 = starting_amount * swap_1_rate * (Decimal('1') - self.trading_fee)

            # Determine second and third swaps
            if direction == "forward":
                if a_quote == b_quote:
                    swap_2_rate = prices_dict.get(t_pair['pair_b'] + '_bid', Decimal('0'))
                    direction_trade_2 = "quote_to_base"
                    contract_2 = t_pair['pair_b']
                    
                    if b_base == c_base:
                        swap_3_rate = self.safe_divide(Decimal('1'), prices_dict.get(t_pair['pair_c'] + '_ask', Decimal('Infinity')))
                        direction_trade_3 = "base_to_quote"
                        contract_3 = t_pair['pair_c']
                    else:
                        swap_3_rate = prices_dict.get(t_pair['pair_c'] + '_bid', Decimal('0'))
                        direction_trade_3 = "quote_to_base"
                        contract_3 = t_pair['pair_c']
                elif a_quote == b_base:
                    swap_2_rate = self.safe_divide(Decimal('1'), prices_dict.get(t_pair['pair_b'] + '_ask', Decimal('Infinity')))
                    direction_trade_2 = "base_to_quote"
                    contract_2 = t_pair['pair_b']
                    
                    if b_quote == c_base:
                        swap_3_rate = self.safe_divide(Decimal('1'), prices_dict.get(t_pair['pair_c'] + '_ask', Decimal('Infinity')))
                        direction_trade_3 = "base_to_quote"
                        contract_3 = t_pair['pair_c']
                    else:
                        swap_3_rate = prices_dict.get(t_pair['pair_c'] + '_bid', Decimal('0'))
                        direction_trade_3 = "quote_to_base"
                        contract_3 = t_pair['pair_c']
                else:
                    swap_2_rate = prices_dict.get(t_pair['pair_b'] + '_bid', Decimal('0'))
                    direction_trade_2 = "quote_to_base"
                    contract_2 = t_pair['pair_b']
                    
                    if b_quote == c_base:
                        swap_3_rate = self.safe_divide(Decimal('1'), prices_dict.get(t_pair['pair_c'] + '_ask', Decimal('Infinity')))
                        direction_trade_3 = "base_to_quote"
                        contract_3 = t_pair['pair_c']
                    else:
                        swap_3_rate = prices_dict.get(t_pair['pair_c'] + '_bid', Decimal('0'))
                        direction_trade_3 = "quote_to_base"
                        contract_3 = t_pair['pair_c']

            else:  # reverse
                if a_base == b_base:
                    swap_2_rate = self.safe_divide(Decimal('1'), prices_dict.get(t_pair['pair_b'] + '_ask', Decimal('Infinity')))
                    direction_trade_2 = "base_to_quote"
                    contract_2 = t_pair['pair_b']
                    
                    if b_quote == c_quote:
                        swap_3_rate = prices_dict.get(t_pair['pair_c'] + '_bid', Decimal('0'))
                        direction_trade_3 = "quote_to_base"
                        contract_3 = t_pair['pair_c']
                    else:
                        swap_3_rate = self.safe_divide(Decimal('1'), prices_dict.get(t_pair['pair_c'] + '_ask', Decimal('Infinity')))
                        direction_trade_3 = "base_to_quote"
                        contract_3 = t_pair['pair_c']
                elif a_base == b_quote:
                    swap_2_rate = prices_dict.get(t_pair['pair_b'] + '_bid', Decimal('0'))
                    direction_trade_2 = "quote_to_base"
                    contract_2 = t_pair['pair_b']
                    
                    if b_base == c_quote:
                        swap_3_rate = prices_dict.get(t_pair['pair_c'] + '_bid', Decimal('0'))
                        direction_trade_3 = "quote_to_base"
                        contract_3 = t_pair['pair_c']
                    else:
                        swap_3_rate = self.safe_divide(Decimal('1'), prices_dict.get(t_pair['pair_c'] + '_ask', Decimal('Infinity')))
                        direction_trade_3 = "base_to_quote"
                        contract_3 = t_pair['pair_c']

            # Calculate final arbitrage rate
            final_amount = starting_amount
            for swap_rate in [swap_1_rate, swap_2_rate, swap_3_rate]:
                final_amount *= swap_rate * (Decimal('1') - self.trading_fee)
            
            arbitrage_rate = (final_amount / starting_amount) - Decimal('1')

            surface_dict[direction] = {
                "swap_1": (swap_1, swap_2, swap_1_rate, direction_trade_1),
                "swap_2": (contract_2, swap_2_rate, direction_trade_2),
                "swap_3": (contract_3, swap_3_rate, direction_trade_3),
                "arbitrage_rate": arbitrage_rate
            }

        return surface_dict

# Example usage:
if __name__ == "__main__":
    api_key = "YOUR_API_KEY"
    api_secret = "YOUR_API_SECRET"
    
    trader = BitvavoTrader(api_key, api_secret)
    
    # Get market data
    market_data = trader.get_market_data()
    
    # Collect tradeable pairs
    tradeable_pairs = trader.collect_tradeables(market_data)
    
    # Structure triangular pairs
    triangular_pairs = trader.structure_triangular_pairs(tradeable_pairs)
    
    # Calculate arbitrage opportunities
    for t_pair in triangular_pairs:
        prices = trader.get_price_for_t_pair(t_pair, market_data)
        arb_rates = trader.cal_triangular_arb_surface_rate(t_pair, prices)
        
        # Print arbitrage opportunities above a certain threshold
        threshold = Decimal('0.01')  # 1% profit
        for direction, data in arb_rates.items():
            if data['arbitrage_rate'] > threshold:
                print(f"Arbitrage opportunity found: {t_pair['combined']}")
                print(f"Direction: {direction}")
                print(f"Profit rate: {data['arbitrage_rate']:.2%}")
                print("-----")
