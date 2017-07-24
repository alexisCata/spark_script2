import datetime
import math

from exchange_rate_db import EXCHANGE_RATE_DB, TIMESTAMPS_DB


from bisect import bisect_right

def find_le(a, x):
    'Find rightmost value less than or equal to x'
    i = bisect_right(a, x)
    if i:
        return a[i-1]
    raise ValueError


def trunc5(v):
    # return math.trunc(v*10.**N)/10.**N
    return math.trunc(v*100000.)/100000.


def exchange_amount(from_currency,
                    to_currency,
                    timestamp,
                    amount=1.,
                    exchange_rate_type='M'
                    ):
    try:
        if isinstance(timestamp, datetime.datetime):
            timestamp = timestamp.date()

        lookup_timestamp = find_le(TIMESTAMPS_DB, timestamp)

        r1 = EXCHANGE_RATE_DB[exchange_rate_type]['EXCHANGE_RATES'][lookup_timestamp][from_currency]
        if r1 is None:
            return None

        r2 = EXCHANGE_RATE_DB[exchange_rate_type]['EXCHANGE_RATES'][lookup_timestamp][to_currency]
        if r2 is None:
            return None

        return trunc5(float(amount) * r1 / r2)

    except:
        return None


if __name__ == '__main__':
    # print exchange_amount('GBP', 'USD', datetime.date(2018, 12, 30), 1., 'M')
    print exchange_amount('EUR', 'USD', datetime.date(2017, 8, 1), 1., 'M')