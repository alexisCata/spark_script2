from datetime import datetime
from manager.spark_manager import SparkSession
from settings import URL, DRIVER, USER, PASS, logger
from exchange import exchange_amount
from tables import tables


decimals_dict = {}


def decode_sql(*args):
    if len(args)==4:
        if args[0] == args[1]:
            return args[2]
        else:
            return args[3]


def replace_sql(word, to_replace, replace_with):

    return word.replace(to_replace, replace_with)


def round_sql(number, decimals):
    return round(number, decimals)


def exchange_amount_decimal(from_currency, to_currency, timestamp, amount=1., exchange_rate_type='M'):
    return exchange_amount(from_currency, to_currency, timestamp, amount, exchange_rate_type) or 0.00


def load_dataframe(table):
    session = SparkSession.builder.getOrCreate()
    dataframe = session.read.jdbc(url=URL,
                                  table=table,
                                  properties={"driver": DRIVER,
                                              "user": USER,
                                              "password": PASS})
    return dataframe


# Re_Fu_Calcular_Noch_Imp
def calculate_night_amount(start_date, end_date, number_units, number_passengers, service_unit_type, indicator_ps,
                           unit_amount, number_days=None):
    """
    It calculates de number of days or night and the total amount considering the dates, the units and service type.
    Note: 
        If number_days is not null it will not be calculated again and we are calculating the total amount
        If indicator_ps is equal to 'T' the unit_amount must be per day
    :param start_date: Service start date
    :param end_date: Sercie end date
    :param number_units: Number of units (passengers) of the service
    :param number_passengers: Number of passengers
    :param service_unit_type: Service unit type : 'D'ay, 'N'ight, 'P'unctual
    :param indicator_ps: Indicator of 'P'ax or 'S'ervice
    :param unit_amount: Unit amount
    :param number_days: Number of days/night of the service
    :return: number_days, total_amount
    """

    if not start_date or not end_date:
        return 0

    if not number_days:
        if service_unit_type == 'N':
            number_days = (end_date.date() - start_date.date()).days
        elif service_unit_type == 'S':
            number_days = (end_date.date() - start_date.date()).days + 1
        else:
            number_days = 1

    aux_amount = unit_amount * number_days

    total_amount = aux_amount * number_passengers if indicator_ps == 'P' else aux_amount * number_units

    return total_amount


def round_ccy(amount, ccy):
    """
    It round an amount getting the number of decimals from acq_atlas_general_gn_t_divisa
    :param amount: 
    :param ccy: 
    :return: 
    """

    try:
        # df = load_dataframe(tables['dwc_gen_t_general_currency'])
        # decimals = df.filter(df.cod_divisa == ccy).select("decimales").collect()[0][0]
        decimals = decimals_dict[ccy]
        rounded_amount = round(amount, decimals)
    except Exception as e:
        logger.error("Error on round_ccy: {}".format(e))
        rounded_amount = amount

    return rounded_amount


def load_round_ccy(manager):
    """
    It round an amount getting the number of decimals from acq_atlas_general_gn_t_divisa
    :param amount: 
    :param ccy: 
    :return: 
    """

    try:

        global decimals_dict

        df = manager.get_dataframe(tables['dwc_gen_t_general_currency'])
        decimals = df.select("cod_divisa", "decimales").collect()

        for v in decimals:
            decimals_dict[v[0]] = v[1]

    except Exception as e:
        logger.error("Error on load_round_ccy: {}".format(e.message))


def date_to_datetime(value):
    if not value:
        return None
        logger.info("NONEDATE: {}".format(value))

    if isinstance(value, unicode):
        logger.info("DATE: {}".format(value))
        value = datetime.strptime(value, '%Y-%m-%d').date()

    return value.strftime('%Y-%m-%d')

