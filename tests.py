import unittest
from datetime import datetime, timedelta
from mock import MagicMock
from iface.select_fields import *
from iface.functions import *


class SelectFieldsTest(unittest.TestCase):
    def test_get_seq_lists(self):
        dataframe_mock = MagicMock()
        dataframe_mock.select.return_value.collect.return_value = ([0, 0], [1, 1], [2, 2])
        result1, result2 = get_seq_lists(dataframe_mock)
        self.assertEquals(type(result1), list)
        self.assertEquals(type(result2), list)
        self.assertEquals(result1, [0, 1, 2])
        self.assertEquals(result2, [0, 1, 2])


class FuntionsTest(unittest.TestCase):
    def test_decode_sql(self):
        result = decode_sql(1, 1, 3, 4)
        self.assertEquals(result, 3)
        result = decode_sql(1, 2, 3, 4)
        self.assertEquals(result, 4)

    def test_calculate_night_amount(self):
        start_date = None
        end_date = None
        number_units = 0
        number_passengers = 0
        service_unit_type = 0
        indicator_ps = 0
        unit_amount = 0
        number_days = 0

        result = calculate_night_amount(start_date, end_date, number_units, number_passengers,
                                        service_unit_type, indicator_ps, unit_amount, number_days)

        self.assertEquals(result, 0)

        start_date = datetime.now()
        end_date = datetime.now() + timedelta(days=5)
        number_days = 10
        unit_amount = 100
        number_units = 2
        result = calculate_night_amount(start_date, end_date, number_units, number_passengers,
                                        service_unit_type, indicator_ps, unit_amount, number_days)
        self.assertEquals(result, 2000)

        indicator_ps = 'P'
        number_passengers = 3
        result = calculate_night_amount(start_date, end_date, number_units, number_passengers,
                                        service_unit_type, indicator_ps, unit_amount, number_days)
        self.assertEquals(result, 3000)

        number_days = 0
        service_unit_type = 'N'
        result = calculate_night_amount(start_date, end_date, number_units, number_passengers,
                                        service_unit_type, indicator_ps, unit_amount, number_days)
        self.assertEquals(result, 1500)

        service_unit_type = 'S'
        result = calculate_night_amount(start_date, end_date, number_units, number_passengers,
                                        service_unit_type, indicator_ps, unit_amount, number_days)
        self.assertEquals(result, 1800)


if __name__ == '__main__':
    unittest.main()
