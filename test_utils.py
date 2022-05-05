from utils import load_node_table, parse_time, get_rollup_range, time_windows
import pandas as pd
import unittest


def datetime(s):
    return pd.to_datetime(s, utc=True)


class TestUtils(unittest.TestCase):

    # integration test with node production table api
    def test_load_node_table(self):
        load_node_table()

    def test_parse_time(self):
        # check relative time
        now = datetime("2021-10-11 10:34:23")
        self.assertEqual(parse_time("-3h", now=now), datetime("2021-10-11 07:34:23"))
        # check absolute time
        self.assertEqual(parse_time("2021-10-11 11:22:33", now=now), datetime("2021-10-11 11:22:33"))

    def test_get_rollup_range(self):
        now = datetime("2021-10-11 10:34:23")
        start, end = get_rollup_range(parse_time("-3h", now=now), parse_time("-1h", now=now))
        self.assertEqual(start, datetime("2021-10-11 07:00:00"))
        self.assertEqual(end, datetime("2021-10-11 09:00:00"))

    def test_time_windows(self):
        now = datetime("2021-10-11 10:01:23")
        start, end = get_rollup_range(parse_time("-3h", now=now), parse_time("-1h", now=now))
        windows = list(time_windows(start, end, "1h"))
        expect = [
            (datetime("2021-10-11 07:00:00"), datetime("2021-10-11 08:00:00")),
            (datetime("2021-10-11 08:00:00"), datetime("2021-10-11 09:00:00")),
        ]
        self.assertEqual(windows, expect)


if __name__ == "__main__":
    unittest.main()
