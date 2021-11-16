from check_nodes_influxdb import load_node_table
import unittest


class TestUtils(unittest.TestCase):

    def test_load_node_table(self):
        load_node_table()


if __name__ == "__main__":
    unittest.main()
