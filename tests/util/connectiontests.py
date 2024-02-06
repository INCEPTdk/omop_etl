import unittest
from collections import OrderedDict

from etl.util.connection import ConnectionDetails, get_connection_details


class ConnectionUnitTests(unittest.TestCase):
    def test_full_config(self):
        config = {
            "dbms": "abc",
            "port": 1234,
            "server": "myhost",
            "schema": "myschema",
            "user": "admin",
            "password": "secret",
            "dbname": "mydatabase",
        }
        expected_details = ConnectionDetails(
            dbms=config["dbms"],
            dbname=config["dbname"],
            port=config["port"],
            host=config["server"],
            user=config["user"],
            password=config["password"],
            schema=config["schema"],
        )

        self.assertEqual(expected_details, get_connection_details(config))

    def test_full_config_different_order(self):
        config = {
            "password": "secret",
            "dbname": "mydatabase",
            "server": "myhost",
            "schema": "myschema",
            "user": "admin",
            "port": 1234,
            "dbms": "abc",
        }
        expected_details = ConnectionDetails(
            dbms=config["dbms"],
            dbname=config["dbname"],
            port=config["port"],
            host=config["server"],
            user=config["user"],
            password=config["password"],
            schema=config["schema"],
        )

        self.assertEqual(expected_details, get_connection_details(config))

    def test_full_config_ordered_dict(self):
        config = OrderedDict(
            {
                "password": "secret",
                "dbname": "mydatabase",
                "server": "myhost",
                "schema": "myschema",
                "user": "admin",
                "port": 1234,
                "dbms": "abc",
            }
        )
        expected_details = ConnectionDetails(
            dbms=config["dbms"],
            dbname=config["dbname"],
            port=config["port"],
            host=config["server"],
            user=config["user"],
            password=config["password"],
            schema=config["schema"],
        )

        self.assertEqual(expected_details, get_connection_details(config))

    def test_missing_dbname(self):
        config = {
            "password": "secret",
            "server": "myhost",
            "schema": "myschema",
            "user": "admin",
            "port": 1234,
            "dbms": "abc",
        }
        with self.assertRaises(KeyError):
            _ = get_connection_details(config)

    def test_missing_user(self):
        config = {
            "password": "secret",
            "server": "myhost",
            "schema": "myschema",
            "dbname": "mydatabase",
            "port": 1234,
            "dbms": "abc",
        }
        with self.assertRaises(KeyError):
            _ = get_connection_details(config)

    def test_missing_port_invalid_db(self):
        config = {
            "password": "secret",
            "dbname": "mydatabase",
            "server": "myhost",
            "schema": "myschema",
            "user": "admin",
            "dbms": "abc",
        }
        expected_details = ConnectionDetails(
            dbms=config["dbms"],
            dbname=config["dbname"],
            port=None,
            host=config["server"],
            user=config["user"],
            password=config["password"],
            schema=config["schema"],
        )

        self.assertEqual(expected_details, get_connection_details(config))

    def test_missing_port_valid_db(self):
        config = {
            "password": "secret",
            "dbname": "mydatabase",
            "server": "myhost",
            "schema": "myschema",
            "user": "admin",
            "dbms": "postgresql",
        }
        # This is really testing implementation details here, but
        # this is something that we need to cover in tests
        expected_details = ConnectionDetails(
            dbms=config["dbms"],
            dbname=config["dbname"],
            port=5432,
            host=config["server"],
            user=config["user"],
            password=config["password"],
            schema=config["schema"],
        )

        self.assertEqual(expected_details, get_connection_details(config))

    def test_missing_port_valid_db_with_port(self):
        config = {
            "password": "secret",
            "dbname": "mydatabase",
            "server": "myhost",
            "schema": "myschema",
            "user": "admin",
            "dbms": "postgresql",
            "port": 5556,
        }
        # This is really testing implementation details here, but
        # this is something that we need to cover in tests
        expected_details = ConnectionDetails(
            dbms=config["dbms"],
            dbname=config["dbname"],
            port=5556,
            host=config["server"],
            user=config["user"],
            password=config["password"],
            schema=config["schema"],
        )
        self.assertEqual(expected_details, get_connection_details(config))


__all__ = ["ConnectionUnitTests"]
