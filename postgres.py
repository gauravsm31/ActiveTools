#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import DataFrameWriter

class PostgresConnector(object):
    def __init__(self):
        self.database_name = 'postgres'
        self.hostname = 'ip-10-0-0-10'
        self.url_connect = "jdbc:postgresql://{hostname}:5432/{db}".format(hostname=self.hostname, db=self.database_name)
        self.properties = {"user":'DBUSER',
                      "password":'POSTGRES_PW',
                      "driver": "org.postgresql.Driver"
                     }

    def get_writer(self, df):
        return DataFrameWriter(df)

    def write(self, df, table, mode):
        my_writer = self.get_writer(df)
        my_writer.jdbc(self.url_connect, table, mode, self.properties)
