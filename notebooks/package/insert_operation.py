from csv import reader
import socket, struct
import re
import argparse
import gzip
import json
import logging
import queue
import socketserver
import threading
import time
import asyncio
import nest_asyncio
nest_asyncio.apply()
from collections import namedtuple


from package.ipfix import IPFIXTemplateNotRecognized
#from package.utils import *
from package.utils import UnknownExportVersion, parse_packet, flow_filter_v4, flow_filter_v6
from package.v9 import V9TemplateNotRecognized
from package.mysql_os import MysqlOperation
from package.influxdb_os import InfluOperation

def re_combine(line):
            line = line.replace("\n", "")
            row = line.split(' ')
            row = list(filter(None, row))
            return row

class InsertOperation():
        def __init__(self):
            self.myTool = MysqlOperation()
            self.influTool = InfluOperation()


	# open file in read mode
	# insert records to mysql and influxdb

        def InsertToDB(self, path_main, path_ts):
            asyncio.run(asyncio.gather(self.myTool.myInsert(path_main), self.influTool.influInsert(path_ts)))
                        # asyncio.run(asyncio.gather(self.myTool.insertRecords(*flow_list), InsertRecords(*flow_list)))
                        # print(flow_list)
                        # break

