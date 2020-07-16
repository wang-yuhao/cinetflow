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
            with open(path_main, 'r') as read_obj, open(path_ts, 'r') as read_obj_time:
                flow_list = []
                flow_time_list = []
                row = ()
                for line,line_time in zip(read_obj, read_obj_time):
                    row = re_combine(line)
                    row_time = re_combine(line_time)
                    if(len(row) == 9):
                        (srcaddr, first_Y_M_D, first_H_M_S,last_Y_M_D, last_H_M_S, flows, packets, byte, bps) = row
                        try:
                            socket.inet_aton(srcaddr)
                            srcaddr = struct.unpack('!L', socket.inet_aton(srcaddr))[0]
                        except socket.error:
                            continue
                            # srcaddr = struct.unpack('!QQ', socket.inet_pton(socket.AF_INET6, srcaddr))[0]
                        first = first_Y_M_D + first_H_M_S
                        first = re.split("-|:|\.",first)
                        first = int("".join(first[0:5]))
                        last = last_Y_M_D+last_H_M_S
                        last = re.split("-|:|\.",last)
                        last = int("".join(last[0:5]))
                        flows = int(flows)
                        packets = int(float(packets))
                        byte = int(float(byte))
                        bps = int(float(bps))
                        row = (srcaddr, first, last, flows, packets, byte, bps) 
                    elif(len(row) == 10):
                        (srcaddr, first_Y_M_D, first_H_M_S,last_Y_M_D, last_H_M_S, flows, packets, byte, byte_unit, bps) = row
                        try:
                            socket.inet_aton(srcaddr)
                            srcaddr = struct.unpack('!L', socket.inet_aton(srcaddr))[0]
                        except socket.error:
                            continue
                            # srcaddr = struct.unpack('!QQ', socket.inet_pton(socket.AF_INET6, srcaddr))[0]
                        first = first_Y_M_D + first_H_M_S
                        first = re.split("-|:|\.",first)
                        first = int("".join(first[0:5]))
                        last = last_Y_M_D+last_H_M_S
                        last = re.split("-|:|\.",last)
                        last = int("".join(last[0:5]))
                        flows = int(flows)
                        packets = int(float(packets))
                        if(byte_unit == 'M'):
                            byte = int(float(byte)) * 10**6
                        elif(byte_unit == 'G'):
                            byte = int(float(byte)) * 10**9
                        elif(bps == 'M'): 
                            bps = byte_unit * 10**6
                        elif(bps == 'G'): 
                            bps = byte_unit * 10**9
                        row = (srcaddr, first, last, flows, packets, byte, bps) 
                    elif(len(row) == 11):
                        (srcaddr, first_Y_M_D, first_H_M_S,last_Y_M_D, last_H_M_S, flows, packets, byte, byte_unit, bps, bps_unit) = row
                        try:
                            socket.inet_aton(srcaddr)
                            srcaddr = struct.unpack('!L', socket.inet_aton(srcaddr))[0]
                        except socket.error:
                            continue
                            # srcaddr = struct.unpack('!QQ', socket.inet_pton(socket.AF_INET6, srcaddr))[0]
                        first = first_Y_M_D + first_H_M_S
                        first = re.split("-|:|\.",first)
                        first = int("".join(first[0:5]))
                        last = last_Y_M_D+last_H_M_S
                        last = re.split("-|:|\.",last)
                        last = int("".join(last[0:5]))
                        flows = int(flows)
                        if(byte == 'M'):
                            packets = int(float(packets)) * 10**6
                            if(bps == 'M'):
                                byte = int(float(byte_unit)) * 10**6
                            elif(bps == 'G'):
                                byte = int(float(byte_unit)) * 10**9
                            bps = bps_unit                    
                        elif(byte == 'G'):
                            packets = int(float(packets)) * 10**9
                            if(bps == 'M'):
                                byte = int(float(byte_unit)) * 10**6
                            elif(bps == 'G'):
                                byte = int(float(byte_unit)) * 10**9
                            bps = bps_unit   
                        else:    
                            packets = int(float(packets))
                            if(byte_unit == 'M'):
                                byte = int(float(byte)) * 10**6
                            elif(byte_unit == 'G'):
                                byte = int(float(byte)) * 10**9
                            if(bps_unit == 'M'):
                                bps = int(float(bps)) * 10**6
                            elif(bps_unit == 'M'):
                                bps = int(float(bps)) * 10**9
                        row = (srcaddr, first, last, flows, packets, byte, bps) 
                    elif(len(row) == 12):
                        (srcaddr, first_Y_M_D, first_H_M_S,last_Y_M_D, last_H_M_S, flows, packets, packets_unit, byte, byte_unit, bps, bps_unit) = row
                        try:
                            socket.inet_aton(srcaddr)
                            srcaddr = struct.unpack('!L', socket.inet_aton(srcaddr))[0]
                        except socket.error:
                            continue
                            # srcaddr = struct.unpack('!QQ', socket.inet_pton(socket.AF_INET6, srcaddr))[0]
                        first = first_Y_M_D + first_H_M_S
                        first = re.split("-|:|\.",first)
                        first = int("".join(first[0:5]))
                        last = last_Y_M_D+last_H_M_S
                        last = re.split("-|:|\.",last)
                        last = int("".join(last[0:5]))
                        flows = int(float(flows))
                        if(packets_unit == 'M'):
                            packets = int(float(packets)) * 10**6
                        elif(packets_unit == 'G'):
                            packets = int(float(packets)) * 10**9
                        if(byte_unit == 'M'):
                            byte = int(float(byte)) * 10**6
                        elif(byte_unit == 'G'):
                            byte = int(float(byte)) * 10**9
                        if(bps_unit == 'M'):
                            bps = int(float(bps)) * 10**6
                        elif(bps_unit == 'M'):
                            bps = int(float(bps)) * 10**9
                        row = (srcaddr, first, last, flows, packets, byte, bps) 
                    flow_list.append(row)
                    (last_Y_M_D_time, last_H_M_S_time, srcaddr, srcport, proto) = row_time
                    last = last_Y_M_D_time + last_H_M_S_time
                    last = re.split("-|:|\.",last)
                    last = int("".join(last[0:5]))
                    row_time = (last, srcaddr, srcport, proto)
                    flow_time_list.append(row_time)
                    if(len(flow_list) == 100000):
                        asyncio.run(asyncio.gather(self.myTool.myInsert(*flow_list), self.influTool.influInsert(*flow_time_list)))
                        flow_list = []
                        flow_time_list = []
                        # asyncio.run(asyncio.gather(self.myTool.insertRecords(*flow_list), InsertRecords(*flow_list)))
                        # print(flow_list)
                        # break

