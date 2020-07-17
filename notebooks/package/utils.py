#!/usr/bin/env python3

"""
This file belongs to https://github.com/bitkeks/python-netflow-v9-softflowd.

Copyright 2016-2020 Dominik Pataky <software+pynetflow@dpataky.eu>
Licensed under MIT License. See LICENSE.
"""

import struct
import socket  

from typing import Union

from .ipfix import IPFIXExportPacket
from .v1 import V1ExportPacket
from .v5 import V5ExportPacket
from .v9 import V9ExportPacket



def re_combine(line):
            line = line.replace("\n", "")
            row = line.split(' ')
            row = list(filter(None, row))
            return row


class UnknownExportVersion(Exception):
    def __init__(self, data, version):
        self.data = data
        self.version = version
        r = repr(data)
        data_str = ("{:.25}..." if len(r) >= 28 else "{}").format(r)
        super().__init__(
            "Unknown NetFlow version {} for data {}".format(version, data_str)
        )


def get_export_version(data):
    return struct.unpack('!H', data[:2])[0]


def parse_packet(data: Union[str, bytes], templates=None):
    if templates is None:  # compatibility for v1 and v5
        templates = {}

    if type(data) == str:
        # hex dump as string
        data = bytes.fromhex(data)
    elif type(data) == bytes:
        # check representation based on utf-8 decoding result
        try:
            # hex dump as bytes, but not hex
            dec = data.decode()
            data = bytes.fromhex(dec)
        except UnicodeDecodeError:
            # use data as given, assuming hex-formatted bytes
            pass

    version = get_export_version(data)
    if version == 1:
        return V1ExportPacket(data)
    elif version == 5:
        return V5ExportPacket(data)
    elif version == 9:
        return V9ExportPacket(data, templates["netflow"])
    elif version == 10:
        return IPFIXExportPacket(data, templates["ipfix"])
    raise UnknownExportVersion(data, version)

def str_ip_to_int(str_ip):
    int_ip = struct.unpack('!I', socket.inet_aton(str_ip))[0]  
    return int_ip

def int_ip_to_str(int_ip):
    str_ip = socket.inet_ntoa(struct.pack('!I', int_ip))  
    return str_ip

# filter srcaddr, srcport, first, last, protocol, flows, packets, bytes from flow
def flow_filter_v4(client, export):
    # if v5
    if(export.header.version == 5):
        flows_count = export.header.count
        # convert string ip to int ip for save storage space 
        if isinstance(client[0], str):
            srcaddr = str_ip_to_int(client[0])
        else:
            srcaddr = client[0]
     
        srcport = client[1]

        first = export.flows[0].FIRST_SWITCHED
        last = export.flows[0].LAST_SWITCHED
        protocol = export.flows[0].PROTO
        packets = export.flows[0].IN_PACKETS
        byte = export.flows[0].IN_OCTETS 
        del export.flows[0]     
        
        for flow in export.flows:
            if(flow.FIRST_SWITCHED < first):
                first = flow.FIRST_SWITCHED
                
            if(flow.LAST_SWITCHED > last):
                last = flow.LAST_SWITCHED

            packets += flow.IN_PACKETS
            byte += flow.IN_OCTETS

    # if v9 and IPV4
    elif(export.header.version == 9):
        flows_count = export.header.count
        # convert string ip to int ip for save storage space 
        if isinstance(client[0], str):
            srcaddr = str_ip_to_int(client[0])
        else:
            srcaddr = client[0]
     
        srcport = client[1]

        first = export.flows[0].FIRST_SWITCHED
        last = export.flows[0].LAST_SWITCHED
        protocol = export.flows[0].PROTOCOL
        packets = export.flows[0].IN_PKTS
        byte = export.flows[0].IN_BYTES 
        del export.flows[0]     
        
        for flow in export.flows:
            if(flow.FIRST_SWITCHED < first):
                first = flow.FIRST_SWITCHED
                
            if(flow.LAST_SWITCHED > last):
                last = flow.LAST_SWITCHED

            packets += flow.IN_PKTS
            byte += flow.IN_BYTES
    return srcaddr, srcport, first, last, protocol, flows_count, packets, byte


def flow_filter_v6(client, export):
    if(export.header.version == 9):
        flows_count = export.header.count
        srcaddr = export.client[0]
        srcport = export.client[1]

        first = export.flows[0].FIRST_SWITCHED
        last = export.flows[0].LAST_SWITCHED
        protocol = export.flows[0].PROTOCOL
        packets = export.flows[0].IN_PKTS
        byte = export.flows[0].IN_BYTES 
        del export.flows[0]     
        
        for flow in export.flows:
            if(flow.FIRST_SWITCHED < first):
                first = flow.FIRST_SWITCHED
                
            if(flow.LAST_SWITCHED > last):
                last = flow.LAST_SWITCHED

            packets += flow.IN_PKTS
            byte += flow.IN_BYTES

    return srcaddr, srcport, first, last, protocol, flows_count, packets, byte

