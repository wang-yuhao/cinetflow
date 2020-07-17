import asyncio
import aiomysql
import matplotlib.pyplot as plt
import datetime
import socket
import struct
import re

from package.time_tracking import plot_time_tracking
from package.utils import re_combine


class MysqlOperation():
    def __init__(self):
        # logger.info("Starting connection with mysql {}:{}".format(host, port))
        try:
            print("start to connect db! ")
            self.loop = asyncio.new_event_loop()
            self.pool = self.loop.run_until_complete(aiomysql.create_pool(host='127.0.0.1', port=3306,
                                    user='root', password='phpipamadmin',
                                    db='assetdb', charset='utf8'))
            self.flag = True
            self.total_time = []
            self.curr_time = []
            self.first_query_curr_time = []
            self.first_query_total_time = []
            self.second_query_curr_time = []
            self.second_query_total_time = []  
            self.plot_name = "mysql_time_tracking"
            self.first_query = "mysql_first_query_time_tracking"
            self.second_query = "mysql_second_query_time_tracking"
            print("succeed to connect db!")
        except asyncio.CancelledError:
            raise asyncio.CancelledError
        except Exception as ex:
            print("Initialize mysql failed：{}".format(ex.args[0]))
            return False
        
    async def getCurosr(self):
        '''
        get mysql connection and cursor object for the write and read operation
        :param pool:
        :return:
        '''
        conn = await self.pool.acquire()
        cur = await conn.cursor()
        return conn, cur   
    
    async def batchInsert(self, sql, values):
        # first get the connection and cursor object
        conn, cur = await self.getCurosr()
        try:
            # excute sql command
            if(self.flag == True):
            	await cur.execute("DROP TABLE assetdb_main")
            	await cur.execute("CREATE TABLE assetdb_main (srcaddr INT UNSIGNED, first BIGINT UNSIGNED, last BIGINT UNSIGNED, flows INT UNSIGNED, packets BIGINT UNSIGNED, bytes BIGINT UNSIGNED, bps BIGINT UNSIGNED, PRIMARY KEY(`srcaddr`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 PARTITION BY HASH(srcaddr) partitions 100;")
            await cur.executemany(sql, values)
            await conn.commit()
            self.flag = False
            # return sql excuted lines
            return cur.rowcount
        finally:
            # release connection
            await self.pool.release(conn)

    async def Query(self, sql):
        # first get the connection and cursor object
        conn, cur = await self.getCurosr()
        try:
            # excute sql command
            await cur.execute(sql)
            result = await cur.fetchall()
            # return sql excuted lines
            return result
        finally:
            # release connection
            await self.pool.release(conn)

    async def valueQuery(self, sql, value):
        # first get the connection and cursor object
        conn, cur = await self.getCurosr()
        try:
            # excute sql command
            await cur.execute(sql, value)
            result = await cur.fetchall()
            # return sql excuted lines
            return result
        finally:
            # release connection
            await self.pool.release(conn)

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()
        print("close pool!")  
        

    async def myInsert(self, path_main):
        start_time = datetime.datetime.now()
        # read file as list
        with open(path_main, 'r') as read_obj:
                flow_list = []
                row = ()
                for line in read_obj:
                    row = re_combine(line)
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

                    if(len(flow_list)==100000):
                        sql = "INSERT IGNORE INTO assetdb_main (srcaddr, first, last, flows, packets, bytes, bps) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE `last` = VALUES(`last`), `flows`= VALUES(`flows`) + `flows`, `packets`= VALUES(`packets`) + `packets`, `bytes`= VALUES(`bytes`) + `bytes`, `bps` = VALUES(`bps`)"
                        task = asyncio.ensure_future(self.batchInsert( sql, flow_list))
                        result = self.loop.run_until_complete(task)
                        print("insert res:", result)

                sql = "INSERT IGNORE INTO assetdb_main (srcaddr, first, last, flows, packets, bytes, bps) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE `last` = VALUES(`last`), `flows`= VALUES(`flows`) + `flows`, `packets`= VALUES(`packets`) + `packets`, `bytes`= VALUES(`bytes`) + `bytes`, `bps` = VALUES(`bps`)"
                task = asyncio.ensure_future(self.batchInsert( sql, flow_list))
                result = self.loop.run_until_complete(task)
                print("insert res:", result)
                        
        end_time = datetime.datetime.now()
        consume_time = end_time - start_time
        consume_time = consume_time.total_seconds()
        end_time = end_time.strftime("%H:%M:%S")
        print(consume_time) 
        self.curr_time.append(end_time)
        self.total_time.append(consume_time)
        print(end_time + " execute insert cost: " + str(consume_time) + "seconds")
        plot_time_tracking(self.total_time, self.curr_time, self.plot_name)
        
        # The first query statement consumes time:
        start_time = datetime.datetime.now()
        sql = "select srcaddr,bytes from assetdb_main order by bytes DESC limit 10"
        task = asyncio.ensure_future(self.Query( sql))
        result = self.loop.run_until_complete(task)
        print("The top 10 ip which have transported the most bytes:")
        print(result)        
        end_time = datetime.datetime.now()
        consume_time = end_time - start_time
        consume_time = consume_time.total_seconds()
        end_time = end_time.strftime("%H:%M:%S")
        self.first_query_curr_time.append(end_time)
        self.first_query_total_time.append(consume_time)
        plot_time_tracking(self.first_query_total_time, self.first_query_curr_time, self.first_query)
        print(end_time + " The first query statement consumes time: " + str(consume_time) + " seconds")

        # The second query statement consumes time:
        start_time = datetime.datetime.now()
        sql = "select sum(bytes),round(last/100) from assetdb_main where last > %s group by round(last / 100)"
        last_10_hourse = datetime.datetime.now() - datetime.timedelta(hours = 10)
        last_10_hourse = last_10_hourse.strftime("%Y%m%d%H%M") 
        value = int(last_10_hourse)
        task = asyncio.ensure_future(self.valueQuery( sql, value))
        result = self.loop.run_until_complete(task)
        print("Network traffic transmitted per hour in the last ten hours: ")
        print(result)        
        end_time = datetime.datetime.now()
        consume_time = end_time - start_time
        consume_time = consume_time.total_seconds()
        end_time = end_time.strftime("%H:%M:%S")
        self.second_query_curr_time.append(end_time)
        self.second_query_total_time.append(consume_time)
        plot_time_tracking(self.second_query_total_time, self.second_query_curr_time, self.second_query)
        print(end_time + " The second query statement consumes time: " + str(consume_time) + " seconds")
                    # close connection

                    # self.loop.run_until_complete(self.close())
