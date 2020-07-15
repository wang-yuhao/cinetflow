import asyncio
import time
import pandas as pd
from influxdb import DataFrameClient

class InnodbOperation():
    def __init__(self):
        # logger.info("Starting connection with mysql {}:{}".format(host, port))
        try:
            print("start to connect innodb! ")
            user = 'root'
            password = '0318'
            dbname = 'assetdb_ts'

            self.loop = asyncio.new_event_loop()
            self.client = DataFrameClient('localhost', 8086, user, password, dbname)
            print("succeed to connect innodb!")


        #except asyncio.CancelledError:
            #raise asyncio.CancelledError
        except Exception as ex:
            print("innodb数据库连接失败：{}".format(ex.args[0]))
            return False
       
        
    async def insertRecords(self, *records):

        # insert many
        # records = [(970485761, 63975, 20200528140731, 20200528140931,1, 18, 295,  482341), (3093939127, 38885, 20200528140731, 20200528142533, 1, 3, 112, 215847), (2110402172, 10110, 20200528140731, 20200528142534, 2, 1, 243, 115125)]

        # write DataFrame into influxdb and count the total time
        # Filter 'srcaddr', 'arcport', and 'First' from records, and transform list to DataFrame
        new_record = pd.DataFrame(data=records, columns=['srcaddr','srcport','First','last', 'protocol', 'flows', 'packets', 'bytes'])
        new_record = pd.DataFrame(data=new_record, columns=['srcaddr','srcport','First'])
        
        #new_record["srcaddr"] = new_record["srcaddr"].map(lambda x: struct.unpack('!I', socket.inet_aton(x))[0])
        
        # convert the format of values in 'First' column to datetime  
        new_record['First'] = 20200709035501 
        print(new_record['First'])
        new_record['First'] = pd.to_datetime(new_record['First'], format='%Y%m%d%H%M%S')
        
        # Set the 'First' as index
        new_record.set_index('First', inplace=True)
        print("Write DataFrame")
        client_write_start_time = time.perf_counter()
        print(new_record)
        await self.client.write_points(new_record, 'demo', tag_columns=['srcaddr'], time_precision='ms', batch_size=10000, protocol='line')
        client_write_end_time = time.perf_counter()
        # inf_write_time.append(client_write_end_time - client_write_start_time)
        print("Client Library Write: {inf_write}s".format(inf_write))
