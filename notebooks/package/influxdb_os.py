import asyncio
import time
import pandas as pd
import datetime
from aioinflux import InfluxDBClient
from package.time_tracking import plot_time_tracking

class InfluOperation():
    def __init__(self):
        # logger.info("Starting connection with mysql {}:{}".format(host, port))
        try:
            print("start to connect influxdb! ")
            self.flag = True
            self.total_time = []
            self.curr_time = []
            self.plot_name = "influ_time_tracking"
        except asyncio.CancelledError:
            raise asyncio.CancelledError
        except Exception as ex:
            print("influxdb数据库连接失败：{}".format(ex.args[0]))
            return False

    async def influInsert(self, *records):
        # write DataFrame into influxdb and count the total time
        # Filter 'srcaddr', 'arcport', and 'First' from records, and transform list to DataFrame
        new_record = pd.DataFrame(data=records, columns=['Last', 'srcaddr','srcport','protocol'])
        # new_record = pd.DataFrame(data=new_record, columns=['srcaddr','srcport','First'])
        
        #new_record["srcaddr"] = new_record["srcaddr"].map(lambda x: struct.unpack('!I', socket.inet_aton(x))[0])
        
        # convert the format of values in 'First' column to datetime  
        # new_record['Last'] = 20200709035501 
        # print(new_record['First'])
        new_record['Last'] = pd.to_datetime(new_record['Last'], format='%Y%m%d%H%M%S')
        
        # Set the 'First' as index
        new_record.set_index('Last', inplace=True)
        async with InfluxDBClient(username='root', password='0318',db='assetdb_ts') as client:
            start_time = datetime.datetime.now()
            await client.create_database(db='assetdb_ts')
            await client.write(new_record, 'demo', tag_columns=['srcaddr'])
            end_time = datetime.datetime.now()
            consume_time = end_time - start_time
            consume_time = consume_time.total_seconds()
            end_time = end_time.strftime("%H:%M:%S")
            #time_records.append(consume_time)
            print(consume_time) 
            self.curr_time.append(end_time)
            self.total_time.append(consume_time)
            plot_time_tracking(self.total_time, self.curr_time, self.plot_name)
            print(end_time + " execute insert cost: " + str(consume_time) + "seconds")
            #resp = await client.query('SELECT value FROM cpu_load_short')
            #print(resp)
            print("Write DataFrame")

# asyncio.get_event_loop().run_until_complete(main())
