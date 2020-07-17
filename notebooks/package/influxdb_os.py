import asyncio
import time
import pandas as pd
import datetime
import re
from aioinflux import InfluxDBClient, iterpoints
from package.time_tracking import plot_time_tracking
from package.utils import re_combine

class InfluOperation():
    def __init__(self):
        # logger.info("Starting connection with mysql {}:{}".format(host, port))
        try:
            print("start to initialize influxdb! ")
            self.flag = True
            self.total_time = []
            self.curr_time = []
            self.first_query_curr_time = []
            self.first_query_total_time = []
            self.second_query_curr_time = []
            self.second_query_total_time = []             
            self.plot_name = "influ_time_tracking"
            self.first_query = "influ_first_query_time_tracking"
            self.second_query = "influ_second_query_time_tracking"
        except asyncio.CancelledError:
            raise asyncio.CancelledError
        except Exception as ex:
            print("Initialize influxdb failed：{}".format(ex.args[0]))
            return False

    async def influInsert(self, path_ts):
        async with InfluxDBClient(username='root', password='0318', db='assetdb') as client:
            if(self.flag == True):
                await client.query('drop measurement assetdb_ts')
                self.flag = False
            flow_time_list = []
            with open(path_ts, 'r') as read_obj_time:
                start_time = datetime.datetime.now()
                for line_time in read_obj_time:
                    row_time = re_combine(line_time)
                    (last_Y_M_D_time, last_H_M_S_time, srcaddr, srcport, proto) = row_time
                    last = last_Y_M_D_time + last_H_M_S_time
                    last = re.split("-|:|\.",last)
                    last = int("".join(last[0:5]))
                    row_time = (last, srcaddr, srcport, proto)
                    flow_time_list.append(row_time)
                    if(len(flow_time_list) == 100000):
                    # write DataFrame into influxdb and count the total time
                    # Filter 'srcaddr', 'arcport', and 'First' from records, and transform list to DataFrame
                        new_record = pd.DataFrame(data=flow_time_list, columns=['Last', 'srcaddr','srcport','protocol'])
                        new_record['Last'] = pd.to_datetime(new_record['Last'], format='%Y%m%d%H%M%S')
                        # Set the 'First' as index
                        new_record.set_index('Last', inplace=True)
                        #async with InfluxDBClient(username='root', password='0318', db='assetdb') as client:
                            #if(self.flag == True):
                                #await client.query('drop measurement assetdb_ts')
                                #await client.create_database(db='assetdb')
                            #self.flag = False
                        await client.write(new_record, 'assetdb_ts', tag_columns=['srcaddr'])
                        print("Write DataFrame")
                        flow_time_list = []

            new_record = pd.DataFrame(data=flow_time_list, columns=['Last', 'srcaddr', 'srcport', 'protocol'])
            new_record['Last'] = pd.to_datetime(new_record['Last'], format='%Y%m%d%H%M%S')
        
            # Set the 'First' as index
            new_record.set_index('Last', inplace=True)

            start_time = datetime.datetime.now()
            await client.create_database(db='assetdb')
            await client.write(new_record, 'assetdb_ts', tag_columns=['srcaddr'])
            end_time = datetime.datetime.now()
            consume_time = end_time - start_time
            consume_time = consume_time.total_seconds()
            end_time = end_time.strftime("%H:%M:%S")
            #time_records.append(consume_time)
            print(consume_time) 
            self.curr_time.append(end_time)
            self.total_time.append(consume_time)
            plot_time_tracking(self.total_time, self.curr_time, self.plot_name)
            print(end_time + " Influxdb execute insert cost: " + str(consume_time) + "seconds")
            #resp = await client.query('SELECT value FROM cpu_load_short')
            #print(resp)
            print("Write DataFrame")

            # consume time for insertion
            end_time = datetime.datetime.now()
            consume_time = end_time - start_time
            consume_time = consume_time.total_seconds()
            end_time = end_time.strftime("%H:%M:%S")
            self.curr_time.append(end_time)
            self.total_time.append(consume_time)
            plot_time_tracking(self.total_time, self.curr_time, self.plot_name)
            print(end_time + " Influxdb execute insert cost: " + str(consume_time) + "seconds")
            
            # The first query statement consumes time:
            start_time = datetime.datetime.now()

            resp = await client.query('SELECT value FROM assetdb_ts where arcport=\'80\'')
            print("All hosts open srcport 80 (Influxdb): ")
            for i in iterpoints(resp):
                print(i)

            end_time = datetime.datetime.now()
            consume_time = end_time - start_time
            consume_time = consume_time.total_seconds()
            end_time = end_time.strftime("%H:%M:%S")
            self.first_query_curr_time.append(end_time)
            self.first_query_total_time.append(consume_time)
            plot_time_tracking(self.first_query_total_time, self.first_query_curr_time, self.first_query)
            print(end_time + " The first query statement consumes time (Influxdb): " + str(consume_time) + " seconds")
            
            # The second query statement consumes time:
            start_time = datetime.datetime.now()
            resp = await client.query('SELECT protocol,srcaddr FROM assetdb_ts where time > now() - 7h') 
            print("All active IPs and protocols have opened port 443 in the last 7 hours (Influxdb): ")
            for i in iterpoints(resp):
                print(i)
            end_time = datetime.datetime.now()
            consume_time = end_time - start_time
            consume_time = consume_time.total_seconds()
            end_time = end_time.strftime("%H:%M:%S")
            self.second_query_curr_time.append(end_time)
            self.second_query_total_time.append(consume_time)
            plot_time_tracking(self.second_query_total_time, self.second_query_curr_time, self.second_query)
            print(end_time + " The second query statement consumes time (Influxdb): " + str(consume_time) + " seconds")
# asyncio.get_event_loop().run_until_complete(main())
