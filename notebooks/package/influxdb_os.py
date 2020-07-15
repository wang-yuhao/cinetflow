import asyncio
import time
import pandas as pd
from aioinflux import InfluxDBClient


        
async def InsertRecords(*records):
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

    async with InfluxDBClient(username='root', password='0318',db='assetdb_ts') as client:
       await client.create_database(db='assetdb_ts')
       await client.write(new_record, 'demo', tag_columns=['srcaddr'], asset_class='equities')
       #resp = await client.query('SELECT value FROM cpu_load_short')
       #print(resp)
       print("Write DataFrame")

# asyncio.get_event_loop().run_until_complete(main())
