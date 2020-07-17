import os
import asyncio
import nest_asyncio
nest_asyncio.apply()
from collections import namedtuple
from queue import Queue
from threading import Thread,Timer
#from package.insert_operation import InsertOperation
from package.mysql_os import MysqlOperation
from package.influxdb_os import InfluOperation

# Monitor and manage the specifical folder, 
class MonitorFolder():
    # Read all files name into a list
    def __init__(self, path):
        self.q = Queue()
        self.path = path
        files = os.listdir(path)
        files.sort()
        self.flag = files[-1]
        for i in files:
            self.q.put(i)

# TODO change 10 seconds to 5 minutes

    # put all files name into a queue
    # every 10 seconds scan the folder once
    def inQueue(self):
        files = os.listdir(self.path)
        files.sort()
        for i in files:
            if(i > self.flag):
                self.q.put(i)
        self.flag = files[-1]
        print("inQueue working!")
        Timer(10, self.inQueue).start()
    
    # get files name from the queue
    def outQueue(self):
        while True:
            print("outQueue working!")
            yield self.q.get(block=True, timeout=None)

if __name__ == "__main__" :         
    myTool = MysqlOperation()
    influTool = InfluOperation()
    path = './data/test'
    reader = MonitorFolder(path)
    reader.inQueue()
    src_path = './data/test/'
    dst_path = './data/csv/'
    #worker = InsertOperation()
    for i in reader.outQueue():
        print(i)
        (file_format, name) = i.split('.')
        file_main_path = dst_path + name + '_main.csv'
        file_ts_path = dst_path + name + '_ts.csv'
        os.system('nfdump -r ' + src_path + i + ' -A srcip,srcport,proto -q -o \'fmt: %te %sa %sp %pr\' > ' + file_ts_path)
        os.system('nfdump -r ' + src_path + i + ' -A srcip -q -o \'fmt: %sa %ts %te %fl %pkt %byt %bps\' > ' + file_main_path)

        #worker.InsertToDB(file_main_path,file_ts_path )
        asyncio.run(asyncio.gather(myTool.myInsert(file_main_path), influTool.influInsert(file_ts_path)))
