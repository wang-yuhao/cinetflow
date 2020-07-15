import asyncio
import aiomysql

class MysqlOperation():
    def __init__(self):
        # logger.info("Starting connection with mysql {}:{}".format(host, port))
        try:
            print("start to connect db! ")
            self.loop = asyncio.new_event_loop()
            self.pool = self.loop.run_until_complete(aiomysql.create_pool(host='127.0.0.1', port=3306,
                                    user='root', password='phpipamadmin',
                                    db='assetdb', charset='utf8'))
            print("succeed to connect db!")


        except asyncio.CancelledError:
            raise asyncio.CancelledError
        except Exception as ex:
            print("mysql数据库连接失败：{}".format(ex.args[0]))
            return False
        
    async def getCurosr(self):
        '''
        获取db连接和cursor对象，用于db的读写操作
        :param pool:
        :return:
        '''
        conn = await self.pool.acquire()
        cur = await conn.cursor()
        return conn, cur   
    
    async def batchInsert(self, sql, values):
 
        #start = time.time() * 1000
        # first get the connection and cursor object
        conn, cur = await self.getCurosr()
        try:
            # excute sql command
            await cur.execute("DROP TABLE assetdb_main")
            await cur.execute("CREATE TABLE assetdb_main (srcaddr INT UNSIGNED, first BIGINT UNSIGNED, last BIGINT UNSIGNED, flows INT UNSIGNED, packets BIGINT UNSIGNED, bytes BIGINT UNSIGNED, bps BIGINT UNSIGNED, PRIMARY KEY(`srcaddr`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 PARTITION BY HASH(srcaddr) partitions 100;")
            await cur.executemany(sql, values)
            await conn.commit()
            # return sql excuted lines
            return cur.rowcount
        finally:
            # release connection
            await self.pool.release(conn)
            #end = time.time() * 1000
            print("execute insert cost: ")

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()
        print("close pool!")  
        
    async def insertRecords(self, *records):

        # insert many
        # records = [(970485761, 63975, 20200528140731, 20200528140931,1, 18, 295,  482341), (3093939127, 38885, 20200528140731, 20200528142533, 1, 3, 112, 215847), (2110402172, 10110, 20200528140731, 20200528142534, 2, 1, 243, 115125)]
        sql = "INSERT IGNORE INTO assetdb_main (srcaddr, first, last, flows, packets, bytes, bps) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE `last` = VALUES(`last`), `flows`= VALUES(`flows`) + `flows`, `packets`= VALUES(`packets`) + `packets`, `bytes`= VALUES(`bytes`) + `bytes`, `bps` = VALUES(`bps`)"
        task = asyncio.ensure_future(self.batchInsert( sql, records))
        result = self.loop.run_until_complete(task)
        print("insert res:", result)

        # close connection

        self.loop.run_until_complete(self.close())
