
"""
安装python
yum install rh-python36 -y
/opt/rh/rh-python36/root/usr/bin/python3 -V
参考
https://github.com/mingrammer/redis-migrate/blob/master/migrator/main.py
用例
/opt/rh/rh-python36/root/usr/bin/python3 redis-copy.py
"""

import sys
if __name__ == '__main__':
    sys.dont_write_bytecode = True

import redis

#REDIS_ADDR_SRC = "-h 127.0.0.1 -db 2  --tls False"
#REDIS_ADDR_DST = "-h 127.0.0.1 -db 12"
print("REDIS_ADDR_SRC:", REDIS_ADDR_SRC)
print("REDIS_ADDR_DST:", REDIS_ADDR_DST)

def parse_redis_addr(s):
    m = {}
    l = s.split()
    for k, v in zip(l[::2], l[1::2]):
        m[k] = v
    return m

REDIS_ADDR_SRC = parse_redis_addr(REDIS_ADDR_SRC)
REDIS_ADDR_DST = parse_redis_addr(REDIS_ADDR_DST)
print("parse_redis_addr(REDIS_ADDR_SRC):", REDIS_ADDR_SRC)
print("parse_redis_addr(REDIS_ADDR_DST):", REDIS_ADDR_DST)

redis_src = redis.StrictRedis(
    host=REDIS_ADDR_SRC["-h"],
    port=int(REDIS_ADDR_SRC.get("-p", 6379)),
    password=REDIS_ADDR_SRC.get("-a", None),
    db=int(REDIS_ADDR_SRC.get("-db", 0)),
    ssl=(REDIS_ADDR_SRC.get("--tls", "") == "True"),
    charset='utf8')

redis_dst = redis.StrictRedis(
    host=REDIS_ADDR_DST["-h"],
    port=int(REDIS_ADDR_DST.get("-p", 6379)),
    password=REDIS_ADDR_DST.get("-a", None),
    db=int(REDIS_ADDR_DST.get("-db", 0)),
    ssl=(REDIS_ADDR_DST.get("--tls", "") == "True"),
    charset='utf8')

def copy_db():
    print("copy_db BEGIN")
    total=redis_src.dbsize()
    print("keycount=",total)

    cursor = 0
    count = 1000
    movecount = 0
    notreplace = 0
    errorcount = 0
    num = 0
    while True:
        cursor, keys = redis_src.scan(cursor, count=count)
        pipeline_src = redis_src.pipeline(transaction=False)
        for key in keys:
            pipeline_src.pttl(key)
            pipeline_src.dump(key)
            num = num+1
        dumps = pipeline_src.execute()

        pipeline_dst = redis_dst.pipeline(transaction=False)
        for key, ttl, data in zip(keys, dumps[::2], dumps[1::2]):
            #print(key,ttl,data)
            if data != None:
                pipeline_dst.restore(key, ttl if ttl > 0 else 0, data, replace=True)

        results = pipeline_dst.execute(False)
        for key, result in zip(keys, results):
            if result == b'OK':
                movecount = movecount+1
            elif result == b'BUSYKEY Target key name already exists.':
                notreplace = notreplace+1
            else:
                errorcount = errorcount+1
                print('Migration failed on key {}: {}'.format(key, result))
        
        sys.stdout.write("%d/%d"%(num,total))
        sys.stdout.write("%\r")
        sys.stdout.flush()

        if cursor == 0:
            break

    print("movecount={} notreplace={} errorcount={}".format(movecount, notreplace, errorcount))
    print("copy_db FINISHED")

if __name__ == '__main__':
    copy_db()


