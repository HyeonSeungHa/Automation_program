from multiprocessing import cpu_count


# Socket Path

bind = 'unix:/run/tsum20_fastapi.sock'



# Worker Options

workers = 10

worker_class = 'uvicorn.workers.UvicornWorker'



# Logging Options

loglevel = 'debug'

accesslog = '/home/ubuntu/work/tsum_v2/logs/access_log'

errorlog =  '/home/ubuntu/work/tsum_v2/logs/error_log'