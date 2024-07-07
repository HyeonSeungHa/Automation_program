import traceback
import base64
from io import BytesIO
import os
from PIL import Image
from fastapi import FastAPI, Request, Response
import logging
from logging.handlers import TimedRotatingFileHandler
from logging.config import dictConfig
from kombu import Queue, Exchange
from datetime import datetime
import numpy as np
import time, json
import unicodedata
from celery import Celery
from celery.result import AsyncResult
import asyncio
import redis
from concurrent.futures import ProcessPoolExecutor
import pytesseract
NUM_OF_QUEUE = 5   # 큐 갯수

tsumApp = FastAPI(root_path="/")
# 개발
# tsumApp = FastAPI(root_path="/tsum20/")
# Celeryapp 생성
celeryapp = Celery('tasks')
# celeryconfig 파일 로드
celeryapp.config_from_object('celeryconfig')
rd = redis.Redis(host='redis 주소', port=9876, db=1)

# 큐(Queue) 정의
worker_queues = []
for i in range(1, NUM_OF_QUEUE+1):
    queue_name = f'worker_queue_{i}'
    worker_queue = Queue(
        queue_name, 
        exchange=Exchange(queue_name, type='direct'), 
        routing_key=queue_name
    )
    worker_queues.append(worker_queue)
# Public Queue 생성
celeryapp.conf.task_queues = tuple(worker_queues) + (
    Queue('public', Exchange('public'), routing_key='public'),
)

# 라우팅(Routing) 정의
celeryapp.conf.task_routes = {
    'tasks.worker_task': lambda worker_id: {'queue': f'worker_queue_{worker_id}'},
    'tasks.public_task': {'queue': 'public'},
    
}

# 클래스 변수 생성
class VarForFastAPI:
    strtTm = None
    do = None
    worker_id = None
    numbersearch = None
    current_queue = 0
    pass

"""
Numberserarch 함수를 이용해 차량 번호를 받았을 때 문자인지 체크
"""
def is_korean_chr(chr_u):
    category = unicodedata.category
    if category(chr_u)[0:2] == 'Lo' : # other characters
        if 'HANGUL' in unicodedata.name(chr_u) : return True
    return False

"""
dict 형태의 결과 값을 json 형태로 변환
"""
def jsnRs(content, content_type='application/json; charset=utf-8', code: int = 201):
    reply = Response(json.dumps(content))
    reply.headers["Content-Type"] = content_type
    logging.info('js replyed')
    reply.status_code = code
    return reply

# 404 favicon pass
@tsumApp.get('/favicon')
def favicon():
    logging.info('request from browser...ignore favicon')
    
    return {'msg' : 'favicon'}

"""
요청이 들어왔을 때 실행 되는 본 함수 전 혹은 후에 실행되는 함수
"""

@tsumApp.middleware("http")
async def middleware(request: Request, call_next):
    try:
        
        response = before_request(request=request)
        logging.info('response :::',response)
        
        if response.status_code != 203:
            # 요청이 들어왔을 때 실행 되는 본 함수
            response = await call_next(request)
            pass
        else:
            response.status_code = 200 # 203->200 으로 변경해줌.
    except:
        logging.error('request err')
        logging.error(traceback.format_exc())
    finally:
        after_request(request)
        
        return response

"""
요청 전 실행되는 
"""
def before_request(request):
    # 초기 Response 생성
    response = Response('default response', status_code = 201) 
    logging.info('before_request')
    
    VarForFastAPI.strtTm = time.time()
    # 요청 url에 force가 있을 시
    if 'force' in request.url.path:
        # 파싱 (ex - do = 'open', worker_id = '1')
        query_info = request.url.query.split('&')
        for itm in query_info:
            if 'do' in itm:
                logging.info('receive force')
                VarForFastAPI.do = itm.split('=')[1]
                pass
                # 요청에 do만 있는 경우 worker_id = '1'(default)
                if 'worker_id' not in itm:
                    VarForFastAPI.worker_id = '1'

            elif 'worker_id' in itm:
                VarForFastAPI.worker_id = itm.split('=')[1]
                
                pass
            pass
        # 생성된 worker 개수보다 worker_id에 더 큰 숫자를 입력시 'count err' 반환
        if int(VarForFastAPI.worker_id) > len(worker_queues):
            
            return jsnRs({"forceErr": "worker count err"}, code=203)
        else:
            if VarForFastAPI.do == '00':
                return jsnRs({"force": "00"}, code=203)
        
            elif VarForFastAPI.do == '90':
                return jsnRs({"force": "90"}, code=203)
    else:
        if "NumberSearch" in request.url.path:
            fail_count = 0

            task_cnt_dict = dict()

            for i in range(len(worker_queues)):
                queue_status_name = f'worker_queue_{i+1}_status'
                if rd.exists(queue_status_name):
                    value = rd.get(queue_status_name)
                    value_dict = json.loads(value) # value_dict['task_cnt]

                    if value_dict['requestBlocker'] == False and task_cnt_dict.get(queue_status_name , None) is None:
                        task_cnt_dict[queue_status_name] = 0 # 초기값 생성
                        task_cnt_dict[queue_status_name] = int(value_dict['task_cnt'])

                    if (value_dict['requestBlocker'] == True and value_dict['clientStatus'] == False) or value_dict['requestBlocker'] == True:
                        fail_count += 1
                        pass
                    pass
                pass
            
            if fail_count == len(worker_queues):
                return jsnRs({'err' :'worker all close!'}, code=203)

            if len(task_cnt_dict) > 0:
                worker_key = list(task_cnt_dict.keys())
                worker_key.sort()
               
                # redis에서 task_cnt + 1
                if VarForFastAPI.current_queue >= len(worker_key):
                    VarForFastAPI.current_queue = 0
               
                
                target_queue = worker_key[VarForFastAPI.current_queue]
                update_task_cnt(worker_status_quename=target_queue)
        
                target_worker_queue = target_queue.replace('_status' , '')
                VarForFastAPI.numbersearch = celeryapp.signature('app.numbersearch', queue=target_worker_queue , expires=3600)
                VarForFastAPI.current_queue = (VarForFastAPI.current_queue + 1) % len(worker_key)

        elif "startTSUM" in request.url.path:
            pass
            
        elif "init" in request.url.path:
            # 값 받아오기W
            if 'init' in request.url.path:
                if '=' not in request.url.query:
                    init_worker_id = '1'
                else:
                    init_worker_id = request.url.query.split('=')[1]
            else:
                pass
            init_queue_status_name = f'worker_queue_{int(init_worker_id)}_status'
            if rd.exists(init_queue_status_name):
                init_value = rd.get(init_queue_status_name)
                init_value_dict = json.loads(init_value)  
            
            if init_value_dict['requestBlocker']:
                logging.info('init : 클라이언트 작업중')
                return jsnRs({"resultCode": "10"}, code=203) # 클라이언트가 작업 중인 경우
            if not init_value_dict['clientStatus']:
                logging.info('init : 클라이언트 종료')
                return jsnRs({"resultCode": "90"}, code=203) # 클라이언트가 종료 중인 경우
            pass
        elif "check" in request.url.path:
            pass
        elif "ENV" in request.url.path:
            pass
        elif "requestBlocker" in request.url.path:
            pass
        elif "clientStatus" in request.url.path:
            pass
        elif "workingList" in request.url.path:
            pass
        else:
            return jsnRs({"carNo": "", "resultCode": "400"})
    
    return response

"""
RequestBlock, Clientstate 체크 
"""
@tsumApp.get('/check')
def check(worker_id: int = 1):
    try:
        if worker_id > len(worker_queues):
            return {'Out of Range' : f' Worker 개수 {len(worker_queues)}개 숫자를 다시 입력하세요.'}
        else:
            check = celeryapp.signature('app.check', queue=f'worker_queue_{worker_id}', expires=3600)
            check_delay = check.delay()
            check_result = check_delay.get()
            Response.state_code = 200
            
            return check_result
    except:
        logging.error(traceback.format_exc())
        return jsnRs({"resultCode": "400"})
"""
동작중인 worker 상태 체크
"""
@tsumApp.get('/workingList')
def workingList():
    try:
        with ProcessPoolExecutor() as executor:
                result = executor.submit(get_all_worker_status)
                response = result.result()

        return response
    except:
        logging.error(traceback.format_exc())
        return jsnRs({"resultCode": "400"})  

"""
Worker에서 처음 실행 될 때 설정한 셋팅 확인(server or dev)
"""
@tsumApp.get('/ENV')
def ENV(worker_id: int = 1):
    try:
        if worker_id > len(worker_queues):
            return {'Out of Range' : f' Worker 개수 {len(worker_queues)}개 숫자를 다시 입력하세요.'}
        else:
            env = celeryapp.signature('app.env', queue=f'worker_queue_{worker_id}', expires=3600)
            env_delay = env.delay()
            env_result = env_delay.get()
            Response.state_code = 200

            return env_result
    except:
        logging.error(traceback.format_exc())
        return jsnRs({"resultCode": "400"})
"""
RequestBlock  체크
"""
@tsumApp.get('/requestBlocker')
def check_requestBlocker(worker_id: int = 1):
    try:
        if worker_id > len(worker_queues):
            return {'Out of Range' : f' Worker 개수 {len(worker_queues)}개 숫자를 다시 입력하세요.'}
        else:
            requestblocker = celeryapp.signature('app.requestblocker', queue=f'worker_queue_{worker_id}', expires=3600)
            requestblocker_delay = requestblocker.delay()
            requestblocker_result = requestblocker_delay.get()
            Response.state_code = 200

            return requestblocker_result
    except:
        logging.error(traceback.format_exc())
        return jsnRs({"resultCode": "400"})
"""
ClientStatus 체크
"""
@tsumApp.get('/clientStatus')
def check_clientStatus(worker_id: int = 1):
    try:
        if worker_id > len(worker_queues):
            return {'Out of Range' : f' Worker 개수 {len(worker_queues)}개 숫자를 다시 입력하세요.'}
        else:
            clientstatus = celeryapp.signature('app.clientstatus', queue=f'worker_queue_{worker_id}', expires=3600)
            clientstatus_delay = clientstatus.delay()
            clientstatus_result = clientstatus_delay.get()
            Response.state_code = 200

            return clientstatus_result
    except:
        logging.error(traceback.format_exc())
        return jsnRs({"resultCode": "400"})
"""
초기화
"""
@tsumApp.get('/init')
def init(worker_id: int = 1):
    try:
        if worker_id > len(worker_queues):
            return {'Out of Range' : f' Worker 개수 {len(worker_queues)}개 숫자를 다시 입력하세요.'}
        else:
            init = celeryapp.signature('app.init', queue=f'worker_queue_{worker_id}', expires=3600)
            init_delay = init.delay()
            init_result = init_delay.get()
            Response.state_code = 200

            return init_result
    except:
        logging.error(traceback.format_exc())
        return jsnRs({"resultCode": "400"})
"""
클라이언트 재기동
"""
@tsumApp.get('/startTSUM')
def startTSUM(worker_id: int = 1):
    try:
        if worker_id > len(worker_queues):
            return {'Out of Range' : f' Worker 개수 {len(worker_queues)}개 숫자를 다시 입력하세요.'}
        else:
            starttsum = celeryapp.signature('app.starttsum', queue=f'worker_queue_{worker_id}', expires=3600)
            starttsum_delay = starttsum.delay()
            starttsum_result = starttsum_delay.get()
            Response.state_code = 200
            
            return starttsum_result
    except:
        logging.error(traceback.format_exc())
        return jsnRs({"resultCode": "400"})
"""
요청한 차량번호로 데이터를 반환
"""
@tsumApp.get('/NumberSearch')
async def NumberSearch(searchGb: str = None, carNo: str = None):
    try:
       
        numbersearch_delay = VarForFastAPI.numbersearch.delay(searchGb, carNo)
        while AsyncResult(numbersearch_delay.id).status != 'SUCCESS':
            await asyncio.sleep(1)
            pass
        numbersear_data_from_redis = numbersearch_delay.get()
        
        numbersearch_result = json.loads(numbersear_data_from_redis['Information'])
        
        if searchGb == '2':
    
            with ProcessPoolExecutor() as executor:
                result = executor.submit(process_image, numbersearch_result['carNo'])
                response = result.result()
            
            numbersearch_result.update({'carNo' : response})
        Response.state_code = 200
        
        return numbersearch_result
    except:
        logging.error(traceback.format_exc())
        return jsnRs({"resultCode": "400"})
"""
본 함수(요청)실행 후 실행
"""
def after_request(TSUMresponse):
    try:
        endTm = time.time()
        logging.info('after_request')
        logging.info(str(endTm-VarForFastAPI.strtTm) + ' sec')
        # 요청에 force가 있을 경우에만 
        if "force" in TSUMresponse.url.path and (int(VarForFastAPI.worker_id) <= len(worker_queues)):
            force = celeryapp.signature('app.force', queue=f'worker_queue_{int(VarForFastAPI.worker_id)}', expires=3600)
            force_delay = force.delay(VarForFastAPI.do, int(VarForFastAPI.worker_id))
            res = force_delay.get() 
            logging.info(f"실제 worker 처리 결과: {res}")  
            pass
                
    except:
        logging.error(traceback.format_exc())
        pass

    return TSUMresponse 
"""
task count 체크
"""
def update_task_cnt(worker_status_quename):
    try:
        byte_status = rd.get(worker_status_quename)
        status = json.loads(byte_status)

        status.update({'task_cnt' : status['task_cnt'] + 1})

        rd.set(worker_status_quename, json.dumps(status))
        logging.info(f'requestBlocker : {status["requestBlocker"]}, clientStatus : {status["clientStatus"]}, task_cnt : {status["task_cnt"]}')
    except:
        logging.error(traceback.format_exc())
"""
차대 번호로 조회 시 프로그램 이미지 캡쳐 후 pytesseract 사용해서 텍스트로 변환
"""
def process_image(base64_encoded_img):
    carNo = ""
    try:
        ocr_img_path = './OCR_IMAGES'
        os.makedirs(ocr_img_path, exist_ok=True)
        decoded_image = base64.b64decode(base64_encoded_img.encode('utf-8'))
        
        buf = BytesIO(decoded_image)
        buf.seek(0)
        pil_image = Image.open(buf)
        
        config = '--tessdata-dir "/usr/share/tesseract-ocr/4.00/tessdata/" -l Hang'
        text = pytesseract.image_to_string(pil_image ,config=config)
        carNo = ''.join([i for i in text if is_korean_chr(i) or i.isdecimal()])
        
        pil_image.save(f"{ocr_img_path}/{carNo}.jpg", overwrite=True)
    except:
        logging.error(traceback.format_exc())
    logging.info(f"carNO : {carNo}")
    return carNo

"""
동작중인 worker 상태를 체크 후 dict 형태로 반환 
"""
def get_all_worker_status():
    try:
        keys = rd.keys('worker_queue_*')
        data = {} # redis에서 가져오는 상태값
        
        result = {} # 최종 결과물 (리턴 값)

        for key in keys:
            status = {} # redis 상태 값을 참조해 생성한 새로운 상태값 (env, status, point, running)
            decode_key = key.decode('utf-8') # 디코딩 된 key 값
            value = rd.get(key)

            data[decode_key] = value.decode('utf-8') # data dict에 디코딩된 키 값으로 value 값 적재
            dict_data = json.loads(data[decode_key]) # 키 값에 해당하는 value값
            
            status['ENV'] = dict_data['ENV']
            if dict_data['requestBlocker'] == False:
                status['status'] = '00'
            else:
                status['status'] = '90'
            status['point'] = dict_data['point']
            status['running_status'] = dict_data['running_status']

            replace_key = decode_key.replace('_queue', '') # worker_queue_1_status > worker_1_status
            result[replace_key] = status
            
    except:
        logging.error(traceback.format_exc())
        logging.info('get_all_worker_status err')

    return result
