# celery -A tasks worker --loglevel=info -P solo -Q worker_queue_num,public
import base64 # pip install pybase64
import traceback
from celery import Celery
from celery.signals import worker_shutting_down

import logging
from logging.handlers import TimedRotatingFileHandler, SMTPHandler
from logging.config import dictConfig
import sys
from datetime import datetime
from io import BytesIO
import pyautogui
import pyperclip
from PIL import Image
import numpy as np

import os, time, json

import configparser

from pywinauto.application import Application
from pywinauto.findwindows import ElementNotFoundError

import psutil

import unicodedata

# timeout handler
from threading import Thread
import functools

import webbrowser
import redis
from comtypes import COMError

app = Celery('tasks', broker='redis 주소', backend='redis 주소')
rd = redis.StrictRedis(host='redis 주소', port=9876, db=1)


class Var:
    init = 0  # init value
    carNo = 0  # init value
    uia_app = None
    last_call = None
    force = False
    requestBlocker = False
    clientStatus = False
    worker_status_quename  = None
    queue = None
    target_queue = []
app.conf.update(
    worker_task_log_format='[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d][%(task_name)s(%(task_id)s)]: %(message)s',
    worker_task_log_level='WARNING'

    )
app.conf.worker_hijack_root_logger = False




"""
Numberserarch 함수를 이용해 차량 번호를 받았을 때 문자인지 체크
"""
def is_korean_chr(chr_u):
    category = unicodedata.category
    if category(chr_u)[0:2] == 'Lo':  # other characters
        if 'HANGUL' in unicodedata.name(chr_u): return True
    return False

@app.task(name='app.loadconfig')
# 설정 파일 로드
def loadConfig():
    try:

        config = configparser.ConfigParser()
    except Exception as e:
        logging.error('import configparser error : ' + str(e))
    else:
        try:
            config_path = 'C:/work/TSUM/settings.ini'
            config.read(config_path)
        except Exception as e:
            logging.info('error! : unable to load setting.ini, path : ' + config_path)
            pass
        else:
            Var.ENV = config.get('TSUM_SETTINGS', 'ENV')
            Var.HOST = config.get('TSUM_SETTINGS', 'HOST')
            Var.PORT = config.get('TSUM_SETTINGS', 'PORT')
            Var.USER = config.get('TSUM_SETTINGS', 'USER')
            Var.PASSWORD = config.get('TSUM_SETTINGS', 'PASSWORD')
            Var.queue = config.get('TSUM_SETTINGS', 'WORKER_QUEUE')
            Var.target_queue = [Var.queue , 'public']
            for qName in Var.target_queue:
                if "public" in qName:
                    continue
                Var.worker_status_quename = f"{qName}_status"
                pass
    return 'loadconfig'

# 스크린샷 핸들러
class ScreenshotHandler(logging.StreamHandler):
    def __init__(self, projectName, log_file_path):
        logging.Handler.__init__(self)
        self.projectName = projectName
        self.log_file_path = log_file_path

    def emit(self, record):
        self.pic = pyautogui.screenshot(
            self.projectName + "_" + datetime.now().strftime('%Y%m%d-%H%M%S') + "_screenshot.png")

        file_list = os.listdir("./")
        file_list_png = [file for file in file_list if file.endswith("_screenshot.png")]
        file_list_png.sort(key=lambda x: os.path.getmtime(x))

        if len(file_list_png) > 50:  # 스크린샷 50개 이상이면 가장 오래된 파일을 삭제함.
            os.remove(file_list_png[0])

# 로그 셋팅
@app.task(name='app.setlogger')
def setLogger(projectName):
    print(projectName)
    log_file_path = 'Z:/log/' + projectName + '/' + Var.queue.upper() + '/'
    os.makedirs(log_file_path, exist_ok=True)

    os.chdir(log_file_path)
    print(os.getcwd())

    log_filename = projectName + '.log'
    print(log_filename)

    logform = '%(asctime)s | %(name)s | %(levelname)s | %(message)s'

    dictConfig({
        'version': 1,
        'formatters': {'default': {
            'format': logform,
        }},
        'handlers': {'wsgi': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
            'formatter': 'default'
        }},
        'root': {
            'level': 'INFO',
            'handlers': ['wsgi']
        }
    })

    logger = logging.getLogger()
    formatter = logging.Formatter(logform)
    
    strh = logging.StreamHandler()
    strh.setLevel(logging.INFO)
    strh.setFormatter(formatter)
    logger.addHandler(strh)

    # TimedRotatingFileHandler
    trfh = TimedRotatingFileHandler(filename=log_filename, when='midnight', interval=1, backupCount=0, utc=True)
    trfh.setLevel(logging.INFO)
    trfh.suffix = '%Y-%m-%d'
    trfh.setFormatter(formatter)
    logger.addHandler(trfh)

    worker_logger = logging.getLogger('celery')
    worker_logger.setLevel(logging.INFO)
    logger.addHandler(worker_logger)
    # screenshot_handler
    if Var.ENV == 'prod':
        screenshot_handler = ScreenshotHandler(projectName=projectName, log_file_path=log_file_path)
        screenshot_handler.setLevel(logging.ERROR)
        logger.addHandler(screenshot_handler)
    else:  # local, test etc...
        pass
    
    return 'setlogger'


# 클라이언트 종료
def clientDeduplication():
    # kill all
    Clients = []
    mainClient = None
    try:
        for p in psutil.process_iter(attrs=["pid", "name"]):
            if p.info["name"] == 'MiPlatform320U.exe':
                Clients.append(p.info["pid"])
    except KeyError:
        logging.error('error in clientDeduplication(), no MiPlatform320U.exe')
    except Exception as e:
        logging.error('error in clientDeduplication()')
        logging.error(str(e))

    else:
        for c in Clients:
            os.system("taskkill /pid {} /f".format(c))


@app.task(name='app.startclient')
# 클라이언트 시작
def startClient():
    try:
        
        Var.clientStatus = False
        rd.set(Var.worker_status_quename , json.dumps({"ENV":Var.ENV, "requestBlocker":False , "clientStatus":False, "task_cnt":0, "point":"0", "running_status":False}) )

        logging.info('kill all Client')
        clientDeduplication()  # kill all
        
        logging.info('do startClient')
        update_check = check_update()
        logging.info('클라이언트 업데이트 체크')
        if update_check == 200:
            get_pid = get_client_pid()
            logging.info('클라이언트 실행 체크')

            if get_pid == 200:
                tsum_login = login()
                logging.info('로그인 시도')

                if tsum_login == 200:
                    logging.info('팝업 체크')
                    pop_up_1()
                    pop_up_2()
                    pop_up_3()
                    pop_up_4()
                    last_pop_up = pop_up_5()

                    if last_pop_up == 200:
                        initialize()
                        logging.info('초기화 작업')
                        # Point 조회 및 redis에 적재
                        tapp = Var.uia_app.window()
                        point = tapp['Pane3'].window_text()
                        
                        update_status([('point' , point)])
                        return 200
                    else:
                        logging.error('팝업 닫기(Step 4) 에러')
                        raise
                else:
                    logging.error('로그인(Step 3) 에러')
                    raise
            else:
                logging.error('클라인어트 PID 가져오기(Step 2) 에러')
                
                raise
        else:
            logging.error('클라이언트 업데이트 체크(Step 1) 에러')
            raise
    except:
        logging.error(traceback.format_exc())
        return None
    
    

# 클라이언트 업데이트 체크 : Step 1
def check_update():
    try:
        Application(backend='uia').start(
            'C:/Windows/SysWOW64/MiUpdater321.exe -L TRUE -R FALSE -D WIN32U -V 3.2 -K CTS -X http://211.236.84.168/CTS/CTS_ci_main_Win32.xml -Wd 1024 -Ht 768 -LE TRUE -BI "%component%next_upd.gif"')


    except Exception as e:
        logging.error('error while start ie : ' + str(e))
        return {"carNo": "", "resultCode": "3100"}

    else:
        logging.info('업데이트 체크 완료')
        return 200


# 클라이언트 PID 가져오기 : Step 2
def get_client_pid():
    try:
        while True:
            Var.dict_pids = {p.info["name"]: p.info["pid"] for p in psutil.process_iter(attrs=["pid", "name"])}
            try:
                Var.mainClient = Var.dict_pids['MiPlatform320U.exe']
                Var.uia_app = Application(backend="uia").connect(process=Var.mainClient)
            except KeyError:
                logging.error('wait 1 sec for get pid for MiPlatform320U.exe')
                time.sleep(1)
                continue
            except:
                logging.error('error get pid key')
                continue
            else:
                logging.info('client : on')
                break

    except Exception as e:
        logging.error('!error get pid key : ' + str(e))
        return {"carNo": "", "resultCode": "3100"}

    else:
        logging.info('PID 가져오기 성공')
        return 200


# 로그인 : Step 3
def login():
    count = 0
    try:
        while not Var.uia_app.Dialog.child_window(auto_id="12", control_type="Pane").exists():
            logging.info('wait 1 sec for login window')
            time.sleep(1)
            count += 1
            if count > 10:
                logging.info('loading error, over 10 sec')
                return {"carNo": "", "resultCode": "3100"}
        else:
            try:
                # check log in ID
                # 아이디 자동 입력이 잠시 로딩되는 동안 빈칸으로 처리됨
                while Var.uia_app.top_window().child_window(title=Var.USER, auto_id="11",
                                                            control_type="Pane").window_text() == '':
                    logging.info('wait 0.2 sec for id')
                    time.sleep(0.2)
                    count += 1
                    if count > 20:
                        logging.info('id loading error, over 4 sec')
                        return {"carNo": "", "resultCode": "3100"}

            except Exception as e:
                logging.error('!error login fail : no id : ' + str(e))
                return {"carNo": "", "resultCode": "3100"}
            else:
                # type password
                Var.uia_app.Dialog.child_window(auto_id="12", control_type="Pane").type_keys(Var.PASSWORD)
                logging.info('login : password')

    except Exception as e:
        logging.error('login failed : ' + str(e))
        return {"carNo": "", "resultCode": "3100"}

    else:
        try:
            # check login button exist
            while not Var.uia_app.Dialog.child_window(auto_id="13", control_type="Button").exists():
                logging.info('wait 0.2 sec for login button')
                time.sleep(0.2)
                count += 1
                if count > 20:
                    logging.info('loading error, over 4 sec')
                    return {"carNo": "", "resultCode": "3100"}

        except ElementNotFoundError:  # 해당 엘레먼트를 찾을 수 없을 때
            logging.error('ElementNotFoundError')
            return {"carNo": "", "resultCode": "3100"}

        except Exception as e:
            logging.error('!error check login button: ' + str(e))
            return {"carNo": "", "resultCode": "3100"}

        else:
            # push login button
            Var.uia_app.Dialog.child_window(auto_id="13", control_type="Button").click_input()
            logging.info('로그인 성공')
            return 200


# 팝업 체크 후 닫기 : Step 4-1
def pop_up_1():
    # 비밀번호 변경 팝업
    count = 0
    try:
        while not Var.uia_app.top_window().child_window(title="비밀번호 변경", control_type="Pane").exists():
            logging.info('wait 0.5 sec for password change event')
            time.sleep(0.5)
            count += 1
            if count > 3:
                logging.info('비밀번호 변경 팝업')
                break
        else:
            Var.uia_app.top_window().child_window(title="비밀번호 변경", control_type="Pane").child_window(title="다음에 변경",
                                                                                                     auto_id="13",
                                                                                                     control_type="Button").click()

            logging.info('비밀번호 변경 팝업 닫기 완료')

    except Exception as e:
        logging.error('!error while password change event : ' + str(e))
        return {"carNo": "", "resultCode": "3100"}

    else:
        logging.info('팝업 체크 후 닫기 완료 : Step 4-1')


# 팝업 체크 후 닫기 : Step 4-2
def pop_up_2():
    # 팝업
    count = 0
    try:
        while not Var.uia_app.Dialog.child_window(title="닫기", auto_id="11", control_type="Button").exists():
            logging.info('wait 0.5 sec for notice1')
            time.sleep(0.5)
            count += 1
            if count > 3:
                logging.info('!pass notice1')
                return {"carNo": "", "resultCode": "3100"}
        else:
            Var.uia_app.Dialog.child_window(title="닫기", auto_id="11", control_type="Button").click_input()
            logging.info('notice1 : close')

    except Exception as e:
        logging.error('!error notice1 : ' + str(e))
        return {"carNo": "", "resultCode": "3100"}

    else:
        logging.info('팝업 체크 후 닫기 완료 : Step 4-2')


# 팝업 체크 후 닫기 : Step 4-3
def pop_up_3():
    # 월 마다 뜨는 시스템 공지 팝업
    count = 0
    try:
        while not Var.uia_app.top_window().child_window(title="시스템공지", control_type="Pane").exists():
            logging.info('wait 0.5 sec for system notice')
            time.sleep(0.5)
            count += 1
            if count > 3:
                logging.info('pass system notice event')
                break
        else:
            # 시스템 공지 팝업
            logging.info('monthly system notice confirmed')
            Var.uia_app.top_window().child_window(title="시스템공지", control_type="Pane").Pane2.click_input()

            pyautogui.hotkey('ctrl', 'a')
            pyautogui.hotkey('ctrl', 'c')
            system_notice_text = pyperclip.paste()
            logging.info(system_notice_text)
            Var.uia_app.top_window().child_window(title="오늘 그만보기 ", auto_id="15", control_type="Button").click()
            Var.uia_app.top_window().child_window(title="닫기", auto_id="16", control_type="Button").click()
            logging.info('system notice : close')

    except Exception as e:
        logging.error('!error monthly system notice : ' + str(e))
        return {"carNo": "", "resultCode": "3100"}
    else:
        logging.info('팝업 체크 후 닫기 완료 : Step 4-3')


# 팝업 체크 후 닫기 : Step 4-4
def pop_up_4():
    # menu

    count = 0
    try:
        while not Var.uia_app.top_window().child_window(auto_id="10000").Pane.child_window(auto_id="14").exists():
            logging.info('wait 0.5 sec for menu1')
            time.sleep(0.5)
            count += 1
            if count > 3:
                logging.info('menu1 not exist')
                return {"carNo": "", "resultCode": "3100"}
        else:
            Var.uia_app.top_window().child_window(auto_id="10000").Pane.child_window(auto_id="14").click_input()
            logging.info('menu1 : click')
            count = 0
            while not Var.uia_app.top_window().MenuItem1.exists():
                logging.info('wait 0.5 sec for menu2')
                time.sleep(0.5)
                count += 1
                if count > 3:
                    logging.info('menu2 not exist')
                    return {"carNo": "", "resultCode": "3100"}
            else:
                Var.uia_app.top_window().MenuItem1.click_input()
                logging.info('menu2 : click')

    except Exception as e:
        logging.error('!error menu : ' + str(e))
        return {"carNo": "", "resultCode": "3100"}

    else:
        logging.info('팝업 체크 후 닫기 완료 : Step 4-4')


# 팝업 체크 후 닫기 : Step 4-5
def pop_up_5():
    # when daily notice1 pop up
    try:
        count = 0
        while not Var.uia_app.top_window().child_window(title="오늘 그만보기 ", auto_id="16",
                                                        control_type="Button").exists():  # startClient 공지2
            logging.info('wait 0.5 sec for daily notice1')
            time.sleep(0.5)
            count += 1
            if count > 3:
                logging.info('pass daily notice1 event')
                break
        else:
            Var.uia_app.top_window().child_window(title="오늘 그만보기 ", auto_id="16", control_type="Button").click_input()
            Var.uia_app.top_window().child_window(title="닫기", auto_id="17", control_type="Button").click_input()
            logging.info('daily notice1 : close')

        # when daily notice2 pop up

        count = 0
        while not Var.uia_app.top_window().child_window(title="오늘 그만보기 ", auto_id="14", control_type="Button").exists():
            logging.info('wait 0.5 sec for daily notice2')
            time.sleep(0.5)
            count += 1
            if count > 3:
                logging.info('pass daily notice2 event')
                break
        else:
            Var.uia_app.top_window().child_window(title="오늘 그만보기 ", auto_id="14", control_type="Button").click_input()
            Var.uia_app.top_window().child_window(title="닫기", auto_id="13", control_type="Button").click_input()
            logging.info('daily notice2 : close')

        logging.info('startClient done')
        Var.clientStatus = True
       
        update_status([('clientStatus' , True)])
        

    except:
        logging.error('팝업 체크 후 닫기 에러: Step 4-5')
        logging.error(traceback.format_exc())
        return 400
    else:
        logging.info('팝업 체크 후 닫기 완료 : Step 4-5')
        return 200

# 초기화 작업
def initialize():
    logging.info('Client initialize : start')
    
    try:
        Var.uia_app.window().window(title="초기화", control_type="Button").click()
    except COMError:  # 해당 엘레먼트와 상호작용 할 수 없을 때
        logging.error('COMError')
        return False
    except ElementNotFoundError:  # 해당 엘레먼트를 찾을 수 없을 때
        logging.error('ElementNotFoundError')
        return False
    except Exception as e:
        logging.error('!error uncategorized, client initialize: ' + str(e))
        return False
    else:
        if Var.uia_app.top_window().child_window(title="확인", auto_id="12", control_type="Button").exists():
            Var.uia_app.top_window().Pane0.Pane2.click_input()
            pyautogui.hotkey('ctrl', 'a')
            pyautogui.hotkey('ctrl', 'c')
            notice_text = pyperclip.paste()

            logging.info('Client Timeout!!!')
            logging.info(notice_text[:30])
            Var.uia_app.top_window().child_window(title="확인", auto_id="12", control_type="Button").click()
            logging.info('Client shutdown : Timeout 1h')
            Var.clientStatus = False
            update_status([('clientStatus' , False)])
    
            return False
        else:
            logging.info('Client normal')
            logging.info('requestBlocker : ' + str(Var.requestBlocker) + ', clientStatus : ' + str(Var.clientStatus))
            return True
    finally:
        Var.last_call = time.time()
        logging.info('set last_call : ' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(Var.last_call)))
        logging.info('Client initialize : done')


@app.task(name='app.numbersearch', bind=True)
def NumberSearch(self, searchGb: str = None, carNo: str = None):
    try:
        
        update_status([('running_status' , True)])
        self.update_state(state='START', meta={'step': 'start'})
        logging.info('receive NumberSearch')
        logging.error('here')
        self.update_state(state='PROGRESS', meta={'step': 'NumberSearch'})
        searchGb = searchGb.replace(" ", "")
        carNo = carNo.replace(" ", "")
        logging.info('get searchGb, value : ' + searchGb)

        if searchGb == '1':
            if carNo == '':
                logging.info('!!CRITICAL!! : blank carNo received and searchGb :' + searchGb)
                minus_task_cnt()
                return {"carNo": carNo, "resultCode": "2100"}
            elif carNo != ''.join([i for i in carNo if is_korean_chr(i) or i.isdecimal()]):
                logging.info('!!CRITICAL!! : carNo format error (kr X) and searchGb :' + searchGb)
                minus_task_cnt()
                return {"carNo": carNo, "resultCode": "2100"}

        elif searchGb == '2':
            if carNo == '':
                logging.info('!!CRITICAL!! : blank carNo received and searchGb :' + searchGb)
                minus_task_cnt()
                return {"carNo": carNo, "resultCode": "2100"}
            elif carNo != ''.join([i for i in carNo if i.isalpha() or i.isdecimal()]):
                logging.info('!!CRITICAL!! : carNo format error (kr X) and searchGb :' + searchGb)
                minus_task_cnt()
                return {"carNo": carNo, "resultCode": "2100"}
        else:
            minus_task_cnt()
            return {"searchGb": searchGb, "resultCode": "2100"}

        # check Client status, is running, initiate
        if Var.uia_app.is_process_running():
            init_rst = initialize()
            if init_rst:
                logging.info('Client Checked, ')
            else:
                logging.info('Cannot initialize client...')
                logging.info('do startClient()')
                startClientResponse = startClient()
                if startClientResponse == 200:
                    logging.info('restart Client successfully!!!')
        else:
            logging.info('no Client...')
            startClientResponse = startClient()
            if startClientResponse == 200:
                logging.info('start Client successfully!!!')

        logging.info('start NumberSearch')
    except:
        logging.error('error NumberSearch')
        logging.error(traceback.format_exc())
    try:
        logging.info('run NumberSearch, requestBlocker set TRUE')

        if searchGb == '1':
            pass
        elif searchGb == '2':
            Var.uia_app.top_window().ComboBox.click_input()
            Var.uia_app.top_window().type_keys('{DOWN}')

        tapp = Var.uia_app.window()
        tapp.window(title="", auto_id="14", control_type="Pane").type_keys(carNo)

        tapp.window(title="정보조회", auto_id="12", control_type="Button").click_input()
        tapp.window(title="예", auto_id="13", control_type="Button").click_input()

        tapp.Pane0.Pane2.click_input()
        pyautogui.hotkey('ctrl', 'a')
        pyautogui.hotkey('ctrl', 'c')
        number_search_rst = pyperclip.paste()

        tapp.window(title="확인", auto_id="12", control_type="Button").click_input()
        point = tapp['Pane3'].window_text()
        if number_search_rst.find('자동차 정보 조회 하여') >= 0:
            operationGb = '운행차량'
        elif number_search_rst.find('말소차량입니다.') >= 0:
            operationGb = '말소차량'
        elif number_search_rst.find('조회된 결과가 없습니다') >= 0:
            init_rst = initialize()
            minus_task_cnt(point=point)
            return {"carNo": carNo, "resultCode": "2100"}
        elif number_search_rst.find('오류가 발생하였습니다') >= 0:
            init_rst = initialize()
            minus_task_cnt(point=point)
            return {"carNo": carNo, "resultCode": "9000"}
        elif number_search_rst.find('Network 오류입니다') >= 0:
            init_rst = initialize()
            minus_task_cnt(point=point)
            return {"carNo": carNo, "resultCode": "9000"}
        else:
            operationGb = number_search_rst

        try:
            tableLoadWait()
        except Exception as e:
            logging.info('table loading error : ' + str(e))
            Information = None
        else:

            tableItem = {
                "operationGb": operationGb,
                "carNm": tapp['Pane30'].window_text(),
                "mdlYear": tapp.window(auto_id="36", control_type="Pane").window_text(),
                "vid": tapp.window(auto_id="35", control_type="Pane").window_text(),
                "carClrNm": tapp.window(auto_id="32", control_type="Pane").window_text(),
                "fstRegDt": tapp.window(auto_id="39", control_type="Pane").window_text(),
                "fomNm": tapp.window(auto_id="69", control_type="Pane").window_text(),
            }

            # point = tapp['Pane3'].window_text()
            
            if searchGb == '1':
                Information = {"carNo": carNo,
                            "resultCode": "0000",
                            "userID" : Var.USER,
                            "point": point,
                            "tableItem": tableItem, }
            # 차대 번호로 조회 시 
            elif searchGb == '2':
                window = tapp.wrapper_object()
                window.iface_transform.Resize(1032, 780)
                img = tapp.capture_as_image() # 전체 캡쳐 (조회된 화면만)
                np_arr = np.array(img)
                crop = np_arr[180:200,40:150,:] # 차량 번호가 있는 부분 좌표 설정
                crop_image = Image.fromarray(crop) # 이미지 생성
                buff = BytesIO()
                crop_image.save(buff , format='JPEG')
                img_data = buff.getvalue()
                encoded_image = base64.b64encode(img_data).decode('utf-8') # base64로 인코딩
            
                Information = {"carNo": encoded_image,
                            "resultCode": "0000",
                            "userID" : Var.USER,
                            "point": point,
                            "tableItem": tableItem, }
            
        finally:
            init_rst = initialize()  # 초기화 버튼 클릭

    except Exception as e:
        logging.error('!error while NumberSearch : ' + str(e))
        logging.error('requestBlocker : ' + str(Var.requestBlocker) + ', clientStatus : ' + str(Var.clientStatus))
        return {"carNo": carNo, "resultCode": "9000"}
    
    else:
        minus_task_cnt(point=point)
        self.update_state(state='SUCCESS', meta={'step': 'NumberSearch' , 'Information': json.dumps(Information)}) 
    # return Information

"""
timeout을 데코레이터로 사용하는 함수가 지정한 시간까지 처리하지 못하면 예외처리
"""
def timeout(seconds_before_timeout):
    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            res = [Exception('function [%s] timeout [%s seconds] exceeded!' % (func.__name__, seconds_before_timeout))]

            def newFunc():
                try:
                    res[0] = func(*args, **kwargs)
                except Exception as e:
                    res[0] = e

            t = Thread(target=newFunc)
            t.daemon = True
            try:
                t.start()
                t.join(seconds_before_timeout)
            except Exception as e:
                logging.info('error starting thread')
                raise e
            ret = res[0]
            if isinstance(ret, BaseException):
                raise ret
            return ret

        return wrapper

    return deco


@timeout(10)
def tableLoadWait():
    while Var.uia_app.window().window(auto_id="39", control_type="Pane").window_text() == '____.__.__':
        time.sleep(1)
        logging.info('table contents not loaded...')


@app.task(name='app.check')
def check():

    check_dict={}
    byte_status = rd.get(Var.worker_status_quename)
    status = json.loads(byte_status)

    check_dict['ENV'] = Var.ENV
    for qName in Var.target_queue:
        if "public" in qName:
            pass
        else:
            check_dict['Worker_id'] = qName
            if Var.requestBlocker == False:
                check_dict['Status'] = '00'
            else:
                check_dict['Status'] = '90'
            check_dict['Point'] = status['point']
            check_dict['running_status'] = status['running_status']
    
    return check_dict
    
@app.task(name='app.list')
def working_list(worker_cnt: int = 0):
    working_list_dict = {}

    for id in range(worker_cnt):
        byte_status = rd.get(f'worker_queue_{id+1}_status')
        status = json.loads(byte_status)

        working_list_dict['ENV'] = Var.ENV
        
        working_list_dict['worker_id'] = f'worker_queue_{id+1}'
        if Var.requestBlocker == False:
            working_list_dict['status'] = '00'
        else:
            working_list_dict['status'] = '90'
        working_list_dict['point'] = status['point']
        working_list_dict['running_status'] = status['running_status']

    return working_list_dict

@app.task(name='app.env')
def ENV():
    check_mssg = {'Var.ENV': str(Var.ENV)}
    return check_mssg


@app.task(name='app.requestblocker')
def check_requestBlocker():
    check_mssg = {'Var.requestBlocker': str(Var.requestBlocker)}
    return check_mssg


@app.task(name='app.clientstatus')
def check_clientStatus():
    check_mssg = {'Var.clientStatus': str(Var.clientStatus)}
    return check_mssg


@app.task(name='app.init')
def init():
    logging.info('requestBlocker : ' + str(Var.requestBlocker) + ', clientStatus : ' + str(Var.clientStatus))
    init_rst = initialize()
    if init_rst:
        logging.info('request for init : initialized')
        logging.info('requestBlocker : ' + str(Var.requestBlocker) + ', clientStatus : ' + str(Var.clientStatus))
        return {'resultCode': '00'}
    else:
        logging.info('request for init : fail to initialized')
        logging.info('requestBlocker : ' + str(Var.requestBlocker) + ', clientStatus : ' + str(Var.clientStatus))
        return {'resultCode': '90'}
    


@app.task(name='app.starttsum')
def startTSUM():
    
    logging.info('requestBlocker : ' + str(Var.requestBlocker) + ', clientStatus : ' + str(Var.clientStatus))
    startClientResponse = startClient()
    
    if startClientResponse == 200:
        logging.info('requestBlocker : ' + str(Var.requestBlocker) + ', clientStatus : ' + str(Var.clientStatus))
        return {'resultCode': '00'}
    else:
        logging.info('requestBlocker : ' + str(Var.requestBlocker) + ', clientStatus : ' + str(Var.clientStatus))
        return {'resultCode': '90'}
    

@app.task(name='app.force')
def force(do: str = None, worker_id: int = 1):
    if do == '00':
        Var.requestBlocker = False
        
        update_status([('requestBlocker' , False)])
        # Var.force = False
    elif do == '90':
        Var.requestBlocker = True
        
        update_status([('requestBlocker' , True)])
        # Var.force = True
    else:
        pass
    return {'worker_id' : worker_id,
            'requestBlocker' : Var.requestBlocker,
            'clientStatus' : Var.clientStatus}

@worker_shutting_down.connect
def worker_shutdown(**kwargs):
    # worker_status 딕셔너리 업데이트
    

    # # Redis에 업데이트된 상태값 저장
    
    rd.delete(Var.worker_status_quename)
    pass

def update_status(value_list: list = []):
    

    byte_status = rd.get(Var.worker_status_quename)
    status = json.loads(byte_status)

    for itm in value_list:
        key = itm[0] # string 
        value = itm[1]
        if key == 'task_cnt':
            
            status['task_cnt'] = status['task_cnt'] - int(value)
            
            status.update({key:status['task_cnt']})
        else:
            status.update({ key:value })
        pass
    rd.set(Var.worker_status_quename , json.dumps(status))
    logging.info(f'ENV : {status["ENV"]}, requestBlocker : {status["requestBlocker"]}, clientStatus : {status["clientStatus"]}, task_cnt : {status["task_cnt"]}, point : {status["point"]}, running_status : {status["running_status"]}')

def minus_task_cnt(point: str = None):
    if point == None:
        update_value_list = [] # redis에 업데이트 해줄 상태값들 모음
        update_value_list.append(('task_cnt', 1))
        update_value_list.append(('running_status', False))
        update_status(update_value_list)
    else:
        update_value_list = [] # redis에 업데이트 해줄 상태값들 모음
        update_value_list.append(('task_cnt', 1))
        update_value_list.append(('point', point))
        update_value_list.append(('running_status', False))
        update_status(update_value_list)
        
def main():
    Var.requestBlocker = False
    Var.clientStatus = False

    loadConfig()

    setLogger(Var.ENV)

    startClient()  
    

    
if __name__ == '__main__':

    main()
    app.start(argv=['worker', '-l', 'info', '-P','solo','-Q',f'{Var.queue},public'])
    

    
