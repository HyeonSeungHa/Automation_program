# Automation_program
- **차량번호, 차대번호로 요청 시 해당 데이터를 이용해 차량 정보를 조회 후 결과 값을 반환하는 프로그램**
<br/><br/><br/><br/>
## V1.0 문제점
- pywinauto, Flask로 개발된 프로그램
- 모든 작업이 연결되어 있기에 한곳에서 에러 발생 시 프로그램 정상동작 불가
- 처리속도가 느림
- 병렬 처리 불가
<br/><br/><br/><br/>
## V2.0(버전업)
- **구조 이미지**
![image](https://github.com/user-attachments/assets/afef0d5a-7bd4-4e0d-8300-8c54131b9e26)
![image](https://github.com/user-attachments/assets/b28873a4-d3eb-4817-a7db-bea637fc768a)
<br/><br/><br/><br/>
- 기존 Flask로 개발되어있던 프로그램을 FastAPI로 변경
- 앞단에 Nginx를 추가하여 트래픽 분배
- Redis를 이용하여 요청을 저장
- 현재 Worker의 상태를 체크하는 큐(Status_Queue), 요청을 처리하는 큐(Worker_Queue_Num)로 구분
- Celery를 이용하여 요청을 병렬처리
- 로드밸런싱 작업 추가
