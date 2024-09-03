import logging
import os
from datetime import datetime

class LoggerHandler:
    def __init__(self, log_dir='log_data'):
        self.log_dir = log_dir
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        self._setup_log_directory()
        self._setup_file_handler()

    def _setup_log_directory(self):
        # 로그 폴더가 없으면 생성
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)

    def _setup_file_handler(self):
        # 현재 시간 가져오기 및 문자열로 변환
        now = datetime.now()
        current_time_str = now.strftime("%Y-%m-%d_%H-%M-%S")

        # 로그 파일 핸들러 설정
        log_file_path = os.path.join(self.log_dir, f'{current_time_str}.log')
        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # 핸들러를 로거에 추가
        self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger