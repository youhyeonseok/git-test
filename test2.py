import streamlit as st
import mysql.connector
import sys
import pandas as pd
from mysql.connector import Error
import logging
import os
from datetime import datetime
import matplotlib.pyplot as plt

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
class DataBaseHandler:

    def __init__(self, host, user, password, database_name, charset="utf8mb4", collation="utf8mb4_general_ci"):
        self.host = host
        self.user = user
        self.password = password
        self.database_name = database_name
        self.charset = charset
        self.collation = collation

        self.logger_handler = LoggerHandler()
        self.logger = self.logger_handler.get_logger()
        self.conn = None
        self.connect_database()

    @staticmethod
    def dtype_to_sql(dtype):
        dtype_str = str(dtype)  # dtype을 문자열로 변환
        if "float" in dtype_str:
            return "FLOAT"
        elif "int" in dtype_str:
            return "INT"
        elif "object" in dtype_str:  # 문자열 타입은 object로 표현됩니다.
            return "VARCHAR(255)"  # 길이를 지정할 수 있습니다.
        elif "datetime" in dtype_str:
            return "DATETIME"
        else:
            return "TEXT"  # 기본값으로 TEXT를 사용

    def exception_handler_decorator(self, func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            
            except mysql.connector.errors.InterfaceError as ie:
                self.logger.error(f"MySQL 서버에 연결할 수 없습니다: {ie}")
                print(f"MySQL 서버에 연결할 수 없습니다: {ie}")
                self.conn.close()
                sys.exit()

            except mysql.connector.errors.ProgrammingError as pe:
                self.logger.error(f"SQL 쿼리에서 프로그래밍 오류가 발생했습니다: {pe}")
                print(f"SQL 쿼리에서 프로그래밍 오류가 발생했습니다: {pe}")
                self.conn.close()
                sys.exit()

            except mysql.connector.errors.IntegrityError as ie:
                self.logger.error(f"데이터 무결성 문제가 발생했습니다: {ie}")
                print(f"데이터 무결성 문제가 발생했습니다: {ie}")
                self.conn.close()
                sys.exit()

            except mysql.connector.errors.DataError as de:
                self.logger.error(f"데이터 관련 오류가 발생했습니다: {de}")
                print(f"데이터 관련 오류가 발생했습니다: {de}")
                self.conn.close()
                sys.exit()

            except mysql.connector.errors.OperationalError as oe:
                self.logger.error(f"운영 중 문제가 발생했습니다: {oe}")
                print(f"운영 중 문제가 발생했습니다: {oe}")
                self.conn.close()
                sys.exit()

            except RuntimeError as re:
                self.logger.error(f"데이터 베이스 연결 파라미터 오류가 발생했습니다: {re}")
                print(f"데이터 베이스 연결 파라미터 오류가 발생했습니다: {re}")
                self.conn.close()
                sys.exit()

            except Error as e:
                self.logger.error(f"예기치 않은 오류가 발생했습니다: {e}")
                print(f"예기치 않은 오류가 발생했습니다: {e}")
                self.conn.close()
                sys.exit()

        return wrapper

    def connect_database(self):
        decorated_connect = self.exception_handler_decorator(self._connect_database)
        decorated_connect()

    def _connect_database(self):
        self.conn = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database_name,
            charset=self.charset,
            collation=self.collation
        )

        if self.conn.is_connected():
            self.logger.info("데이터베이스에 성공적으로 연결되었습니다.")
            print("데이터베이스에 성공적으로 연결되었습니다.")

    def select_columns_list(self, table_name):
        decorated_select_columns_list = self.exception_handler_decorator(self._select_columns_list)
        return decorated_select_columns_list(table_name)

    def _select_columns_list(self, table_name):
        cursor = self.conn.cursor(buffered=True)
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
        cursor.close()
        field_names = [i[0] for i in cursor.description]
        return field_names

    def view_table_list(self):
        decorated_view_table = self.exception_handler_decorator(self._view_table_list)
        return decorated_view_table()

    def _view_table_list(self):
        cursor = self.conn.cursor()
        cursor.execute(
            "SHOW TABLES;"
        )
        tables_name = cursor.fetchall()
        cursor.close()
        self.logger.info(f"데이터 베이스 테이블 조회 성공 :{tables_name}")
        print(f"데이터 베이스 테이블 조회 성공 :{tables_name}")

        return tables_name

    def create_table(self, table_name, data):
        decorated_create_table = self.exception_handler_decorator(self._create_table)
        decorated_create_table(table_name, data)

    def _create_table(self, table_name, data):
        cursor = self.conn.cursor()
        sql = f"CREATE TABLE {table_name} ("

        for dtype, column in zip(data.dtypes.tolist(), data.columns):
            sql += f"{column} {self.dtype_to_sql(dtype)},"

        sql = sql.rstrip(',')  # 마지막 쉼표 제거
        sql += ");"
        cursor.execute(sql)
        cursor.close()
        self.logger.info(f"데이터 베이스 테이블 생성 성공 :{table_name}")
        print(f"데이터 베이스 테이블 생성 성공 :{table_name}")

    def read_table(self,table_name, columns = [None]):
        decorated_read_table = self.exception_handler_decorator(self._read_table)
        return decorated_read_table(table_name, columns)

    def _read_table(self,table_name, columns = [None]):
        cursor = self.conn.cursor(buffered=True)

        if columns[0] == None:
            sql = f"SELECT * FROM {table_name}"
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()

            self.logger.info(f"데이터 베이스 테이블 데이터 불러오기 성공 :{table_name}")
            print(f"데이터 베이스 테이블 데이터 불러오기 성공 :{table_name}")
            return pd.DataFrame(result, columns=self.select_columns_list(table_name))
        
        else:
            temp = ""
            for col in columns:
                temp += f"{col},"
            sql = f"SELECT {temp[:-1]} FROM {table_name}" # temp에서 마지막 , 제거를 위해 -1로 슬라이싱함
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()

            self.logger.info(f"데이터 베이스 테이블 데이터 불러오기 성공 :{table_name}")
            print(f"데이터 베이스 테이블 데이터 불러오기 성공 :{table_name}")
            return pd.DataFrame(result, columns)
            
    def write_table(self, table_name, data):
        decorated_write_table = self.exception_handler_decorator(self._write_table)
        decorated_write_table(table_name, data)

    def _write_table(self, table_name, data):
        self.create_table(table_name, data)

        cursor = self.conn.cursor()
        columns = ", ".join(data.columns)
        placeholders = ", ".join(["%s"] * len(data.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        values = [tuple(row) for row in data.values]
        cursor.executemany(insert_query, values)

        # commit 호출시 모든 변경사항이 확정됨 (insert, update, delete)
        self.conn.commit()
        cursor.close()
        self.logger.info(f"{table_name} 테이블 데이터 삽입 성공")
        print(f"{table_name} 테이블 데이터 삽입 성공")

    def update_table(self, table_name, data):
        decorated_update_table = self.exception_handler_decorator(self._update_table)
        decorated_update_table(table_name, data)
    
    def _update_table(self, table_name, data):
        cursor = self.conn.cursor()
        columns = ", ".join(data.columns)
        placeholders = ", ".join(["%s"] * len(data.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        values = [tuple(row) for row in data.values]
        cursor.executemany(insert_query, values)

        # commit 호출시 모든 변경사항이 확정됨 (insert, update, delete)
        self.conn.commit()
        cursor.close()
        self.logger.info(f"{table_name} 테이블 데이터 업데이트 성공")
        print(f"{table_name} 테이블 데이터 업데이트 성공")

    def delete_table(self, table_name):
        decorated_delete_table = self.exception_handler_decorator(self._delete_table)
        decorated_delete_table(table_name)

    def _delete_table(self, table_name):
        cursor = self.conn.cursor()
        sql = f"DROP TABLE {table_name}"
        cursor.execute(sql)
        self.conn.commit()
        cursor.close()
        self.logger.info(f"{table_name} 테이블 삭제 성공")
        print(f"{table_name} 테이블 삭제 성공")

class RealTimeDataReader(DataBaseHandler):

    def __init__(self, host, user, password, database_name, charset="utf8mb4", collation="utf8mb4_general_ci"):
        super().__init__(host, user, password, database_name, charset, collation)

    def read_last_row(self,table_name):
        decorated_read_last_row = self.exception_handler_decorator(self._read_last_row)
        return decorated_read_last_row(table_name)

    def _read_last_row(self, table_name):
        cursor = self.conn.cursor()
        sql = f"SELECT * FROM {table_name};"
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()

        # self.logger.info(f"{table_name} 테이블 데이터 불러오기 성공")
        # print(f"{table_name} 테이블 데이터 불러오기 성공")

        return result[-1]

# # 실제 사용 시
# if __name__ == "__main__":
#     db_handler = DataBaseHandler('127.0.0.1', 'root', '1234', 'cnc_data_db', 'utf8mb4', 'utf8mb4_general_ci')
#     real_time_handler = RealTimeDataReader('127.0.0.1', 'root', '1234', 'cnc_data_db', 'utf8mb4', 'utf8mb4_general_ci')
#     # print(real_time_handler.read_last_row("cnc_data"))
#     # import pandas as pd
#     # data = pd.read_csv("test_data/x_train_current_only.csv",index_col = 0)
#     # db_handler.create_table("test3", data)
#     # db_handler.write_table("test5",data)
#     # db_handler.update_table("test7",data)
#     # for i in range(2, 6):
#     #     db_handler.delete_table(f"test{i}")
#     # columns = ["CNC_X_Position", "CNC_Z_Current"]
#     # data = db_handler.read_table("cnc_data", columns=columns)
#     # columns = db_handler.select_columns_list("cnc_data")
#     # print(pd.DataFrame(data,columns=columns))

#     import time
#     while True:
#         start = time.time()
#         real_time_handler.connect_database()
#         # result = real_time_handler.read_last_row("test3")
#         real_time_handler.conn.close()
#         end = time.time()
#         print(f"Result : {123}, Total time taken: {end - start:.2f} seconds")  # 총 소요 시간 출력

def session_state_ck():
    for key, _ in st.session_state.items():
        if key == "initCk":
            return True
    return False

def Initialize():
    st.session_state["initCk"] = True
    st.session_state["handler"] = False
    st.session_state["db_handler"] = None
    st.session_state["table_list"] = []
    st.session_state["table_list_triger"] = False

if __name__ == "__main__":

    if not session_state_ck():
        Initialize()

    def get_db_handler():
        return DataBaseHandler('cnc-database.cl0igskqmp5x.us-east-1.rds.amazonaws.com', 'root', '12345678', 'cnc_database', 'utf8mb4', 'utf8mb4_general_ci')
    def get_table_list():
        st.session_state["table_list"] = st.session_state["db_handler"].view_table_list()

    if st.session_state["handler"] == False:
        db_handler = get_db_handler()
        st.session_state["db_handler"] = db_handler
        st.session_state["handler"] = True

    if st.session_state["table_list_triger"] == False:
        st.session_state["table_list_triger"] = True
        get_table_list()

    options = st.session_state["table_list"]
    options = [item[0] for item in options]

    # 제목과 서브 헤더
    st.title('Smec CNC Cloud')
    st.subheader('데이터 베이스 데이터 보기')

    if st.button('테이블 새로고침'):
        get_table_list()

    choice = st.selectbox('테이블을 선택:', options,key="read")
    if choice:
        dataframe = st.session_state["db_handler"].read_table(choice)
        st.write(dataframe)

    # 사용자가 선택할 수 있는 데이터 프레임의 열 나열
    option = st.selectbox(
        '시각화할 데이터 선택',
        dataframe.columns)

    # 선택된 열을 기반으로 간단한 플롯 생성
    fig, ax = plt.subplots()
    ax.plot(dataframe[option], label=f'Column {option}')
    ax.set_title(f'Plot of Column {option}')
    ax.set_xlabel('Index')
    ax.set_ylabel('Value')
    ax.legend()

    # Streamlit을 사용하여 플롯 표시
    st.pyplot(fig)


    st.markdown("""
        <hr style='border: 2px solid #000000;'>
        """, unsafe_allow_html=True)
    st.subheader('데이터 베이스에 데이터 추가')

    # CSV 파일 업로드
    uploaded_file = st.file_uploader("CSV 파일을 선택해주세요", type=["csv"])
    if uploaded_file:
        data = pd.read_csv(uploaded_file, index_col=0)
        st.session_state['data'] = data  # 업로드된 데이터를 세션 상태에 저장
        st.write(data)  # 데이터를 화면에 표시

    # 새 테이블 명 입력과 저장 버튼
    new_table_name = st.text_input("새로운 테이블 명을 입력해주세요.")
    if st.button('데이터 베이스에 저장'):
        if new_table_name and 'data' in st.session_state and st.session_state['data'] is not None:
            st.session_state["db_handler"].write_table(new_table_name, st.session_state['data'])
            st.success('데이터가 성공적으로 저장되었습니다.')
        else:
            st.error('데이터를 저장할 수 없습니다. 모든 필드를 채워주세요.')

    st.markdown("""
        <hr style='border: 2px solid #000000;'>
        """, unsafe_allow_html=True)
    st.subheader('데이터 베이스에 데이터 삭제')

    choice2 = st.selectbox('테이블을 선택:', options,key="delete")
    if st.button('테이블 삭제'):
        st.session_state["db_handler"].delete_table(choice2)