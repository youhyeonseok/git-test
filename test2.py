import streamlit as st
from database_handler import DataBaseHandler
db_handler = DataBaseHandler('127.0.0.1', 'root', '1234', 'cnc_data_db', 'utf8mb4', 'utf8mb4_general_ci')

# 제목을 추가합니다.
st.title('Streamlit 선택창 예제')

# 선택할 옵션 목록을 정의합니다.
options = db_handler.view_table_list()
options = [item[0] for item in options]

# 선택창을 만듭니다.
choice = st.selectbox('테이블을 선택:', options)
dataframe = db_handler.read_table(choice)
st.write(dataframe)