import pymysql as MySQLdb
import gspread
import pandas as pd
import yaml

### можно менять
query = '''SELECT Count(creatorId) AS Количество_игр
FROM cg_game;
'''

# читаем переменные из файла config.yaml
with open('config.yaml', 'r') as f:
    config = yaml.load(f, Loader=yaml.SafeLoader)

# инициализация соединения с mysql
mysql_cn = MySQLdb.connect(
    host=config['mysql']['host'],
    port=config['mysql']['port'],
    user=config['mysql']['user'],
    passwd=config['mysql']['passwd'],
    db=config['mysql']['db'],
    charset='utf8',
)

# выполняется запрос query и данные кладутся в переменную df_mysql
df_mysql = pd.read_sql(query, con=mysql_cn)

# закрытие соединения с mysql
mysql_cn.close()

# метнуть в гугел
DOC_ID = config['google']['doc_id']
sheet_name = config['google']['sheet']


# инициализация соединения с гуглом
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
gc = gspread.service_account(filename=config['google']['filename'], scopes=SCOPES)

# wks это нужный нам лист гуглодокумента
wks = gc.open_by_key(DOC_ID).worksheet(sheet_name)
# записывается данные в лист гуглодокумента
wks.update([df_mysql.columns.values.tolist()] + df_mysql.values.tolist())

# выгрузить локально в csv
df_mysql.to_csv('test.csv', index=None)
