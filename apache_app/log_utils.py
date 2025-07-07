import sqlite3
import configparser
import os
import re
from datetime import datetime

# Загрузка или создание конфига

def load_config(path='config.ini'):
    cfg = configparser.ConfigParser()
    if os.path.exists(path):
        cfg.read(path)
    if 'Database' not in cfg:
        cfg['Database'] = {'URI': 'sqlite:///logs.db'}
    if 'Logs' not in cfg:
        cfg['Logs'] = {'Directory': 'logs', 'Pattern': 'access.log'}
    with open(path, 'w', encoding='utf-8') as f:
        cfg.write(f)
    return cfg

# Инициализация БД (логи + пользователи)

def init_db(db_uri):
    if db_uri.startswith('sqlite:///'):
        db_path = db_uri.replace('sqlite:///', '')
    else:
        db_path = db_uri
    folder = os.path.dirname(db_path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS log_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ip TEXT, date TEXT, method TEXT,
        url TEXT, status INTEGER,
        size INTEGER, user_agent TEXT
    )''')
    cur.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT
    )''')
    conn.commit()
    conn.close()
    return db_path

# Парсинг строки лога

def parse_line(line):
    pat = r"^(\S+) \S+ \S+ \[(.*?)\] \"(.*?)\" (\d+) (\d+) \".*?\" \"(.*?)\""
    m = re.match(pat, line)
    if not m:
        return None
    ip, datestr, req, status, size, ua = m.groups()
    parts = req.split()
    method = parts[0] if parts else ''
    url = parts[1] if len(parts) > 1 else ''
    try:
        dt = datetime.strptime(datestr, '%d/%b/%Y:%H:%M:%S %z')
        date = dt.isoformat()
    except:
        date = datestr
    return (ip, date, method, url, int(status), int(size), ua)

# Запись логов в БД

def parse_and_store(db_path, logs_dir, pattern):
    log_file = os.path.join(logs_dir, pattern)
    if not os.path.exists(log_file):
        return False, f"Лог-файл не найден: {log_file}"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cnt = 0
    with open(log_file, 'r', encoding='utf-8') as f:
        for ln in f:
            ent = parse_line(ln.strip())
            if ent:
                cur.execute(
                    'INSERT INTO log_entries(ip,date,method,url,status,size,user_agent) VALUES(?,?,?,?,?,?,?)',
                    ent
                )
                cnt += 1
    conn.commit()
    conn.close()
    return True, cnt

# Получение логов

def get_logs(db_path, filters):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    where, params = [], []
    for k in ('ip','keyword','date_from','date_to'):
        v = filters.get(k)
        if v:
            if k == 'keyword':
                where.append('url LIKE ?'); params.append('%'+v+'%')
            elif k in ('date_from','date_to'):
                op = '>=' if k=='date_from' else '<='
                where.append(f'date {op} ?'); params.append(v)
            else:
                where.append(f'{k} = ?'); params.append(v)
    clause = 'WHERE ' + ' AND '.join(where) if where else ''
    q = f"SELECT * FROM log_entries {clause} ORDER BY date DESC LIMIT ?"
    params.append(filters.get('limit',100))
    cur.execute(q, params)
    rows = cur.fetchall()
    conn.close()
    return rows

# Регистрация / валидация пользователя

def register_user(db_path, username, password):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute('INSERT INTO users(username,password) VALUES(?,?)', (username,password))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def validate_user(db_path, username, password):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute('SELECT id FROM users WHERE username=? AND password=?', (username,password))
    ok = cur.fetchone() is not None
    conn.close()
    return ok
