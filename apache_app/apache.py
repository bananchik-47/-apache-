import sqlite3
import configparser
import os
import re
import sys
from datetime import datetime
import argparse
from tqdm import tqdm
import time

class SimpleLogTool:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.load_settings()
        db_file_path = self.config['БазаДанных']['ПУТЬ'].replace('sqlite:///', '')
        self.database_file = db_file_path
        self.setup_database()

    def load_settings(self):
        self.config.read('config.ini')
        if not self.config.sections():
            self.config['БазаДанных'] = {'ПУТЬ': 'sqlite:///logs.db'}
            self.config['Логи'] = {'Папка': 'logs', 'ИмяФайла': 'access.log'}
            with open('config.ini', 'w') as config_file:
                self.config.write(config_file)

    def setup_database(self):
        folder = os.path.dirname(self.database_file)
        if folder:
            os.makedirs(folder, exist_ok=True)

        conn = sqlite3.connect(self.database_file)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS log_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ip TEXT,
                date TEXT,
                method TEXT,
                url TEXT,
                status INTEGER,
                size INTEGER,
                user_agent TEXT
            )
        ''')
        conn.commit()
        conn.close()

    def import_logs(self):
        log_file_path = os.path.join(
            self.config['Логи']['Папка'],
            self.config['Логи']['ИмяФайла']
        )

        if not os.path.exists(log_file_path):
            print(f"⛔ Файл не найден: {log_file_path}")
            print("Посмотри config.ini")
            return False

        with open(log_file_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f)

        conn = sqlite3.connect(self.database_file)
        cursor = conn.cursor()
        inserted = 0

        print(f"\n📂 Открываем файл логов: {log_file_path}")
        with tqdm(total=total_lines, desc="Обработка", unit="строк") as progress:
            with open(log_file_path, 'r', encoding='utf-8') as log_file:
                for line in log_file:
                    line = line.strip()
                    try:
                        parsed = self.parse_line(line)
                        if parsed:
                            cursor.execute('''
                                INSERT INTO log_entries 
                                (ip, date, method, url, status, size, user_agent)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', parsed)
                            inserted += 1
                    except Exception as error:
                        print(f"\n⚠ Ошибка: {error}", file=sys.stderr)
                    finally:
                        progress.update(1)
                        time.sleep(0.001)

        conn.commit()
        conn.close()

        print(f"\n✅ Завершено. Обработано: {inserted}/{total_lines}")
        print(f"Сохранено в базу: {self.database_file}")
        return True

    def parse_line(self, log_line):
        regex = r'^(\S+) \S+ \S+ \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'
        match = re.match(regex, log_line)

        if not match:
            return None

        ip, date_str, request, status, size, referrer, user_agent = match.groups()

        try:
            method, url, _ = request.split(' ', 2)
        except:
            return None

        try:
            date_parsed = datetime.strptime(date_str, '%d/%b/%Y:%H:%M:%S %z').isoformat()
        except:
            date_parsed = date_str

        if user_agent == '-':
            user_agent = None

        return (
            ip,
            date_parsed,
            method,
            url,
            int(status),
            int(size),
            user_agent
        )

    def show_logs(self, filters):
        conn = sqlite3.connect(self.database_file)
        cursor = conn.cursor()

        where_conditions = []
        parameters = []

        if filters.get('ip'):
            where_conditions.append("ip = ?")
            parameters.append(filters['ip'])
        if filters.get('keyword'):
            where_conditions.append("url LIKE ?")
            parameters.append(f"%{filters['keyword']}%")
        if filters.get('date_from'):
            where_conditions.append("date >= ?")
            parameters.append(filters['date_from'])
        if filters.get('date_to'):
            where_conditions.append("date <= ?")
            parameters.append(filters['date_to'])

        where_sql = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        limit_sql = f"LIMIT {filters.get('limit', 100)}"

        query = f"SELECT * FROM log_entries {where_sql} ORDER BY date DESC {limit_sql}"

        cursor.execute(query, parameters)
        logs = cursor.fetchall()

        if not logs:
            print("\n🔍 Ничего не найдено.")
            return

        print("\n📋 Найденные записи:")
        print("-" * 120)
        print(f"| {'IP':<15} | {'Дата':<20} | {'Метод':<6} | {'URL':<40} | {'Код':<6} | {'Размер':<6} |")
        print("-" * 120)

        for log in logs:
            print(f"| {log[1]:<15} | {log[2][:19]:<20} | {log[3]:<6} | {log[4][:40]:<40} | {log[5]:<6} | {log[6]:<6} |")

        print("-" * 120)
        print(f"📊 Всего строк: {len(logs)}")
        conn.close()

def print_help():
    print("\n📌 Простенький анализатор логов (Apache)")
    print("Как пользоваться:")
    print("  python apache.py parse       - Загрузить логи из access.log в базу")
    print("  python apache.py show        - Посмотреть логи (первые 100 штук)")
    print("\nМожно добавить:")
    print("  --ip IP_ADDRESS           - Фильтр по IP")
    print("  --keyword СЛОВО           - Фильтр по URL")
    print("  --date-from YYYY-MM-DD    - Начало периода")
    print("  --date-to YYYY-MM-DD      - Конец периода")
    print("  --limit СКОЛЬКО           - Сколько строк (по умолчанию 100)")
    print("\nПримеры:")
    print("  python apache.py show --ip 127.0.0.1 --limit 20")
    print("  python apache.py show --keyword admin --date-from 2024-01-01")

def main():
    if len(sys.argv) == 1:
        print_help()
        return

    parser = argparse.ArgumentParser(description='Анализ логов', usage='python apache.py [parse|show] [опции]')
    subparsers = parser.add_subparsers(dest='command', required=True)

    subparsers.add_parser('parse', help='Загрузить логи в базу')
    show_parser = subparsers.add_parser('show', help='Показать логи')
    show_parser.add_argument('--ip', help='IP адрес')
    show_parser.add_argument('--keyword', help='Ключевое слово в URL')
    show_parser.add_argument('--date-from', help='С какой даты')
    show_parser.add_argument('--date-to', help='По какую дату')
    show_parser.add_argument('--limit', type=int, default=100, help='Сколько строк')

    args = parser.parse_args()
    analyzer = SimpleLogTool()

    if args.command == 'parse':
        analyzer.import_logs()
    elif args.command == 'show':
        analyzer.show_logs({
            'ip': args.ip,
            'keyword': args.keyword,
            'date_from': args.date_from,
            'date_to': args.date_to,
            'limit': args.limit
        })

if __name__ == '__main__':
    print("🛠️ Простой инструмент анализа логов")
    main()
