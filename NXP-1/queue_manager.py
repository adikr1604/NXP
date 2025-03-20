import sqlite3
import json
import subprocess
import time
from datetime import datetime, timedelta

''' Class to read json file and 
provide methods for load and retrieve table info'''


class TableMapping:
    def __init__(self, file_path):
        self.file_path = file_path
        self.tables = self.load_tables()

    def load_tables(self):
        with open(self.file_path, 'r') as file:
            data = json.load(file)
        return data['tables'][0]

    def get_table_info(self, table_name):
        return self.tables.get(table_name, None)


'''Inserts tables into queue'''


class QueueInserter:
    def __init__(self, db_path, table_mapping):
        self.db_path = db_path
        self.table_mapping = table_mapping
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS QUEUE (
                table_name TEXT PRIMARY KEY,
                insert_time TIMESTAMP,
                trigger_time TIMESTAMP,
                status TEXT,
                priority INTEGER
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS QUEUE_HISTORY (
                table_name TEXT,
                status TEXT,
                timestamp TIMESTAMP,
                priority INTEGER
            )
        ''')
        self.conn.commit()

    def insert_table(self, table_name):
        table_info = self.table_mapping.get_table_info(table_name)
        if table_info:
            priority = int(table_info['priority'])
            insert_time = datetime.now()
            self.cursor.execute('''
                INSERT INTO QUEUE (table_name, insert_time, status, priority)
                VALUES (?, ?, ?, ?)
            ''', (table_name, insert_time, 'Q', priority))
            self.cursor.execute('''
                INSERT INTO QUEUE_HISTORY (table_name, status, timestamp, priority)
                VALUES (?, ?, ?, ?)
            ''', (table_name, 'Q', insert_time, priority))
            self.conn.commit()

    def query_teradata(self):
        # Placeholder for actual Teradata query logic
        return ["database_1.table_1", "database_1.table_2", "database_3.table_1"]

    def run(self):
        tables_to_reload = self.query_teradata()
        for table in tables_to_reload:
            self.insert_table(table)


'''Select and runs job from the queue'''


class ReloadSelector:
    def __init__(self, db_path, table_mapping):
        self.db_path = db_path
        self.table_mapping = table_mapping
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def update_priorities(self):
        self.cursor.execute('SELECT table_name, insert_time FROM QUEUE')
        rows = self.cursor.fetchall()
        for row in rows:
            table_name, insert_time = row
            if isinstance(insert_time, str):
                insert_time = datetime.strptime(insert_time, '%Y-%m-%d %H:%M:%S.%f')
            table_info = self.table_mapping.get_table_info(table_name)
            max_staleness = int(table_info['max_staleness'])
            elapsed_minutes = (datetime.now() - insert_time).total_seconds() / 60
            remaining_minutes = max_staleness - elapsed_minutes
            score = self.calculate_score(remaining_minutes)
            priority = score * int(table_info['priority'])
            self.cursor.execute('''
                UPDATE QUEUE
                SET priority = ?
                WHERE table_name = ?
            ''', (priority, table_name))
        self.conn.commit()

    def calculate_score(self, remaining_minutes):
        if remaining_minutes > 60:
            return 0
        elif 45 < remaining_minutes <= 60:
            return 1
        elif 30 < remaining_minutes <= 45:
            return 2
        elif 15 < remaining_minutes <= 30:
            return 3
        elif 10 < remaining_minutes <= 15:
            return 4
        elif 5 < remaining_minutes <= 10:
            return 5
        elif 3 < remaining_minutes <= 5:
            return 6
        else:
            return 7

    def run(self):
        while True:
            self.update_priorities()
            self.cursor.execute('SELECT table_name FROM QUEUE WHERE status = "Q" ORDER BY priority DESC LIMIT 1')
            row = self.cursor.fetchone()
            if row:
                table_name = row[0]
                self.cursor.execute('''
                    UPDATE QUEUE
                    SET status = "R", trigger_time = ?
                    WHERE table_name = ?
                ''', (datetime.now(), table_name))
                self.cursor.execute('''
                    INSERT INTO QUEUE_HISTORY (table_name, status, timestamp, priority)
                    VALUES (?, "R", ?, ?)
                ''', (table_name, datetime.now(),
                      self.cursor.execute('SELECT priority FROM QUEUE WHERE table_name = ?', (table_name,)).fetchone()[
                          0]))
                self.conn.commit()
                self.run_job(table_name)
                self.cursor.execute('''
                    DELETE FROM QUEUE
                    WHERE table_name = ?
                ''', (table_name,))
                self.cursor.execute('''
                    INSERT INTO QUEUE_HISTORY (table_name, status, timestamp, priority)
                    VALUES (?, "F", ?, ?)
                ''', (table_name, datetime.now(),
                      self.cursor.execute('SELECT priority FROM QUEUE WHERE table_name = ?', (table_name,)).fetchone()[
                          0]))
                self.conn.commit()
            else:
                time.sleep(60)

    def run_job(self, table_name):
        # Placeholder for actual job running logic
        print(f"Running job for {table_name}")
        # Simulate job execution
        time.sleep(1)

        # Delete the table from the QUEUE table
        self.cursor.execute('''
            DELETE FROM QUEUE
            WHERE table_name = ?
        ''', (table_name,))
        self.conn.commit()