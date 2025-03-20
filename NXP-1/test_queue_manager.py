import unittest
from queue_manager import QueueInserter, ReloadSelector, TableMapping
from datetime import datetime, timedelta
import sqlite3
import os

class TestQueueManager(unittest.TestCase):
    def setUp(self):
        self.db_path = 'test_queue.db'
        self.table_mapping = TableMapping('sample_data/tables.json')
        self.queue_inserter = QueueInserter(self.db_path, self.table_mapping)
        self.reload_selector = ReloadSelector(self.db_path, self.table_mapping)
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

    def tearDown(self):
        os.remove(self.db_path)

    def test_insert_table(self):
        self.queue_inserter.insert_table('database_1.table_1')
        self.cursor.execute('SELECT * FROM QUEUE WHERE table_name = "database_1.table_1"')
        self.assertIsNotNone(self.cursor.fetchone())

    def test_update_priorities(self):
        self.queue_inserter.insert_table('database_1.table_1')
        self.reload_selector.update_priorities()
        self.cursor.execute('SELECT priority FROM QUEUE WHERE table_name = "database_1.table_1"')
        priority = self.cursor.fetchone()[0]
        self.assertGreater(priority, 0)

    def test_run_job(self):
        self.queue_inserter.insert_table('database_1.table_1')
        self.reload_selector.run_job('database_1.table_1')
        self.cursor.execute('SELECT * FROM QUEUE WHERE table_name = "database_1.table_1"')
        self.assertIsNone(self.cursor.fetchone())

if __name__ == '__main__':
    unittest.main()