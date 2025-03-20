from queue_manager import QueueInserter, ReloadSelector, TableMapping

if __name__ == '__main__':
    db_path = 'sample_data/sample_queue.db'
    table_mapping = TableMapping('sample_data/tables.json')
    queue_inserter = QueueInserter(db_path, table_mapping)
    reload_selector = ReloadSelector(db_path, table_mapping)

    queue_inserter.run()
    reload_selector.run()