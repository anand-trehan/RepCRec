'''
author: Anand Trehan
'''

from typing import Dict

class Transaction:
    
    def __init__(self, id: str, start_time: int):
        self.id = id  # String to hold the transaction ID
        self.start_time = start_time  # Integer to hold the start time of the transaction
        self.transaction_record : Dict[str,list] = {} #dictionary with all transactions
        self.commit_time = None
        self.snapshot = {}
        self.snapshot_sites = {}

    def get_id(self):
        return self.id

    def get_start_time(self):
        return self.start_time