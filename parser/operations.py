'''
author: Sarthak Khandelwal
'''

import re


VALID_OPERATIONS = ['begin', 'r', 'w', 'dump', 'end', 'fail', 'recover']

class Operations:
    """
    The operation class that represents operations for all transactions. Every line
    entered on the stdin is parsed as a operation object and processed. 
    """

    def __init__(self, raw_str):
        """
        Checks if a raw string is a valid operation and parses it.
        Operations supported:
        
        begin(T1) : starts a new transaction with id T1. Id can be any string.
        R(T1,x1): read x1 in transaction T1
        W(T1, x1, 10): write x1 to 10 in transaction T1
        dump(): dump all values at all sites in the database
        end(T1): end/try to commit transaction T1
        fail(1): fail site 1
        recover(1): recover site 1
        
        """
        self._raw_str = raw_str.lower()
        self.op_type = self._raw_str.split('(')[0]
        if self.op_type == 'exit':
            print('Bye!')
            exit()
        if self.op_type not in VALID_OPERATIONS:
            print("Not a valid operation!")
        else:
            op_values_search = re.search(r'\((.*?)\)', self._raw_str)
            op_values = op_values_search.group(1).split(',')
            if self.op_type == 'begin' or  self.op_type == 'end' \
                or  self.op_type == 'recover' or  self.op_type == 'fail':
                self.id = op_values[0].strip()
            elif self.op_type == 'r':
                self.id = op_values[0].strip()
                self.variable = op_values[1].strip()
            elif self.op_type == 'w':
                self.id = op_values[0].strip()
                self.variable = op_values[1].strip()
                self.value = op_values[2].strip()
            elif self.op_type == 'end':
                self.id =  op_values[0].strip()
            elif self.op_type == 'fail':
                self.id = op_values[0].strip()
