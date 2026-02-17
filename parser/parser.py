'''
author: Sarthak Khandelwal
'''

import fileinput
from parser.operations import Operations

class Parser:
    
    def run(self):
        """Reads commands line by line from a input file to stdin and created a operation object."""
        for line in fileinput.input():
            operation = Operations(line.strip())
            yield operation