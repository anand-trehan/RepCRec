from parser.parser import Parser
from transaction_handling.transaction import Transaction
from transaction_handling.transactionManager import TransactionManager
from sites.site_object import Site

def main():
    """The main control loop that reads every operations and processes it."""
    
    parser = Parser()
    transaction_manager = TransactionManager()
    
    for operation in parser.run():
        transaction_manager.processOperation(operation, False)



if __name__=='__main__':
    main()