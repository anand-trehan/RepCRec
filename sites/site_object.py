'''
authors: Sarthak Khandelwal, Anand Trehan
'''

from sites.data import Data
from transaction_handling.serialization import SerializationGraph
from transaction_handling.transaction import Transaction

class Site:
    """Site object representing each site in our database"""

    def __init__(self, health: bool, last_down_time: int, site_id: int):
        self.health = health
        self.site_id = site_id # use this site_id to intialise data and graph  
        self.data = self._initialise_data()  
        self.graph = self._initialise_graph()  
        self.last_down_time = last_down_time
        self.ssi_info = {}

    def _initialise_data(self):
        """
        Initialised a given site, with data. Even numbered variables are present at 
        all sites whereas odd numbered ones are present at specific sites only.
        """
        data_dict = {}
        for i in range(20):
            var_name = 'x%i' % (i + 1)
            var_value = (i + 1) * 10
            var_obj = Data(var_name, var_value)
            if (i+1)%2==0:
                data_dict.update({var_name: var_obj})
            elif 1+((i+1)%10)==self.site_id:
                data_dict.update({var_name: var_obj})
            else:
                continue
        return data_dict
    
    def _initialise_graph(self):
        """Creates a empty graph for the site"""
        graph = SerializationGraph()
        return graph
    
    def _create_sets(self, transaction):
        write_set = set()
        read_set = set()
        transaction_record = transaction.transaction_record
        for key in transaction_record.keys():
            for operations in transaction_record[key]:
                if operations[0] == "r":
                    read_set.add(key)
                elif operations[0] == "w":
                    write_set.add(key)
        return read_set, write_set

    def write(self, transaction : Transaction, tick):
        """Write data to the site for a given transaction."""
        transaction_record = transaction.transaction_record
        for key in transaction_record.keys():
            for operations in transaction_record[key]:
                if operations[0] == "w":
                    if key in self.data:
                        self.data[key].setValue(operations[1], tick)
                    else:
                        continue
                else:
                    continue
        return
    
    def write_data(self, variable, value, tick):
        """Update a particular variable value for the site."""
        self.data[variable].setValue(value, tick)

    def updateGraph(self, transaction : Transaction, tick : int):
        """Update the graph at the site."""

        read_set, write_set = self._create_sets(transaction)
        if not self.ssi_info:
            # print("Comitted %s" % transaction.id)
            transaction.commit_time = tick
            self.ssi_info.update({transaction.id : transaction})
            self.graph.add_edge(transaction.id, '', '')
            return True

        for tx in self.ssi_info.keys():
            # print(tx)
            tx_obj = self.ssi_info[tx]
            tx_read_set, tx_write_set = self._create_sets(tx_obj)
            if tx_obj.commit_time < transaction.start_time:
                if not tx_write_set.isdisjoint(write_set):
                    self.graph.add_edge(tx_obj.id, transaction.id, "ww") 
                if not tx_write_set.isdisjoint(read_set):
                    self.graph.add_edge(tx_obj.id, transaction.id, "wr") 
            if tx_obj.start_time < tick:
                if not tx_read_set.isdisjoint(write_set):
                    self.graph.add_edge(tx_obj.id, transaction.id, "rw")
            if transaction.start_time < tx_obj.commit_time:
                if not read_set.isdisjoint(tx_write_set):
                    self.graph.add_edge(transaction.id, tx_obj.id,  "rw")
        if self.graph.has_cycle_with_two_rw():
            print("Abort %s" % transaction.id)
            self.remove_aborted_transaction(transaction)
            return False
        else:
            self.ssi_info.update({transaction.id : transaction})
            # print("Comitted %s" % transaction.id)
            return True
    
    def remove_aborted_transaction(self, transaction : Transaction):
        """
        Remove a transaction from the snapshot isolation information
        and the directed graph.
        """
        self.graph.remove_transaction(transaction.id)
        if transaction.id in self.ssi_info.keys():
            self.ssi_info.pop(transaction.id)
        

    def failSite(self):
        """Mark site as failed."""
        self.health = False
        self.ssi_info = {}
        return self.health

    def recoverSite(self, lastdowntime : int):
        """Recover site."""
        self.health = True
        self.last_down_time = lastdowntime
        return self.health