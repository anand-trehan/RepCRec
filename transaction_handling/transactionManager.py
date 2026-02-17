'''
authors: Anand Trehan, Sarthak Khandelwal
'''

from transaction_handling.transaction import Transaction
from sites.site_object import Site
from parser.operations import Operations
from typing import List, Dict
from tabulate import tabulate

class TransactionManager:
    """
    Handles all the transactions for our databases, has the core logic
    for available copies algorithm and snapshot isolation.
    """
    
    def __init__(self):
        self.transactions : Dict[str,Transaction] = {}
        self.ticker = 0 #timestamp
        self.num_sites=10
        self.pending_transactions: List[Operations] = []
        self.sites: List[Site] = self.__initialise_all_sites() 

    def __initialise_all_sites(self):
        """Create a site object for each site and store them with the manager"""
        all_sites=[]
        for i in range(1,self.num_sites+1):
            temp=Site(True,-1,i) #all sites intialised in good health
            all_sites.append(temp)
        return all_sites
    
    def processOperation(self, operation: "Operations", is_pending: bool):
        """
        Process a particular valid operation. If a operation cannot execute 
        due to available copies reason, it added to a list of pending transactions.
        If a pending transaction is being processed, the timestamp is not updated.
        Depending on the type of operation, respective method is invoked to handle it.
        """
        if not is_pending:
            self.ticker+=1 #for each operation ticker increments by 1
        if operation.op_type=="begin":
            new_transaction = Transaction(operation.id,self.ticker)
            self.add_transaction(new_transaction,operation.id)
        elif operation.op_type=="r":
            #return read value if exists and updates the transaction record
            canReadOutput = self.canRead(operation.id, operation.variable)
            if canReadOutput==2:
                pass #everything is ok!
            elif canReadOutput==1:
                #add to pending since we need to wait
                self.pending_transactions.append(operation)
            else:
                #transaction aborted
                pass
        elif operation.op_type=="w":
            # checks if write is possible and updates the transaction record
            if self.canWrite(operation.id, operation.variable, operation.value, self.ticker):
                pass
            else:
                #add to pending since we need to wait
                self.pending_transactions.append(operation)
        elif operation.op_type=="end":
            self.end(operation.id, self.ticker)
        elif operation.op_type=="fail":
            self.fail(int(operation.id))
        elif operation.op_type=="recover":
            self.recover(int(operation.id), self.ticker-1)
        elif operation.op_type=="dump":
            self.dump()
        elif operation.op_type=="querystate":
            pass
        else:
            pass
        return

    def canRead(self,transaction_id: str, variable : str):
        """
        Check if a particular transaction can read that variable.
        We first check if the transaction is valid and currently still running, if so
        we check if the transaction has written to the varible itself. If it has, we return
        the value that the transaction itself wrote. If the transaction did not write to 
        the variable, we check for the value in the database's snapshot at the time the 
        transaction started. This avoid reading from any concurrent writes that may have happended
        to the variable. If none of the sites containing that variable are currently up, then
        the read simply waits otherwise it reads from a site and returns the value. 
        """
        if not transaction_id in self.transactions:
            print("Transaction has already aborted %s" % transaction_id)
            return 0
        else:
            if variable in self.transactions[transaction_id].transaction_record.keys():
                for operation in reversed(self.transactions[transaction_id].transaction_record[variable]):
                    if operation[0] == 'w':
                        print("Read successful")
                        print(variable + ": " +operation[1])
                        return 2
        
        if variable in self.transactions[transaction_id].snapshot:
            avl_site=False
            for site_id in self.transactions[transaction_id].snapshot_sites[variable]:
                if self.sites[site_id-1].health==True:
                    avl_site=True
                
            if not avl_site:
                print("Waiting Transaction because of no available sites %s" % transaction_id)
                return 1
            
            if variable in self.transactions[transaction_id].transaction_record:
                self.transactions[transaction_id].transaction_record[variable].extend([["r"]])
            else:
                self.transactions[transaction_id].transaction_record[variable]=[["r"]]
            #print read result
            print("Read successful")
            print(variable+": "+str(self.transactions[transaction_id].snapshot[variable]))
            return 2
        
        # if variable isn't in snapshot then we either abort in case of replicated variable or we wait 
        if int(variable.split("x")[1])%2==1:
            #print wait message
            print("Waiting Transaction because of no available sites %s" % transaction_id)
            return 1
        else:
            #print abort message
            print("Aborted Transaction because of no available sites %s" % transaction_id)
            # remove aborted transaction from all sites
            for site in self.sites:
                #if site.health==True:
                site.remove_aborted_transaction(self.transactions[transaction_id])
            # remove transaction from the Transaction manager
            self.transactions.pop(transaction_id)
            return 0

    def canWrite(self, transaction_id: str, variable : str, value : int, tick: int):
        """
        Checks if a particular transaction can write to a variable. The transaction manager
        checks if any site is available for the transaction to write that variable, if they are
        we say that a write is possible. If none of the sites are available to write, the 
        transaction waits.
        """
        if not transaction_id in self.transactions:
            print("Transaction has already aborted %s" % transaction_id)
            return False
        var_num = int(variable.split('x')[-1])
        available_sites = []
        if var_num % 2 == 1:
            if self.sites[(var_num % 10)].health:
                available_sites.append(1 + (var_num % 10))
        else:
            for site in self.sites:
                if site.health==True:
                    available_sites.append(1 + self.sites.index(site))
                else:
                    continue
        # Print waiting message
        if len(available_sites) > 0:
            print("We can write to ", available_sites)
            if variable in self.transactions[transaction_id].transaction_record:
                self.transactions[transaction_id].transaction_record[variable].extend([["w", value, available_sites, tick]])
            else:
                self.transactions[transaction_id].transaction_record[variable]=[["w", value, available_sites, tick]]
            return True
        else:
            print("Waiting Transaction because of no available sites %s" % transaction_id)
            return False

    def canCommit(self, transaction: "Transaction", potential_commit_time : int ):
        """
        This method checks if a commit is allowed by the following two rules:
            1. First committer wins: First check if any commits to the same variable were 
            done by another transaction between our transaction start time 
            and commit time (now) if any such transaction exists then we cannot commit

            2. Cycles with 2 consecutive RW edge: We check if adding the transaction to 
            each site's serialization graph will causes a cycle with two consecutive RW edges.
            If so, we abort the transaction and remove it's information from all sites and
            the transaction manager.
        """
        canWeCommit=True
        write_dict = self._create_write_dict(transaction)
        for obj in write_dict:
            for site in self.sites:
                if site.health==True:
                    if obj in site.data:
                        if site.data[obj].lastWriteTime > transaction.start_time:
                            canWeCommit=False
                            return False
        # send transaction -> call update graph on all sites -> if even one fails then do not commit
        for site in self.sites:
            if site.health==True:
                if not site.updateGraph(transaction,potential_commit_time):
                    canWeCommit=False
                    return False
        if canWeCommit : 
            return True
        else:
            return False
    
    def canCommitAC(self, transaction : Transaction):
        """
        This method checks if a commit is allowed by the available copies rule.
        If the transaction has written to a site, which later failed then the transaction
        can no longer commit. 
        """
        for var in transaction.transaction_record.keys():
            for operation in transaction.transaction_record[var]:
                if operation[0] == 'w':
                    for site_id in operation[2]:
                        site : Site = self.sites[site_id - 1]
                        if (site.last_down_time > operation[3]) or site.health==False:
                            print("Site %s was down after %s wrote to it" % (str(site.site_id), transaction.id))
                            return False 
        return True


    def end(self, transaction_id : str, timestamp : int):
        """
        Processes the end transaction operation which check if a commit is allowed
        by both the available copies alogrithm and snapshot isolation. If a commit 
        is allowed, we go through every write in the commit and update the data value 
        for each site that was healthy when the write was processed. If a transaction fails
        we go and remove the snapshot isolation information and the transaction from each
        site's serialization graphs. 
        """
        if not transaction_id in self.transactions:
            print("Transaction has already aborted %s" % transaction_id)
            return False
        # if cancommit gives us true then call site.write else abort transaction
        canWeEndAC = self.canCommitAC(self.transactions[transaction_id])
        
        canWeEndSSI = self.canCommit(self.transactions[transaction_id],timestamp)
        if canWeEndAC and canWeEndSSI:
            transaction = self.transactions[transaction_id]
            for var in transaction.transaction_record.keys():
                for operation in transaction.transaction_record[var]:
                    if operation[0] == 'w':
                        for site_id in operation[2]:
                            site : Site = self.sites[site_id - 1]
                            site.write_data(var, operation[1], timestamp)
            print("Transaction committed %s" % transaction_id)
            # update commit time
            self.transactions[transaction_id].commit_time=timestamp
            #if there are no other active transactions besides the committed one then this transaction can be removed from all sites
            if any(key!=transaction_id for key in self.transactions):
                #dont do anything
                pass
            else:
                #delete from all sites
                for site in self.sites:
                    #if site.health==True:
                    site.remove_aborted_transaction(self.transactions[transaction_id])
                #remove transaction from transaction manager
                self.transactions.pop(transaction_id)
        else:
            #abort transaction
            print("Aborted Transaction because it cannot commit %s" % transaction_id) 
            #remove all references to aborted transactions from the SSI graph at each site
            for site in self.sites:
                #if site.health==True:
                site.remove_aborted_transaction(self.transactions[transaction_id])
            #remove transaction from transaction manager
            self.transactions.pop(transaction_id)
        return

    def dump(self):
        """
        Prints the values of each variable at every site. Can be used at any point
        after any command. The table is arranged into proper columns and rows.
        To view the structure nicely, slightly reduce the font size of your terminal 
        so that it fits in one page.
        """
        table = []
        variables = [f"x{i}" for i in range(1, 21)]  # x1 to x20
        site_list = []
        for site in self.sites:
            site_list.append(site.data)
        for i, site in enumerate(site_list, start=1):  # For each site
            row = [f"S{i}"]  # Start with site name
            for var in variables:
                # Append value or '*' if the key is missing
                if var in site:
                    row.append(site[var].value)
                else:
                    row.append('*')
                # row.append(site.get(var, '*').value)
            table.append(row)
        header = [""] + variables
        print(tabulate(table, headers=header, tablefmt="grid"))

    def fail(self, site_id : int):
        """
        Marks a site as failed.
        """
        self.sites[site_id-1].failSite()
        print("Site "+str(site_id)+" failed")
        return

    def recover(self, site_id : int, last_down_time : int):
        """
        Brings back a failed site to healthy state. Once a site
        recovers, we go and check if there are any pending transactions that
        can now proceed. 
        """
        self.sites[site_id-1].recoverSite(last_down_time)
        print("Site "+str(site_id)+" recovered")
        # add non replicated variables back to all snapshots 
        for var in self.sites[site_id-1].data.keys():
            var_num = int(var.split('x')[-1])
            if var_num%2==1:
                for transaction_id in self.transactions.keys():
                    if var not in self.transactions[transaction_id].snapshot.keys():
                        self.transactions[transaction_id].snapshot.update({var:self.sites[site_id-1].data[var].value})
                        self.transactions[transaction_id].snapshot_sites[var]=[site_id]
                        if site_id not in self.transactions[transaction_id].snapshot_sites[var]:
                            self.transactions[transaction_id].snapshot_sites[var].extend([site_id])
        #handle pending
        self.handle_pending_transactions()
        return

    def queryState(self):
        # TODO
        pass
    
    def add_transaction(self, transaction: "Transaction",T_id: str):
        """
        Add a new transaction to the transaction manager
        """
        self.update_initial_snapshot(transaction)
        self.transactions[T_id]=transaction
        return
    
    def update_initial_snapshot(self, transaction: "Transaction"):
        """
        Creates a snapshot of the database when a new transaction comes. This
        transaction will use this snapshot for all it's reads. It captures the
        current state of the database when the transaction started.
        """
        snapshot = {}
        snapshot_sites = {}
        for site in self.sites:
            if site.health==True:
                for variable in site.data.keys():
                    var = site.data[variable]
                    # for each non replicated site, if variable exists and site is up we directly add to snapshot
                    if int(variable.split("x")[1])%2==1:
                        snapshot.update({var.variableName:var.value})
                        if var in snapshot_sites:   
                            snapshot_sites[var.variableName].extend([site.site_id])
                        else:
                            snapshot_sites[var.variableName]=[site.site_id]
                    # for each replicated site check if transaction started after the last commit time of variable and 
                    # if the site was last down at a time before the variable's last commit time.
                    else:
                        if var.lastWriteTime > site.last_down_time and var.lastWriteTime < transaction.get_start_time() : 
                            snapshot.update({var.variableName:var.value})
                            if var in snapshot_sites:   
                                snapshot_sites[var.variableName].extend([site.site_id])
                            else:
                                snapshot_sites[var.variableName]=[site.site_id]
                        else:
                            continue
            else:
                continue
        transaction.snapshot=snapshot  
        transaction.snapshot_sites=snapshot_sites   
        return
    
    def handle_pending_transactions(self):
        """
        Handles the processing of pending transaction, triggered when a site recovers
        """
        l = len(self.pending_transactions)
        i = 0
        while i < l:
            self.processOperation(self.pending_transactions[0],True)
            self.pending_transactions.pop(0)
            i+=1
        return

    def _create_write_dict(self, transaction):
        """
        Creates a dictionary of all the writes done by a particular transaction
        """
        write_dict = {}
        transaction_record = transaction.transaction_record
        for key in transaction_record.keys():
            for operations in transaction_record[key]:
                if operations[0] == "w":
                    write_dict.update({key:operations[1]})
        return write_dict

