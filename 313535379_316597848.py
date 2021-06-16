import pyodbc
import textwrap
import datetime
import threading
from os import listdir
import numpy as np
from collections import OrderedDict
import ctypes

X = 22


class Connector:
    """
    This class responsible for connection between the database and the server. It help to synchronize between threads
    as they share the memory.
    """

    def __init__(self):
        # map between db name and categories
        self.categories_map = {}
        # hold update inventory as we keep checking if the order is available
        self.update_inventory = {}
        # hold exception as we want to know why the the transaction wasn't complete
        self.out_of_order_tran = {}
        # holds connection object and cursor for lock transaction for each transactionID
        self.conn = {}
        # execute history to commit for each table
        self.to_commit = {}
        self.history = {}
        # build map between db
        cnxn_db = pyodbc.connect(DRIVER='{SQL Server};',
                                 SERVER='technionddscourse.database.windows.net;',
                                 DATABASE="dbteam",
                                 UID="dbteam",
                                 PWD="Qwerty12!",
                                 autocommit=False)
        cursor_db = cnxn_db.cursor()
        cursor_db.execute(f"SELECT * from CategoriesToSites")
        categories_list = cursor_db.fetchall()
        for item in categories_list:
            self.categories_map[item[0]] = item[1]

    def connect_to(self, transactionID, s_id):
        """
        :param transactionID:
        :param s_id:
        connect to database for each transaction and s_id
        """
        # Dont open a new connection if not needed
        if transactionID in self.conn.keys():
            if s_id in self.conn[transactionID].keys():
                return
        name = self.categories_map[s_id]
        conn = pyodbc.connect(DRIVER='{SQL Server};',
                              SERVER='technionddscourse.database.windows.net;',
                              DATABASE=name,
                              UID=name,
                              PWD="Qwerty12!",
                              autocommit=False,
                              )
        cursor = conn.cursor()
        # Insert the new connection and cursor into the right dict
        # update the to_commit
        if transactionID in self.conn.keys():
            self.conn[transactionID].update({s_id: (conn, cursor)})
            self.to_commit[transactionID].update(
                {s_id: {'uInventory': [], 'iInventory': [], 'order': [], 'orderLog': []}})
            self.history[transactionID].update({s_id: []})
        else:
            self.conn[transactionID] = {s_id: (conn, cursor)}
            self.to_commit[transactionID] = {s_id: {'uInventory': [], 'iInventory': [], 'order': [], 'orderLog': []}}
            self.history[transactionID] = {s_id: []}
            self.out_of_order_tran[transactionID] = []

    def try_update_lock(self, s_id, transactionID, p_id):
        """
        :param s_id:
        :param transactionID:
        :param p_id:
        :return: True if it mange to update the lock from read to write, False if it didnt
        :notice that when lock transaction is execute they always committed in the same function as normal transaction
        are not
        """
        f = '%Y-%m-%d %H:%M:%S'
        # check if lock is free
        sql = f"select * from Locks where productID = {p_id} and transactionID != '{str(transactionID)}' "
        self.conn[transactionID][s_id][1].execute(sql)
        rows = self.conn[transactionID][s_id][1].fetchall()
        # notify the log
        self.conn[transactionID][s_id][1].execute(
            f"insert into Log values ('{datetime.datetime.now().strftime(f)}', 'Locks', "
            f"'{str(transactionID)}', {p_id}, 'read', 'select * from Locks where productID = {p_id} and transactionID "
            f"!= ''{str(transactionID)}'' ')")
        self.conn[transactionID][s_id][0].commit()
        # if lock is free
        if len(rows) == 0:
            # delete the the current log
            self.conn[transactionID][s_id][1].execute(f"delete from Locks where productId = {p_id}")
            self.conn[transactionID][s_id][0].commit()
            # notice the log
            self.conn[transactionID][s_id][1].execute(
                f"insert into Log values ('{datetime.datetime.now().strftime(f)}', 'Locks',"
                f"'{str(transactionID)}', {p_id},'delete', "
                f"'delete from Locks where productId = {p_id}')")
            self.conn[transactionID][s_id][0].commit()
            # acquire the lock
            self.conn[transactionID][s_id][1].execute(
                f"insert into Locks values ('{str(transactionID)}', {p_id}, 'write')")
            self.conn[transactionID][s_id][0].commit()
            # notify the log
            self.conn[transactionID][s_id][1].execute(
                f"insert into Log values ('{datetime.datetime.now().strftime(f)}', 'Locks'"
                f", '{str(transactionID)}', {p_id},'insert', 'insert into Locks values (''{str(transactionID)}'', "
                f"{p_id}, ""write""' )")
            self.conn[transactionID][s_id][0].commit()
            return True
        self.conn[transactionID][s_id][0].commit()
        return False

    def release_all_lock(self, transactionID):
        """
        :param transactionID:
        :return: delete all appropriate locks
        :notice this is 2PL strict
        """
        f = '%Y-%m-%d %H:%M:%S'
        for s_id in self.conn[transactionID]:
            locks = self.conn[transactionID][s_id][1].execute(
                f"select productId from Locks where transactionID = '{transactionID}'").fetchall()
            for p_id in locks:
                # delete from table
                self.conn[transactionID][s_id][1].execute(f"delete from Locks where productId = {p_id[0]} and "
                                                          f"transactionID = '{str(transactionID)}'")
                # notify log
                self.conn[transactionID][s_id][1].execute(
                    f"insert into Log values ('{datetime.datetime.now().strftime(f)}', 'Locks',"
                    f"'{str(transactionID)}', {p_id[0]},'delete', "
                    f"'delete from Locks where productId = {p_id} and transactionID = ''{str(transactionID)}'' ')")
        self.conn[transactionID][s_id][0].commit()

    def try_acquire_lock(self, s_id, transactionID, p_id, l_type):
        """
        :param s_id:
        :param transactionID:
        :param p_id:
        :param l_type:
        :return: True if acquire the lock, False otherwise
        """
        f = '%Y-%m-%d %H:%M:%S'
        # create the right query
        if l_type == 'read':
            sql = f"select * from Locks where productID = {p_id} and lockType = 'write'"
        else:
            sql = f"select * from Locks where productID = {p_id}"
        # check if it can acquire the lock
        rows = self.conn[transactionID][s_id][1].execute(sql).fetchall()
        # notice the log
        self.conn[transactionID][s_id][1].execute(
            f"insert into Log values ('{datetime.datetime.now().strftime(f)}', 'Locks', "
            f"'{str(transactionID)}', {p_id}, 'read', 'select * from Locks where productID = {p_id} "
            f"and lockType = ""write""')")
        self.conn[transactionID][s_id][0].commit()
        # if can acquire lock
        if len(rows) == 0:
            # acquire the lock
            self.conn[transactionID][s_id][1].execute(
                f"insert into Locks values ('{str(transactionID)}', {p_id}, '{l_type}')")
            # notify log
            self.conn[transactionID][s_id][1].execute(
                f"insert into Log values ('{datetime.datetime.now().strftime(f)}', 'Locks'"
                f", '{str(transactionID)}', {p_id},'insert', 'insert into Locks values (''{str(transactionID)}'', "
                f"{p_id}, ''{l_type}'')' )")
            # commit
            self.conn[transactionID][s_id][0].commit()
            return True
        self.conn[transactionID][s_id][0].commit()
        return False

    def insertInventory(self, s_id, transactionID, p_id, table, values):
        """
        :param s_id:
        :param transactionID:
        :param p_id:
        :param table:
        :param values:
        :return:
        :notice: execute the the query to the right connection, DOSEN'T COMMIT IT
        """
        f = '%Y-%m-%d %H:%M:%S'
        # set right query
        self.to_commit[transactionID][s_id]['iInventory'].append((p_id, values))
        self.to_commit[transactionID][s_id]['orderLog']. \
            append((f'{datetime.datetime.now().strftime(f)}',
                    'ProductsInventory', transactionID, p_id, 'insert',
                    f'insert into ProductsInventory values ({p_id}, {values})'))

    def insertOrder(self, s_id, transactionID, p_id, values):
        """
        :param s_id:
        :param transactionID:
        :param p_id:
        :param table:
        :param values:
        :return:
        :notice: execute the the query to the right connection, DOSEN'T COMMIT IT
        """
        f = '%Y-%m-%d %H:%M:%S'
        # set right query
        self.to_commit[transactionID][s_id]['order'].append((f'{str(transactionID)}', p_id, values))
        self.to_commit[transactionID][s_id]['orderLog']. \
            append((f'{datetime.datetime.now().strftime(f)}',
                    'ProductsInventory', transactionID, p_id, 'insert',
                    f'insert into ProductsOrdered values ({str(transactionID)}, {p_id}), {values}'))

    def updateInventory(self, s_id, transactionID, p_id, quantity):
        """
        :param s_id:
        :param transactionID:
        :param p_id:
        :param quantity:
        :return:
        notice: execute update query to the right connection, DOSEN'T COMMIT IT
        """
        f = '%Y-%m-%d %H:%M:%S'
        self.to_commit[transactionID][s_id]['uInventory'].append((quantity, p_id))
        # notify log
        self.to_commit[transactionID][s_id]['orderLog']. \
            append((f'{datetime.datetime.now().strftime(f)}', 'ProductsInventory', transactionID, p_id, 'update',
                    f'update ProductsInventory set inventory = {quantity} where productID = {p_id}'))

    def read_inventory(self, s_id, transactionID, p_id):
        """
        :param s_id:
        :param transactionID:
        :param p_id:
        :return: inventory as int
        """
        f = '%Y-%m-%d %H:%M:%S'
        # if we don't need to read from the db
        if s_id in self.update_inventory.keys():
            if p_id in self.update_inventory[s_id]:
                return self.update_inventory[s_id][p_id]
        # if we need to read
        inventory = self.conn[transactionID][s_id][1].execute(
            f"select inventory from ProductsInventory where productID = {p_id}").fetchall()
        # write to log
        self.conn[transactionID][s_id][0].execute(
            f"insert into Log values ('{datetime.datetime.now().strftime(f)}','ProductsInventory', "
            f"'{str(transactionID)}', {p_id}, 'read', 'select inventory from ProductsInventory where productID= {p_id}')")
        self.conn[transactionID][s_id][1].commit()
        # update update_inventory if we needed to read
        if s_id in self.update_inventory.keys():
            if p_id in self.update_inventory[s_id]:
                self.update_inventory[s_id][p_id] = inventory[0][0]
            else:
                self.update_inventory[s_id].update({p_id: inventory[0][0]})
        else:
            self.update_inventory.update({s_id: {p_id: inventory[0][0]}})
        if len(inventory) == 0:
            return 0
        return inventory[0][0]

    def commit(self, transactionID, manage_Transation=False):
        """
        :param mange_Transcation:
        :param order:
        :param transactionID:
        :return:
        :commit all execute query in specific transactionID
        """
        for s_id in self.to_commit[transactionID].keys():
            for type in self.to_commit[transactionID][s_id].keys():
                if len(self.to_commit[transactionID][s_id][type]) == 0:
                    continue
                elif type == 'orderLog':
                    self.conn[transactionID][s_id][1].executemany("insert into Log values (?, ?, ?, ?, ?, ?)",
                                                                  self.to_commit[transactionID][s_id][type])
                elif type == 'iInventory':
                    self.conn[transactionID][s_id][1].executemany("insert into ProductsInventory values (?, ?)",
                                                                  self.to_commit[transactionID][s_id][type])
                elif type == 'uInventory':
                    if manage_Transation:
                        for inventory, p_id in self.to_commit[transactionID][s_id]['uInventory']:
                            self.history[transactionID][s_id].append((self.update_inventory[s_id][p_id], p_id))
                            self.update_inventory[s_id][p_id] = inventory
                    self.conn[transactionID][s_id][1].executemany("update ProductsInventory set inventory = ? where"
                                                                  " productID = ?",
                                                                  self.to_commit[transactionID][s_id][type])
                elif type == 'order':
                    self.conn[transactionID][s_id][1].executemany("insert into ProductsOrdered values (?, ?, ?)",
                                                                  self.to_commit[transactionID][s_id][type])
                self.conn[transactionID][s_id][0].commit()

    def rollback(self, transactionID):
        """
        :param transactionID:
        :return:
        :rollback all execute query in specific transactionID
        """
        for s_id in self.conn[transactionID]:
            self.conn[transactionID][s_id][0].rollback()

    def close_connection(self, transactionID):
        """
        :param transactionID:
        :return:
        :close all connection for specific transactionID
        """
        for s_id in self.conn[transactionID]:
            self.conn[transactionID][s_id][0].close()
            if s_id in self.update_inventory.keys():
                del self.update_inventory[s_id]
        del self.conn[transactionID], self.history[transactionID], self.to_commit[transactionID],
        self.out_of_order_tran[transactionID]

    def undo(self, transactionID):
        for s_id in self.conn[transactionID]:
            self.conn[transactionID][s_id][1].execute(f"delete from ProductsOrdered where "
                                                      f"transactionID = '{transactionID}'")
            self.conn[transactionID][s_id][1].execute(f"delete from Log where transactionID = '{transactionID}' "
                                                      f"relation != 'Locks'")
            if len(self.history[transactionID][s_id]) != 0:
                self.conn[transactionID][s_id][1].executemany("update ProductsInventory set inventory = ? where"
                                                              " productID = ?",
                                                              self.history[transactionID][s_id])


class Thread_with_exception(threading.Thread):
    """
    This class is use for executing update transaction. This class inherits form thread because we want to raise a an
    exception after timeout
    """

    def __init__(self, transactionID, order, conn):
        threading.Thread.__init__(self)
        self.transactionID = transactionID
        self.order = order
        self.t_conn = conn
        self.timeout = False
        self.working = False

    def run(self):
        # target function of the thread class
        try:
            num_of_locks = 0
            # TODO error one row
            # first we acquire read locks check if inventory is enough, if so we acquire write lock
            while num_of_locks < len(self.order.T[0]):
                for s_id, p_id, quantity in self.order:
                    # check if theres any write lock
                    if self.t_conn.try_acquire_lock(s_id, self.transactionID, p_id, 'read'):
                        # set num_of_locks
                        num_of_locks = num_of_locks + 1
                        # read inventory
                        inventory = self.t_conn.read_inventory(s_id, self.transactionID, p_id)
                        # if out of order
                        if inventory < quantity:
                            # notify and go back
                            print("inventory", inventory, "quantity", quantity, p_id, self.transactionID)
                            self.t_conn.out_of_order_tran[self.transactionID] = "out of order"
                            return
                        else:
                            # wait until we update the lock
                            while not (self.t_conn.try_update_lock(s_id, self.transactionID, p_id)):
                                pass
                            # execute updateInventory
                            self.t_conn.updateInventory(s_id, self.transactionID, int(p_id), int(inventory - quantity))
                            self.t_conn.insertOrder(s_id, self.transactionID, int(p_id), int(quantity))
            # commit all the changes
            self.t_conn.commit(self.transactionID, True)
            self.t_conn.release_all_lock(self.transactionID)
        except Exception as e:
            # if any Exception occur we want to print why.
            self.t_conn.out_of_order_tran[self.transactionID] = str(e)
        finally:
            # always release lock and rollback all the un-save changes
            if len(self.t_conn.out_of_order_tran[self.transactionID]) > 0 or self.timeout:
                self.t_conn.rollback(self.transactionID)
                self.t_conn.undo(self.transactionID)
            self.t_conn.rollback(self.transactionID)
            self.t_conn.release_all_lock(self.transactionID)

    def get_id(self):
        # returns id of the respective thread
        if hasattr(self, '_thread_id'):
            return self._thread_id
        for id, thread in threading._active.items():
            if thread is self:
                return id

    def raise_exception(self):
        thread_id = self.get_id()
        self.timeout = True
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id,
                                                         ctypes.py_object(SystemExit))
        if res > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
            print('Exception raise failure')


def create_tables():
    cnxn = pyodbc.connect(DRIVER='{SQL Server};',
                          SERVER='technionddscourse.database.windows.net;',
                          DATABASE="tmyr",
                          UID="tmyr",
                          PWD="Qwerty12!",
                          autocommit=False)
    cursor = cnxn.cursor()
    sql_query = textwrap.dedent("""
    CREATE TABLE ProductsInventory (
                                   productID int,
                                   inventory int
                                       CHECK (inventory>=0)
                                       PRIMARY KEY (productID)
                                    );
    CREATE TABLE ProductsOrdered (
                                 transactionID varchar(30),
                                 productID int,
                                     FOREIGN KEY (productID) REFERENCES ProductsInventory(productID),
                                 amount int,
                                       CHECK (amount>=1),
                                       PRIMARY KEY (transactionID,productID)
                                );
    CREATE TABLE Log (
                     rowID int IDENTITY(1, 1) PRIMARY KEY ,
                     timestamp datetime,
                     relation varchar(20) CHECK (relation = 'ProductsInventory' or
                                                 relation = 'ProductsOrdered' or
                                                 relation = 'Locks'),
                     transactionID varchar(30),
                     productID int,
                     FOREIGN KEY (productID) REFERENCES ProductsInventory(productID),
                     action varchar(7) CHECK (action = 'read' or
                                           action = 'update' or
                                           action = 'delete' or
                                           action = 'insert'),
                     record varchar(2500)
                    );

    CREATE TABLE Locks (
                                 transactionID varchar(30),
                                 productID int,
                                 FOREIGN KEY (productID) REFERENCES ProductsInventory(productID),
                                 lockType varchar (7) CHECK (lockType = 'read' or
                                                         lockType = 'write'),

                                 PRIMARY KEY (transactionID,productID, lockType)
                        );
    """)
    cursor.execute(sql_query)
    cnxn.commit()


def find_csv_filenames(path_to_dir, suffix=".csv"):
    filenames = listdir(path_to_dir)
    return [filename for filename in filenames if filename.endswith(suffix)]


def manage_transactions(t):
    conn = Connector()
    path = "orders"
    order_list = find_csv_filenames(path)

    order_list.sort()

    check_inventory = OrderedDict()
    # turn the whole folder into an ordered dict
    for order in order_list:
        transactionID = str(order).replace('.csv', '') + '_' + str(X)
        file_name = 'orders/' + order
        check_inventory[transactionID] = np.genfromtxt(file_name, delimiter=',', skip_header=1, dtype=np.int64)

    for transactionID, order in check_inventory.items():
        # connect all the needed connection for the transaction
        if isinstance(order.T[0], np.int64):
            conn.connect_to(transactionID, order.T[0])
        else:
            for s_id in order.T[0]:
                conn.connect_to(transactionID, s_id)
        # prepare the thread
        p = Thread_with_exception(transactionID, order, conn)
        p.start()
        # wait t second for the thread
        p.join(t)
        # if the thread is alive we need to raise timeout
        if p.is_alive():
            print(transactionID, "timeout")
            # stop p
            p.raise_exception()
            # wait till p is finished
            p.join()
        # if p is done but didnt succeed
        elif 0 < len(conn.out_of_order_tran[transactionID]):
            print(transactionID, conn.out_of_order_tran[transactionID])
        else:
            print(transactionID, "succeed")
    # close all connection
    conn.close_connection(transactionID)


def update_inventory(transcationID):
    conn = Connector()
    f = '%Y-%m-%d %H:%M:%S'
    conn.connect_to(transcationID, X)
    # check if the there a write lock on product 1.
    sql = 'select * from Locks where productID = 1 and locktype = "write"'
    rows = conn.conn[transcationID][X][1].execute(
        "select * from Locks where productID = 1 and locktype = 'write'"). \
        fetchall()
    # If there isn't a lock on product number 1, the table might not been initialized
    if len(rows) == 0:
        try:
            # Try to write this read attempt in log
            conn.conn[transcationID][X][1].execute(
                f"insert into Log values ('{datetime.datetime.now().strftime(f)}', 'Locks', '{str(transcationID)}'"
                f", 1, 'read', '{sql}')")
        except pyodbc.DatabaseError as err:
            #  If the exception arise because FOREIGN KEY the product wasn't inserted to the table yet
            if "FOREIGN KEY" not in str(err):
                print("ERROR occur while trying to initiate the data table")
                return
            #  else, we need to initiate the productInventory table
            else:
                # insert product
                conn.insertInventory(X, transcationID, 1, 'ProductsInventory', 52)
                for i in range(2, 13):
                    conn.insertInventory(X, transcationID, i, 'ProductsInventory', 48)
                conn.commit(transcationID)
                # After all the product are initialize we can return
                return

    # The table are already initialized
    conn.conn[transcationID][X][0].commit()

    # Looping until locks are free from the other transaction
    num_of_locks = 0
    while num_of_locks < 12:
        for i in range(1, 13):
            if conn.try_acquire_lock(X, transcationID, i, 'write'):
                num_of_locks = num_of_locks + 1
    # After all locks are obtain we can update
    conn.updateInventory(X, transcationID, 1, 52)
    for i in range(2, 13):
        conn.updateInventory(X, transcationID, i, 48)
    conn.commit(transcationID)
    conn.release_all_lock(transcationID)
    conn.close_connection(transcationID)


if __name__ == '__main__':
    update_inventory(3)
    manage_transactions(100)
