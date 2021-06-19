import pyodbc
import textwrap
import datetime
import threading
from os import listdir
import numpy as np
from collections import OrderedDict
import ctypes

# Local site ID
X = 22


class Connector:
    """
    This class responsible for connection between the database and the server.
    """

    def __init__(self):
        # map between db name and categories
        cnxn_db = pyodbc.connect(DRIVER='{SQL Server};',
                                 SERVER='technionddscourse.database.windows.net;',
                                 DATABASE="dbteam",
                                 UID="dbteam",
                                 PWD="Qwerty12!",
                                 autocommit=False)
        cursor_db = cnxn_db.cursor()
        cursor_db.execute(f"SELECT * from CategoriesToSites")
        categories_list = cursor_db.fetchall()
        self.connect = {}
        self.categories_map = {}
        for item in categories_list:
            self.categories_map[item[0]] = item[1]

    def connect_to(self, s_id):
        """
        :param s_id:
        connect to database for each transaction and s_id
        """
        # Dont open a new connection if not needed
        if s_id in self.connect.keys():
            return self.connect[s_id]
        name = self.categories_map[s_id]
        conn = pyodbc.connect(DRIVER='{SQL Server};',
                              SERVER='technionddscourse.database.windows.net;',
                              DATABASE=name,
                              UID=name,
                              PWD="Qwerty12!",
                              autocommit=False,
                              )
        cursor = conn.cursor()
        self.connect[s_id] = (conn, cursor)
        return self.connect[s_id]

    def close_connection(self):
        """
        :return:
        :close all connection for specific transactionID
        """
        for s_id in self.connect.keys():
            self.connect[s_id][0].close()


class Thread_with_exception(threading.Thread):
    """
    This class is use for executing update transaction. This class inherits form thread because we want to raise a an
    exception after timeout.
    Each transaction is considered as thread.
    """

    def __init__(self, transactionID, order, conn):
        """
        :param transactionID:
        :param order: numpy array that hold the order request
        :param conn: connector object to handle requests
        """
        threading.Thread.__init__(self)

        self.transactionID = transactionID
        # np table of order
        self.order = order
        # connection class
        self.conn = conn
        # True if time out
        self.timeout = False
        # hold exception as we want to know why the the transaction wasn't complete
        self.my_error = None
        # queries to execute history to commit for each table
        self.to_commit = {}
        # history of inventory in case of abort
        self.history = {}
        # which look has been taken
        self.lock_taken = {}
        # Time format
        self.f = '%Y-%m-%d %H:%M:%S'
        # initialize
        if order is not None:
            for s_id, p_id, _ in self.order:
                if s_id not in self.to_commit.keys():
                    self.to_commit[s_id] = {'uInventory': [], 'iInventory': [], 'order': [], 'orderLog': []}
                    self.history[s_id] = []
                    self.lock_taken[s_id] = {p_id: []}
                self.lock_taken[s_id].update({p_id: []})
        else:
            self.to_commit[X] = {'uInventory': [], 'iInventory': [], 'order': [], 'orderLog': []}
            self.lock_taken[X] = {i: [] for i in range(13)}
            self.history[X] = []

    def run(self):
        # target function of the thread class
        try:
            num_of_locks = 0
            # first we acquire read locks check if inventory is enough, if so we acquire write lock
            while num_of_locks < len(self.order):
                for s_id, p_id, quantity in self.order:
                    # check if theres any write lock
                    if self.try_acquire_lock(s_id, p_id, 'read'):
                        # set num_of_locks
                        num_of_locks = num_of_locks + 1
                        # read inventory
                        inventory = self.read_inventory(s_id, p_id)
                        # if out of order
                        if inventory < quantity:
                            # notify and go back
                            self.my_error = f"out of order ! siteID:{s_id}, pID:{p_id}, wanted amount:{quantity}," \
                                            f" inventory:{inventory}"
                            return
                        else:
                            # wait until we update the lock
                            while not (self.try_update_lock(s_id, p_id)):
                                pass
                            # execute updateInventory
                            self.update_inventory1(s_id, int(p_id), int(inventory - quantity))
                            self.insert_order(s_id, int(p_id), int(quantity))
            # commit all the changes
            self.commit()
            self.release_all_lock()
        except Exception as e:
            # if any Exception occur we want to print why.
            self.my_error = str(e)
        finally:
            # always release lock and rollback all the un-save changes
            if self.my_error is not None or self.timeout:
                self.rollback()
                self.undo()
            self.rollback()
            self.release_all_lock()

    def try_update_lock(self, s_id, p_id):
        """
        :param s_id:
        :param p_id:
        :return: True if it mange to update the lock from read to write, False if it didnt
        :notice that when lock transaction is execute they always committed in the same function as normal transaction
        are not
        """
        # check if lock is free
        sql = f"select * from Locks where productID = {p_id} and transactionID != '{str(self.transactionID)}' "
        self.conn[s_id][1].execute(sql)
        rows = self.conn[s_id][1].fetchall()
        # notify the log
        self.conn[s_id][1].execute(
            f"insert into Log values ('{datetime.datetime.now().strftime(self.f)}', 'Locks', "
            f"'{str(self.transactionID)}', {p_id}, 'read', 'select * from Locks where productID = {p_id} "
            f"and transactionID "
            f"!= ''{str(self.transactionID)}'' ')")
        self.conn[s_id][0].commit()
        # if lock is free
        if len(rows) == 0:
            # delete the the current log
            self.conn[s_id][1].execute(f"delete from Locks where productId = {p_id}")
            self.conn[s_id][0].commit()
            self.lock_taken[s_id][p_id] = []
            # acquire the lock
            self.lock_taken[s_id][p_id] = ['write']
            self.conn[s_id][1].execute(
                f"insert into Locks values ('{str(self.transactionID)}', {p_id}, 'write')")
            self.conn[s_id][0].commit()

            # notice the log
            self.conn[s_id][1].execute(
                f"insert into Log values ('{datetime.datetime.now().strftime(self.f)}', 'Locks',"
                f"'{str(self.transactionID)}', {p_id},'delete', "
                f"'delete from Locks where productId = {p_id}')")
            self.conn[s_id][0].commit()
            # notify the log
            self.conn[s_id][1].execute(
                f"insert into Log values ('{datetime.datetime.now().strftime(self.f)}', 'Locks'"
                f", '{str(self.transactionID)}', {p_id},'insert', 'insert into Locks values "
                f"(''{str(self.transactionID)}'', "
                f"{p_id}, ""write""' )")
            self.conn[s_id][0].commit()
            return True
        return False

    def release_all_lock(self):
        """
        :return: delete all appropriate locks
        :notice this is 2PL strict
        """
        # delete all locks
        for s_id in self.lock_taken.keys():
            for p_id, l_type in self.lock_taken[s_id].items():
                # if any lock from this item been taken
                if len(l_type) > 0:
                    count = self.conn[s_id][1].execute(f"delete from Locks where productId = {p_id} and "
                                                       f"transactionID = '{str(self.transactionID)}' "
                                                       f"and lockType = '{l_type[0]}'").rowcount
                    if count > 0:
                        # notify log if needed (incase of timeout we might miss something and the lock already released)
                        self.conn[s_id][1].execute(
                            f"insert into Log values ('{datetime.datetime.now().strftime(self.f)}', 'Locks',"
                            f"'{str(self.transactionID)}', {p_id},'delete', "
                            f"'delete from Locks where productId = {p_id} and transactionID = "
                            f"''{str(self.transactionID)}'' ')")
            self.conn[s_id][0].commit()
        # notify dict
        for s_id in self.lock_taken.keys():
            for p_id, _ in self.lock_taken[s_id].items():
                self.lock_taken[s_id][p_id] = []

    def try_acquire_lock(self, s_id, p_id, l_type):
        """
        :param s_id:
        :param p_id:
        :param l_type:
        :return: True if acquire the lock, False otherwise
        """
        # create the right query
        if l_type == 'read':
            sql = f"select * from Locks where productID = {p_id} and lockType = 'write'"
        else:
            sql = f"select * from Locks where productID = {p_id}"
        # check if it can acquire the lock
        rows = self.conn[s_id][1].execute(sql).fetchall()
        # notice the log
        self.conn[s_id][1].execute(
            f"insert into Log values ('{datetime.datetime.now().strftime(self.f)}', 'Locks', "
            f"'{str(self.transactionID)}', {p_id}, 'read', 'select * from Locks where productID = {p_id} "
            f"and lockType = ""write""')")
        self.conn[s_id][0].commit()
        # if can acquire lock
        if len(rows) == 0:
            # remember that the lock been taken
            self.lock_taken[s_id][p_id].append(l_type)
            # acquire the lock
            self.conn[s_id][1].execute(
                f"insert into Locks values ('{str(self.transactionID)}', {p_id}, '{l_type}')")
            self.conn[s_id][0].commit()
            # notify log
            self.conn[s_id][1].execute(
                f"insert into Log values ('{datetime.datetime.now().strftime(self.f)}', 'Locks'"
                f", '{str(self.transactionID)}', "
                f"{p_id},'insert', 'insert into Locks values (''{str(self.transactionID)}'', "
                f"{p_id}, ''{l_type}'')' )")
            # commit
            self.conn[s_id][0].commit()
            return True
        return False

    def insert_inventory(self, s_id, p_id, values):
        """
        :param s_id:
        :param p_id:
        :param values:
        :return:
        :notice: execute the the query to the right connection, DOSEN'T COMMIT IT
        """
        if self.lock_taken[s_id][p_id][0] != 'write':
            self.my_error = "Tried to write with out lock"
            raise Exception
            # set right query
            self.to_commit[s_id]['iInventory'].append((p_id, values))
            self.to_commit[s_id]['orderLog']. \
                append((f'{datetime.datetime.now().strftime(self.f)}',
                        'ProductsInventory', self.transactionID, p_id, 'insert',
                        f'insert into ProductsInventory values ({p_id}, {values})'))

    def insert_order(self, s_id, p_id, values):
        """
        :param s_id:
        :param p_id:
        :param values:
        :return:
        :notice: execute the the query to the right connection, DOSEN'T COMMIT IT
        """
        if self.lock_taken[s_id][p_id][0] != 'write':
            self.my_error = "Tried to write with out lock"
            raise Exception
        # set right query
        self.to_commit[s_id]['order'].append((f'{str(self.transactionID)}', p_id, values))
        self.to_commit[s_id]['orderLog']. \
            append((f'{datetime.datetime.now().strftime(self.f)}',
                    'ProductsInventory', self.transactionID, p_id, 'insert',
                    f'insert into ProductsOrdered values ({str(self.transactionID)}, {p_id}), {values}'))

    def update_inventory1(self, s_id, p_id, quantity):
        """
        :param s_id:
        :param p_id:
        :param quantity:
        :return:
        notice: execute update query to the right connection, DOSEN'T COMMIT IT
        """
        if self.lock_taken[s_id][p_id][0] != 'write':
            self.my_error = "Tried to write without lock"
            raise Exception
        self.to_commit[s_id]['uInventory'].append((quantity, p_id))
        # notify log
        self.to_commit[s_id]['orderLog']. \
            append((f'{datetime.datetime.now().strftime(self.f)}', 'ProductsInventory', self.transactionID, p_id,
                    'update',
                    f'update ProductsInventory set inventory = {quantity} where productID = {p_id}'))

    def rollback(self):
        """
        :rollback all execute query in specific transactionID
        """
        for s_id in self.conn.keys():
            self.conn[s_id][0].rollback()

    def undo(self):
        """
        delete (and return ProductsInventory to the state before transaction)
         the  all the history of the the transaction from the tables.
        """
        for s_id in self.conn.keys():
            self.conn[s_id][1].execute(f"delete from ProductsOrdered where "
                                       f"transactionID = '{self.transactionID}'")
            self.conn[s_id][1].execute(f"delete from Log where transactionID = '{self.transactionID}' and "
                                       f"relation != 'Locks'")
            if len(self.history[s_id]) != 0:
                self.conn[s_id][1].executemany("update ProductsInventory set inventory = ? where"
                                               " productID = ?",
                                               self.history[s_id])
            self.conn[s_id][0].commit()

    def read_inventory(self, s_id, p_id):
        """
        :param s_id:
        :param p_id:
        :return: inventory as int
        """
        if self.lock_taken[s_id][p_id][0] != 'read':
            self.my_error = "Tried to read with out lock"
            raise Exception
        inventory = self.conn[s_id][1].execute(
            f"select inventory from ProductsInventory where productID = {p_id}").fetchall()
        self.history[s_id].append((int(inventory[0][0]), int(p_id)))
        # write to log
        self.conn[s_id][0].execute(
            f"insert into Log values ('{datetime.datetime.now().strftime(self.f)}','ProductsInventory', "
            f"'{str(self.transactionID)}', {p_id}, 'read', 'select inventory from ProductsInventory where productID= {p_id}')")
        self.conn[s_id][1].commit()
        return inventory[0][0]

    def commit(self):
        """
        :return:
        :commit all execute query in specific transactionID
        """
        for s_id in self.to_commit.keys():
            for type in self.to_commit[s_id].keys():
                if len(self.to_commit[s_id][type]) == 0:
                    continue
                elif type == 'orderLog':
                    self.conn[s_id][1].executemany("insert into Log values (?, ?, ?, ?, ?, ?)",
                                                   self.to_commit[s_id][type])
                elif type == 'iInventory':
                    self.conn[s_id][1].executemany("insert into ProductsInventory values (?, ?)",
                                                   self.to_commit[s_id][type])
                elif type == 'uInventory':
                    self.conn[s_id][1].executemany("update ProductsInventory set inventory = ? where"
                                                   " productID = ?",
                                                   self.to_commit[s_id][type])
                elif type == 'order':
                    self.conn[s_id][1].executemany("insert into ProductsOrdered values (?, ?, ?)",
                                                   self.to_commit[s_id][type])
                self.conn[s_id][0].commit()

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

    def insert_first_time(self):
        my_product = [(1, 52)]
        for i in range(2, 13):
            my_product.append((i, 48))
        self.conn[X][1].executemany("insert into ProductsInventory values (?, ?)", my_product)
        self.conn[X][0].commit()


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
        list_conn = {}
        if isinstance(order.T[0], np.int64):
            list_conn[order.T[0]] = (conn.connect_to(order.T[0]))
            set_order = [[order[0], order[1], order[2]]]
        else:
            for s_id in order.T[0]:
                list_conn[s_id] = (conn.connect_to(s_id))
            set_order = order
        # prepare the thread
        p = Thread_with_exception(transactionID, set_order, list_conn)
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
        elif p.my_error is not None:
            print(transactionID, p.my_error)
        else:
            print(transactionID, "succeed")
    # close all connection
    conn.close_connection()


def update_inventory(transcationID):
    connection = Connector()
    f = '%Y-%m-%d %H:%M:%S'
    conn1 = {X: connection.connect_to(X)}
    conn = Thread_with_exception(transcationID, None, conn1)
    # check if the there a write lock on product 1.
    sql = 'select * from Locks where productID = 1 and locktype = "write"'
    rows = conn1[X][1].execute(
        "select * from Locks where productID = 1 and locktype = 'write'"). \
        fetchall()
    # If there isn't a lock on product number 1, the table might not been initialized
    if len(rows) == 0:
        try:
            # Try to write this read attempt in log
            conn1[X][1].execute(
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
                conn.insert_first_time()
                # After all the product are initialize we can return
                return

    # The table are already initialized
    conn1[X][0].commit()

    # Looping until locks are free from the other transaction
    num_of_locks = 0
    while num_of_locks < 12:
        for i in range(1, 13):
            if conn.try_acquire_lock(X, i, 'write'):
                num_of_locks = num_of_locks + 1
    # After all locks are obtain we can update
    conn.update_inventory1(X, 1, 52)
    for i in range(2, 13):
        conn.update_inventory1(X, i, 48)
    conn.commit()
    conn.release_all_lock()
    del conn, conn1
    connection.close_connection()


if __name__ == '__main__':
    create_tables()
    update_inventory(1)
    update_inventory(2)
    manage_transactions(60)
