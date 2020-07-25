import sqlite3
import os
from _thread import start_new_thread
from ..DebugUtilities import debug
import traceback
try:
    from __main__ import main_logger
except Exception as e:
    main_logger = debug.debug(file_out = "main.log")
db_logger = debug.debug(file_out = "db.log")
db_logger.console = False

def SingleInstance(cl, *args, **kw):
    instances = {}
    def _singleton(*args,**kwargs):
       if cl not in instances:
            instances[cl] = cl(*args, **kw)
       if instances[cl].closed == True:
           instances[cl].closed = False
           instances[cl] = cl(*args, **kw)
       return instances[cl]
    return _singleton

@SingleInstance
class Sql_Executor:
    def __init__(self,db_file):
        self.db_file = db_file
        self.Buffer = []
        self.count = 1
        self.lock_state = "read"
        self.output = []
        self.output_token = None
        self.active = False
        self.exec_info = None
        self.closed = False
        start_new_thread(self.start_executing,())
        while not self.active:
             _ = 1
    def __eq__(self,comp):
        return comp.db_file == self.db_file
    def generate_token(self):
        self.count += 1
        return self.count
    def run_query(self,query):
        token = self.generate_token()
        self.Buffer.append([query,token])

        a= 1
        while True:

            if a == 1:

                a = 0
            if self.output_token == token and self.lock_state == "waiting for read":
                break
            if self.output_token == token and self.lock_state == "error":
                raise self.output
        output = self.output
        exec_info = self.exec_info
        self.lock_state = "read"
        return output,exec_info
    def start_executing(self):
        try:
            db_logger.log("(Sql Executor): "+"Starting serial db manager...")

            while not self.closed:

                if (len(self.Buffer) > 0) and (self.lock_state == "read"):
                    try:
                        con = sqlite3.connect(self.db_file)
                        cur = con.cursor()
                        self.lock_state = "processing"
                        self.output_token = self.Buffer[0][1]
                        db_logger.log("(Sql Executor): "+"Executing '" + self.Buffer[0][0] + "'" )
                        self.exec_info = cur.execute(self.Buffer[0][0])
                        del self.Buffer[0]
                        self.output = cur.fetchall()
                        con.commit()
                        con.close()
                        self.lock_state = "waiting for read"
                    except Exception as e:
                        self.output = e
                        self.lock_state = "error"
                        self.closed = True
                        traceback.print_exc()
                        db_logger.log("(Sql Executor): Error Occured -> " + str(e))
                self.active = True
        except Exception as e:
                db_logger.log("(Sql Executor): ",str(e))
                #raise e
                traceback.print_exc()
class indices:
    def __init__(self,*args):
        self.indices = [arg for arg in args]
        if len(self.indices) == 1:
            self.indices = [self.indices[0]] + self.indices
    def __getitem__(self,index):
        return self.indices[index]
    def __setitem__(self,index,value):
        if type(index) != int:
            raise TypeError("index must be of type int")
        if type(value) != int:
            raise TypeError("index values must be of type int")
        self.indices[index] = value
    def __and__(self,other):
        return indices(*list(filter(lambda x: x in self.indices,other.indices)))
    def __or__(self,other):
        return indices( *(self.indices+list(filter(lambda x: x not in self.indices,other.indices))) )
class SchemaMismatchError(Exception):
    def __init__(self,message,logger = None):
        self.message = message
        if not(logger is None):
            logger.log("(SQLite Handler): "+message)
    def __str__(self):
        return self.message
class TableDoesNotExistError(Exception):
    def __init__(self,message,logger = None):
        self.message = message
        if not(logger is None):
            logger.log("(SQLite Handler): "+message)
    def __str__(self):
        return self.message
class ColumnDoesNotExistError(Exception):
    def __init__(self,message,logger = None):
        self.message = message
        if not(logger is None):
            logger.log("(SQLite Handler): "+message)
    def __str__(self):
        return self.message
class UnsupportedDataTypeError(Exception):
    def __init__(self,message,logger = None):
        self.message = message
        if not(logger is None):
            logger.log("(SQLite Handler): "+message)
    def __str__(self):
        return self.message
class QuerySyntaxError(Exception):
    def __init__(self,message,logger = None):
        self.message = message
        if not(logger is None):
            logger.log("(SQLite Handler): "+message)
    def __str__(self):
        return self.message
class InvalidOperandError(Exception):
    def __init__(self,message,logger = None):
        self.message = message
        if not(logger is None):
            logger.log("(SQLite Handler): "+message)
    def __str__(self):
        return self.message
class MismatchInNumberOfValuesToUnpack(Exception):
    def __init__(self,message,logger = None):
        self.message = message
        if not(logger is None):
            logger.log("(SQLite Handler): "+message)
    def __str__(self):
        return self.message

class column:
    def __init__(self,name,data_type=str):
        self.data_type = data_type
        self.name = name
        self.rows = []
        if data_type in [int,float,str]:
            self.type = {int:'int',float:'real',str:'text'}[data_type]
        else:
            #self.type = None#INCOMPLETE
            raise UnsupportedDataTypeError('Currently only int, float and string data-tyoes are supported.')
    def cast(self):
        return self.data_type(self.data)
    def __eq__(self,other):
        query = other if type(other)!= str else f"'{other}'"
        if type(other) == str:
            raw , _ = self.sql_processor.run_query(f'SELECT id from {self.table.name} where {self.name} like {query}')
        else:
            raw , _ = self.sql_processor.run_query(f'SELECT id from {self.table.name} where {self.name} = {query}')
        return indices(*[id[0] for id in raw ])
    def __gt__(self,other):
        query = other if type(other)!= str else f"'{other}'"
        raw , _ = self.sql_processor.run_query(f'SELECT id from {self.table.name} where {self.name} > {query}')
        return indices(*[id[0] for id in raw ])
    def __lt__(self,other):
        query = other if type(other)!= str else f"'{other}'"
        raw , _ = self.sql_processor.run_query(f'SELECT id from {self.table.name} where {self.name} < {query}')
        return indices(*[id[0] for id in raw ])
    def __ne__(self,other):
        query = other if type(other)!= str else f"'{other}'"
        raw , _ = self.sql_processor.run_query(f'SELECT id from {self.table.name} where {self.name} != {query}')
        return indices(*[id[0] for id in raw ])
    def __ge__(self,other):
        query = other if type(other)!= str else f"'{other}'"
        raw , _ = self.sql_processor.run_query(f'SELECT id from {self.table.name} where {self.name} >= {query}')
        return indices(*[id[0] for id in raw ])
    def __le__(self,other):
        query = other if type(other)!= str else f"'{other}'"
        raw , _ = self.sql_processor.run_query(f'SELECT id from {self.table.name} where {self.name} = {query}')
        return indices(*[id[0] for id in raw ])
    def __iadd__(self,other):
        operator = '||' if type(other) == str else '+'
        operand = f"'{other}'"  if type(other) == str else f'{other}'
        self.sql_processor.run_query(f"UPDATE {self.table.name} SET {self.name} = {self.name} {operator} {operand}")
    def __isub__(self,other):
        operator = '-'
        if type(other) == str:
            raise InvalidOperandError('Operation {operator} can not be used with string.')
        self.sql_processor.run_query(f"UPDATE {self.table.name} SET {self.name} = {self.name} {operator} {other}")
    def __imul__(self,other):
        operator = '*'
        if type(other) == str:
            raise InvalidOperandError('Operation {operator} can not be used with string.')
        self.sql_processor.run_query(f"UPDATE {self.table.name} SET {self.name} = {self.name} {operator} {other}")
    def __idiv__(self,other):
        operator = '/'
        if type(other) == str:
            raise InvalidOperandError('Operation {operator} can not be used with string.')
        self.sql_processor.run_query(f"UPDATE {self.table.name} SET {self.name} = {self.name} {operator} {other}")

        #COMPLETE THIS FOR ALL OTHER OPERANDS
class table:
    def __init__(self,name,columns=[]):
        self.name = name
        self.schema = columns
        #self.rows = []
    def db_pull_data(self):
        raw = self.sql_processor.run_query("SELECT * FROM {} ".format(self.name))
    def __getitem__(self,index):
        #[self.sql_processor.run_query("SELECT * FROM {} ".format(self.name))]
        if type(index) == str:
            if index in [col.name for col in self.schema]:
                return {col.name: col for col in self.schema}[index]
            else:
                raise IndexError("Column does not exist in table")
        elif type(index) == indices:
            query = f"select * from {self.name} where id in {str(tuple(index.indices))};"
            raw,_ = self.sql_processor.run_query(query)
            return [ {self.schema[col].name: self.schema[col].data_type(row[col+1]) for col in range(len(self.schema))} for row in raw]#Returns array of dicts
        elif type(index) == tuple:
            conditional_query = index[0]

            if type(conditional_query) == indices:
                Query_Builder = ""
                for col in index[1:]:
                    if type(col) != str:
                        raise UnsupportedDataTypeError(f'Expected column as str but found {type(col)}.')
                    else:
                        if col not in [Col.name for Col in self.schema]:
                            raise ColumnDoesNotExistError(f'Column {col} does not exist in table {self.name}.')
                        else:
                            Query_Builder += f',{col}'
                Query_Builder = Query_Builder[1:]

                query = f"select {Query_Builder} from {self.name} where id in {str(tuple(conditional_query.indices))};"
                #print(query)
                raw,_ = self.sql_processor.run_query(query)
                #print(raw,end='\r\n\r\n')
                return [ {index[1:][new_col]: self.schema[self.schema.index(index[1:][new_col])].data_type(row[new_col]) for new_col in range(len(index[1:]))} for row in raw]#Returns array of dicts
            else:
                #FELT CUTE MIGHT DELETE THIS LATER
                raise QuerySyntaxError('Index passed as tuple must be like (indices, *str) ')

        else:
            raise UnsupportedDataTypeError('Only index of type str and type indices are accepted.')
    def __setitem__(self,index, value):
        if type(index) == str and type(value) == self.schema[self.schema.index(index)].data_type():
            self.sql_processor.run_query(f'UPDATE {self.name} SET {index} = {value}' if type(value) != str else f"UPDATE {self.name} SET {index} = '{value}'")
        if type(index) == indices:
            if type(value) == list and len(indices.indices) == len(value):# Assuming all the assignments are list(dict()) type
                for row in value:
                    for key in value.keys():
                        if not(key.lower() in [col.name.lower() for col in self.schema]):
                            raise ColumnDoesNotExistError(f'Column {col} does not exist in table {self.name}.')
                    val_list = [f"""{key} = {value[key] if type(value[key]) != str else "'"+value[key]+"'"  }""" for key in value.keys()]
                    val_query = ""
                    for val in val_list:
                        val_query += f', {val}'
                    val_query = val_query[1:]

                    self.sql_processor.run_query(f'UPDATE {self.name} SET {val_query} where id == {index[0].indices[value.index(row)]}')
            elif type(value) == dict:
                for key in value.keys():
                    if not(key.lower() in [col.name.lower() for col in self.schema]):
                        raise ColumnDoesNotExistError(f'Column {col} does not exist in table {self.name}.')
                val_list = [f"""{key} = {value[key] if type(value[key]) != str else "'"+value[key]+"'"  }""" for key in value.keys()]
                val_query = ""
                for val in val_list:
                    val_query += f', {val}'
                val_query = val_query[1:]
                self.sql_processor.run_query(f'UPDATE {self.name} SET {val_query} where id in {str(tuple(index.indices))};')
            else:
                raise MismatchInNumberOfValuesToUnpack('Number of rows provided do not match with the number of rows selected in database')
        if type(index) == tuple:
            conditional_query = index[0]
            if type(conditional_query) == indices:
                if len(index[1:]) >= 1 and type(value) == dict and len(value.keys()) == len(index[1:]):
                    for key in value.keys():
                        if not(key.lower() in [col.name.lower() for col in self.schema]):
                            raise ColumnDoesNotExistError(f'Column {col} does not exist in table {self.name}.')
                    val_list = [f"""{key} = {value[key] if type(value[key]) != str else "'"+value[key]+"'"  }""" for key in value.keys()]
                    val_query = ""
                    for val in val_list:
                        val_query += f', {val}'
                    val_query = val_query[1:]
                    self.sql_processor.run_query(f'UPDATE {self.name} SET {val_query} where id in {str(tuple(index[0].indices))};')
                elif len(index[1:]) >= 1 and type(value) == list and len(value.keys()) == len(index[0].indices):
                    for row in value:
                        if len(row.keys()) == len(index[1:]) and [key.lower()for key in row.keys()] == [col.lower() for col in index[1:]]:
                            val_list =[f"""{key} = {value[key] if type(value[key]) != str else "'"+value[key]+"'"  }""" for key in value.keys()]
                            val_query = ""
                            for val in val_list:
                                val_query += f', {val}'
                            val_query = val_query[1:]
                            self.sql_processor.run_query(f'UPDATE {self.name} SET {val_query} where id == {index[0].indices[value.index(row)]}')
                        else:
                            raise MismatchInNumberOfValuesToUnpack("Columns being tried to access do not match with provided data.")
                else:
                    raise MismatchInNumberOfValuesToUnpack('Number of rows provided do not match with the number of rows selected in database')
            else:
                #FELT CUTE MIGHT DELETE THIS LATER
                raise QuerySyntaxError('Index passed as tuple must be like (indices, *str) ')

    #DEFINE THE DELETE ITEM FUNCTION
    def append(self,data):
        blank_data  = {col.name:None for col in self.schema}
        if type(data) == list:
            new_data = blank_data.copy()
            for row in data:
                for col in row.keys():
                    if col.lower() in [c.lower() for c in new_data.keys()]:
                        new_data[col] = row[col]
                    else:
                        raise SchemaMismatchError(f"Column {col} does not exist in table {self.name}")
                col_str = ""
                data_str = ""
                for cell in new_data.keys():
                    if new_data[cell] != None:
                        col_str += f', {cell}'
                        d_type = self.schema[[c.name for c in  self.schema].index(cell)].data_type
                        data_str += f", '{new_data[cell]}'" if d_type == str else f", {new_data[cell]}"
                if len(col_str)>1 and len(data_str)>1:
                    col_str = col_str[1:]
                    data_str = data_str[1:]
                    self.sql_processor.run_query(f"INSERT INTO {self.name} ({col_str}) VALUES ({data_str})")
                else:
                    raise MismatchInNumberOfValuesToUnpack("No data for insertion found")
        elif type(data) == dict:
            new_data = blank_data.copy()
            list_data = [data]
            for row in list_data:
                for col in row.keys():
                    if col.lower() in [c.lower() for c in new_data.keys()]:
                        new_data[col] = row[col]
                    else:
                        raise SchemaMismatchError(f"Column {col} does not exist in table {self.name}")
                col_str = ""
                data_str = ""
                for cell in new_data.keys():
                    if new_data[cell] != None:
                        col_str += f', {cell}'
                        d_type = self.schema[[c.name for c in  self.schema].index(cell)].data_type
                        data_str += f", '{new_data[cell]}'" if d_type == str else f", {new_data[cell]}"
                if len(col_str)>1 and len(data_str)>1:
                    col_str = col_str[1:]
                    data_str = data_str[1:]
                    self.sql_processor.run_query(f"INSERT INTO {self.name} ({col_str}) VALUES ({data_str})")
                else:
                    raise MismatchInNumberOfValuesToUnpack("No data for insertion found")
        else:
            raise UnsupportedDataTypeError("Values to insert must be a list or a dict")

    def __delitem__(self,index):
        if type(index) == str:
            if index.lower() in [c.name.lower() for c in self.schema]:
                self.sql_processor.run_query(f"ALTER table {self.name} DROP COLUMN {index}")
                del self.schema[[c.name.lower() for c in  self.schema].index(index.lower())]
        if type(index) == indices:
            self.sql_processor.run_query(f"DELETE from {self.name} where id in {str(tuple(index.indices))}")#Need SQL QUERY HERE
        if type(index) == tuple:
            if type(index[0]) == indices:
                dummy = ""
                for col in index[1:]:
                    dummy += f", {col} = null"
                dummy = dummy[1:]
                self.sql_processor.run_query(f"UPDATE {self.name} SET {dummy} where id in {str(tuple(index[0].indices))}")#Need SQL QUERY HERE
            else:
                for col in index:
                    if col.lower() in [c.name.lower() for c in self.schema]:
                        self.sql_processor.run_query("ALTER table {self.name} DROP COLUMN {col}")
                        del self.schema[[c.name.lower() for c in self.schema].index(col.lower())]


class database:
    def __init__(self,db_file = "main.db",tables=[],ignore_redundant = True,create_if_missing = True,clean_redundant = True):
        self.db_file = db_file
        self.sql_processor = Sql_Executor(self.db_file)

        self.tables = tables
        self.create_if_missing = create_if_missing
        self.ignore_redundant = ignore_redundant
        # self.start()
        self.load()
    def load(self):
        tables = self.getTables()
        for table in self.tables:
            table.sql_processor = self.sql_processor
            if not(table.name.lower() in [t.lower() for t in tables ]):
                    generated_query_list = str(["id INTEGER PRIMARY KEY AUTOINCREMENT"] + [column.name+f" {column.type} " for column in table.schema]).replace("'","")[1:-1]
                    generated_query_list = f'create table {table.name}({generated_query_list})'
                    self.sql_processor.run_query(generated_query_list)
            redundant_columns = [col.lower() for col in self.getColumns(table.name)]
            for column in table.schema:
                column.sql_processor = self.sql_processor
                column.table = table
                if not(column.name.lower() in [col.lower() for col in self.getColumns(table.name)]):
                    if self.create_if_missing:
                        self.sql_processor.run_query(f'alter table {table.name} add column {column.lower()} text;')
                    else:
                        raise SchemaMismatchError("Schema defined in class contains a column which does not exist in data base. To forcefully mirror changes in database, set create_if_missing to True.")
                del redundant_columns[redundant_columns.index(column.name.lower())]
            for column in redundant_columns:
                if not(self.ignore_redundant):
                    if self.clean_redundant:
                        self.sql_processor.run_query(f'alter table {table.name} drop column {column.lower()};')
                    else:
                        raise SchemaMismatchError("Additional columns were found in database file which is not defined in class. To ignore this set ignore_redundant to True. Like-wise to forcefully mirror changes in database, set clean_redundant to True.")
    def getTables(self):
        raw,_ = self.sql_processor.run_query("SELECT name FROM sqlite_master WHERE type='table'")
        return [i[0] for i in raw]
    def getColumns(self,table_name):
        _,info = self.sql_processor.run_query("SELECT * FROM {}".format(table_name))
        return [col[0] for col in info.description]
    def __getitem__(self,index):
        try:
            return {table.name:table for table in self.tables}[index]
        except KeyError:
            raise TableDoesNotExistError(f'Table {index} does not exist in the database.')
    def __delitem__(self,index):
        if type(index) == str:
            if index.lower() in [table.name.lower() for table in self.tables]:
                self.sql_processor.run_query(f"DROP TABLE {index.lower()}")
            else:
                raise TableDoesNotExistError(f"Table {index} does not exist in the database")
        elif type(index) == tuple:
            for item in index:
                self.__delitem__(item)
        else:
            raise QuerySyntaxError("Expected a str or tuple of str.")
    def close(self):
        self.sql_processor.closed = True

    def start(self):
        self.sql_processor.closed = False
        #self.Buffer = []
        self.sql_processor =self.sql_processor.__init__(self.db_file)

        
# def test():
#     main = database("main.db",tables=[table('crawled',columns=[column("url",str),column("localpath",str),column("status",str),column("resourceType",str),column("origin",str)])])
#     #print(main['crawled']['localpath']=='%%')
#     #print((main['crawled']['localpath']=='%').indices)
#     #print(main['crawled'][main['crawled']['localpath']=='%'])
#     print(main['crawled'][main['crawled']['localpath']=="%help%",'localpath','status'])
#     x = main['crawled'][main['crawled']['localpath']=="%help%",'localpath','status']
#     x = x[0]
#     main['crawled'][main['crawled']['localpath']=="%help%",'localpath','status'] = {'localpath':"Not Found", 'status':'Error 404'}
#     print(main['crawled'][main['crawled']['localpath']=="%help%",'localpath','status'])
#     main['crawled'][main['crawled']['localpath']=="%help%",'localpath','status'] = x
#     print(main['crawled'][main['crawled']['localpath']=="%help%",'localpath','status'])
# if __name__ == "__main__":
#     test()
