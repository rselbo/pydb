from collections.abc import Callable
from typing import Any
import logging


class DbProperty:
    """ DBProperty class that also allows a columntype property"""

    def __init__(self, 
                 fget: Callable[[Any], Any] | None = ...,
                 fset: Callable[[Any, Any], None] | None = ...,
                 fdel: Callable[[Any], None] | None = ...,
                 fcolumntype: Callable[[Any], Any] | None = ...,
                 doc=None):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        self.fcolumntype = fcolumntype
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        if self.fget is None:
            raise AttributeError("unreadable attribute")
        return self.fget(obj)

    def __set__(self, obj, value):
        if self.fset is None:
            raise AttributeError("can't set attribute")
        self.fset(obj, value)

    def __delete__(self, obj):
        if self.fdel is None:
            raise AttributeError("can't delete attribute")
        self.fdel(obj)

    def __get_fcolumntype__(self, obj):
        if self.fcolumntype is None:
            raise AttributeError("no db type defined")
        self.fcolumntype(obj)

    def getter(self, fget):
        return type(self)(fget, self.fset, self.fdel, self.fcolumntype, self.__doc__)

    def setter(self, fset):
        return type(self)(self.fget, fset, self.fdel, self.fcolumntype, self.__doc__)

    def deleter(self, fdel):
        return type(self)(self.fget, self.fset, fdel, self.fcolumntype, self.__doc__)
    
    def columntype(self, fcolumntype):
        return type(self)(self.fget, self.fset, self.fdel, fcolumntype, self.__doc__)

class DBObject:
    """Doc"""
    def __init__(self, tableName):
        self._tableName = tableName
        self._columns = dict()

        #Introspection to find properties
        for p in dir(self.__class__):
            v = getattr(self.__class__, p)
            if isinstance(v, DbProperty):
                self._column(p, v)

    def _column(self, name, prop):
        self._columns[name] = prop

    def create(self):
        test = list()
        for k,p in self._columns.items():
            test.append(f'{k} {p.__get_fcolumntype__(self)}')
        columns = ', '.join(test)
        sql = f'create table if not exists {self._tableName} ({columns})'
        return sql

    def select(self):
        columns = ', '.join(self._columns.keys())
        sql = f'select {columns} from {self._tableName};'
        return sql

    def insert(self):
        columns = ', '.join(self._columns.keys())
        actual_values = tuple([v.fget(self) for v in self._columns.values()])
        values = ', '.join(['?' for _ in actual_values])
        sql = f'insert into {self._tableName} ({columns}) values ({values});'
        return sql, actual_values

class Database():
    def __init__(self, connection):
        self._connection = connection

    def close(self):
        self._connection.close()

    def execute(self, sql:str):
        return self._connection.execute(sql)

    def create(self, obj:DBObject):
        self._connection.execute(obj.create())

    def select(self, type:DBObject):
        cur = self._connection.execute(type.select())

        result = list()
        for row in cur:
            obj = type.__class__()
            for i,col in enumerate(type._columns):
                setattr(obj, col, row[i])
            result.append(obj)
        return result

    def insert(self, obj:DBObject):
        sql, values = obj.insert()
        self._connection.execute(sql, values)
