import pytest
import logging 
import pydb
from sqlite3 import connect, Connection
from collections.abc import Callable
from typing import Any
from os import remove

logging.basicConfig(level=logging.DEBUG)


class Entity(pydb.DBObject):
    @pydb.DbProperty
    def id(self):
        return self._id

    @id.columntype
    def id(self):
        return 'int primary key'

    @id.setter
    def id(self, v):
        self._id = v

    @pydb.DbProperty
    def name(self):
        return self._name

    @name.columntype
    def name(self):
        return 'text'

    @name.setter
    def name(self, v):
        self._name = v

    @pydb.DbProperty
    def val(self):
        return self._val

    @val.columntype
    def val(self):
        return 'int'

    @val.setter
    def val(self, v):
        self._val = v
    
    def __init__(self):
        pydb.DBObject.__init__(self, 'test_table')

@pytest.fixture
def tmpDb():
    conn = pydb.Database(connect(':memory:'))
    yield conn
    conn.close()

@pytest.fixture
def db_with_table(tmpDb):
    createObj = Entity()
    tmpDb.create(createObj)
    yield tmpDb

@pytest.fixture
def db_with_data(db_with_table):
    insertObj = Entity()
    insertObj.id = 1
    insertObj.name = 'test'
    insertObj.val = 42
    db_with_table.insert(insertObj)
    yield db_with_table

def test_pydb_create(caplog, tmpDb):
    # Arrange
    caplog.set_level(logging.DEBUG)
    createObj = Entity()

    # Act
    tmpDb.create(createObj)

    # Assert
    assert tmpDb.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='test_table';").fetchone()[0] == 'test_table'

def test_pydb_insert(caplog, db_with_table):
    # Arrange
    caplog.set_level(logging.DEBUG)
    insertObj = Entity()
    insertObj.id = 1
    insertObj.name = 'test'
    insertObj.val = 42

    # Act
    db_with_table.insert(insertObj)

    # Assert
    row = db_with_table.execute(f"SELECT * FROM test_table;").fetchone()
    assert row[0] == 1
    assert row[1] == 'test'
    assert row[2] == 42

def test_pydb_select(caplog, db_with_data):
    # Arrange
    caplog.set_level(logging.DEBUG)

    # Act
    res = db_with_data.select(Entity())

    # Assert
    assert res is not None
    assert isinstance(res, list)
    assert len(res) == 1
    resultObj = res[0]
    assert isinstance(resultObj, Entity)
    assert resultObj.id == 1
    assert resultObj.name == 'test'
    assert resultObj.val == 42
