import config
import sqlite3 as sql
import os

class RunsDB:
    '''Handles runs database access
    '''
    @classmethod
    def Initialize(cls):
        is not os.path.exists(config.runs_db_address):
            raise RuntimeError('Runs DB not found?')
        cls.db = sql.connect(config.runs_db_address)

    @classmethod
    def Select(cls, selection, cuts):
        cursor = cls.db.execute('SELECT {selection} FROM runs WHERE {cuts};'.format(selection=selection, cuts=cuts))
        return cursor

    @classmethod
    def Update(cls, update, cuts):
        cls.db.execute('UPDATE runs SET {update} WHERE {cuts};'.format(update=update, cuts=cuts))
        cls.db.commit()

    @classmethod
    def Shutdown(cls):
        cls.db.close()
