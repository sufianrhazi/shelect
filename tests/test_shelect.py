import pytest
from sqlglot import parse_one

from shelect.ast_utils import extract_tables 

def get_file_tables(sql):
    ast = parse_one(sql, dialect="sqlite")
    return extract_tables(ast)

def test_simple_select():
    sql = 'SELECT * FROM "./data.csv" AS d'
    assert get_file_tables(sql) == {"./data.csv"}

def test_simple_select_no_alias():
    sql = 'SELECT * FROM "./data.csv"'
    assert get_file_tables(sql) == {"./data.csv"}

def test_join_select():
    sql = '''
        SELECT a.id, b.name
        FROM "./users.csv" AS a
        JOIN "./names.json" AS b ON a.id = b.user_id
    '''
    assert get_file_tables(sql) == {"./users.csv", "./names.json"}

def test_subquery_select():
    sql = '''
        SELECT *
        FROM (
            SELECT id FROM "./inner.csv" AS x WHERE x.enabled = 1
        ) AS sub
        JOIN "./outer.json" AS y ON sub.id = y.id
    '''
    assert get_file_tables(sql) == {"./inner.csv", "./outer.json"}

def test_cte_select():
    sql = '''
        WITH temp AS (
            SELECT * FROM "./base.csv" AS base
        )
        SELECT * FROM temp JOIN "./other.json" AS o ON temp.id = o.id
    '''
    assert get_file_tables(sql) == {"./base.csv", "./other.json"}

def test_syntax_error():
    sql = 'SELECT FROM WHERE'
    with pytest.raises(Exception, match=r'Expected table name'):
        parse_one(sql, dialect="sqlite")

