# test_shelect.py

import pytest
from sqlglot import parse_one
from shelect import extract_file_tables, rewrite_table_paths

def get_file_table_map(sql):
    ast = parse_one(sql, dialect="sqlite")
    return extract_file_tables(ast)

def get_rewritten_sql(sql):
    ast = parse_one(sql, dialect="sqlite")
    rewrite_table_paths(ast)
    return ast.sql(dialect="sqlite")


def test_simple_select():
    sql = 'SELECT * FROM "./data.csv" AS d'
    assert get_file_table_map(sql) == {"d": "./data.csv"}
    assert get_rewritten_sql(sql) == "SELECT * FROM d"

def test_join_select():
    sql = '''
        SELECT a.id, b.name
        FROM "./users.csv" AS a
        JOIN "./names.json" AS b ON a.id = b.user_id
    '''
    assert get_file_table_map(sql) == {"a": "./users.csv", "b": "./names.json"}
    assert "FROM a JOIN b ON" in get_rewritten_sql(sql)

def test_subquery_select():
    sql = '''
        SELECT *
        FROM (
            SELECT id FROM "./inner.csv" AS x WHERE x.enabled = 1
        ) AS sub
        JOIN "./outer.json" AS y ON sub.id = y.id
    '''
    assert get_file_table_map(sql) == {"x": "./inner.csv", "y": "./outer.json"}
    rewritten = get_rewritten_sql(sql)
    assert "FROM (SELECT id FROM x" in rewritten
    assert "JOIN y ON" in rewritten

def test_cte_select():
    sql = '''
        WITH temp AS (
            SELECT * FROM "./base.csv" AS base
        )
        SELECT * FROM temp JOIN "./other.json" AS o ON temp.id = o.id
    '''
    assert get_file_table_map(sql) == {"base": "./base.csv", "o": "./other.json"}
    rewritten = get_rewritten_sql(sql)
    assert "FROM base" in rewritten
    assert "JOIN o ON" in rewritten
