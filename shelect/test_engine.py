import pytest
from sqlglot import parse, parse_one
from pathlib import Path
import json

from shelect.engine import Engine 
from shelect.output_fake import OutputFake 
from shelect.filesystem_fake import FilesystemFake 

happypath_csv_input = 'name,value\nfoo,1\nbar,2\nbaz,\n,4\n'
happypath_json_input = json.dumps([
    { 'name': 'foo', 'value': 1 },
    { 'name': 'bar', 'value': 2 },
    { 'name': 'baz', 'value': None },
    { 'name': None, 'value': 4 },
])

def test_happypath_read_csv_write_table():
    fs = FilesystemFake()
    output = OutputFake()
    engine = Engine(fs, output, 'table')
    fs.test_set_file(Path('./data.csv'), happypath_csv_input)
    engine.run_statement(parse_one('SELECT * FROM "./data.csv"'))
    assert output.test_get_output() == '''
name | value
-----+------
foo  | 1    
bar  | 2    
baz  |      
     | 4    
'''.lstrip()

def test_happypath_read_csv_write_csv():
    fs = FilesystemFake()
    output = OutputFake()
    engine = Engine(fs, output, 'csv')
    fs.test_set_file(Path('./data.csv'), happypath_csv_input)
    engine.run_statement(parse_one('SELECT * FROM "./data.csv"'))
    assert output.test_get_output() == '''
name,value\r
foo,1\r
bar,2\r
baz,\r
,4\r
'''.lstrip()

def test_happypath_read_csv_write_json():
    fs = FilesystemFake()
    output = OutputFake()
    engine = Engine(fs, output, 'json')
    fs.test_set_file(Path('./data.csv'), happypath_csv_input)
    engine.run_statement(parse_one('SELECT * FROM "./data.csv"'))
    assert output.test_get_output() == '''
name,value\r
foo,1\r
bar,2\r
baz,\r
,4\r
'''.lstrip()

def test_happypath_read_csv_write_json():
    fs = FilesystemFake()
    output = OutputFake()
    engine = Engine(fs, output, 'json')
    fs.test_set_file(Path('./data.csv'), happypath_csv_input)
    engine.run_statement(parse_one('SELECT * FROM "./data.csv"'))
    parsed = json.loads(output.test_get_output())
    
    assert parsed == [
        { 'name': 'foo', 'value': '1', },
        { 'name': 'bar', 'value': '2', },
        { 'name': 'baz', 'value': '', },
        { 'name': '', 'value': '4', },
    ]

def test_happypath_read_json_write_table():
    fs = FilesystemFake()
    output = OutputFake()
    engine = Engine(fs, output, 'table')
    fs.test_set_file(Path('./data.json'), happypath_json_input)
    engine.run_statement(parse_one('SELECT * FROM "./data.json"'))
    assert output.test_get_output() == '''
name | value
-----+------
foo  | 1    
bar  | 2    
baz  | NULL 
NULL | 4    
'''.lstrip()

def test_happypath_read_json_write_csv():
    fs = FilesystemFake()
    output = OutputFake()
    engine = Engine(fs, output, 'csv')
    fs.test_set_file(Path('./data.json'), happypath_json_input)
    engine.run_statement(parse_one('SELECT * FROM "./data.json"'))
    assert output.test_get_output() == '''
name,value\r
foo,1\r
bar,2\r
baz,\r
,4\r
'''.lstrip()

def test_happypath_read_json_write_json():
    fs = FilesystemFake()
    output = OutputFake()
    engine = Engine(fs, output, 'json')
    fs.test_set_file(Path('./data.json'), happypath_json_input)
    engine.run_statement(parse_one('SELECT * FROM "./data.json"'))
    parsed = json.loads(output.test_get_output())
    
    assert parsed == [
        { 'name': 'foo', 'value': '1', },
        { 'name': 'bar', 'value': '2', },
        { 'name': 'baz', 'value': None, },
        { 'name': None, 'value': '4', },
    ]

def test_happypath_join():
    example_people_csv = 'id,name\n1,Alice\n2,Bob\n3,Carlos\n4,Dani'
    example_values_json = '[{"id":"1","value":"10"},{"id":"2","value":"20"},{"id":"3","value":"30"},{"id":"3","value":"40"}]'

    fs = FilesystemFake()
    output = OutputFake()

    fs.test_set_file(Path('./examples/people.csv'), example_people_csv)
    fs.test_set_file(Path('./examples/values.json'), example_values_json)

    engine = Engine(fs, output, 'table')

    engine.run_statement(parse_one('''
SELECT p.name, SUM(v.value)
FROM "./examples/people.csv" AS p
LEFT JOIN "./examples/values.json" AS V USING (id)
GROUP BY 1 ORDER BY 2 DESC
'''))
    assert '''
name   | SUM(v.value)
-------+-------------
Carlos | 70          
Bob    | 20          
Alice  | 10          
Dani   | NULL        
'''.lstrip() == output.test_get_output()

def test_empty_execution():
    fs = FilesystemFake()
    output = OutputFake()

    engine = Engine(fs, output, 'table')

    # Odd test case, but in the repl if a semicolon after a comment is
    # encountered, a statement is produced that results in a query that has no
    # results at all
    executed = False
    for statement in parse('-- comment\n;'):
        if statement:
            engine.run_statement(statement)
            executed = True

    assert executed
    assert '' == output.test_get_output()
