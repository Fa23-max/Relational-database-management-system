"""
SQL Parser for Mini RDBMS using Lark
"""

from typing import List, Dict, Any, Optional, Union
from lark import Lark, Transformer, Token as LarkToken
from dataclasses import dataclass
from enum import Enum

# SQL Grammar for Mini RDBMS
SQL_GRAMMAR = """
    ?start: query
    
    ?query: select_query
         | insert_query
         | update_query
         | delete_query
         | create_table_query
         | create_index_query
    
    // SELECT queries
    select_query: "SELECT" select_columns "FROM" table_name [where_clause] [join_clause]
    
    select_columns: "*" -> select_all
                  | column_list
    
    column_list: column_name ("," column_name)*
    
    where_clause: "WHERE" condition
    
    condition: expr (("AND" | "OR") expr)*
    
    ?expr: column_name operator value
         | column_name operator column_name
         | "(" condition ")"
    
    operator: "=" | "!=" | ">" | "<" | ">=" | "<="
    
    join_clause: "INNER" "JOIN" table_name "ON" column_name "=" column_name
    
    // INSERT queries
    insert_query: "INSERT" "INTO" table_name "VALUES" "(" value_list ")"
    
    value_list: value ("," value)*
    
    // UPDATE queries
    update_query: "UPDATE" table_name "SET" assignment_list [where_clause]
    
    assignment_list: assignment ("," assignment)*
    
    assignment: column_name "=" value
    
    // DELETE queries
    delete_query: "DELETE" "FROM" table_name [where_clause]
    
    // CREATE TABLE
    create_table_query: "CREATE" "TABLE" table_name "(" column_def_list ")"
    
    column_def_list: column_def ("," column_def)*
    
    column_def: column_name data_type column_constraints*
    
    data_type: "INT" -> int_type
             | "TEXT" -> text_type
             | "FLOAT" -> float_type
             | "BOOL" -> bool_type
    
    column_constraints: "PRIMARY" "KEY" -> primary_key
                     | "UNIQUE" -> unique
                     | "NOT" "NULL" -> not_null
    
    // CREATE INDEX
    create_index_query: "CREATE" "INDEX" index_name "ON" table_name "(" column_name ")"
    
    // Basic elements
    table_name: IDENTIFIER
    column_name: IDENTIFIER
    index_name: IDENTIFIER
    
    value: NUMBER -> number_value
         | STRING -> string_value
         | "TRUE" -> true_value
         | "FALSE" -> false_value
    
    IDENTIFIER: /[a-zA-Z_][a-zA-Z0-9_]*/
    STRING: /'[^']*'/
    NUMBER: /[0-9]+(\.[0-9]+)?/
    
    %import common.WS
    %ignore WS
"""

class DataType(Enum):
    INT = "INT"
    TEXT = "TEXT"
    FLOAT = "FLOAT"
    BOOL = "BOOL"

class ConstraintType(Enum):
    PRIMARY_KEY = "PRIMARY_KEY"
    UNIQUE = "UNIQUE"
    NOT_NULL = "NOT_NULL"

@dataclass
class Column:
    name: str
    data_type: DataType
    constraints: List[ConstraintType]

@dataclass
class Table:
    name: str
    columns: List[Column]

@dataclass
class Index:
    name: str
    table_name: str
    column_name: str

@dataclass
class Value:
    value: Union[int, float, str, bool]

@dataclass
class Assignment:
    column: str
    value: Value

@dataclass
class Condition:
    left: Union[str, Value]
    operator: str
    right: Union[str, Value]

@dataclass
class SelectQuery:
    columns: List[str]
    table: str
    where_clause: Optional[Condition] = None
    join_clause: Optional[Dict[str, str]] = None

@dataclass
class InsertQuery:
    table: str
    values: List[Value]

@dataclass
class UpdateQuery:
    table: str
    assignments: List[Assignment]
    where_clause: Optional[Condition] = None

@dataclass
class DeleteQuery:
    table: str
    where_clause: Optional[Condition] = None

@dataclass
class CreateTableQuery:
    table: Table

@dataclass
class CreateIndexQuery:
    index: Index

class SQLTransformer(Transformer):
    def number_value(self, args):
        value = args[0].value
        return Value(int(value) if '.' not in value else float(value))
    
    def string_value(self, args):
        value = args[0].value[1:-1]  # Remove quotes
        return Value(value)
    
    def true_value(self, args):
        return Value(True)
    
    def false_value(self, args):
        return Value(False)
    
    def int_type(self, args):
        return DataType.INT
    
    def text_type(self, args):
        return DataType.TEXT
    
    def float_type(self, args):
        return DataType.FLOAT
    
    def bool_type(self, args):
        return DataType.BOOL
    
    def primary_key(self, args):
        return [ConstraintType.PRIMARY_KEY]
    
    def unique(self, args):
        return [ConstraintType.UNIQUE]
    
    def not_null(self, args):
        return [ConstraintType.NOT_NULL]
    
    def column_def(self, args):
        name = args[0].value
        data_type = args[1]
        constraints = []
        for constraint in args[2:]:
            constraints.extend(constraint)
        return Column(name, data_type, constraints)
    
    def column_def_list(self, args):
        return list(args)
    
    def table_name(self, args):
        return args[0].value
    
    def column_name(self, args):
        return args[0].value
    
    def index_name(self, args):
        return args[0].value
    
    def assignment(self, args):
        column = args[0].value
        value = args[1]
        return Assignment(column, value)
    
    def assignment_list(self, args):
        return list(args)
    
    def condition(self, args):
        if len(args) == 1:
            return args[0]
        # Handle AND/OR logic here
        left = args[0]
        operator = args[1].value
        right = args[2]
        return Condition(left, operator, right)
    
    def expr(self, args):
        if len(args) == 1:
            return args[0]
        elif len(args) == 3:
            left = args[0]
            operator = args[1].value
            right = args[2]
            return Condition(left, operator, right)
    
    def where_clause(self, args):
        return args[0]
    
    def join_clause(self, args):
        return {
            'table': args[0].value,
            'left_column': args[1].value,
            'right_column': args[2].value
        }
    
    def select_all(self, args):
        return ['*']
    
    def column_list(self, args):
        return [arg.value if isinstance(arg, LarkToken) else arg for arg in args]
    
    def value_list(self, args):
        return list(args)
    
    def select_columns(self, args):
        return args[0]
    
    def select_query(self, args):
        columns = args[0]
        table = args[1].value
        where_clause = None
        join_clause = None
        
        for arg in args[2:]:
            if isinstance(arg, Condition):
                where_clause = arg
            elif isinstance(arg, dict):
                join_clause = arg
        
        return SelectQuery(columns, table, where_clause, join_clause)
    
    def insert_query(self, args):
        table = args[0].value
        values = args[1]
        return InsertQuery(table, values)
    
    def update_query(self, args):
        table = args[0].value
        assignments = args[1]
        where_clause = None
        if len(args) > 2:
            where_clause = args[2]
        return UpdateQuery(table, assignments, where_clause)
    
    def delete_query(self, args):
        table = args[0].value
        where_clause = None
        if len(args) > 1:
            where_clause = args[1]
        return DeleteQuery(table, where_clause)
    
    def create_table_query(self, args):
        table_name = args[0].value
        columns = args[1]
        table = Table(table_name, columns)
        return CreateTableQuery(table)
    
    def create_index_query(self, args):
        index_name = args[0].value
        table_name = args[1].value
        column_name = args[2].value
        index = Index(index_name, table_name, column_name)
        return CreateIndexQuery(index)

class SQLParser:
    def __init__(self):
        self.parser = Lark(SQL_GRAMMAR, start='start', transformer=SQLTransformer())
    
    def parse(self, sql: str):
        try:
            return self.parser.parse(sql)
        except Exception as e:
            raise ValueError(f"SQL parsing error: {str(e)}")
