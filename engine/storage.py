"""
Storage Engine for Mini RDBMS
"""

import json
import os
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, asdict
from enum import Enum
from parser.parser import DataType, ConstraintType, Column

class ValueType(Enum):
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"

@dataclass
class Row:
    data: Dict[str, Any]
    
    def get(self, column: str) -> Any:
        return self.data.get(column)
    
    def set(self, column: str, value: Any) -> None:
        self.data[column] = value
    
    def to_dict(self) -> Dict[str, Any]:
        return self.data.copy()

@dataclass
class TableSchema:
    name: str
    columns: List[Column]
    primary_key: Optional[str] = None
    
    def get_column(self, name: str) -> Optional[Column]:
        for col in self.columns:
            if col.name == name:
                return col
        return None
    
    def validate_row(self, row: Row) -> bool:
        # Check required columns
        for col in self.columns:
            if ConstraintType.NOT_NULL in col.constraints and col.name not in row.data:
                raise ValueError(f"Column '{col.name}' cannot be NULL")
        
        # Check data types
        for col_name, value in row.data.items():
            col = self.get_column(col_name)
            if col:
                if not self._validate_type(value, col.data_type):
                    raise ValueError(f"Invalid type for column '{col_name}'")
        
        return True
    
    def _validate_type(self, value: Any, data_type: DataType) -> bool:
        if data_type == DataType.INT:
            return isinstance(value, int)
        elif data_type == DataType.FLOAT:
            return isinstance(value, (int, float))
        elif data_type == DataType.TEXT:
            return isinstance(value, str)
        elif data_type == DataType.BOOL:
            return isinstance(value, bool)
        return False

class Table:
    def __init__(self, schema: TableSchema):
        self.schema = schema
        self.rows: List[Row] = []
        self.next_row_id = 1
    
    def insert(self, row: Row) -> int:
        self.schema.validate_row(row)
        
        # Add row ID if not present
        if 'row_id' not in row.data:
            row.data['row_id'] = self.next_row_id
            self.next_row_id += 1
        
        # Check unique constraints
        self._check_unique_constraints(row)
        
        self.rows.append(row)
        return row.data['row_id']
    
    def select(self, columns: Optional[List[str]] = None) -> List[Row]:
        if columns is None or '*' in columns:
            return [Row(r.data.copy()) for r in self.rows]
        
        result = []
        for row in self.rows:
            filtered_data = {col: row.data[col] for col in columns if col in row.data}
            result.append(Row(filtered_data))
        return result
    
    def update(self, assignments: Dict[str, Any], where_clause=None) -> int:
        count = 0
        for row in self.rows:
            if where_clause is None or self._matches_condition(row, where_clause):
                for col, value in assignments.items():
                    row.set(col, value)
                self.schema.validate_row(row)
                count += 1
        return count
    
    def delete(self, where_clause=None) -> int:
        if where_clause is None:
            count = len(self.rows)
            self.rows.clear()
            return count
        
        original_count = len(self.rows)
        self.rows = [row for row in self.rows if not self._matches_condition(row, where_clause)]
        return original_count - len(self.rows)
    
    def _check_unique_constraints(self, new_row: Row) -> None:
        for col in self.schema.columns:
            if ConstraintType.UNIQUE in col.constraints or ConstraintType.PRIMARY_KEY in col.constraints:
                if col.name in new_row.data:
                    for existing_row in self.rows:
                        if (col.name in existing_row.data and 
                            existing_row.data[col.name] == new_row.data[col.name]):
                            raise ValueError(f"Duplicate value for unique column '{col.name}'")
    
    def _matches_condition(self, row: Row, condition) -> bool:
        # Simple condition matching - can be extended
        if hasattr(condition, 'left') and hasattr(condition, 'operator') and hasattr(condition, 'right'):
            left_val = row.get(condition.left) if isinstance(condition.left, str) else condition.left.value
            right_val = row.get(condition.right) if isinstance(condition.right, str) else condition.right.value
            
            if condition.operator == '=':
                return left_val == right_val
            elif condition.operator == '!=':
                return left_val != right_val
            elif condition.operator == '>':
                return left_val > right_val
            elif condition.operator == '<':
                return left_val < right_val
            elif condition.operator == '>=':
                return left_val >= right_val
            elif condition.operator == '<=':
                return left_val <= right_val
        
        return True

class StorageEngine:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.tables: Dict[str, Table] = {}
        self.load_from_disk()
    
    def create_table(self, schema: TableSchema) -> None:
        if schema.name in self.tables:
            raise ValueError(f"Table '{schema.name}' already exists")
        
        # Set primary key
        for col in schema.columns:
            if ConstraintType.PRIMARY_KEY in col.constraints:
                schema.primary_key = col.name
                break
        
        self.tables[schema.name] = Table(schema)
        self.save_to_disk()
    
    def get_table(self, name: str) -> Optional[Table]:
        return self.tables.get(name)
    
    def table_exists(self, name: str) -> bool:
        return name in self.tables
    
    def list_tables(self) -> List[str]:
        return list(self.tables.keys())
    
    def save_to_disk(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        
        for table_name, table in self.tables.items():
            # Save schema
            schema_data = {
                'name': table.schema.name,
                'columns': [
                    {
                        'name': col.name,
                        'data_type': col.data_type.value,
                        'constraints': [c.value for c in col.constraints]
                    }
                    for col in table.schema.columns
                ],
                'primary_key': table.schema.primary_key
            }
            
            schema_file = os.path.join(self.data_dir, f"{table_name}_schema.json")
            with open(schema_file, 'w') as f:
                json.dump(schema_data, f, indent=2)
            
            # Save rows
            rows_data = [row.to_dict() for row in table.rows]
            rows_file = os.path.join(self.data_dir, f"{table_name}_rows.json")
            with open(rows_file, 'w') as f:
                json.dump(rows_data, f, indent=2)
    
    def load_from_disk(self) -> None:
        if not os.path.exists(self.data_dir):
            return
        
        # Load all tables from disk
        for filename in os.listdir(self.data_dir):
            if filename.endswith('_schema.json'):
                table_name = filename.replace('_schema.json', '')
                
                # Load schema
                schema_file = os.path.join(self.data_dir, filename)
                with open(schema_file, 'r') as f:
                    schema_data = json.load(f)
                
                columns = []
                for col_data in schema_data['columns']:
                    column = Column(
                        name=col_data['name'],
                        data_type=DataType(col_data['data_type']),
                        constraints=[ConstraintType(c) for c in col_data['constraints']]
                    )
                    columns.append(column)
                
                schema = TableSchema(
                    name=schema_data['name'],
                    columns=columns,
                    primary_key=schema_data.get('primary_key')
                )
                
                table = Table(schema)
                
                # Load rows
                rows_file = os.path.join(self.data_dir, f"{table_name}_rows.json")
                if os.path.exists(rows_file):
                    with open(rows_file, 'r') as f:
                        rows_data = json.load(f)
                    
                    for row_data in rows_data:
                        row = Row(row_data)
                        table.rows.append(row)
                        
                        # Update next_row_id
                        if 'row_id' in row_data and row_data['row_id'] >= table.next_row_id:
                            table.next_row_id = row_data['row_id'] + 1
                
                self.tables[table_name] = table
