"""
Main Database class for Mini RDBMS
"""

from typing import List, Dict, Any, Optional, Union
from parser.parser import (
    SelectQuery, InsertQuery, UpdateQuery, DeleteQuery,
    CreateTableQuery, CreateIndexQuery, Value, Assignment, Condition
)
from .storage import StorageEngine, TableSchema, Row
from .index import IndexManager
from .executor import QueryExecutor

class Database:
    def __init__(self, data_dir: str = "data"):
        self.storage = StorageEngine(data_dir)
        self.index_manager = IndexManager()
        self.executor = QueryExecutor(self.storage, self.index_manager)
    
    def execute(self, sql: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results"""
        from parser.parser import SQLParser
        
        parser = SQLParser()
        query = parser.parse(sql)
        
        if isinstance(query, SelectQuery):
            return self._execute_select(query)
        elif isinstance(query, InsertQuery):
            return self._execute_insert(query)
        elif isinstance(query, UpdateQuery):
            return self._execute_update(query)
        elif isinstance(query, DeleteQuery):
            return self._execute_delete(query)
        elif isinstance(query, CreateTableQuery):
            return self._execute_create_table(query)
        elif isinstance(query, CreateIndexQuery):
            return self._execute_create_index(query)
        else:
            raise ValueError(f"Unsupported query type: {type(query)}")
    
    def _execute_select(self, query: SelectQuery) -> List[Dict[str, Any]]:
        """Execute SELECT query"""
        table = self.storage.get_table(query.table)
        if not table:
            raise ValueError(f"Table '{query.table}' does not exist")
        
        # Get all rows
        rows = table.select(query.columns)
        
        # Apply WHERE clause
        if query.where_clause:
            rows = [row for row in rows if self._matches_condition(row, query.where_clause)]
        
        # Apply JOIN clause (simplified)
        if query.join_clause:
            join_table = self.storage.get_table(query.join_clause['table'])
            if join_table:
                rows = self._apply_join(rows, join_table, query.join_clause)
        
        # Convert to list of dicts
        result = []
        for row in rows:
            if '*' in query.columns:
                result.append(row.to_dict())
            else:
                filtered_data = {col: row.get(col) for col in query.columns if col in row.data}
                result.append(filtered_data)
        
        return result
    
    def _execute_insert(self, query: InsertQuery) -> List[Dict[str, Any]]:
        """Execute INSERT query"""
        table = self.storage.get_table(query.table)
        if not table:
            raise ValueError(f"Table '{query.table}' does not exist")
        
        # Convert values to actual Python types
        row_data = {}
        columns = [col.name for col in table.schema.columns]
        
        for i, value in enumerate(query.values):
            if i < len(columns):
                col_name = columns[i]
                row_data[col_name] = value.value
        
        row = Row(row_data)
        row_id = table.insert(row)
        
        # Update indexes
        self.index_manager.insert_into_indexes(query.table, row_data, row_id)
        
        # Save to disk
        self.storage.save_to_disk()
        
        return [{"affected_rows": 1, "row_id": row_id}]
    
    def _execute_update(self, query: UpdateQuery) -> List[Dict[str, Any]]:
        """Execute UPDATE query"""
        table = self.storage.get_table(query.table)
        if not table:
            raise ValueError(f"Table '{query.table}' does not exist")
        
        # Convert assignments
        assignments = {}
        for assignment in query.assignments:
            assignments[assignment.column] = assignment.value.value
        
        # Get rows that will be updated for index maintenance
        rows_to_update = []
        if query.where_clause:
            rows_to_update = [row for row in table.select() if self._matches_condition(row, query.where_clause)]
        else:
            rows_to_update = table.select()
        
        # Remove from indexes
        for row in rows_to_update:
            self.index_manager.delete_from_indexes(query.table, row.data, row.get('row_id'))
        
        # Perform update
        affected_rows = table.update(assignments, query.where_clause)
        
        # Add back to indexes
        updated_rows = []
        if query.where_clause:
            updated_rows = [row for row in table.select() if self._matches_condition(row, query.where_clause)]
        else:
            updated_rows = table.select()
        
        for row in updated_rows:
            self.index_manager.insert_into_indexes(query.table, row.data, row.get('row_id'))
        
        # Save to disk
        self.storage.save_to_disk()
        
        return [{"affected_rows": affected_rows}]
    
    def _execute_delete(self, query: DeleteQuery) -> List[Dict[str, Any]]:
        """Execute DELETE query"""
        table = self.storage.get_table(query.table)
        if not table:
            raise ValueError(f"Table '{query.table}' does not exist")
        
        # Get rows that will be deleted for index maintenance
        rows_to_delete = []
        if query.where_clause:
            rows_to_delete = [row for row in table.select() if self._matches_condition(row, query.where_clause)]
        else:
            rows_to_delete = table.select()
        
        # Remove from indexes
        for row in rows_to_delete:
            self.index_manager.delete_from_indexes(query.table, row.data, row.get('row_id'))
        
        # Perform delete
        affected_rows = table.delete(query.where_clause)
        
        # Save to disk
        self.storage.save_to_disk()
        
        return [{"affected_rows": affected_rows}]
    
    def _execute_create_table(self, query: CreateTableQuery) -> List[Dict[str, Any]]:
        """Execute CREATE TABLE query"""
        self.storage.create_table(query.table)
        return [{"message": f"Table '{query.table.name}' created successfully"}]
    
    def _execute_create_index(self, query: CreateIndexQuery) -> List[Dict[str, Any]]:
        """Execute CREATE INDEX query"""
        self.index_manager.create_index(
            query.index.name,
            query.index.table_name,
            query.index.column_name
        )
        return [{"message": f"Index '{query.index.name}' created successfully"}]
    
    def _matches_condition(self, row: Row, condition: Condition) -> bool:
        """Check if a row matches a condition"""
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
        else:
            return False
    
    def _apply_join(self, rows: List[Row], join_table, join_clause: Dict[str, str]) -> List[Row]:
        """Apply INNER JOIN (simplified implementation)"""
        joined_rows = []
        
        for left_row in rows:
            left_key = left_row.get(join_clause['left_column'])
            if left_key is not None:
                # Find matching rows in join table
                for right_row in join_table.select():
                    right_key = right_row.get(join_clause['right_column'])
                    if left_key == right_key:
                        # Merge rows
                        merged_data = left_row.to_dict()
                        right_data = right_row.to_dict()
                        
                        # Prefix column names from right table to avoid conflicts
                        for key, value in right_data.items():
                            if key != 'row_id':  # Don't duplicate row_id
                                merged_data[f"{join_table.schema.name}_{key}"] = value
                        
                        joined_rows.append(Row(merged_data))
                        break  # Simplified: only first match
        
        return joined_rows
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get information about a table"""
        table = self.storage.get_table(table_name)
        if not table:
            return None
        
        return {
            'name': table.schema.name,
            'columns': [
                {
                    'name': col.name,
                    'type': col.data_type.value,
                    'constraints': [c.value for c in col.constraints]
                }
                for col in table.schema.columns
            ],
            'row_count': len(table.rows),
            'primary_key': table.schema.primary_key
        }
    
    def list_tables(self) -> List[str]:
        """List all tables"""
        return self.storage.list_tables()
