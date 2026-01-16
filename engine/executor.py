"""
Query Execution Engine for Mini RDBMS
"""

from typing import List, Dict, Any, Optional
from .storage import StorageEngine, Row
from .index import IndexManager

class QueryExecutor:
    def __init__(self, storage: StorageEngine, index_manager: IndexManager):
        self.storage = storage
        self.index_manager = index_manager
    
    def execute_select(self, table_name: str, columns: List[str], 
                      where_clause=None, join_clause=None) -> List[Dict[str, Any]]:
        """Execute a SELECT query with optimization"""
        table = self.storage.get_table(table_name)
        if not table:
            raise ValueError(f"Table '{table_name}' does not exist")
        
        # Check if we can use an index for WHERE clause
        if where_clause and self._can_use_index(table_name, where_clause):
            return self._execute_select_with_index(table, columns, where_clause, join_clause)
        else:
            return self._execute_select_full_scan(table, columns, where_clause, join_clause)
    
    def _can_use_index(self, table_name: str, where_clause) -> bool:
        """Check if we can use an index for the WHERE clause"""
        # Simplified: check if there's an index on the column in the WHERE clause
        if hasattr(where_clause, 'left') and isinstance(where_clause.left, str):
            index_names = self.index_manager.get_table_indexes(table_name)
            return len(index_names) > 0
        return False
    
    def _execute_select_with_index(self, table, columns: List[str], 
                                 where_clause, join_clause) -> List[Dict[str, Any]]:
        """Execute SELECT using index lookup"""
        # Get the first index for the table
        index_names = self.index_manager.get_table_indexes(table.schema.name)
        if not index_names:
            return self._execute_select_full_scan(table, columns, where_clause, join_clause)
        
        index_name = index_names[0]
        index = self.index_manager.get_index(index_name)
        
        if not index or not hasattr(where_clause, 'left'):
            return self._execute_select_full_scan(table, columns, where_clause, join_clause)
        
        # Extract the search key
        search_key = where_clause.right.value if hasattr(where_clause, 'right') else None
        
        if search_key is None:
            return self._execute_select_full_scan(table, columns, where_clause, join_clause)
        
        # Search using index
        row_ids = index.search(search_key)
        
        # Get rows by ID
        result_rows = []
        for row in table.rows:
            if row.get('row_id') in row_ids:
                result_rows.append(row)
        
        return self._format_results(result_rows, columns)
    
    def _execute_select_full_scan(self, table, columns: List[str], 
                                where_clause, join_clause) -> List[Dict[str, Any]]:
        """Execute SELECT with full table scan"""
        rows = table.select(columns)
        
        # Apply WHERE clause
        if where_clause:
            rows = [row for row in rows if self._matches_condition(row, where_clause)]
        
        # Apply JOIN clause
        if join_clause:
            join_table = self.storage.get_table(join_clause['table'])
            if join_table:
                rows = self._apply_join(rows, join_table, join_clause)
        
        return self._format_results(rows, columns)
    
    def _matches_condition(self, row: Row, condition) -> bool:
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
        """Apply INNER JOIN"""
        joined_rows = []
        
        for left_row in rows:
            left_key = left_row.get(join_clause['left_column'])
            if left_key is not None:
                for right_row in join_table.select():
                    right_key = right_row.get(join_clause['right_column'])
                    if left_key == right_key:
                        merged_data = left_row.to_dict()
                        right_data = right_row.to_dict()
                        
                        for key, value in right_data.items():
                            if key != 'row_id':
                                merged_data[f"{join_table.schema.name}_{key}"] = value
                        
                        joined_rows.append(Row(merged_data))
                        break
        
        return joined_rows
    
    def _format_results(self, rows: List[Row], columns: List[str]) -> List[Dict[str, Any]]:
        """Format rows for output"""
        result = []
        for row in rows:
            if '*' in columns:
                result.append(row.to_dict())
            else:
                filtered_data = {col: row.get(col) for col in columns if col in row.data}
                result.append(filtered_data)
        
        return result
