"""
Indexing System for Mini RDBMS - B-Tree Implementation
"""

from typing import Any, List, Optional, Dict, Tuple
from dataclasses import dataclass
from enum import Enum

class Order(Enum):
    LT = 0
    EQ = 1
    GT = 2

@dataclass
class BTreeNode:
    keys: List[Any]
    children: List['BTreeNode']
    is_leaf: bool
    
    def __init__(self, is_leaf: bool = True):
        self.keys = []
        self.children = []
        self.is_leaf = is_leaf

class BTreeIndex:
    def __init__(self, order: int = 4):  # Order determines max keys per node
        self.order = order
        self.root = BTreeNode(is_leaf=True)
    
    def insert(self, key: Any, value: Any) -> None:
        """Insert a key-value pair into the B-Tree"""
        root = self.root
        
        # If root is full, split it first
        if len(root.keys) == (2 * self.order) - 1:
            new_root = BTreeNode(is_leaf=False)
            new_root.children.append(root)
            self._split_child(new_root, 0)
            self.root = new_root
            root = new_root
        
        self._insert_non_full(root, key, value)
    
    def search(self, key: Any) -> List[Any]:
        """Search for a key and return all associated values"""
        return self._search(self.root, key)
    
    def search_range(self, start_key: Any, end_key: Any) -> List[Tuple[Any, Any]]:
        """Search for all keys in range [start_key, end_key]"""
        result = []
        self._search_range(self.root, start_key, end_key, result)
        return result
    
    def delete(self, key: Any) -> bool:
        """Delete a key from the B-Tree"""
        return self._delete(self.root, key)
    
    def _insert_non_full(self, node: BTreeNode, key: Any, value: Any) -> None:
        """Insert into a node that is guaranteed not to be full"""
        i = len(node.keys) - 1
        
        if node.is_leaf:
            # Insert new key into leaf node
            node.keys.append(None)
            node.children.append(None)  # Placeholder for value
            
            while i >= 0 and self._compare_keys(key, node.keys[i]) == Order.LT:
                node.keys[i + 1] = node.keys[i]
                node.children[i + 1] = node.children[i]
                i -= 1
            
            node.keys[i + 1] = key
            node.children[i + 1] = [value] if node.children[i + 1] is None else node.children[i + 1]
        else:
            # Find the child to descend to
            while i >= 0 and self._compare_keys(key, node.keys[i]) == Order.LT:
                i -= 1
            i += 1
            
            # If child is full, split it
            if len(node.children[i].keys) == (2 * self.order) - 1:
                self._split_child(node, i)
                if self._compare_keys(key, node.keys[i]) == Order.GT:
                    i += 1
            
            self._insert_non_full(node.children[i], key, value)
    
    def _split_child(self, parent: BTreeNode, index: int) -> None:
        """Split a child node of a parent"""
        order = self.order
        child = parent.children[index]
        new_child = BTreeNode(is_leaf=child.is_leaf)
        
        # Move the middle key up to the parent
        parent.keys.insert(index, child.keys[order - 1])
        parent.children.insert(index + 1, new_child)
        
        # Transfer keys to new child
        new_child.keys = child.keys[order:]  # Keys after the middle
        child.keys = child.keys[:order - 1]  # Keys before the middle
        
        # Transfer children if not leaf
        if not child.is_leaf:
            new_child.children = child.children[order:]
            child.children = child.children[:order]
    
    def _search(self, node: BTreeNode, key: Any) -> List[Any]:
        """Search for a key starting from a given node"""
        i = 0
        
        # Find the first key greater than or equal to the search key
        while i < len(node.keys) and self._compare_keys(key, node.keys[i]) == Order.GT:
            i += 1
        
        if i < len(node.keys) and self._compare_keys(key, node.keys[i]) == Order.EQ:
            return node.children[i] if node.children[i] is not None else []
        
        if node.is_leaf:
            return []
        
        return self._search(node.children[i], key)
    
    def _search_range(self, node: BTreeNode, start_key: Any, end_key: Any, result: List[Tuple[Any, Any]]) -> None:
        """Search for keys in range and append to result list"""
        i = 0
        
        # Find the starting position
        while i < len(node.keys) and self._compare_keys(start_key, node.keys[i]) == Order.GT:
            i += 1
        
        # Process keys in range
        while i < len(node.keys) and self._compare_keys(node.keys[i], end_key) != Order.GT:
            if not node.is_leaf:
                self._search_range(node.children[i], start_key, end_key, result)
            
            if node.children[i] is not None:
                for value in node.children[i]:
                    result.append((node.keys[i], value))
            
            i += 1
        
        # Process the last child if not leaf
        if not node.is_leaf and i < len(node.children):
            self._search_range(node.children[i], start_key, end_key, result)
    
    def _delete(self, node: BTreeNode, key: Any) -> bool:
        """Delete a key from the B-Tree"""
        # Simplified deletion - would need full implementation for production
        i = 0
        
        while i < len(node.keys) and self._compare_keys(key, node.keys[i]) == Order.GT:
            i += 1
        
        if i < len(node.keys) and self._compare_keys(key, node.keys[i]) == Order.EQ:
            if node.is_leaf:
                # Simple case: remove from leaf
                del node.keys[i]
                del node.children[i]
                return True
            else:
                # Complex case: remove from internal node
                # This would involve finding predecessor/successor
                # For simplicity, we'll just mark as deleted
                node.children[i] = None
                return True
        
        if node.is_leaf:
            return False
        
        return self._delete(node.children[i], key)
    
    def _compare_keys(self, key1: Any, key2: Any) -> Order:
        """Compare two keys and return their order"""
        if key1 < key2:
            return Order.LT
        elif key1 > key2:
            return Order.GT
        else:
            return Order.EQ

class IndexManager:
    def __init__(self):
        self.indexes: Dict[str, BTreeIndex] = {}  # index_name -> BTreeIndex
        self.table_indexes: Dict[str, List[str]] = {}  # table_name -> list of index_names
    
    def create_index(self, index_name: str, table_name: str, column_name: str) -> None:
        """Create a new index"""
        if index_name in self.indexes:
            raise ValueError(f"Index '{index_name}' already exists")
        
        self.indexes[index_name] = BTreeIndex()
        
        if table_name not in self.table_indexes:
            self.table_indexes[table_name] = []
        self.table_indexes[table_name].append(index_name)
    
    def drop_index(self, index_name: str) -> None:
        """Drop an index"""
        if index_name not in self.indexes:
            raise ValueError(f"Index '{index_name}' does not exist")
        
        del self.indexes[index_name]
        
        # Remove from table_indexes
        for table_name, index_list in self.table_indexes.items():
            if index_name in index_list:
                index_list.remove(index_name)
    
    def get_index(self, index_name: str) -> Optional[BTreeIndex]:
        """Get an index by name"""
        return self.indexes.get(index_name)
    
    def get_table_indexes(self, table_name: str) -> List[str]:
        """Get all index names for a table"""
        return self.table_indexes.get(table_name, [])
    
    def insert_into_indexes(self, table_name: str, row_data: Dict[str, Any], row_id: int) -> None:
        """Insert a row into all relevant indexes"""
        for index_name in self.get_table_indexes(table_name):
            index = self.get_index(index_name)
            if index:
                # This is simplified - we'd need to track which column each index is for
                # For now, we'll assume the index is on the first column
                if row_data:
                    first_key = next(iter(row_data.values()))
                    index.insert(first_key, row_id)
    
    def delete_from_indexes(self, table_name: str, row_data: Dict[str, Any], row_id: int) -> None:
        """Delete a row from all relevant indexes"""
        for index_name in self.get_table_indexes(table_name):
            index = self.get_index(index_name)
            if index:
                if row_data:
                    first_key = next(iter(row_data.values()))
                    index.delete(first_key)
    
    def search_index(self, index_name: str, key: Any) -> List[Any]:
        """Search for a key in a specific index"""
        index = self.get_index(index_name)
        if index:
            return index.search(key)
        return []
    
    def search_index_range(self, index_name: str, start_key: Any, end_key: Any) -> List[Tuple[Any, Any]]:
        """Search for keys in range in a specific index"""
        index = self.get_index(index_name)
        if index:
            return index.search_range(start_key, end_key)
        return []
