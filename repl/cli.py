"""
Interactive REPL for Mini RDBMS
"""

import sys
import readline
import os
from typing import List, Dict, Any
from engine.database import Database

class MiniDBREPL:
    def __init__(self, data_dir: str = "data"):
        self.db = Database(data_dir)
        self.running = True
        self.history_file = os.path.expanduser("~/.minidb_history")
        
        # Setup command history
        try:
            readline.read_history_file(self.history_file)
        except FileNotFoundError:
            pass
        
        readline.set_history_length(1000)
        readline.write_history_file(self.history_file)
    
    def run(self):
        """Start the REPL"""
        print("Mini RDBMS v1.0")
        print("Type 'help' for available commands or 'exit' to quit.")
        print()
        
        while self.running:
            try:
                command = input("MiniDB > ").strip()
                
                if not command:
                    continue
                
                # Handle special commands
                if command.lower() in ['exit', 'quit']:
                    self.running = False
                    continue
                elif command.lower() == 'help':
                    self.show_help()
                    continue
                elif command.lower() == 'tables':
                    self.show_tables()
                    continue
                elif command.lower().startswith('describe '):
                    table_name = command.lower().replace('describe ', '').strip()
                    self.describe_table(table_name)
                    continue
                elif command.lower() == 'clear':
                    os.system('cls' if os.name == 'nt' else 'clear')
                    continue
                
                # Execute SQL command
                try:
                    results = self.db.execute(command)
                    self.display_results(results)
                except Exception as e:
                    print(f"Error: {str(e)}")
                
            except KeyboardInterrupt:
                print("\nUse 'exit' to quit.")
            except EOFError:
                print("\nGoodbye!")
                break
        
        # Save history
        readline.write_history_file(self.history_file)
    
    def show_help(self):
        """Display help information"""
        help_text = """
Available Commands:
------------------
SQL Commands:
  CREATE TABLE table_name (column_name data_type [constraints], ...)
  INSERT INTO table_name VALUES (value1, value2, ...)
  SELECT * FROM table_name [WHERE condition]
  SELECT column1, column2 FROM table_name [WHERE condition]
  UPDATE table_name SET column1 = value1, column2 = value2 [WHERE condition]
  DELETE FROM table_name [WHERE condition]
  CREATE INDEX index_name ON table_name(column_name)

Special Commands:
  help           - Show this help message
  tables         - List all tables
  describe table - Show table schema
  clear          - Clear screen
  exit/quit      - Exit the REPL

Data Types:
  INT    - Integer values
  TEXT   - String values
  FLOAT  - Floating point numbers
  BOOL   - Boolean values (TRUE/FALSE)

Constraints:
  PRIMARY KEY   - Primary key constraint
  UNIQUE        - Unique constraint
  NOT NULL      - Not null constraint

Operators:
  =, !=, >, <, >=, <=  - Comparison operators
  AND, OR             - Logical operators

Examples:
  CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);
  INSERT INTO users VALUES (1, 'John Doe', 'john@example.com');
  SELECT * FROM users WHERE id > 0;
  UPDATE users SET name = 'Jane Doe' WHERE id = 1;
  DELETE FROM users WHERE id = 1;
        """
        print(help_text)
    
    def show_tables(self):
        """List all tables"""
        tables = self.db.list_tables()
        if tables:
            print("Tables:")
            for table in tables:
                table_info = self.db.get_table_info(table)
                if table_info:
                    print(f"  {table} ({table_info['row_count']} rows)")
        else:
            print("No tables found.")
    
    def describe_table(self, table_name: str):
        """Show table schema"""
        table_info = self.db.get_table_info(table_name)
        if table_info:
            print(f"Table: {table_info['name']}")
            print(f"Primary Key: {table_info['primary_key'] or 'None'}")
            print(f"Row Count: {table_info['row_count']}")
            print("\nColumns:")
            print("  {:<20} {:<10} {:<20}".format("Name", "Type", "Constraints"))
            print("  " + "-" * 50)
            for col in table_info['columns']:
                constraints = ", ".join(col['constraints']) if col['constraints'] else "None"
                print("  {:<20} {:<10} {:<20}".format(
                    col['name'], col['type'], constraints
                ))
        else:
            print(f"Table '{table_name}' not found.")
    
    def display_results(self, results: List[Dict[str, Any]]):
        """Display query results in a formatted table"""
        if not results:
            print("No results returned.")
            return
        
        # Check if this is a status message (like affected_rows)
        if len(results) == 1 and len(results[0]) == 1:
            key = list(results[0].keys())[0]
            if key in ['affected_rows', 'row_id', 'message']:
                for result in results:
                    for k, v in result.items():
                        print(f"{k.replace('_', ' ').title()}: {v}")
                return
        
        # Display table results
        columns = list(results[0].keys())
        
        # Calculate column widths
        col_widths = {}
        for col in columns:
            col_widths[col] = max(len(str(col)), 
                                 max(len(str(row.get(col, ''))) for row in results))
        
        # Print header
        header = " | ".join(str(col).ljust(col_widths[col]) for col in columns)
        print(header)
        print("-" * len(header))
        
        # Print rows
        for row in results:
            row_str = " | ".join(str(row.get(col, '')).ljust(col_widths[col]) for col in columns)
            print(row_str)
        
        print(f"\n{len(results)} row(s) returned.")

def main():
    """Main entry point for REPL"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Mini RDBMS REPL')
    parser.add_argument('--data-dir', default='data', 
                       help='Data directory for database files')
    
    args = parser.parse_args()
    
    repl = MiniDBREPL(args.data_dir)
    repl.run()

if __name__ == "__main__":
    main()
