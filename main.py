"""
Main entry point for Mini RDBMS
"""

import sys
import argparse
from repl.cli import MiniDBREPL
from engine.database import Database

def main():
    parser = argparse.ArgumentParser(description='Mini RDBMS - A simple relational database management system')
    parser.add_argument('--mode', choices=['repl', 'test'], default='repl',
                       help='Run mode: repl (interactive) or test (run demo queries)')
    parser.add_argument('--data-dir', default='data',
                       help='Data directory for database files')
    
    args = parser.parse_args()
    
    if args.mode == 'repl':
        # Start interactive REPL
        repl = MiniDBREPL(args.data_dir)
        repl.run()
    elif args.mode == 'test':
        # Run demo queries
        run_demo(args.data_dir)

def run_demo(data_dir: str):
    """Run demonstration queries"""
    print("Mini RDBMS Demo")
    print("=" * 50)
    
    db = Database(data_dir)
    
    demo_queries = [
        ("Create users table", """
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INT
        );
        """),
        
        ("Create orders table", """
        CREATE TABLE orders (
            id INT PRIMARY KEY,
            user_id INT,
            amount FLOAT,
            description TEXT
        );
        """),
        
        ("Insert users", """
        INSERT INTO users VALUES (1, 'John Doe', 'john@example.com', 25);
        INSERT INTO users VALUES (2, 'Jane Smith', 'jane@example.com', 30);
        INSERT INTO users VALUES (3, 'Bob Johnson', 'bob@example.com', 35);
        """),
        
        ("Insert orders", """
        INSERT INTO orders VALUES (1, 1, 99.99, 'Laptop');
        INSERT INTO orders VALUES (2, 2, 49.99, 'Mouse');
        INSERT INTO orders VALUES (3, 1, 29.99, 'Keyboard');
        """),
        
        ("Select all users", "SELECT * FROM users;"),
        
        ("Select users older than 25", "SELECT name, age FROM users WHERE age > 25;"),
        
        ("Update user age", "UPDATE users SET age = 26 WHERE id = 1;"),
        
        ("Join users and orders", """
        SELECT users.name, orders.amount 
        FROM users 
        JOIN orders ON users.id = orders.user_id;
        """),
        
        ("Delete order", "DELETE FROM orders WHERE id = 2;"),
        
        ("Final users", "SELECT * FROM users;"),
    ]
    
    for description, query in demo_queries:
        print(f"\n{description}:")
        print(f"Query: {query.strip()}")
        try:
            results = db.execute(query)
            if results:
                for result in results:
                    print(f"Result: {result}")
            else:
                print("Query executed successfully.")
        except Exception as e:
            print(f"Error: {str(e)}")
        print("-" * 30)

if __name__ == "__main__":
    main()
