"""
SQL Tokenizer for Mini RDBMS
"""

import re
from typing import List, Dict, Any, Optional
from enum import Enum

class TokenType(Enum):
    # Literals
    NUMBER = "NUMBER"
    STRING = "STRING"
    IDENTIFIER = "IDENTIFIER"
    
    # Keywords
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    INSERT = "INSERT"
    INTO = "INTO"
    VALUES = "VALUES"
    UPDATE = "UPDATE"
    SET = "SET"
    DELETE = "DELETE"
    CREATE = "CREATE"
    TABLE = "TABLE"
    INDEX = "INDEX"
    ON = "ON"
    PRIMARY = "PRIMARY"
    KEY = "KEY"
    UNIQUE = "UNIQUE"
    NOT = "NOT"
    NULL = "NULL"
    JOIN = "JOIN"
    INNER = "INNER"
    
    # Operators
    EQUALS = "EQUALS"
    NOT_EQUALS = "NOT_EQUALS"
    GREATER = "GREATER"
    LESS = "LESS"
    GREATER_EQUAL = "GREATER_EQUAL"
    LESS_EQUAL = "LESS_EQUAL"
    AND = "AND"
    OR = "OR"
    
    # Punctuation
    COMMA = "COMMA"
    SEMICOLON = "SEMICOLON"
    LEFT_PAREN = "LEFT_PAREN"
    RIGHT_PAREN = "RIGHT_PAREN"
    ASTERISK = "ASTERISK"
    
    # Special
    EOF = "EOF"
    WHITESPACE = "WHITESPACE"

class Token:
    def __init__(self, token_type: TokenType, value: str, position: int):
        self.type = token_type
        self.value = value
        self.position = position
    
    def __repr__(self):
        return f"Token({self.type}, '{self.value}', {self.position})"

class SQLTokenizer:
    def __init__(self):
        self.keywords = {
            'SELECT': TokenType.SELECT,
            'FROM': TokenType.FROM,
            'WHERE': TokenType.WHERE,
            'INSERT': TokenType.INSERT,
            'INTO': TokenType.INTO,
            'VALUES': TokenType.VALUES,
            'UPDATE': TokenType.UPDATE,
            'SET': TokenType.SET,
            'DELETE': TokenType.DELETE,
            'CREATE': TokenType.CREATE,
            'TABLE': TokenType.TABLE,
            'INDEX': TokenType.INDEX,
            'ON': TokenType.ON,
            'PRIMARY': TokenType.PRIMARY,
            'KEY': TokenType.KEY,
            'UNIQUE': TokenType.UNIQUE,
            'NOT': TokenType.NOT,
            'NULL': TokenType.NULL,
            'JOIN': TokenType.JOIN,
            'INNER': TokenType.INNER,
            'AND': TokenType.AND,
            'OR': TokenType.OR,
        }
        
        self.token_patterns = [
            (TokenType.NUMBER, r'\d+(\.\d+)?'),
            (TokenType.STRING, r"'([^']*)'"),
            (TokenType.NOT_EQUALS, r'!=|<>'),
            (TokenType.GREATER_EQUAL, r'>='),
            (TokenType.LESS_EQUAL, r'<='),
            (TokenType.EQUALS, r'='),
            (TokenType.GREATER, r'>'),
            (TokenType.LESS, r'<'),
            (TokenType.COMMA, r','),
            (TokenType.SEMICOLON, r';'),
            (TokenType.LEFT_PAREN, r'\('),
            (TokenType.RIGHT_PAREN, r'\)'),
            (TokenType.ASTERISK, r'\*'),
            (TokenType.WHITESPACE, r'\s+'),
            (TokenType.IDENTIFIER, r'[a-zA-Z_][a-zA-Z0-9_]*'),
        ]
        
        self.regex_patterns = [(token_type, re.compile(pattern)) 
                              for token_type, pattern in self.token_patterns]
    
    def tokenize(self, input_string: str) -> List[Token]:
        tokens = []
        position = 0
        input_length = len(input_string)
        
        while position < input_length:
            matched = False
            
            for token_type, pattern in self.regex_patterns:
                match = pattern.match(input_string, position)
                if match:
                    value = match.group(0)
                    
                    if token_type == TokenType.WHITESPACE:
                        position = match.end()
                        matched = True
                        break
                    
                    # Check for keywords
                    if token_type == TokenType.IDENTIFIER:
                        upper_value = value.upper()
                        if upper_value in self.keywords:
                            token_type = self.keywords[upper_value]
                    
                    tokens.append(Token(token_type, value, position))
                    position = match.end()
                    matched = True
                    break
            
            if not matched:
                raise ValueError(f"Unexpected character at position {position}: '{input_string[position]}'")
        
        tokens.append(Token(TokenType.EOF, '', position))
        return tokens
