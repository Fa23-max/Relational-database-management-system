"""
FastAPI Backend for Mini RDBMS Web Application
"""

from fastapi import FastAPI, HTTPException, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import sys
import os

# Add parent directory to path to import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.database import Database
from engine.storage import TableSchema
from parser.parser import DataType, ConstraintType, Column

app = FastAPI(title="Mini RDBMS Web App", description="A simple web interface for Mini RDBMS")

# Initialize database
db = Database(data_dir="web_data")

# Setup templates
templates = Jinja2Templates(directory="web_app/templates")

# Pydantic models for API
class CreateTableRequest(BaseModel):
    table_name: str
    columns: List[Dict[str, Any]]

class InsertRequest(BaseModel):
    table_name: str
    data: Dict[str, Any]

class UpdateRequest(BaseModel):
    table_name: str
    data: Dict[str, Any]
    where_clause: Optional[str] = None

class DeleteRequest(BaseModel):
    table_name: str
    where_clause: Optional[str] = None

class QueryRequest(BaseModel):
    query: str

# Routes
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard"""
    tables = db.list_tables()
    table_info = []
    for table_name in tables:
        info = db.get_table_info(table_name)
        if info:
            table_info.append(info)
    
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "tables": table_info
    })

@app.get("/table/{table_name}", response_class=HTMLResponse)
async def view_table(request: Request, table_name: str):
    """View table data"""
    try:
        results = db.execute(f"SELECT * FROM {table_name}")
        table_info = db.get_table_info(table_name)
        
        return templates.TemplateResponse("table.html", {
            "request": request,
            "table_name": table_name,
            "data": results,
            "columns": list(results[0].keys()) if results else [],
            "table_info": table_info
        })
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/create-table", response_class=HTMLResponse)
async def create_table_form(request: Request):
    """Show create table form"""
    return templates.TemplateResponse("create_table.html", {
        "request": request,
        "data_types": [dt.value for dt in DataType],
        "constraints": [ct.value for ct in ConstraintType]
    })

@app.post("/create-table")
async def create_table(
    request: Request,
    table_name: str = Form(...),
    columns: str = Form(...)
):
    """Create a new table"""
    try:
        # Parse columns JSON
        import json
        columns_data = json.loads(columns)
        
        # Create column objects
        column_objects = []
        for col_data in columns_data:
            constraints = []
            if col_data.get('primary_key'):
                constraints.append(ConstraintType.PRIMARY_KEY)
            if col_data.get('unique'):
                constraints.append(ConstraintType.UNIQUE)
            if col_data.get('not_null'):
                constraints.append(ConstraintType.NOT_NULL)
            
            column = Column(
                name=col_data['name'],
                data_type=DataType(col_data['type']),
                constraints=constraints
            )
            column_objects.append(column)
        
        # Create table schema
        schema = TableSchema(name=table_name, columns=column_objects)
        
        # Execute CREATE TABLE
        create_query = f"CREATE TABLE {table_name} ("
        column_defs = []
        for col in column_objects:
            col_def = f"{col.name} {col.data_type.value}"
            if ConstraintType.PRIMARY_KEY in col.constraints:
                col_def += " PRIMARY KEY"
            if ConstraintType.UNIQUE in col.constraints:
                col_def += " UNIQUE"
            if ConstraintType.NOT_NULL in col.constraints:
                col_def += " NOT NULL"
            column_defs.append(col_def)
        
        create_query += ", ".join(column_defs) + ")"
        db.execute(create_query)
        
        return RedirectResponse(url=f"/table/{table_name}", status_code=303)
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/insert/{table_name}", response_class=HTMLResponse)
async def insert_form(request: Request, table_name: str):
    """Show insert form"""
    try:
        table_info = db.get_table_info(table_name)
        if not table_info:
            raise HTTPException(status_code=404, detail="Table not found")
        
        return templates.TemplateResponse("insert.html", {
            "request": request,
            "table_name": table_name,
            "columns": table_info['columns']
        })
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.post("/insert/{table_name}")
async def insert_data(request: Request, table_name: str):
    """Insert data into table"""
    try:
        form_data = await request.form()
        data = {}
        
        for key, value in form_data.items():
            if key != 'table_name':
                # Convert data types
                col_info = db.get_table_info(table_name)
                if col_info:
                    for col in col_info['columns']:
                        if col['name'] == key:
                            if col['type'] == 'INT':
                                data[key] = int(value) if value else None
                            elif col['type'] == 'FLOAT':
                                data[key] = float(value) if value else None
                            elif col['type'] == 'BOOL':
                                data[key] = value.lower() == 'true'
                            else:
                                data[key] = value
                            break
        
        # Build INSERT query
        columns = list(data.keys())
        values = list(data.values())
        
        # Format values properly
        formatted_values = []
        for val in values:
            if isinstance(val, str):
                formatted_values.append(f"'{val}'")
            elif isinstance(val, bool):
                formatted_values.append('TRUE' if val else 'FALSE')
            else:
                formatted_values.append(str(val) if val is not None else 'NULL')
        
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(formatted_values)})"
        db.execute(query)
        
        return RedirectResponse(url=f"/table/{table_name}", status_code=303)
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/edit/{table_name}/{row_id}", response_class=HTMLResponse)
async def edit_form(request: Request, table_name: str, row_id: int):
    """Show edit form"""
    try:
        results = db.execute(f"SELECT * FROM {table_name} WHERE row_id = {row_id}")
        if not results:
            raise HTTPException(status_code=404, detail="Row not found")
        
        table_info = db.get_table_info(table_name)
        row_data = results[0]
        
        return templates.TemplateResponse("edit.html", {
            "request": request,
            "table_name": table_name,
            "row_id": row_id,
            "data": row_data,
            "columns": table_info['columns'] if table_info else []
        })
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.post("/update/{table_name}/{row_id}")
async def update_data(request: Request, table_name: str, row_id: int):
    """Update data in table"""
    try:
        form_data = await request.form()
        data = {}
        
        for key, value in form_data.items():
            if key not in ['table_name', 'row_id']:
                # Convert data types
                col_info = db.get_table_info(table_name)
                if col_info:
                    for col in col_info['columns']:
                        if col['name'] == key:
                            if col['type'] == 'INT':
                                data[key] = int(value) if value else None
                            elif col['type'] == 'FLOAT':
                                data[key] = float(value) if value else None
                            elif col['type'] == 'BOOL':
                                data[key] = value.lower() == 'true'
                            else:
                                data[key] = value
                            break
        
        # Build UPDATE query
        assignments = []
        for col, val in data.items():
            if isinstance(val, str):
                assignments.append(f"{col} = '{val}'")
            elif isinstance(val, bool):
                assignments.append(f"{col} = {'TRUE' if val else 'FALSE'}")
            else:
                assignments.append(f"{col} = {val if val is not None else 'NULL'}")
        
        query = f"UPDATE {table_name} SET {', '.join(assignments)} WHERE row_id = {row_id}"
        db.execute(query)
        
        return RedirectResponse(url=f"/table/{table_name}", status_code=303)
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/delete/{table_name}/{row_id}")
async def delete_data(request: Request, table_name: str, row_id: int):
    """Delete data from table"""
    try:
        query = f"DELETE FROM {table_name} WHERE row_id = {row_id}"
        db.execute(query)
        
        return RedirectResponse(url=f"/table/{table_name}", status_code=303)
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/query", response_class=HTMLResponse)
async def query_form(request: Request):
    """Show SQL query form"""
    return templates.TemplateResponse("query.html", {
        "request": request
    })

@app.post("/query")
async def execute_query(request: Request, query: str = Form(...)):
    """Execute custom SQL query"""
    try:
        results = db.execute(query)
        
        return templates.TemplateResponse("query_results.html", {
            "request": request,
            "query": query,
            "data": results,
            "columns": list(results[0].keys()) if results else []
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# API Endpoints
@app.post("/api/tables")
async def api_create_table(request: CreateTableRequest):
    """API endpoint to create table"""
    try:
        # Build CREATE TABLE query
        columns = []
        for col in request.columns:
            col_def = f"{col['name']} {col['type']}"
            if col.get('primary_key'):
                col_def += " PRIMARY KEY"
            if col.get('unique'):
                col_def += " UNIQUE"
            if col.get('not_null'):
                col_def += " NOT NULL"
            columns.append(col_def)
        
        query = f"CREATE TABLE {request.table_name} ({', '.join(columns)})"
        db.execute(query)
        
        return {"message": f"Table '{request.table_name}' created successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/insert")
async def api_insert(request: InsertRequest):
    """API endpoint to insert data"""
    try:
        columns = list(request.data.keys())
        values = list(request.data.values())
        
        # Format values
        formatted_values = []
        for val in values:
            if isinstance(val, str):
                formatted_values.append(f"'{val}'")
            elif isinstance(val, bool):
                formatted_values.append('TRUE' if val else 'FALSE')
            else:
                formatted_values.append(str(val) if val is not None else 'NULL')
        
        query = f"INSERT INTO {request.table_name} ({', '.join(columns)}) VALUES ({', '.join(formatted_values)})"
        db.execute(query)
        
        return {"message": "Data inserted successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/query")
async def api_query(request: QueryRequest):
    """API endpoint to execute queries"""
    try:
        results = db.execute(request.query)
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/tables")
async def api_list_tables():
    """API endpoint to list tables"""
    try:
        tables = db.list_tables()
        table_info = []
        for table_name in tables:
            info = db.get_table_info(table_name)
            if info:
                table_info.append(info)
        return {"tables": table_info}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/tables/{table_name}")
async def api_get_table(table_name: str):
    """API endpoint to get table data"""
    try:
        results = db.execute(f"SELECT * FROM {table_name}")
        table_info = db.get_table_info(table_name)
        return {
            "data": results,
            "info": table_info
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
