#!/usr/bin/env python
"""
Google Spreadsheet MCP Server
A Model Context Protocol (MCP) server built with FastMCP for interacting with Google Sheets.
"""

import base64
import os
import sys
from typing import List, Dict, Any, Optional, Union
import json
from dataclasses import dataclass
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

# MCP imports
from mcp.server.fastmcp import FastMCP, Context
from mcp.types import ToolAnnotations

# Google API imports
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import google.auth

# Constants
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
CREDENTIALS_CONFIG = os.environ.get('CREDENTIALS_CONFIG')
TOKEN_PATH = os.environ.get('TOKEN_PATH', 'token.json')
CREDENTIALS_PATH = os.environ.get('CREDENTIALS_PATH', 'credentials.json')
SERVICE_ACCOUNT_PATH = os.environ.get('SERVICE_ACCOUNT_PATH', 'service_account.json')
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID', '')  # Working directory in Google Drive

# Tool filtering configuration
# Parse enabled tools from environment variable or command-line argument
def _parse_enabled_tools() -> Optional[set]:
    """
    Parse enabled tools from ENABLED_TOOLS environment variable or --include-tools argument.
    Returns None if all tools should be enabled (default behavior).
    Returns a set of tool names if filtering is requested.
    """
    # Check command-line arguments first
    enabled_tools_str = None
    for i, arg in enumerate(sys.argv):
        if arg == '--include-tools' and i + 1 < len(sys.argv):
            enabled_tools_str = sys.argv[i + 1]
            break
    
    # Fall back to environment variable
    if not enabled_tools_str:
        enabled_tools_str = os.environ.get('ENABLED_TOOLS')
    
    if not enabled_tools_str:
        return None  # No filtering, enable all tools
    
    # Parse comma-separated list and normalize
    tools = {tool.strip() for tool in enabled_tools_str.split(',') if tool.strip()}
    return tools if tools else None

ENABLED_TOOLS = _parse_enabled_tools()

@dataclass
class SpreadsheetContext:
    """Context for Google Spreadsheet service"""
    sheets_service: Any
    drive_service: Any
    folder_id: Optional[str] = None


@asynccontextmanager
async def spreadsheet_lifespan(server: FastMCP) -> AsyncIterator[SpreadsheetContext]:
    """Manage Google Spreadsheet API connection lifecycle"""
    # Authenticate and build the service
    creds = None

    if CREDENTIALS_CONFIG:
        creds = service_account.Credentials.from_service_account_info(json.loads(base64.b64decode(CREDENTIALS_CONFIG)), scopes=SCOPES)
    
    # Check for explicit service account authentication first (custom SERVICE_ACCOUNT_PATH)
    if not creds and SERVICE_ACCOUNT_PATH and os.path.exists(SERVICE_ACCOUNT_PATH):
        try:
            # Regular service account authentication
            creds = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_PATH,
                scopes=SCOPES
            )
            print("Using service account authentication")
            print(f"Working with Google Drive folder ID: {DRIVE_FOLDER_ID or 'Not specified'}")
        except Exception as e:
            print(f"Error using service account authentication: {e}")
            creds = None
    
    # Fall back to OAuth flow if service account auth failed or not configured
    if not creds:
        print("Trying OAuth authentication flow")
        if os.path.exists(TOKEN_PATH):
            with open(TOKEN_PATH, 'r') as token:
                creds = Credentials.from_authorized_user_info(json.load(token), SCOPES)
                
        # If credentials are not valid or don't exist, get new ones
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    print("Attempting to refresh expired token...")
                    creds.refresh(Request())
                    print("Token refreshed successfully")
                    # Save the refreshed token
                    with open(TOKEN_PATH, 'w') as token:
                        token.write(creds.to_json())
                except Exception as refresh_error:
                    print(f"Token refresh failed: {refresh_error}")
                    print("Triggering reauthentication flow...")
                    creds = None  # Clear creds to trigger OAuth flow below

            # If refresh failed or creds don't exist, run OAuth flow
            if not creds:
                try:
                    flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
                    creds = flow.run_local_server(port=0)

                    # Save the credentials for the next run
                    with open(TOKEN_PATH, 'w') as token:
                        token.write(creds.to_json())
                    print("Successfully authenticated using OAuth flow")
                except Exception as e:
                    print(f"Error with OAuth flow: {e}")
                    creds = None
    
    # Try Application Default Credentials if no creds thus far
    # This will automatically check GOOGLE_APPLICATION_CREDENTIALS, gcloud auth, and metadata service
    if not creds:
        try:
            print("Attempting to use Application Default Credentials (ADC)")
            print("ADC will check: GOOGLE_APPLICATION_CREDENTIALS, gcloud auth, and metadata service")
            creds, project = google.auth.default(
                scopes=SCOPES
            )
            print(f"Successfully authenticated using ADC for project: {project}")
        except Exception as e:
            print(f"Error using Application Default Credentials: {e}")
            raise Exception("All authentication methods failed. Please configure credentials.")
    
    # Build the services
    sheets_service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)
    
    try:
        # Provide the service in the context
        yield SpreadsheetContext(
            sheets_service=sheets_service,
            drive_service=drive_service,
            folder_id=DRIVE_FOLDER_ID if DRIVE_FOLDER_ID else None
        )
    finally:
        # No explicit cleanup needed for Google APIs
        pass


# Initialize the MCP server with lifespan management
# Resolve host/port from environment variables with flexible names
_resolved_host = os.environ.get('HOST') or os.environ.get('FASTMCP_HOST') or "0.0.0.0"
_resolved_port_str = os.environ.get('PORT') or os.environ.get('FASTMCP_PORT') or "8000"
try:
    _resolved_port = int(_resolved_port_str)
except ValueError:
    _resolved_port = 8000

# Initialize the MCP server with explicit host/port to ensure binding as configured
mcp = FastMCP("Google Spreadsheet",
              dependencies=["google-auth", "google-auth-oauthlib", "google-api-python-client"],
              lifespan=spreadsheet_lifespan,
              host=_resolved_host,
              port=_resolved_port)


def tool(annotations: Optional[ToolAnnotations] = None):
    """
    Conditional tool decorator that only registers tools if they're enabled.
    
    This wrapper checks ENABLED_TOOLS configuration and only applies the @mcp.tool
    decorator if the tool should be enabled. If ENABLED_TOOLS is None (default),
    all tools are enabled.
    
    Args:
        annotations: Optional ToolAnnotations for the tool
    
    Returns:
        Decorator function
    """
    def decorator(func):
        tool_name = func.__name__
        
        # If no filtering is configured, or if this tool is in the enabled list
        if ENABLED_TOOLS is None or tool_name in ENABLED_TOOLS:
            # Apply the mcp.tool decorator
            if annotations:
                return mcp.tool(annotations=annotations)(func)
            else:
                return mcp.tool()(func)
        else:
            # Don't register this tool - return the function undecorated
            return func
    
    return decorator


@tool(
    annotations=ToolAnnotations(
        title="Get Sheet Data",
        readOnlyHint=True,
    ),
)
def get_sheet_data(spreadsheet_id: str,
                   sheet: str,
                   range: Optional[str] = None,
                   include_grid_data: bool = False,
                   ctx: Context = None) -> Dict[str, Any]:
    """
    Get data from a specific sheet in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        range: Optional cell range in A1 notation (e.g., 'A1:C10'). If not provided, gets all data.
        include_grid_data: If True, includes cell formatting and other metadata in the response.
            Note: Setting this to True will significantly increase the response size and token usage
            when parsing the response, as it includes detailed cell formatting information.
            Default is False (returns values only, more efficient).
    
    Returns:
        Grid data structure with either full metadata or just values from Google Sheets API, depending on include_grid_data parameter
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    # Construct the range - keep original API behavior
    if range:
        full_range = f"{sheet}!{range}"
    else:
        full_range = sheet
    
    if include_grid_data:
        # Use full API to get all grid data including formatting
        result = sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            ranges=[full_range],
            includeGridData=True
        ).execute()
    else:
        # Use values API to get cell values only (more efficient)
        values_result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=full_range
        ).execute()
        
        # Format the response to match expected structure
        result = {
            'spreadsheetId': spreadsheet_id,
            'valueRanges': [{
                'range': full_range,
                'values': values_result.get('values', [])
            }]
        }

    return result

@tool(
    annotations=ToolAnnotations(
        title="Get Sheet Formulas",
        readOnlyHint=True,
    ),
)
def get_sheet_formulas(spreadsheet_id: str,
                       sheet: str,
                       range: Optional[str] = None,
                       ctx: Context = None) -> List[List[Any]]:
    """
    Get formulas from a specific sheet in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        range: Optional cell range in A1 notation (e.g., 'A1:C10'). If not provided, gets all formulas from the sheet.
    
    Returns:
        A 2D array of the sheet formulas.
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    
    # Construct the range
    if range:
        full_range = f"{sheet}!{range}"
    else:
        full_range = sheet  # Get all formulas in the specified sheet
    
    # Call the Sheets API
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=full_range,
        valueRenderOption='FORMULA'  # Request formulas
    ).execute()
    
    # Get the formulas from the response
    formulas = result.get('values', [])
    return formulas

@tool(
    annotations=ToolAnnotations(
        title="Update Cells",
        destructiveHint=True,
    ),
)
def update_cells(spreadsheet_id: str,
                sheet: str,
                range: str,
                data: List[List[Any]],
                ctx: Context = None) -> Dict[str, Any]:
    """
    Update cells in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        range: Cell range in A1 notation (e.g., 'A1:C10')
        data: 2D array of values to update
    
    Returns:
        Result of the update operation
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    
    # Construct the range
    full_range = f"{sheet}!{range}"
    
    # Prepare the value range object
    value_range_body = {
        'values': data
    }
    
    # Call the Sheets API to update values
    result = sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=full_range,
        valueInputOption='USER_ENTERED',
        body=value_range_body
    ).execute()
    
    return result


@tool(
    annotations=ToolAnnotations(
        title="Batch Update Cells",
        destructiveHint=True,
    ),
)
def batch_update_cells(spreadsheet_id: str,
                       sheet: str,
                       ranges: Dict[str, List[List[Any]]],
                       ctx: Context = None) -> Dict[str, Any]:
    """
    Batch update multiple ranges in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        ranges: Dictionary mapping range strings to 2D arrays of values
               e.g., {'A1:B2': [[1, 2], [3, 4]], 'D1:E2': [['a', 'b'], ['c', 'd']]}
    
    Returns:
        Result of the batch update operation
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    
    # Prepare the batch update request
    data = []
    for range_str, values in ranges.items():
        full_range = f"{sheet}!{range_str}"
        data.append({
            'range': full_range,
            'values': values
        })
    
    batch_body = {
        'valueInputOption': 'USER_ENTERED',
        'data': data
    }
    
    # Call the Sheets API to perform batch update
    result = sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=batch_body
    ).execute()
    
    return result


@tool(
    annotations=ToolAnnotations(
        title="Add Rows",
        destructiveHint=True,
    ),
)
def add_rows(spreadsheet_id: str,
             sheet: str,
             count: int,
             start_row: Optional[int] = None,
             ctx: Context = None) -> Dict[str, Any]:
    """
    Add rows to a sheet in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        count: Number of rows to add
        start_row: 0-based row index to start adding. If not provided, adds at the beginning.
    
    Returns:
        Result of the operation
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    
    # Get sheet ID
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    
    for s in spreadsheet['sheets']:
        if s['properties']['title'] == sheet:
            sheet_id = s['properties']['sheetId']
            break
            
    if sheet_id is None:
        return {"error": f"Sheet '{sheet}' not found"}
    
    # Prepare the insert rows request
    request_body = {
        "requests": [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_row if start_row is not None else 0,
                        "endIndex": (start_row if start_row is not None else 0) + count
                    },
                    "inheritFromBefore": start_row is not None and start_row > 0
                }
            }
        ]
    }
    
    # Execute the request
    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=request_body
    ).execute()
    
    return result


@tool(
    annotations=ToolAnnotations(
        title="Add Columns",
        destructiveHint=True,
    ),
)
def add_columns(spreadsheet_id: str,
                sheet: str,
                count: int,
                start_column: Optional[int] = None,
                ctx: Context = None) -> Dict[str, Any]:
    """
    Add columns to a sheet in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: The name of the sheet
        count: Number of columns to add
        start_column: 0-based column index to start adding. If not provided, adds at the beginning.
    
    Returns:
        Result of the operation
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    
    # Get sheet ID
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = None
    
    for s in spreadsheet['sheets']:
        if s['properties']['title'] == sheet:
            sheet_id = s['properties']['sheetId']
            break
            
    if sheet_id is None:
        return {"error": f"Sheet '{sheet}' not found"}
    
    # Prepare the insert columns request
    request_body = {
        "requests": [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": start_column if start_column is not None else 0,
                        "endIndex": (start_column if start_column is not None else 0) + count
                    },
                    "inheritFromBefore": start_column is not None and start_column > 0
                }
            }
        ]
    }
    
    # Execute the request
    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=request_body
    ).execute()
    
    return result


@tool(
    annotations=ToolAnnotations(
        title="List Sheets",
        readOnlyHint=True,
    ),
)
def list_sheets(spreadsheet_id: str, ctx: Context = None) -> List[str]:
    """
    List all sheets in a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
    
    Returns:
        List of sheet names
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    
    # Get spreadsheet metadata
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    
    # Extract sheet names
    sheet_names = [sheet['properties']['title'] for sheet in spreadsheet['sheets']]
    
    return sheet_names


def _get_sheet_id_map(sheets_service, spreadsheet_id: str) -> Dict[int, str]:
    """Return a mapping of sheetId -> sheet title for a spreadsheet."""
    spreadsheet = sheets_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields='sheets(properties(sheetId,title))'
    ).execute()
    return {
        s['properties']['sheetId']: s['properties']['title']
        for s in spreadsheet.get('sheets', [])
    }


def _grid_range_to_a1(grid_range: Dict[str, Any], sheet_title: str) -> str:
    """Convert a GridRange dict to A1 notation like 'Sheet1!A1:C10'."""
    start_col = grid_range.get('startColumnIndex', 0)
    end_col = grid_range.get('endColumnIndex')
    start_row = grid_range.get('startRowIndex', 0)
    end_row = grid_range.get('endRowIndex')

    start = f"{_column_index_to_letter(start_col)}{start_row + 1}"
    if end_col is not None and end_row is not None:
        end = f"{_column_index_to_letter(end_col - 1)}{end_row}"
        return f"{sheet_title}!{start}:{end}"
    return f"{sheet_title}!{start}"


# ---------------------------------------------------------------------------
# Named Range Tools
# ---------------------------------------------------------------------------


@tool(
    annotations=ToolAnnotations(
        title="List Named Ranges",
        readOnlyHint=True,
    ),
)
def list_named_ranges(spreadsheet_id: str,
                      ctx: Context = None) -> List[Dict[str, Any]]:
    """
    List all named ranges in a Google Spreadsheet.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)

    Returns:
        List of named ranges with their name, ID, sheet title, and cell coordinates
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    spreadsheet = sheets_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields='namedRanges,sheets(properties(sheetId,title))'
    ).execute()

    sheet_map = {
        s['properties']['sheetId']: s['properties']['title']
        for s in spreadsheet.get('sheets', [])
    }

    result = []
    for nr in spreadsheet.get('namedRanges', []):
        grid_range = nr.get('range', {})
        sheet_id = grid_range.get('sheetId')
        sheet_title = sheet_map.get(sheet_id)
        a1 = _grid_range_to_a1(grid_range, sheet_title) if sheet_title else None
        result.append({
            'namedRangeId': nr.get('namedRangeId'),
            'name': nr.get('name'),
            'sheetId': sheet_id,
            'sheetTitle': sheet_title,
            'a1Range': a1,
            'startRowIndex': grid_range.get('startRowIndex'),
            'endRowIndex': grid_range.get('endRowIndex'),
            'startColumnIndex': grid_range.get('startColumnIndex'),
            'endColumnIndex': grid_range.get('endColumnIndex'),
        })

    return result


@tool(
    annotations=ToolAnnotations(
        title="Get Named Range Data",
        readOnlyHint=True,
    ),
)
def get_named_range_data(spreadsheet_id: str,
                         named_range: str,
                         ctx: Context = None) -> Dict[str, Any]:
    """
    Get data from a named range in a Google Spreadsheet.

    Named ranges are passed directly to the API without a sheet prefix.
    Use list_named_ranges to discover available named range names.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        named_range: The name of the named range (e.g., 'MyNamedRange')

    Returns:
        Values from the named range
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=named_range
    ).execute()

    return {
        'spreadsheetId': spreadsheet_id,
        'namedRange': named_range,
        'range': result.get('range'),
        'values': result.get('values', [])
    }


@tool(
    annotations=ToolAnnotations(
        title="Get Named Range Formulas",
        readOnlyHint=True,
    ),
)
def get_named_range_formulas(spreadsheet_id: str,
                             named_range: str,
                             ctx: Context = None) -> Dict[str, Any]:
    """
    Get formulas from a named range in a Google Spreadsheet.

    Returns the raw formulas rather than computed values.
    Use list_named_ranges to discover available named range names.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        named_range: The name of the named range (e.g., 'MyNamedRange')

    Returns:
        Formulas from the named range
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=named_range,
        valueRenderOption='FORMULA'
    ).execute()

    return {
        'spreadsheetId': spreadsheet_id,
        'namedRange': named_range,
        'range': result.get('range'),
        'values': result.get('values', [])
    }


@tool(
    annotations=ToolAnnotations(
        title="Update Named Range Data",
        destructiveHint=True,
    ),
)
def update_named_range_data(spreadsheet_id: str,
                            named_range: str,
                            data: List[List[Any]],
                            ctx: Context = None) -> Dict[str, Any]:
    """
    Update cells in a named range in a Google Spreadsheet.

    Use list_named_ranges to discover available named range names.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        named_range: The name of the named range (e.g., 'MyNamedRange')
        data: 2D array of values to write

    Returns:
        Result of the update operation
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    result = sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=named_range,
        valueInputOption='USER_ENTERED',
        body={'values': data}
    ).execute()

    return result


@tool(
    annotations=ToolAnnotations(
        title="Create Named Range",
        destructiveHint=True,
    ),
)
def create_named_range(spreadsheet_id: str,
                       name: str,
                       sheet: str,
                       start_row: int,
                       end_row: int,
                       start_column: int,
                       end_column: int,
                       ctx: Context = None) -> Dict[str, Any]:
    """
    Create a new named range in a Google Spreadsheet.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        name: Name for the new named range (e.g., 'MyRange')
        sheet: Name of the sheet/tab containing the range
        start_row: 0-based start row index (inclusive)
        end_row: 0-based end row index (exclusive)
        start_column: 0-based start column index (inclusive)
        end_column: 0-based end column index (exclusive)

    Returns:
        Result including the new named range ID
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    # Resolve sheet name to sheetId
    sheet_map = _get_sheet_id_map(sheets_service, spreadsheet_id)
    sheet_id = None
    for sid, title in sheet_map.items():
        if title == sheet:
            sheet_id = sid
            break

    if sheet_id is None:
        return {"error": f"Sheet '{sheet}' not found"}

    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [{
                "addNamedRange": {
                    "namedRange": {
                        "name": name,
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": start_row,
                            "endRowIndex": end_row,
                            "startColumnIndex": start_column,
                            "endColumnIndex": end_column,
                        }
                    }
                }
            }]
        }
    ).execute()

    return result


@tool(
    annotations=ToolAnnotations(
        title="Delete Named Range",
        destructiveHint=True,
    ),
)
def delete_named_range(spreadsheet_id: str,
                       named_range_id: str,
                       ctx: Context = None) -> Dict[str, Any]:
    """
    Delete a named range from a Google Spreadsheet.

    Use list_named_ranges to find the namedRangeId.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        named_range_id: The ID of the named range to delete (from list_named_ranges)

    Returns:
        Result of the delete operation
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [{
                "deleteNamedRange": {
                    "namedRangeId": named_range_id
                }
            }]
        }
    ).execute()

    return result


# ---------------------------------------------------------------------------
# Table Tools (Native Google Sheets Tables API, added April 2025)
# ---------------------------------------------------------------------------


def _get_tables(sheets_service, spreadsheet_id: str) -> List[Dict[str, Any]]:
    """Fetch all native tables from a spreadsheet with sheet title context."""
    spreadsheet = sheets_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields='sheets(properties(sheetId,title),tables)'
    ).execute()

    tables = []
    for sheet in spreadsheet.get('sheets', []):
        sheet_title = sheet.get('properties', {}).get('title')
        for table in sheet.get('tables', []):
            table['_sheetTitle'] = sheet_title
            tables.append(table)
    return tables


def _find_table(sheets_service, spreadsheet_id: str,
                table_id: Optional[str] = None,
                table_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Find a specific table by ID or name. Returns the table dict or None."""
    tables = _get_tables(sheets_service, spreadsheet_id)
    for t in tables:
        if table_id and t.get('tableId') == table_id:
            return t
        if table_name and t.get('name') == table_name:
            return t
    return None


@tool(
    annotations=ToolAnnotations(
        title="List Tables",
        readOnlyHint=True,
    ),
)
def list_tables(spreadsheet_id: str,
                ctx: Context = None) -> List[Dict[str, Any]]:
    """
    List all native tables in a Google Spreadsheet.

    Native tables are created via Format > Convert to table in Google Sheets.
    They have typed columns, auto-expanding rows, and structured metadata.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)

    Returns:
        List of tables with name, ID, sheet title, range, and column definitions
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    tables = _get_tables(sheets_service, spreadsheet_id)

    result = []
    for table in tables:
        grid_range = table.get('range', {})
        sheet_title = table.get('_sheetTitle')
        a1 = _grid_range_to_a1(grid_range, sheet_title) if sheet_title else None

        columns = []
        for col in table.get('columns', []):
            columns.append({
                'name': col.get('name'),
                'type': col.get('columnType'),
            })

        result.append({
            'tableId': table.get('tableId'),
            'name': table.get('name'),
            'sheetTitle': sheet_title,
            'a1Range': a1,
            'columns': columns,
        })

    return result


@tool(
    annotations=ToolAnnotations(
        title="Get Table Data",
        readOnlyHint=True,
    ),
)
def get_table_data(spreadsheet_id: str,
                   table_name: Optional[str] = None,
                   table_id: Optional[str] = None,
                   ctx: Context = None) -> Dict[str, Any]:
    """
    Get data from a native table in a Google Spreadsheet.

    Provide either table_name or table_id. Use list_tables to discover tables.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        table_name: Name of the table
        table_id: ID of the table (from list_tables)

    Returns:
        Table data including column headers and row values
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    if not table_name and not table_id:
        return {"error": "Must provide either 'table_name' or 'table_id'"}

    table = _find_table(sheets_service, spreadsheet_id,
                        table_id=table_id, table_name=table_name)
    if not table:
        return {"error": f"Table not found (name={table_name}, id={table_id})"}

    grid_range = table.get('range', {})
    sheet_title = table.get('_sheetTitle')
    a1 = _grid_range_to_a1(grid_range, sheet_title)

    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=a1
    ).execute()

    return {
        'spreadsheetId': spreadsheet_id,
        'tableId': table.get('tableId'),
        'tableName': table.get('name'),
        'range': a1,
        'values': result.get('values', [])
    }


@tool(
    annotations=ToolAnnotations(
        title="Get Table Formulas",
        readOnlyHint=True,
    ),
)
def get_table_formulas(spreadsheet_id: str,
                       table_name: Optional[str] = None,
                       table_id: Optional[str] = None,
                       ctx: Context = None) -> Dict[str, Any]:
    """
    Get formulas from a native table in a Google Spreadsheet.

    Returns the raw formulas rather than computed values.
    Provide either table_name or table_id. Use list_tables to discover tables.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        table_name: Name of the table
        table_id: ID of the table (from list_tables)

    Returns:
        Table formulas including column headers and row formulas
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    if not table_name and not table_id:
        return {"error": "Must provide either 'table_name' or 'table_id'"}

    table = _find_table(sheets_service, spreadsheet_id,
                        table_id=table_id, table_name=table_name)
    if not table:
        return {"error": f"Table not found (name={table_name}, id={table_id})"}

    grid_range = table.get('range', {})
    sheet_title = table.get('_sheetTitle')
    a1 = _grid_range_to_a1(grid_range, sheet_title)

    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=a1,
        valueRenderOption='FORMULA'
    ).execute()

    return {
        'spreadsheetId': spreadsheet_id,
        'tableId': table.get('tableId'),
        'tableName': table.get('name'),
        'range': a1,
        'values': result.get('values', [])
    }


@tool(
    annotations=ToolAnnotations(
        title="Update Table Data",
        destructiveHint=True,
    ),
)
def update_table_data(spreadsheet_id: str,
                      data: List[List[Any]],
                      table_name: Optional[str] = None,
                      table_id: Optional[str] = None,
                      ctx: Context = None) -> Dict[str, Any]:
    """
    Update data in a native table in a Google Spreadsheet.

    Writes the provided 2D array to the table's range, overwriting existing data.
    Provide either table_name or table_id. Use list_tables to discover tables.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        data: 2D array of values to write (should match table dimensions)
        table_name: Name of the table
        table_id: ID of the table (from list_tables)

    Returns:
        Result of the update operation
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    if not table_name and not table_id:
        return {"error": "Must provide either 'table_name' or 'table_id'"}

    table = _find_table(sheets_service, spreadsheet_id,
                        table_id=table_id, table_name=table_name)
    if not table:
        return {"error": f"Table not found (name={table_name}, id={table_id})"}

    grid_range = table.get('range', {})
    sheet_title = table.get('_sheetTitle')
    a1 = _grid_range_to_a1(grid_range, sheet_title)

    result = sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=a1,
        valueInputOption='USER_ENTERED',
        body={'values': data}
    ).execute()

    return result


@tool(
    annotations=ToolAnnotations(
        title="Append Table Rows",
        destructiveHint=True,
    ),
)
def append_table_rows(spreadsheet_id: str,
                      rows: List[List[Any]],
                      table_name: Optional[str] = None,
                      table_id: Optional[str] = None,
                      ctx: Context = None) -> Dict[str, Any]:
    """
    Append rows to a native table in a Google Spreadsheet.

    The table automatically expands to include the new rows.
    Provide either table_name or table_id. Use list_tables to discover tables.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        rows: 2D array of row values to append
        table_name: Name of the table
        table_id: ID of the table (from list_tables)

    Returns:
        Result of the append operation
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    if not table_name and not table_id:
        return {"error": "Must provide either 'table_name' or 'table_id'"}

    table = _find_table(sheets_service, spreadsheet_id,
                        table_id=table_id, table_name=table_name)
    if not table:
        return {"error": f"Table not found (name={table_name}, id={table_id})"}

    grid_range = table.get('range', {})
    sheet_title = table.get('_sheetTitle')
    a1 = _grid_range_to_a1(grid_range, sheet_title)

    result = sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=a1,
        valueInputOption='USER_ENTERED',
        insertDataOption='INSERT_ROWS',
        body={'values': rows}
    ).execute()

    return result


@tool(
    annotations=ToolAnnotations(
        title="Create Table",
        destructiveHint=True,
    ),
)
def create_table(spreadsheet_id: str,
                 sheet: str,
                 start_row: int,
                 end_row: int,
                 start_column: int,
                 end_column: int,
                 name: Optional[str] = None,
                 ctx: Context = None) -> Dict[str, Any]:
    """
    Create a new native table in a Google Spreadsheet.

    Converts the specified range into a structured table. The first row of
    the range is used as column headers.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        sheet: Name of the sheet/tab containing the range
        start_row: 0-based start row index (inclusive)
        end_row: 0-based end row index (exclusive)
        start_column: 0-based start column index (inclusive)
        end_column: 0-based end column index (exclusive)
        name: Optional name for the table

    Returns:
        Result including the new table metadata
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    sheet_map = _get_sheet_id_map(sheets_service, spreadsheet_id)
    sheet_id = None
    for sid, title in sheet_map.items():
        if title == sheet:
            sheet_id = sid
            break

    if sheet_id is None:
        return {"error": f"Sheet '{sheet}' not found"}

    add_table_request = {
        "range": {
            "sheetId": sheet_id,
            "startRowIndex": start_row,
            "endRowIndex": end_row,
            "startColumnIndex": start_column,
            "endColumnIndex": end_column,
        }
    }
    if name:
        add_table_request["name"] = name

    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [{
                "addTable": add_table_request
            }]
        }
    ).execute()

    return result


@tool(
    annotations=ToolAnnotations(
        title="Delete Table",
        destructiveHint=True,
    ),
)
def delete_table(spreadsheet_id: str,
                 table_id: str,
                 ctx: Context = None) -> Dict[str, Any]:
    """
    Delete a native table from a Google Spreadsheet.

    This removes the table structure but does not delete the underlying data.
    Use list_tables to find the tableId.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        table_id: The ID of the table to delete (from list_tables)

    Returns:
        Result of the delete operation
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service

    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [{
                "deleteTable": {
                    "tableId": table_id
                }
            }]
        }
    ).execute()

    return result


@tool(
    annotations=ToolAnnotations(
        title="Copy Sheet",
        destructiveHint=True,
    ),
)
def copy_sheet(src_spreadsheet: str,
               src_sheet: str,
               dst_spreadsheet: str,
               dst_sheet: str,
               ctx: Context = None) -> Dict[str, Any]:
    """
    Copy a sheet from one spreadsheet to another.
    
    Args:
        src_spreadsheet: Source spreadsheet ID
        src_sheet: Source sheet name
        dst_spreadsheet: Destination spreadsheet ID
        dst_sheet: Destination sheet name
    
    Returns:
        Result of the operation
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    
    # Get source sheet ID
    src = sheets_service.spreadsheets().get(spreadsheetId=src_spreadsheet).execute()
    src_sheet_id = None
    
    for s in src['sheets']:
        if s['properties']['title'] == src_sheet:
            src_sheet_id = s['properties']['sheetId']
            break
            
    if src_sheet_id is None:
        return {"error": f"Source sheet '{src_sheet}' not found"}
    
    # Copy the sheet to destination spreadsheet
    copy_result = sheets_service.spreadsheets().sheets().copyTo(
        spreadsheetId=src_spreadsheet,
        sheetId=src_sheet_id,
        body={
            "destinationSpreadsheetId": dst_spreadsheet
        }
    ).execute()
    
    # If destination sheet name is different from the default copied name, rename it
    if 'title' in copy_result and copy_result['title'] != dst_sheet:
        # Get the ID of the newly copied sheet
        copy_sheet_id = copy_result['sheetId']
        
        # Rename the copied sheet
        rename_request = {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": copy_sheet_id,
                            "title": dst_sheet
                        },
                        "fields": "title"
                    }
                }
            ]
        }
        
        rename_result = sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=dst_spreadsheet,
            body=rename_request
        ).execute()
        
        return {
            "copy": copy_result,
            "rename": rename_result
        }
    
    return {"copy": copy_result}


@tool(
    annotations=ToolAnnotations(
        title="Rename Sheet",
        destructiveHint=True,
    ),
)
def rename_sheet(spreadsheet: str,
                 sheet: str,
                 new_name: str,
                 ctx: Context = None) -> Dict[str, Any]:
    """
    Rename a sheet in a Google Spreadsheet.
    
    Args:
        spreadsheet: Spreadsheet ID
        sheet: Current sheet name
        new_name: New sheet name
    
    Returns:
        Result of the operation
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    
    # Get sheet ID
    spreadsheet_data = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet).execute()
    sheet_id = None
    
    for s in spreadsheet_data['sheets']:
        if s['properties']['title'] == sheet:
            sheet_id = s['properties']['sheetId']
            break
            
    if sheet_id is None:
        return {"error": f"Sheet '{sheet}' not found"}
    
    # Prepare the rename request
    request_body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "title": new_name
                    },
                    "fields": "title"
                }
            }
        ]
    }
    
    # Execute the request
    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet,
        body=request_body
    ).execute()
    
    return result


@tool(
    annotations=ToolAnnotations(
        title="Get Multiple Sheet Data",
        readOnlyHint=True,
    ),
)
def get_multiple_sheet_data(queries: List[Dict[str, str]],
                            ctx: Context = None) -> List[Dict[str, Any]]:
    """
    Get data from multiple specific ranges in Google Spreadsheets.
    
    Args:
        queries: A list of dictionaries, each specifying a query. 
                 Each dictionary should have 'spreadsheet_id', 'sheet', and 'range' keys.
                 Example: [{'spreadsheet_id': 'abc', 'sheet': 'Sheet1', 'range': 'A1:B5'}, 
                           {'spreadsheet_id': 'xyz', 'sheet': 'Data', 'range': 'C1:C10'}]
    
    Returns:
        A list of dictionaries, each containing the original query parameters 
        and the fetched 'data' or an 'error'.
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    results = []
    
    for query in queries:
        spreadsheet_id = query.get('spreadsheet_id')
        sheet = query.get('sheet')
        range_str = query.get('range')
        
        if not all([spreadsheet_id, sheet, range_str]):
            results.append({**query, 'error': 'Missing required keys (spreadsheet_id, sheet, range)'})
            continue

        try:
            # Construct the range
            full_range = f"{sheet}!{range_str}"
            
            # Call the Sheets API
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=full_range
            ).execute()
            
            # Get the values from the response
            values = result.get('values', [])
            results.append({**query, 'data': values})

        except Exception as e:
            results.append({**query, 'error': str(e)})
            
    return results


@tool(
    annotations=ToolAnnotations(
        title="Get Multiple Spreadsheet Summary",
        readOnlyHint=True,
    ),
)
def get_multiple_spreadsheet_summary(spreadsheet_ids: List[str],
                                   rows_to_fetch: int = 5,
                                   ctx: Context = None) -> List[Dict[str, Any]]:
    """
    Get a summary of multiple Google Spreadsheets, including sheet names, 
    headers, and the first few rows of data for each sheet.
    
    Args:
        spreadsheet_ids: A list of spreadsheet IDs to summarize.
        rows_to_fetch: The number of rows (including header) to fetch for the summary (default: 5).
    
    Returns:
        A list of dictionaries, each representing a spreadsheet summary. 
        Includes spreadsheet title, sheet summaries (title, headers, first rows), or an error.
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    summaries = []
    
    for spreadsheet_id in spreadsheet_ids:
        summary_data = {
            'spreadsheet_id': spreadsheet_id,
            'title': None,
            'sheets': [],
            'error': None
        }
        try:
            # Get spreadsheet metadata
            spreadsheet = sheets_service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                fields='properties.title,sheets(properties(title,sheetId))'
            ).execute()
            
            summary_data['title'] = spreadsheet.get('properties', {}).get('title', 'Unknown Title')
            
            sheet_summaries = []
            for sheet in spreadsheet.get('sheets', []):
                sheet_title = sheet.get('properties', {}).get('title')
                sheet_id = sheet.get('properties', {}).get('sheetId')
                sheet_summary = {
                    'title': sheet_title,
                    'sheet_id': sheet_id,
                    'headers': [],
                    'first_rows': [],
                    'error': None
                }
                
                if not sheet_title:
                    sheet_summary['error'] = 'Sheet title not found'
                    sheet_summaries.append(sheet_summary)
                    continue
                    
                try:
                    # Fetch the first few rows (e.g., A1:Z5)
                    # Adjust range if fewer rows are requested
                    max_row = max(1, rows_to_fetch) # Ensure at least 1 row is fetched
                    range_to_get = f"{sheet_title}!A1:{max_row}" # Fetch all columns up to max_row
                    
                    result = sheets_service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=range_to_get
                    ).execute()
                    
                    values = result.get('values', [])
                    
                    if values:
                        sheet_summary['headers'] = values[0]
                        if len(values) > 1:
                            sheet_summary['first_rows'] = values[1:max_row]
                    else:
                        # Handle empty sheets or sheets with less data than requested
                        sheet_summary['headers'] = []
                        sheet_summary['first_rows'] = []

                except Exception as sheet_e:
                    sheet_summary['error'] = f'Error fetching data for sheet {sheet_title}: {sheet_e}'
                
                sheet_summaries.append(sheet_summary)
            
            summary_data['sheets'] = sheet_summaries
            
        except Exception as e:
            summary_data['error'] = f'Error fetching spreadsheet {spreadsheet_id}: {e}'
            
        summaries.append(summary_data)
        
    return summaries


@mcp.resource("spreadsheet://{spreadsheet_id}/info")
def get_spreadsheet_info(spreadsheet_id: str) -> str:
    """
    Get basic information about a Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet
    
    Returns:
        JSON string with spreadsheet information
    """
    # Access the context through mcp.get_lifespan_context() for resources
    context = mcp.get_lifespan_context()
    sheets_service = context.sheets_service
    
    # Get spreadsheet metadata
    spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    
    # Extract relevant information
    info = {
        "title": spreadsheet.get('properties', {}).get('title', 'Unknown'),
        "sheets": [
            {
                "title": sheet['properties']['title'],
                "sheetId": sheet['properties']['sheetId'],
                "gridProperties": sheet['properties'].get('gridProperties', {})
            }
            for sheet in spreadsheet.get('sheets', [])
        ]
    }
    
    return json.dumps(info, indent=2)


@tool(
    annotations=ToolAnnotations(
        title="Create Spreadsheet",
        destructiveHint=True,
    ),
)
def create_spreadsheet(title: str, folder_id: Optional[str] = None, ctx: Context = None) -> Dict[str, Any]:
    """
    Create a new Google Spreadsheet.
    
    Args:
        title: The title of the new spreadsheet
        folder_id: Optional Google Drive folder ID where the spreadsheet should be created.
                  If not provided, uses the configured default folder or creates in root.
    
    Returns:
        Information about the newly created spreadsheet including its ID
    """
    drive_service = ctx.request_context.lifespan_context.drive_service
    # Use provided folder_id or fall back to configured default
    target_folder_id = folder_id or ctx.request_context.lifespan_context.folder_id

    # Create the spreadsheet
    file_body = {
        'name': title,
        'mimeType': 'application/vnd.google-apps.spreadsheet',
    }
    if target_folder_id:
        file_body['parents'] = [target_folder_id]
    
    spreadsheet = drive_service.files().create(
        supportsAllDrives=True,
        body=file_body,
        fields='id, name, parents'
    ).execute()

    spreadsheet_id = spreadsheet.get('id')
    parents = spreadsheet.get('parents')
    folder_info = f" in folder {target_folder_id}" if target_folder_id else " in root"
    print(f"Spreadsheet created with ID: {spreadsheet_id}{folder_info}")

    return {
        'spreadsheetId': spreadsheet_id,
        'title': spreadsheet.get('name', title),
        'folder': parents[0] if parents else 'root',
    }


@tool(
    annotations=ToolAnnotations(
        title="Create Sheet",
        destructiveHint=True,
    ),
)
def create_sheet(spreadsheet_id: str,
                title: str,
                ctx: Context = None) -> Dict[str, Any]:
    """
    Create a new sheet tab in an existing Google Spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet
        title: The title for the new sheet
    
    Returns:
        Information about the newly created sheet
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    
    # Define the add sheet request
    request_body = {
        "requests": [
            {
                "addSheet": {
                    "properties": {
                        "title": title
                    }
                }
            }
        ]
    }
    
    # Execute the request
    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=request_body
    ).execute()
    
    # Extract the new sheet information
    new_sheet_props = result['replies'][0]['addSheet']['properties']
    
    return {
        'sheetId': new_sheet_props['sheetId'],
        'title': new_sheet_props['title'],
        'index': new_sheet_props.get('index'),
        'spreadsheetId': spreadsheet_id
    }


@tool(
    annotations=ToolAnnotations(
        title="List Spreadsheets",
        readOnlyHint=True,
    ),
)
def list_spreadsheets(folder_id: Optional[str] = None, ctx: Context = None) -> List[Dict[str, str]]:
    """
    List all spreadsheets in the specified Google Drive folder.
    If no folder is specified, uses the configured default folder or lists from 'My Drive'.
    
    Args:
        folder_id: Optional Google Drive folder ID to search in.
                  If not provided, uses the configured default folder or searches 'My Drive'.
    
    Returns:
        List of spreadsheets with their ID and title
    """
    drive_service = ctx.request_context.lifespan_context.drive_service
    # Use provided folder_id or fall back to configured default
    target_folder_id = folder_id or ctx.request_context.lifespan_context.folder_id
    
    query = "mimeType='application/vnd.google-apps.spreadsheet'"
    
    # If a specific folder is provided or configured, search only in that folder
    if target_folder_id:
        query += f" and '{target_folder_id}' in parents"
        print(f"Searching for spreadsheets in folder: {target_folder_id}")
    else:
        print("Searching for spreadsheets in 'My Drive'")
    
    # List spreadsheets
    results = drive_service.files().list(
        q=query,
        spaces='drive',
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        fields='files(id, name)',
        orderBy='modifiedTime desc'
    ).execute()
    
    spreadsheets = results.get('files', [])
    
    return [{'id': sheet['id'], 'title': sheet['name']} for sheet in spreadsheets]


@tool(
    annotations=ToolAnnotations(
        title="Share Spreadsheet",
        destructiveHint=True,
    ),
)
def share_spreadsheet(spreadsheet_id: str,
                      recipients: List[Dict[str, str]],
                      send_notification: bool = True,
                      ctx: Context = None) -> Dict[str, List[Dict[str, Any]]]:
    """
    Share a Google Spreadsheet with multiple users via email, assigning specific roles.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet to share.
        recipients: A list of dictionaries, each containing 'email_address' and 'role'.
                    The role should be one of: 'reader', 'commenter', 'writer'.
                    Example: [
                        {'email_address': 'user1@example.com', 'role': 'writer'},
                        {'email_address': 'user2@example.com', 'role': 'reader'}
                    ]
        send_notification: Whether to send a notification email to the users. Defaults to True.

    Returns:
        A dictionary containing lists of 'successes' and 'failures'. 
        Each item in the lists includes the email address and the outcome.
    """
    drive_service = ctx.request_context.lifespan_context.drive_service
    successes = []
    failures = []
    
    for recipient in recipients:
        email_address = recipient.get('email_address')
        role = recipient.get('role', 'writer') # Default to writer if role is missing for an entry
        
        if not email_address:
            failures.append({
                'email_address': None,
                'error': 'Missing email_address in recipient entry.'
            })
            continue
            
        if role not in ['reader', 'commenter', 'writer']:
             failures.append({
                'email_address': email_address,
                'error': f"Invalid role '{role}'. Must be 'reader', 'commenter', or 'writer'."
            })
             continue

        permission = {
            'type': 'user',
            'role': role,
            'emailAddress': email_address
        }
        
        try:
            result = drive_service.permissions().create(
                fileId=spreadsheet_id,
                body=permission,
                sendNotificationEmail=send_notification,
                fields='id'
            ).execute()
            successes.append({
                'email_address': email_address, 
                'role': role, 
                'permissionId': result.get('id')
            })
        except Exception as e:
            # Try to provide a more informative error message
            error_details = str(e)
            if hasattr(e, 'content'):
                try:
                    error_content = json.loads(e.content)
                    error_details = error_content.get('error', {}).get('message', error_details)
                except json.JSONDecodeError:
                    pass # Keep the original error string
            failures.append({
                'email_address': email_address,
                'error': f"Failed to share: {error_details}"
            })
            
    return {"successes": successes, "failures": failures}


@tool(
    annotations=ToolAnnotations(
        title="List Folders",
        readOnlyHint=True,
    ),
)
def list_folders(parent_folder_id: Optional[str] = None, ctx: Context = None) -> List[Dict[str, str]]:
    """
    List all folders in the specified Google Drive folder.
    If no parent folder is specified, lists folders from 'My Drive' root.
    
    Args:
        parent_folder_id: Optional Google Drive folder ID to search within.
                         If not provided, searches the root of 'My Drive'.
    
    Returns:
        List of folders with their ID, name, and parent information
    """
    drive_service = ctx.request_context.lifespan_context.drive_service
    
    query = "mimeType='application/vnd.google-apps.folder'"
    
    # If a specific parent folder is provided, search only within that folder
    if parent_folder_id:
        query += f" and '{parent_folder_id}' in parents"
        print(f"Searching for folders in parent folder: {parent_folder_id}")
    else:
        # Search in root of My Drive (folders that don't have any parent folders)
        query += " and 'root' in parents"
        print("Searching for folders in 'My Drive' root")
    
    # List folders
    results = drive_service.files().list(
        q=query,
        spaces='drive',
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        fields='files(id, name, parents)',
        orderBy='name'
    ).execute()
    
    folders = results.get('files', [])
    
    return [
        {
            'id': folder['id'], 
            'name': folder['name'],
            'parent': folder.get('parents', ['root'])[0] if folder.get('parents') else 'root'
        } 
        for folder in folders
    ]




@tool(
    annotations=ToolAnnotations(
        title="Search Spreadsheets by Name or Content",
        readOnlyHint=True,
    ),
)
def search_spreadsheets(query: str,
                        max_results: int = 20,
                        ctx: Context = None) -> List[Dict[str, Any]]:
    """
    Search for spreadsheets in Google Drive by name or content.

    Args:
        query: Search query string. Searches in file name and content.
               Examples: "budget 2024", "sales report", "project tracker"
        max_results: Maximum number of results to return (default 20, max 100)

    Returns:
        List of matching spreadsheets with their ID, name, and metadata
    """
    drive_service = ctx.request_context.lifespan_context.drive_service

    # Limit max_results to reasonable bounds
    max_results = min(max(1, max_results), 100)

    # Build the search query for Google Drive
    # Search only for spreadsheets and match the query in name or fullText
    search_query = (
        f"mimeType='application/vnd.google-apps.spreadsheet' and "
        f"(name contains '{query}' or fullText contains '{query}')"
    )

    try:
        results = drive_service.files().list(
            q=search_query,
            pageSize=max_results,
            spaces='drive',
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            fields='files(id, name, createdTime, modifiedTime, owners, webViewLink)',
            orderBy='modifiedTime desc'
        ).execute()

        files = results.get('files', [])

        return [
            {
                'id': f['id'],
                'name': f['name'],
                'created_time': f.get('createdTime'),
                'modified_time': f.get('modifiedTime'),
                'owners': [owner.get('emailAddress') for owner in f.get('owners', [])],
                'web_link': f.get('webViewLink')
            }
            for f in files
        ]
    except Exception as e:
        return [{'error': f'Search failed: {str(e)}'}]


def _column_index_to_letter(index: int) -> str:
    """Convert 0-based column index to A1 notation letter (0='A', 25='Z', 26='AA', etc.)"""
    result = ""
    while index >= 0:
        result = chr(index % 26 + ord('A')) + result
        index = index // 26 - 1
    return result


@tool(
    annotations=ToolAnnotations(
        title="Find Cells",
        readOnlyHint=True,
    ),
)
def find_in_spreadsheet(spreadsheet_id: str,
                        query: str,
                        sheet: Optional[str] = None,
                        case_sensitive: bool = False,
                        max_results: int = 50,
                        ctx: Context = None) -> List[Dict[str, Any]]:
    """
    Find cells containing a specific value in a Google Spreadsheet.

    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        query: The text to search for in cell values
        sheet: Optional sheet name to search in. If not provided, searches all sheets.
        case_sensitive: Whether the search should be case-sensitive (default False)
        max_results: Maximum number of results to return (default 50)

    Returns:
        List of found cells with their location (sheet, cell in A1 notation) and value
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    results = []

    try:
        # Get spreadsheet metadata to find all sheets
        spreadsheet = sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields='sheets(properties(title,sheetId))'
        ).execute()

        sheets_to_search = []
        for s in spreadsheet.get('sheets', []):
            sheet_title = s.get('properties', {}).get('title')
            if sheet is None or sheet_title == sheet:
                sheets_to_search.append(sheet_title)

        if not sheets_to_search:
            return [{'error': f"Sheet '{sheet}' not found"}]

        search_query = query if case_sensitive else query.lower()

        for sheet_name in sheets_to_search:
            if len(results) >= max_results:
                break

            # Get all data from the sheet
            response = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name
            ).execute()

            values = response.get('values', [])

            for row_idx, row in enumerate(values):
                if len(results) >= max_results:
                    break

                for col_idx, cell_value in enumerate(row):
                    if len(results) >= max_results:
                        break

                    cell_str = str(cell_value)
                    compare_value = cell_str if case_sensitive else cell_str.lower()

                    if search_query in compare_value:
                        cell_ref = f"{_column_index_to_letter(col_idx)}{row_idx + 1}"
                        results.append({
                            'sheet': sheet_name,
                            'cell': cell_ref,
                            'value': cell_value
                        })

        return results

    except Exception as e:
        return [{'error': f'Search failed: {str(e)}'}]


@tool(
    annotations=ToolAnnotations(
        title="Batch Update",
        destructiveHint=True,
    ),
)
def batch_update(spreadsheet_id: str,
                 requests: List[Dict[str, Any]],
                 ctx: Context = None) -> Dict[str, Any]:
    """
    Execute a batch update on a Google Spreadsheet using the full batchUpdate endpoint.
    This provides access to all batchUpdate operations including adding sheets, updating properties,
    inserting/deleting dimensions, formatting, and more.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet (found in the URL)
        requests: A list of request objects. Each request object can contain any valid batchUpdate operation.
                 Common operations include:
                 - addSheet: Add a new sheet
                 - updateSheetProperties: Update sheet properties (title, grid properties, etc.)
                 - insertDimension: Insert rows or columns
                 - deleteDimension: Delete rows or columns
                 - updateCells: Update cell values and formatting
                 - updateBorders: Update cell borders
                 - addConditionalFormatRule: Add conditional formatting
                 - deleteConditionalFormatRule: Remove conditional formatting
                 - updateDimensionProperties: Update row/column properties
                 - and many more...
                 
                 Example requests:
                 [
                     {
                         "addSheet": {
                             "properties": {
                                 "title": "New Sheet"
                             }
                         }
                     },
                     {
                         "updateSheetProperties": {
                             "properties": {
                                 "sheetId": 0,
                                 "title": "Renamed Sheet"
                             },
                             "fields": "title"
                         }
                     },
                     {
                         "insertDimension": {
                             "range": {
                                 "sheetId": 0,
                                 "dimension": "ROWS",
                                 "startIndex": 1,
                                 "endIndex": 3
                             }
                         }
                     }
                 ]
    
    Returns:
        Result of the batch update operation, including replies for each request
    """
    sheets_service = ctx.request_context.lifespan_context.sheets_service
    
    # Validate input
    if not requests:
        return {"error": "requests list cannot be empty"}
    
    if not all(isinstance(req, dict) for req in requests):
        return {"error": "Each request must be a dictionary"}
    
    # Prepare the batch update request body
    request_body = {
        "requests": requests
    }
    
    # Execute the batch update
    result = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=request_body
    ).execute()
    
    return result


def main():
    # Log tool filtering configuration if enabled
    if ENABLED_TOOLS is not None:
        print(f"Tool filtering enabled. Active tools: {', '.join(sorted(ENABLED_TOOLS))}")
    else:
        print("Tool filtering disabled. All tools are enabled.")
    
    # Run the server
    transport = "stdio"
    for i, arg in enumerate(sys.argv):
        if arg == "--transport" and i + 1 < len(sys.argv):
            transport = sys.argv[i + 1]
            break

    mcp.run(transport=transport)
