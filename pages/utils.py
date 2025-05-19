import re
import ast
from typing import Dict, Any, List, Optional, Tuple

def extract_udf_name(code: str) -> str:
    """Extract the UDF function name from code"""
    # Look for function definition
    match = re.search(r'def\s+([a-zA-Z0-9_]+)\s*\(', code)
    if match:
        return match.group(1)
    return "unnamed_udf"

def extract_udf_docs(code: str) -> str:
    """Extract docstring from UDF code"""
    # Parse the code
    try:
        tree = ast.parse(code)
        
        # Look for function definitions
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                # Check if it has a docstring
                if ast.get_docstring(node):
                    return ast.get_docstring(node)
    except SyntaxError:
        # If code can't be parsed, try regex
        pass
    
    # Fallback to regex if AST parsing fails
    match = re.search(r'def\s+[a-zA-Z0-9_]+\s*\([^)]*\):\s*[\'"]([^\'"]*)[\'"]', code, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    return "No description available"

def extract_udf_params(code):
    """Extract parameter information from UDF code."""
    params = {}
    param_pattern = r'def\s+[a-zA-Z_][a-zA-Z0-9_]*\s*\((.*?)\):'
    param_match = re.search(param_pattern, code, re.DOTALL)
    
    if param_match:
        param_str = param_match.group(1).strip()
        if param_str and param_str != "":
            param_parts = []
            current_part = ""
            brace_level = 0
            
            for char in param_str:
                if char == ',' and brace_level == 0:
                    param_parts.append(current_part.strip())
                    current_part = ""
                else:
                    current_part += char
                    if char in '[{(':
                        brace_level += 1
                    elif char in ']})':
                        brace_level = max(0, brace_level - 1)
            
            if current_part:
                param_parts.append(current_part.strip())
            
            for part in param_parts:
                # Skip self parameter
                if part.strip() == 'self':
                    continue
                    
                # Parameter with default value
                if '=' in part:
                    name, default = part.split('=', 1)
                    name = name.strip()
                    
                    # Check for type hints
                    if ':' in name:
                        name, type_hint = name.split(':', 1)
                        name = name.strip()
                        type_hint = type_hint.strip()
                    else:
                        type_hint = "any"
                    
                    # Try to evaluate the default value
                    try:
                        default_val = eval(default.strip())
                        param_type = type(default_val).__name__
                    except:
                        default_val = default.strip()
                        param_type = "str"
                    
                    params[name] = {
                        "type": param_type,
                        "type_hint": type_hint,
                        "default": default_val
                    }
                else:
                    # Parameter without default value
                    name = part.strip()
                    
                    # Check for type hints
                    if ':' in name:
                        name, type_hint = name.split(':', 1)
                        name = name.strip()
                        type_hint = type_hint.strip()
                    else:
                        type_hint = "any"
                        
                    params[name] = {
                        "type": "any",
                        "type_hint": type_hint,
                        "required": True
                    }
    
    return params 