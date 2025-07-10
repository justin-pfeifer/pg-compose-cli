"""
Generate ALTER commands from diff results.
"""

from typing import List, Dict, Any

def generate_alter_commands(diff_result: List[Dict[str, Any]]) -> List[str]:
    """
    Generate ALTER commands from diff results.
    
    Args:
        diff_result: List of diff objects with 'command' field
        
    Returns:
        List of SQL commands as strings
    """
    commands = []
    for obj in diff_result:
        if isinstance(obj, dict) and 'command' in obj:
            commands.append(obj['command'])
        elif hasattr(obj, 'command'):
            commands.append(obj.command)
    return commands 