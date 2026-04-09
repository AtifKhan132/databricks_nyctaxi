import os
import sys

def get_project_root():
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

    if project_root not in sys.path:
        sys.path.append(project_root)
    
    return project_root