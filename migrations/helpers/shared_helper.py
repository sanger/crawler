from datetime import datetime
import sys
import traceback

def print_exception() -> None:
    print(f'An exception occurred, at {datetime.now()}')
    e = sys.exc_info()
    print(e[0]) # exception type
    print(e[1]) # exception message
    if e[2]: # traceback
      traceback.print_tb(e[2], limit=10)