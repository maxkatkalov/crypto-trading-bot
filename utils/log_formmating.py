from datetime import datetime
import inspect


def current_timedate_and_func_name(func_name: str = None) -> str:
    if func_name is None:
        func_name = inspect.currentframe().f_back.f_code.co_name
    return f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, {func_name}]"
