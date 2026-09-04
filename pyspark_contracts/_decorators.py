import functools
from collections.abc import Callable

from pyspark_contracts._contract import Contract


def check_output(
    contract_cls: type[Contract], *, mode: str = "hard", **kwargs
) -> Callable[[Callable], Callable]:
    if not (isinstance(contract_cls, type) and issubclass(contract_cls, Contract)):
        raise TypeError(f"check_output() expects a Contract subclass, got {contract_cls!r}")

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **call_kwargs):
            result = func(*args, **call_kwargs)
            contract_cls().validate(result, mode=mode, **kwargs)
            return result

        return wrapper

    return decorator
