import functools
import inspect
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


def check_input(
    contract_cls: type[Contract], param: str, *, mode: str = "hard", **kwargs
) -> Callable[[Callable], Callable]:
    if not (isinstance(contract_cls, type) and issubclass(contract_cls, Contract)):
        raise TypeError(f"check_input() expects a Contract subclass, got {contract_cls!r}")

    def decorator(func: Callable) -> Callable:
        sig = inspect.signature(func)
        if param not in sig.parameters:
            raise TypeError(f"check_input(): {func.__qualname__}() has no parameter '{param}'")

        @functools.wraps(func)
        def wrapper(*args, **call_kwargs):
            bound = sig.bind(*args, **call_kwargs)
            bound.apply_defaults()
            contract_cls().validate(bound.arguments[param], mode=mode, **kwargs)
            return func(*args, **call_kwargs)

        return wrapper

    return decorator
