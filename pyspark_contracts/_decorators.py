import functools
import inspect
from collections.abc import Callable

from pyspark_contracts._contract import Contract


def check_output(
    contract_cls: type[Contract], *, mode: str = "hard", **kwargs
) -> Callable[[Callable], Callable]:
    """Validates a function's return value against a ``Contract``, moving the call to
    ``validate()`` out of the function body and onto its signature.

    The wrapped function runs first; its return value (assumed to be the DataFrame)
    is then passed to ``contract_cls().validate(result, mode=mode, **kwargs)``. The
    original, unmodified return value is handed back to the caller — validation is a
    side effect, not a transformation. In ``mode="hard"`` (the default), a violation
    raises ``ContractViolationError`` after the function has already run, so the
    caller never receives the bad result.

    Args:
        contract_cls: A :class:`.Contract` subclass (not an instance) — a fresh
            instance is created for every call, since ``Contract`` holds no
            per-instance state.
        mode: Forwarded to ``validate()``. Fixed at decoration time, applied to
            every call.
        **kwargs: Forwarded to ``validate()`` on every call — e.g. extra parameters
            for the contract's ``@check`` methods.

    Example:
        >>> @check_output(FinalDfContract)
        ... def build_final_df(accumulated_df, contracts_df):
        ...     return accumulated_df.join(contracts_df, ...)
    """
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
    """Validates a named parameter against a ``Contract`` before the function runs.

    Uses ``inspect.signature`` to bind the wrapped function's actual call arguments,
    so ``param`` is found whether the caller passed it positionally or by keyword. If
    validation fails in ``mode="hard"`` (the default), ``ContractViolationError`` is
    raised before the wrapped function ever executes. A function needing more than
    one input validated stacks multiple ``@check_input`` decorators.

    Fails fast at decoration time (``TypeError``, not on first call) if ``param``
    isn't a parameter of the decorated function, or if ``contract_cls`` isn't a
    ``Contract`` subclass.

    Args:
        contract_cls: A :class:`.Contract` subclass (not an instance) — a fresh
            instance is created for every call.
        param: Name of the function parameter to validate.
        mode: Forwarded to ``validate()``. Fixed at decoration time, applied to
            every call.
        **kwargs: Forwarded to ``validate()`` on every call.

    Example:
        >>> @check_input(AccumulatedDataContract, param="df")
        ... def process(df):
        ...     return df.filter(...)
    """
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
