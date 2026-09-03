from collections.abc import Callable


def check(description: str) -> Callable[[Callable], Callable]:
    def decorator(func: Callable) -> Callable:
        func._check_description = description
        return func

    return decorator
