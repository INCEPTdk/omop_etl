"""Freezing classes - making them immutable"""

from functools import wraps
from typing import Any, Callable, NoReturn, Union

from .exceptions import FrozenClassException


def freeze_instance(cls: Any) -> Any:
    """Decorator to freeze class instances. Prevents adding new variables to an instance"""
    # pylint: disable=protected-access
    cls.__frozen = False

    def frozensetattr(self, key: str, value: Any) -> Union[None, NoReturn]:
        # pylint: disable=protected-access
        if (
            self.__frozen
            and not hasattr(self, key)
            and key != "_sa_instance_state"
        ):
            raise FrozenClassException(
                f"Model {cls.__name__} is frozen. Cannot set {key} = {value}"
            )
        object.__setattr__(self, key, value)

    def init_decorator(func: Callable[[Any], None]) -> Any:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            func(self, *args, **kwargs)
            # pylint: disable=protected-access
            self.__frozen = True

        return wrapper

    cls.__setattr__ = frozensetattr
    cls.__init__ = init_decorator(cls.__init__)

    return cls
