from abc import ABC, abstractmethod
from datetime import datetime


class CircuitBreakerStorage(ABC):
    """
    Defines the underlying storage for a circuit breaker - the underlying
    implementation should be in a subclass that overrides the method this
    class defines.
    """

    def __init__(self, name: str):
        """
        Creates a new instance identified by `name`.
        """
        self._name = name

    @property
    def name(self) -> str:
        """
        Returns a human friendly name that identifies this state.
        """
        return self._name

    @property
    @abstractmethod
    def state(self) -> str:
        """
        Override this method to retrieve the current circuit breaker state.
        """
        pass

    @state.setter
    @abstractmethod
    def state(self, state: str):
        """
        Override this method to set the current circuit breaker state.
        """
        pass

    @abstractmethod
    def increment_counter(self):
        """
        Override this method to increase the failure counter by one.
        """
        pass

    @abstractmethod
    def reset_counter(self):
        """
        Override this method to set the failure counter to zero.
        """
        pass

    @property
    @abstractmethod
    def counter(self) -> int:
        """
        Override this method to retrieve the current value of the failure counter.
        """
        pass

    @property
    @abstractmethod
    def opened_at(self) -> datetime:
        """
        Override this method to retrieve the most recent value of when the
        circuit was opened.
        """
        pass

    @opened_at.setter
    @abstractmethod
    def opened_at(self, date_time: datetime):
        """
        Override this method to set the most recent value of when the circuit
        was opened.
        """
        pass