import abc


class AxisHelper(metaclass=abc.ABCMeta):
    @staticmethod
    @abc.abstractmethod
    def is_between(value: float, a: float, b: float) -> bool:
        pass

    @staticmethod
    @abc.abstractmethod
    def get_diff(a: float, b: float) -> float:
        pass

    @staticmethod
    @abc.abstractmethod
    def get_middle(a: float, b: float) -> float:
        pass
