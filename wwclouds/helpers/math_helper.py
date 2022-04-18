from math import exp


class MathHelper:
    @staticmethod
    def sigmoid(x: float) -> float:
        return 1 / (1 + exp(-x))
