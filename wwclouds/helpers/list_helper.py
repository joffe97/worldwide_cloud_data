from typing import TypeVar, Generator


T = TypeVar("T")


class ListHelper:
    @staticmethod
    def split_list(content_list: list[T], n: int) -> Generator[list[T], None, None]:
        k, m = divmod(len(content_list), n)
        return (content_list[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))
