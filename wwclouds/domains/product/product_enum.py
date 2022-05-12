from enum import Flag, auto


class ProductEnum(Flag):
    IMAGEDATA = auto()
    IMAGEVISUAL = auto()
    VIDEO = auto()

    @staticmethod
    def from_str(string: str) -> "ProductEnum":
        upper_str = string.upper()
        return getattr(ProductEnum, upper_str)

    @staticmethod
    def from_str_list(str_list: list[str]) -> "ProductEnum":
        return ProductEnum(sum(ProductEnum.from_str(string).value for string in str_list))


if __name__ == '__main__':
    product_enum = ProductEnum.from_str_list(["imagedata", "imagevisual"])
    print(product_enum & (ProductEnum.VIDEO | ProductEnum.IMAGEVISUAL))
