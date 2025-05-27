from typing import Optional


class OrderDish:
    def __init__(self, dish_id: Optional[int] = None, amount: Optional[int] = None,
                 price: Optional[float] = None) -> None:

        self.__dish_id = dish_id
        self.__amount = amount
        self.__price = float(price) if price is not None else None

    def get_dish_id(self) -> Optional[int]:
        return self.__dish_id

    def set_dish_id(self, dish_id: int) -> None:
        self.__dish_id = dish_id

    def get_amount(self) -> Optional[int]:
        return self.__amount

    def set_amount(self, amount: int) -> None:
        self.__amount = amount

    def get_price(self) -> Optional[float]:
        return self.__price

    def set_price(self, price: float) -> None:
        self.__price = float(price) if price is not None else None

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, OrderDish):
            return False
        epsilon = 1e-5
        price_check = abs(self.__price - __value.__price) < epsilon if self.__price is not None and __value.__price is not None else self.__price == __value.__price
        return (self.__dish_id == __value.__dish_id
                and self.__amount == __value.__amount and price_check)

    def __str__(self) -> str:
        return (f'dish_id={self.__dish_id}, '
                f'amount={self.__amount}, price={self.__price}')
