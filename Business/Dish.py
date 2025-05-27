from typing import Optional


class Dish:
    def __init__(self, dish_id: Optional[int] = None, name: Optional[str] = None, price: Optional[float] = None,
                 is_active: Optional[bool] = None) -> None:
        self.__dish_id = dish_id
        self.__name = name
        # Ensure price is always stored as a float if not None
        self.__price = float(price) if price is not None else None
        self.__is_active = is_active

    def get_dish_id(self) -> Optional[int]:
        return self.__dish_id

    def set_dish_id(self, dish_id: int) -> None:
        self.__dish_id = dish_id

    def get_name(self) -> Optional[str]:
        return self.__name

    def set_name(self, name: str) -> None:
        self.__name = name

    def get_price(self) -> Optional[float]:
        return self.__price

    def set_price(self, price: float) -> None:
        self.__price = float(price) if price is not None else None

    def get_is_active(self) -> Optional[bool]:
        return self.__is_active

    def set_is_active(self, is_active: bool) -> None:
        self.__is_active = is_active

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Dish):
            return False
        epsilon = 1e-5
        price_check = abs(self.__price - __value.__price) < epsilon if (
                self.__price is not None and __value.__price is not None) else self.__price == __value.__price

        return (self.__dish_id == __value.__dish_id and self.__name == __value.__name
                and price_check and self.__is_active == __value.__is_active)

    def __str__(self) -> str:
        return f'dish_id={self.__dish_id}, name={self.__name}, price={self.__price}, is_active={self.__is_active}'


class BadDish(Dish):
    def __init__(self) -> None:
        super().__init__(dish_id=-1, name="Unknown", price=-100.0, is_active=False)
