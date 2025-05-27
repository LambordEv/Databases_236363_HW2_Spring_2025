from datetime import datetime
from typing import Optional


class Order:
    def __init__(self, order_id: Optional[int] = None, date: Optional[datetime] = None,
                 delivery_fee: Optional[float] = None, delivery_address: Optional[str] = None) -> None:
        self.__order_id = order_id
        self.__datetime = date
        self.__delivery_fee = delivery_fee
        self.__delivery_address = delivery_address

    def get_order_id(self) -> Optional[int]:
        return self.__order_id

    def set_order_id(self, order_id: int) -> None:
        self.__order_id = order_id

    def get_datetime(self) -> Optional[datetime]:
        return self.__datetime

    def set_datetime(self, date: datetime) -> None:
        self.__datetime = date

    def get_delivery_fee(self) -> Optional[float]:
        return self.__delivery_fee

    def set_delivery_fee(self, delivery_fee: float) -> None:
        self.__delivery_fee = delivery_fee

    def get_delivery_address(self) -> Optional[str]:
        return self.__delivery_address

    def set_delivery_address(self, delivery_address: str) -> None:
        self.__delivery_address = delivery_address

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Order):
            return False
        return (
                self.__order_id == __value.__order_id and
                self.__datetime == __value.__datetime and
                self.__delivery_fee == __value.__delivery_fee and
                self.__delivery_address == __value.__delivery_address
        )

    def __str__(self) -> str:
        return (
            f'order_id={self.__order_id}, date={self.__datetime}, '
            f'delivery_fee={self.__delivery_fee}, delivery_address={self.__delivery_address}'
        )


class BadOrder(Order):
    def __init__(self) -> None:
        super().__init__(order_id=-1, date=datetime.min)
