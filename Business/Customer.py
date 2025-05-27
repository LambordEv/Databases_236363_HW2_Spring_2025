from typing import Optional


class Customer:
    def __init__(self, cust_id: Optional[int] = None, full_name: Optional[str] = None, age: Optional[int] = None,
                 phone: Optional[str] = None) -> None:

        self.__cust_id = cust_id
        self.__full_name = full_name
        self.__phone = phone
        self.__age = age

    def get_cust_id(self) -> Optional[int]:
        return self.__cust_id

    def set_cust_id(self, cust_id: int) -> None:
        self.__cust_id = cust_id

    def get_full_name(self) -> Optional[str]:
        return self.__full_name

    def set_full_name(self, full_name: str) -> None:
        self.__full_name = full_name

    def get_phone(self) -> Optional[str]:
        return self.__phone

    def set_phone(self, phone: str) -> None:
        self.__phone = phone

    def get_age(self) -> Optional[int]:
        return self.__age

    def set_address(self, age: int) -> None:
        self.__age = age

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, Customer):
            return False
        return (self.__cust_id == __value.__cust_id and self.__full_name == __value.__full_name
                and self.__phone == __value.__phone and self.__age == __value.__age)

    def __str__(self) -> str:
        return f'cust_id={self.__cust_id}, full_name={self.__full_name}, phone={self.__phone}, age={self.__age}'


class BadCustomer(Customer):
    def __init__(self) -> None:
        super().__init__(cust_id=-1, full_name="Unknown", phone="Unknown", age=-1)
