from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Customer import Customer, BadCustomer
from Business.Order import Order, BadOrder
from Business.Dish import Dish, BadDish
from Business.OrderDish import OrderDish

# ---------------------------- Tables Declarations: -----------------------------
CUSTOMER_TABLE = '''
Customer
{
    Cust_id             		INTEGER         					NOT NULL, CHECK (Cust_id > 0),
    Full_name           		TEXT            					NOT NULL,
    Age                 		INTEGER         					NOT NULL, CHECK (Age >= 18 AND Age <= 120),
    Phone_num           		VARCHAR(10)     					NOT NULL, CHECK (LENGTH(Phone_num) = 10),
    PRIMARY KEY (Cust_id)
}'''

ORDER_TABLE = '''
Order
{
    Order_id                    INTEGER         					NOT NULL, CHECK (Order_id > 0),
    Date                        TIMESTAMP(0) WITHOUT TIME ZONE      NOT NULL,
    Delivery_fee                DECIMAL         					NOT NULL, CHECK (Delivery_fee >= 0),
    Delivery_address            TEXT            					NOT NULL, CHECK (LENGTH(Delivery_address) >= 5),
    PRIMARY KEY (Order_id)
}'''

DISH_TABLE = '''
Dish
{
    Dish_id             		INTEGER								NOT NULL, CHECK (Dish_id > 0),
    Name                		TEXT		                        NOT NULL, CHECK (LENGTH(Name) >= 4),
    Price               		DECIMAL								NOT NULL, CHECK (Price > 0),
    Is_active           		BOOLEAN								NOT NULL,
    PRIMARY KEY (Dish_id)
}'''

RESERVATION_TABLE = '''
Reservation
{
    Order_id               		INTEGER		                        NOT NULL, FOREIGN KEY (Order_id) REFERENCES Order(Order_id) ON DELETE CASCADE,
    Cust_id             		INTEGER								NOT NULL, FOREIGN KEY (Cust_id) REFERENCES Customer(Cust_id) ON DELETE CASCADE,
    PRIMARY KEY (Order_id)
}'''

ORDER_DETAILS_TABLE = '''
Order_Details
{
    Order_id               		INTEGER		                        NOT NULL, FOREIGN KEY (Order_id) REFERENCES Order(Order_id) ON DELETE CASCADE,
    Dish_id             		INTEGER								NOT NULL, FOREIGN KEY (Dish_id) REFERENCES Dish(Dish_id) ON DELETE CASCADE,
    Dish_amount                 INTEGER                             NOT NULL, CHECK(Dish_amount > 0),
    Dish_price                  DECIMAL                             NOT NULL, CHECK(Dish_price > 0),
    PRIMARY KEY (Order_id, Dish_id)
}'''

CUSTOMER_RATINGS_TABLE = '''
Customer_Ratings
{
    Cust_id               		INTEGER		                        NOT NULL, FOREIGN KEY (Cust_id) REFERENCES Customer(Cust_id) ON DELETE CASCADE,
    Dish_id             		INTEGER								NOT NULL, FOREIGN KEY (Dish_id) REFERENCES Dish(Dish_id) ON DELETE CASCADE,
    Rating                      INTEGER                             NOT NULL, CHECK(Rating > 0 AND Rating <= 5),
    PRIMARY KEY (Cust_id, Dish_id)
}'''


TABLES = [CUSTOMER_TABLE, ORDER_TABLE, DISH_TABLE, RESERVATION_TABLE, ORDER_DETAILS_TABLE, CUSTOMER_RATINGS_TABLE]

# ---------------------------------- CRUD API: ----------------------------------
# Basic database functions
def handle_database_exceptions(query: sql.SQL, e: Exception, print_flag = False) -> ReturnValue:
    result = ReturnValue.ERROR
    if print_flag:
        print('Database Raised An Exception!')
        print(f'The Query that is responsible for the exception - {query}')
        print(e)

    if isinstance(e, DatabaseException.NOT_NULL_VIOLATION):
        result = ReturnValue.BAD_PARAMS
    elif isinstance(e, DatabaseException.CHECK_VIOLATION):
        result = ReturnValue.BAD_PARAMS
    elif isinstance(e, DatabaseException.FOREIGN_KEY_VIOLATION):
        result = ReturnValue.NOT_EXISTS
    elif isinstance(e, DatabaseException.UNIQUE_VIOLATION):
        result = ReturnValue.ALREADY_EXISTS
    elif isinstance(e, DatabaseException.ConnectionInvalid):
        result = ReturnValue.ERROR
    elif isinstance(e, DatabaseException.UNKNOWN_ERROR):
        result = ReturnValue.ERROR
    elif isinstance(e, DatabaseException.database_ini_ERROR):
        result = ReturnValue.ERROR

    return result

def handle_query(query: sql.SQL) -> Tuple[ReturnValue, int, Connector.ResultSet, Exception]:
    query_result = ReturnValue.OK
    rows_amount = 0
    result = None
    recieved_exp = None
    conn = Connector.DBConnector()

    try:
        rows_amount, result = conn.execute(query)
        conn.commit()
    except Exception as e:
        recieved_exp = e
        query_result = handle_database_exceptions(query, e, DEBUG_FLAG)
    finally:
        conn.close()

    return query_result, rows_amount, result, recieved_exp

def return_Value_select(qstatus:ReturnValue, rows_effected)-> ReturnValue:
        if qstatus == ReturnValue.OK and rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        return qstatus


def create_tables() -> None:
    query_string = ''
    for table in TABLES:
        query_string += f'CREATE TABLE {table};\n'

    query = sql.SQL(query_string)
    handle_query(query)


def clear_tables() -> None:
    # TODO: implement
    pass


def drop_tables() -> None:
    # TODO: implement
    pass


# CRUD API

def add_customer(customer: Customer) -> ReturnValue:
    # TODO: implement
    pass


def get_customer(customer_id: int) -> Customer:
    # TODO: implement
    pass


def delete_customer(customer_id: int) -> ReturnValue:
    # TODO: implement
    pass


def add_order(order: Order) -> ReturnValue:
    # TODO: implement
    pass


def get_order(order_id: int) -> Order:
    # TODO: implement
    pass


def delete_order(order_id: int) -> ReturnValue:
    # TODO: implement
    pass


def add_dish(dish: Dish) -> ReturnValue:
    # TODO: implement
    pass


def get_dish(dish_id: int) -> Dish:
    # TODO: implement
    pass


def update_dish_price(dish_id: int, price: float) -> ReturnValue:
    # TODO: implement
    pass


def update_dish_active_status(dish_id: int, is_active: bool) -> ReturnValue:
    # TODO: implement
    pass


def customer_placed_order(customer_id: int, order_id: int) -> ReturnValue:
    # TODO: implement
    pass


def get_customer_that_placed_order(order_id: int) -> Customer:
    # TODO: implement
    pass


def order_contains_dish(order_id: int, dish_id: int, amount: int) -> ReturnValue:
    # TODO: implement
    pass


def order_does_not_contain_dish(order_id: int, dish_id: int) -> ReturnValue:
    # TODO: implement
    pass


def get_all_order_items(order_id: int) -> List[OrderDish]:
    # TODO: implement
    pass


def customer_rated_dish(cust_id: int, dish_id: int, rating: int) -> ReturnValue:
    # TODO: implement
    pass


def customer_deleted_rating_on_dish(cust_id: int, dish_id: int) -> ReturnValue:
    # TODO: implement
    pass

def get_all_customer_ratings(cust_id: int) -> List[Tuple[int, int]]:
    # TODO: implement
    pass
# ---------------------------------- BASIC API: ----------------------------------

# Basic API


def get_order_total_price(order_id: int) -> float:
    # TODO: implement
    pass


def get_customers_spent_max_avg_amount_money() -> List[int]:
    # TODO: implement
    pass


def get_most_ordered_dish_in_period(start: datetime, end: datetime) -> Dish:  
    # TODO: implement
    pass

def did_customer_order_top_rated_dishes(cust_id: int) -> bool:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------

# Advanced API


def get_customers_rated_but_not_ordered() -> List[int]:
    # TODO: implement
    pass


def get_non_worth_price_increase() -> List[int]:
    # TODO: implement
    pass


def get_cumulative_profit_per_month(year: int) -> List[Tuple[int, float]]:
    # TODO: implement
    pass


def get_potential_dish_recommendations(cust_id: int) -> List[int]:
    # TODO: implement
    pass
