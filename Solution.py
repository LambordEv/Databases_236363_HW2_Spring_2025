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

DEBUG_FLAG = False

# ---------------------------- Tables Declarations: -----------------------------
CUSTOMER_TABLE = '''
Customers
(
    Cust_id             		INTEGER         					NOT NULL, CHECK (Cust_id > 0),
    Full_name           		TEXT            					NOT NULL,
    Age                 		INTEGER         					NOT NULL, CHECK (Age >= 18 AND Age <= 120),
    Phone_num           		TEXT             					NOT NULL, CHECK (LENGTH(Phone_num) = 10),
    PRIMARY KEY (Cust_id)
)'''

ORDER_TABLE = '''
Orders
(
    Order_id                    INTEGER         					NOT NULL, CHECK (Order_id > 0),
    Date                        TIMESTAMP(0) WITHOUT TIME ZONE      NOT NULL,
    Delivery_fee                DECIMAL         					NOT NULL, CHECK (Delivery_fee >= 0),
    Delivery_address            TEXT            					NOT NULL, CHECK (LENGTH(Delivery_address) >= 5),
    PRIMARY KEY (Order_id)
)'''

DISH_TABLE = '''
Dishes
(
    Dish_id             		INTEGER								NOT NULL, CHECK (Dish_id > 0),
    Name                		TEXT		                        NOT NULL, CHECK (LENGTH(Name) >= 4),
    Price               		DECIMAL								NOT NULL, CHECK (Price > 0),
    Is_active           		BOOLEAN								NOT NULL,
    PRIMARY KEY (Dish_id)
)'''

RESERVATION_TABLE = '''
Reservations
(
    Order_id               		INTEGER		                        NOT NULL, FOREIGN KEY (Order_id) REFERENCES Orders(Order_id) ON DELETE CASCADE,
    Cust_id             		INTEGER								NOT NULL, FOREIGN KEY (Cust_id) REFERENCES Customers(Cust_id) ON DELETE CASCADE,
    PRIMARY KEY (Order_id)
)'''

ORDER_DETAILS_TABLE = '''
Order_Details
(
    Order_id               		INTEGER		                        NOT NULL, FOREIGN KEY (Order_id) REFERENCES Orders(Order_id) ON DELETE CASCADE,
    Dish_id             		INTEGER								NOT NULL, FOREIGN KEY (Dish_id) REFERENCES Dishes(Dish_id) ON DELETE CASCADE,
    Dish_amount                 INTEGER                             NOT NULL, CHECK(Dish_amount >= 0),
    Dish_price                  DECIMAL                             NOT NULL, CHECK(Dish_price > 0),
    PRIMARY KEY (Order_id, Dish_id)
)'''

CUSTOMER_RATINGS_TABLE = '''
Customer_Ratings
(
    Cust_id               		INTEGER		                        NOT NULL, FOREIGN KEY (Cust_id) REFERENCES Customers(Cust_id) ON DELETE CASCADE,
    Dish_id             		INTEGER								NOT NULL, FOREIGN KEY (Dish_id) REFERENCES Dishes(Dish_id) ON DELETE CASCADE,
    Rating                      INTEGER                             NOT NULL, CHECK(Rating > 0 AND Rating <= 5),
    PRIMARY KEY (Cust_id, Dish_id)
)'''

TABLES = [CUSTOMER_TABLE, ORDER_TABLE, DISH_TABLE, RESERVATION_TABLE, ORDER_DETAILS_TABLE, CUSTOMER_RATINGS_TABLE, ]
Tables_Names = ['Customer_Ratings', 'Order_Details', 'Reservations', 'Customers', 'Orders', 'Dishes']

# ---------------------------- Views Declarations: -----------------------------
ORDER_TOTAL_PRICE_VIEW = '''
CREATE VIEW Order_Total_Price_View AS
SELECT
    O.Order_id AS Order_id,
    SUM(COALESCE(OD.Dish_price, 0) * COALESCE(OD.Dish_amount, 0)) + O.Delivery_fee AS Total_Price
FROM
    Order_Details OD RIGHT JOIN Orders O ON OD.Order_id = O.Order_id
GROUP BY
    O.Order_id
'''

CUSTOMER_AVG_SPENDING_VIEW = '''
CREATE VIEW Customer_Avg_Spending_View AS
SELECT
    C.Cust_id AS Cust_id,
    AVG(OTP.Total_Price) AS Avg_Spending
FROM
    Customers C JOIN Reservations R ON C.Cust_id = R.Cust_id
    LEFT JOIN Order_Total_Price_View OTP ON R.Order_id = OTP.Order_id
GROUP BY
    C.Cust_id
'''

# The View: get_most_ordered_dish_in_period
#   1. Join between Order and OrderDetails, on order_id
#   2. Filter results by the given date (the dates should be in the given range)
#   3. For each Dish_id, sum their amount
DISHES_ORDERED_AMOUNT_VIEW = '''
CREATE VIEW Dishes_Ordered_Amount_View AS
SELECT
    OD.Dish_id AS Dish_id, OD.Dish_amount AS Ordered_Amount, O.Date AS Order_Date
FROM
    Orders O Join Order_Details OD ON O.Order_id = OD.Order_id
'''

# did_customer_order_top_rated_dishes
DISH_AVG_RATING_VIEW = '''
CREATE VIEW Dish_Avg_Rating_View AS
SELECT
    D.Dish_id, AVG(COALESCE(CR.Rating, 3)) AS Avg_rating
FROM Dishes D LEFT JOIN Customer_Ratings CR ON D.Dish_id = CR.Dish_id
GROUP BY D.Dish_id
'''

CUSTOMER_ORDERED_DISHES_VIEW = '''
CREATE VIEW Customer_Ordered_Dishes_View AS
SELECT
    R.Cust_id, OD.Dish_id
FROM
    Reservations R JOIN Order_Details OD ON R.Order_id = OD.Order_id
'''

AVG_PROFIT_PER_ORDER_VIEW = '''
CREATE VIEW Avg_Profit_Per_Order AS
SELECT 
    dish_id, dish_price, COALESCE(AVG(dish_amount), 0)*dish_price AS val 
FROM 
    Order_Details
GROUP BY dish_id, dish_price
'''

MONTHLY_PROFIT_VIEW = '''
CREATE VIEW Monthly_Profit_View AS
SELECT 
    EXTRACT(YEAR FROM O.Date) as Year, EXTRACT(MONTH FROM O.Date) as Month, SUM(OD.Dish_price * OD.Dish_amount) + O.Delivery_fee as Monthly_Profit
FROM 
    Orders O LEFT JOIN Order_Details OD ON O.Order_id = OD.Order_id
GROUP BY EXTRACT(YEAR FROM O.Date), EXTRACT(MONTH FROM O.Date), O.Delivery_fee
'''

SIMILAR_RELATION_VIEW = '''
CREATE VIEW SimilarRelation AS
SELECT DISTINCT cr1.Cust_id as a,
                cr2.Cust_id as b
FROM Customer_Ratings cr1
         JOIN Customer_Ratings cr2 ON cr1.Dish_id = cr2.Dish_id
WHERE cr1.Rating >= 4
  AND cr2.Rating >= 4
  AND cr1.Cust_id < cr2.Cust_id
'''

VIEWS = [ORDER_TOTAL_PRICE_VIEW, CUSTOMER_AVG_SPENDING_VIEW, DISHES_ORDERED_AMOUNT_VIEW, DISH_AVG_RATING_VIEW, CUSTOMER_ORDERED_DISHES_VIEW, AVG_PROFIT_PER_ORDER_VIEW, MONTHLY_PROFIT_VIEW, SIMILAR_RELATION_VIEW]
Views_Names = ['Order_Total_Price_View', 'Customer_Avg_Spending_View', 'Dishes_Ordered_Amount_View', 'Dish_Avg_Rating_View', 'Customer_Ordered_Dishes_View', 'Avg_Profit_Per_Order', 'Monthly_Profit_View', 'SimilarRelation']


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
        query_result = handle_database_exceptions(query, e)
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

    for view in VIEWS:
        query_string += f'{view};\n'

    # print(query_string)

    query = sql.SQL(query_string)
    _, _, _, exp = handle_query(query)
    if(DEBUG_FLAG and None != exp):
        print('create_tables')
        print(exp)

def clear_tables() -> None:
    query_string = '\n'.join([f"DELETE FROM {table} CASCADE;" for table in Tables_Names])
    query = sql.SQL(query_string)
    _, _, _, exp = handle_query(query)
    if (DEBUG_FLAG and None != exp):
        print('clear_tables')
        print(exp)


def drop_tables() -> None:
    query_string = '\n'.join([f"DROP VIEW IF EXISTS {view} CASCADE;" for view in Views_Names])
    query_string += '\n'.join([f"DROP TABLE IF EXISTS {table} CASCADE;" for table in Tables_Names])

    query = sql.SQL(query_string)
    _, _, _, exp = handle_query(query)
    if (DEBUG_FLAG and None != exp):
        print('drop_tables')
        print(exp)


# CRUD API

def add_customer(customer: Customer) -> ReturnValue:
    # TODO - Check Legal Params (Should be done by the DB)
    query_string = 'INSERT INTO Customers VALUES ({cust_id}, {full_name}, {age}, {phone_num});'

    query = sql.SQL(query_string).format(
        cust_id=sql.Literal(customer.get_cust_id()),
        full_name=sql.Literal(customer.get_full_name()),
        age=sql.Literal(customer.get_age()),
        phone_num=sql.Literal(customer.get_phone())
    )
    retVal, _, _, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('add_customer')
        print(exp)

    return retVal


def get_customer(customer_id: int) -> Customer:
    # TODO - Check Legal Params (Should be done by the DB)
    resultCustomer = BadCustomer()

    query_string = f'SELECT * FROM Customers ' \
                   f"WHERE Cust_id = {customer_id};"
    query = sql.SQL(query_string)
    retVal, rowsAmount, resultRows, exp = handle_query(query)
    if (DEBUG_FLAG and None != exp):
        print('get_customer')
        print(exp)

    if(1 == rowsAmount):
        resultCustomer = Customer(resultRows[0]['Cust_id'], resultRows[0]['Full_name'], resultRows[0]['Age'], resultRows[0]['Phone_num'])

    return resultCustomer


def delete_customer(customer_id: int) -> ReturnValue:
    # TODO - Check Legal Params (Should be done by the DB)
    retVal = ReturnValue.OK

    query_string = f'DELETE FROM Customers ' \
                   f"WHERE cust_id = {customer_id};"
    query = sql.SQL(query_string)
    retVal, rowsAffected, _, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('delete_customer')
        print(exp)

    if (0 == rowsAffected):
        retVal = ReturnValue.NOT_EXISTS

    return retVal


def add_order(order: Order) -> ReturnValue:
    # TODO - Check Legal Params (Should be done by the DB)
    query_string = 'INSERT INTO Orders VALUES ({order_id}, {order_date}, {order_delivery_fee}, {order_address});'

    query = sql.SQL(query_string).format(
        order_id=sql.Literal(order.get_order_id()),
        order_date=sql.Literal(order.get_datetime()),
        order_delivery_fee=sql.Literal(order.get_delivery_fee()),
        order_address=sql.Literal(order.get_delivery_address())
    )
    retVal, _, _, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('add_order')
        print(exp)

    return retVal


def get_order(order_id: int) -> Order:
    # TODO - Check Legal Params (Should be done by the DB)
    resultOrder = BadOrder()

    query_string = f'SELECT * FROM Orders ' \
                   f"WHERE Order_id = {order_id};"
    query = sql.SQL(query_string)
    retVal, rowsAmount, resultRows, exp = handle_query(query)
    if (DEBUG_FLAG and None != exp):
        print('get_order')
        print(exp)

    if(1 == rowsAmount):
        resultOrder = Order(resultRows[0]['Order_id'], resultRows[0]['Date'], resultRows[0]['Delivery_fee'], resultRows[0]['Delivery_address'])

    return resultOrder


def delete_order(order_id: int) -> ReturnValue:
    # TODO - Check Legal Params (Should be done by the DB)
    retVal = ReturnValue.OK

    query_string = f'DELETE FROM Orders ' \
                   f"WHERE order_id = {order_id};"
    query = sql.SQL(query_string)
    retVal, rowsAffected, _, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('delete_order')
        print(exp)

    if (0 == rowsAffected):
        retVal = ReturnValue.NOT_EXISTS

    return retVal


def add_dish(dish: Dish) -> ReturnValue:
    # TODO - Check Legal Params (Should be done by the DB)
    query_string = 'INSERT INTO Dishes VALUES ({dish_id}, {dish_name}, {dish_price}, {is_active});'

    query = sql.SQL(query_string).format(
        dish_id=sql.Literal(dish.get_dish_id()),
        dish_name=sql.Literal(dish.get_name()),
        dish_price=sql.Literal(dish.get_price()),
        is_active=sql.Literal(dish.get_is_active())
    )
    retVal, _, _, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('add_dish')
        print(exp)

    return retVal


def get_dish(dish_id: int) -> Dish:
    # TODO - Check Legal Params (Should be done by the DB)
    resultDish = BadDish()

    query_string = f'SELECT * FROM Dishes ' \
                   f"WHERE Dish_id = {dish_id};"
    query = sql.SQL(query_string)
    retVal, rowsAmount, resultRows, exp = handle_query(query)
    if (DEBUG_FLAG and None != exp):
        print('get_dish')
        print(exp)

    if(1 == rowsAmount):
        resultDish = Dish(resultRows[0]['Dish_id'], resultRows[0]['Name'], resultRows[0]['Price'], resultRows[0]['Is_active'])

    return resultDish


def update_dish_price(dish_id: int, price: float) -> ReturnValue:
    # TODO - Check Legal Params (Should be done by the DB)
    query_string = (f'UPDATE Dishes SET Price = {price} WHERE Dish_id = {dish_id} AND Is_active = TRUE;')

    query = sql.SQL(query_string)
    retVal, rowsAffected, _, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('update_dish_price')
        print(exp)

    if (0 == rowsAffected and ReturnValue.OK == retVal):
        retVal = ReturnValue.NOT_EXISTS

    return retVal


def update_dish_active_status(dish_id: int, is_active: bool) -> ReturnValue:
    # TODO - Check Legal Params (Should be done by the DB)
    query_string = f'UPDATE Dishes SET Is_active = {is_active} ' \
                   f"WHERE Dish_id = {dish_id};"
    query = sql.SQL(query_string)
    retVal, rowsAffected, _, exp = handle_query(query)
    if (DEBUG_FLAG and None != exp):
        print('update_dish_active_status')
        print(exp)

    if (0 == rowsAffected  and ReturnValue.OK == retVal):
        retVal = ReturnValue.NOT_EXISTS

    return retVal


def customer_placed_order(customer_id: int, order_id: int) -> ReturnValue:
    # TODO - Check Legal Params (Should be done by the DB)
    query_string = f'INSERT INTO Reservations VALUES ({order_id}, {customer_id});'
    query = sql.SQL(query_string)
    retVal, _, _, exp = handle_query(query)
    if (DEBUG_FLAG and None != exp):
        print('customer_placed_order')
        print(exp)

    return retVal


def get_customer_that_placed_order(order_id: int) -> Customer:
    # TODO - Check Legal Params (Should be done by the DB)
    resultCustomer = BadCustomer()

    query_string = f'SELECT * FROM Customers ' \
                   f'WHERE Cust_id = ' \
                   f'(SELECT R.Cust_id FROM Reservations R ' \
                   f"WHERE Order_id = {order_id});"
    query = sql.SQL(query_string)
    retVal, rowsAmount, resultRows, exp = handle_query(query)
    if (DEBUG_FLAG and None != exp):
        print('get_customer_that_placed_order')
        print(exp)

    if(1 == rowsAmount):
        resultCustomer = Customer(resultRows[0]['Cust_id'], resultRows[0]['Full_name'], resultRows[0]['Age'], resultRows[0]['Phone_num'])

    return resultCustomer


def order_contains_dish(order_id: int, dish_id: int, amount: int) -> ReturnValue:
    # TODO - Check Legal Params (Should be done by the DB)
    query_string = f'INSERT INTO Order_Details ' \
                   f"VALUES ({order_id}, {dish_id}, {amount}, " \
                   f"(SELECT Price FROM Dishes WHERE Dish_id = {dish_id} AND Is_active = TRUE));"

    query = sql.SQL(query_string)
    retVal, rowsAmount, _, exp = handle_query(query)

    if isinstance(exp, DatabaseException.NOT_NULL_VIOLATION):
        retVal = ReturnValue.NOT_EXISTS
    elif (DEBUG_FLAG and None != exp):
        print('order_contains_dish')
        print(exp)

    return retVal


def order_does_not_contain_dish(order_id: int, dish_id: int) -> ReturnValue:
    # TODO - Check Legal Params (Should be done by the DB)
    retVal = ReturnValue.OK

    query_string = f'DELETE FROM Order_Details ' \
                   f"WHERE order_id = {order_id} AND Dish_id = {dish_id};"
    query = sql.SQL(query_string)
    retVal, rowsAffected, _, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('order_does_not_contain_dish')
        print(exp)

    if (0 == rowsAffected):
        retVal = ReturnValue.NOT_EXISTS

    return retVal


def get_all_order_items(order_id: int) -> List[OrderDish]:
    # TODO - Check Legal Params (Should be done by the DB)
    resultList = []
    query_string = f'SELECT * FROM Order_Details ' \
                   f'WHERE order_id = {order_id};'
    query = sql.SQL(query_string)
    retVal, rowsAmount, resultRows, exp = handle_query(query)
    if (DEBUG_FLAG and None != exp):
        print('get_all_order_items')
        print(exp)

    # In case nothing was found the amount of rows will be 0 and the following loop will do nothing,
    # which will mean that the list is empty as initialized
    for i in range(rowsAmount):
        resultList.append(OrderDish(resultRows[i]['Dish_id'], resultRows[i]['Dish_amount'], resultRows[i]['Dish_price']))

    return resultList


def customer_rated_dish(cust_id: int, dish_id: int, rating: int) -> ReturnValue:
    # TODO - Check Legal Params (Should be done by the DB)
    query_string = f'INSERT INTO Customer_Ratings VALUES ({cust_id}, {dish_id}, {rating});'

    query = sql.SQL(query_string)
    retVal, _, _, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('customer_rated_dish')
        print(exp)

    return retVal


def customer_deleted_rating_on_dish(cust_id: int, dish_id: int) -> ReturnValue:
    # TODO - Check Legal Params (Should be done by the DB)
    retVal = ReturnValue.OK

    query_string = f'DELETE FROM Customer_Ratings ' \
                   f"WHERE Cust_id = {cust_id} AND Dish_id = {dish_id};"
    query = sql.SQL(query_string)
    retVal, rowsAffected, _, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('order_does_not_contain_dish')
        print(exp)

    if (0 == rowsAffected):
        retVal = ReturnValue.NOT_EXISTS

    return retVal

def get_all_customer_ratings(cust_id: int) -> List[Tuple[int, int]]:
    # TODO - Check Legal Params (Should be done by the DB)
    resultList = []
    query_string = f'SELECT * FROM Customer_Ratings ' \
                   f'WHERE Cust_id = {cust_id} ' \
                   f'ORDER BY Dish_id ASC;'
    query = sql.SQL(query_string)
    retVal, rowsAmount, resultRows, exp = handle_query(query)
    if (DEBUG_FLAG and None != exp):
        print('get_all_customer_ratings')
        print(exp)

    # In case nothing was found, the number of rows will be 0, and the following loop will do nothing.
    # This means that the list is empty as initialized
    for i in range(rowsAmount):
        resultList.append((resultRows[i]['Dish_id'], resultRows[i]['Rating']))

    return resultList

# ---------------------------------- BASIC API: ----------------------------------

# Basic API


def get_order_total_price(order_id: int) -> float:
    """
    Retrieves the total price of a given order, including the delivery fee.

    :param order_id: The ID of the order.
    :return: The total price of the order as a float. Returns 0.0 if the order is not found or an error occurs.
    """

    # TODO - Check Legal Params (Should be done by the DB)
    totalPriceResult = 0.0

    query_string = f'SELECT Total_Price FROM Order_Total_Price_View WHERE order_id = {order_id};'

    query = sql.SQL(query_string)
    retVal, rowsAmount, resultRows, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('get_order_total_price')
        print(exp)

    if 1 == rowsAmount:
        # According to the assignment this won't happen...
        totalPriceResult = float(resultRows[0]['Total_Price'])

    return totalPriceResult


def get_customers_spent_max_avg_amount_money() -> List[int]:
    """
    Retrieves the IDs of customers who have spent the maximum average amount of money on orders.
    The results are ordered by customer ID in ascending order.

    :return: A list of customer IDs. Returns an empty list if no customers are found or an error occurs.
    """
    resultList = []
    query_string = f'SELECT DISTINCT Cust_id FROM Customer_Avg_Spending_View ' \
                   f'WHERE Avg_Spending = (SELECT MAX(Avg_Spending) FROM Customer_Avg_Spending_View) ' \
                   f'ORDER BY Cust_id ASC;'
    query = sql.SQL(query_string)
    retVal, rowsAmount, resultRows, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('get_customers_spent_max_avg_amount_money')
        print(exp)

    # In case nothing was found, the number of rows will be 0, and the following loop will do nothing.
    # This means that the list is empty as initialized
    for i in range(rowsAmount):
        resultList.append(resultRows[i]['Cust_id'])

    return resultList


# Dishes_Ordered_Amount_View
# Use the View and select the max ordered dish_id (addtional order by dish_id (desc order))
def get_most_ordered_dish_in_period(start: datetime, end: datetime) -> Dish:  
    """
    Retrieves the dish that was ordered the most within a specified time period.

    :param start: The start datetime of the period.
    :param end: The end datetime of the period.
    :return: A Dish object representing the most ordered dish. Returns BadDish if no dishes were ordered in the period or an error occurs.
    """
    resultDish = BadDish()

    query_string = (f'SELECT * FROM Dishes WHERE Dish_id = '
                    f'(SELECT Dish_id FROM Dishes_Ordered_Amount_View '
                    f"WHERE Order_Date BETWEEN '{start}' AND '{end}' "
                    f'GROUP BY Dish_id '
                    f'ORDER BY SUM(Ordered_Amount) DESC, Dish_id ASC '
                    f'LIMIT 1);') # ,  AS Tot_Amount

    query = sql.SQL(query_string)
    retVal, rowsAmount, resultRows, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('get_most_ordered_dish_in_period')
        print(exp)

    # In case nothing was found, the number of rows will be 0, and the following loop will do nothing.
    # This means that the list is empty as initialized
    if 1 == rowsAmount:
        resultDish = Dish(resultRows[0]['Dish_id'], resultRows[0]['Name'], resultRows[0]['Price'], resultRows[0]['Is_active'])

    return resultDish


# A Dish_Avg_Rating_View View of all the dishes and their avg rating - Dishes that are not rated, their avg rating is 3
# A View That Shows all the dishes that are relevant to each customer (Join Reservation with OrderDetails)
# if there are a few dishes with the same rating, chose the one with the lower dish_id
# Query:
# 1. Join between reservations and OrderDetails on order_id
# 2. Select the rows that represent the given customer id
# 3. Check if one of the dishes that are in the result, are included in the DishesRatings view (LIMITED TO 5)
# FALSE - in case customer doesn't exist, has no orders related to him or there are no dishes in the DB
def did_customer_order_top_rated_dishes(cust_id: int) -> bool:
    """
    Checks if a customer has ordered any of the top-rated dishes (dishes with an average rating of 5).

    :param cust_id: The ID of the customer.
    :return: True if the customer has ordered at least one top-rated dish, False otherwise.
    """
    result = False

    query_string = (f'SELECT * FROM Customer_Ordered_Dishes_View '
                    f'WHERE Cust_id = {cust_id} AND Dish_id IN '
                    f'(SELECT Dish_id FROM Dish_Avg_Rating_View ORDER BY Avg_rating DESC LIMIT 5)')

    query = sql.SQL(query_string)
    retVal, rowsAmount, resultRows, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('did_customer_order_top_rated_dishes')
        print(exp)

    if 0 < rowsAmount:
        result = True

    return result


# ---------------------------------- ADVANCED API: ----------------------------------

# Advanced API

# Use the DishesRatings view to find the 5 lowest-rated
# Find all the dishes that were rated by the customer
# Find all the dishes that were ordered by the customer (View)
# (Rated - Ordered) is in (Lowest 5)? on all customers
def get_customers_rated_but_not_ordered() -> List[int]:
    """
    Retrieves the IDs of customers who have rated dishes but have not placed any orders.
    The results are ordered by customer ID in ascending order.

    :return: A list of customer IDs. Returns an empty list if no such customers are found or an error occurs.
    """
    resultList = []

    query_string = (f'SELECT DISTINCT CR.Cust_id FROM Customer_Ratings CR '
                    f'WHERE '
                        f'CR.Rating < 3 '
                        f'AND CR.Dish_id IN (SELECT DAR.Dish_id FROM Dish_Avg_Rating_View DAR ORDER BY DAR.Avg_rating ASC LIMIT 5) '
                        f'AND NOT EXISTS ('
                                f'SELECT COD.Dish_id FROM Customer_Ordered_Dishes_View COD '
                                f'WHERE COD.Cust_id = CR.Cust_id AND COD.Dish_id = CR.Dish_id '
                        f') '
                    f'ORDER BY CR.Cust_id ASC')

    query = sql.SQL(query_string)
    retVal, rowsAmount, resultRows, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('did_customer_order_top_rated_dishes')
        print(exp)

    for i in range(rowsAmount):
        resultList.append(resultRows[i]['Cust_id'])

    return resultList


def get_non_worth_price_increase() -> List[int]:
    """
    Retrieves the IDs of dishes that are not worth a price increase.
    A dish is considered not worth a price increase if its average rating is less than 3.
    The results are ordered by dish ID in ascending order.

    :return: A list of dish IDs. Returns an empty list if all dishes have an average rating of 3 or higher, or if an error occurs.
    """
    resultList = []

    query_string = (f'SELECT curr.Dish_id '
                    f'FROM '
                        f'Avg_Profit_Per_Order ap JOIN '
                        f'(SELECT D.Dish_id AS Dish_id ,D.Price AS Price ,appo.val AS val '
                            f'FROM Avg_Profit_Per_Order appo JOIN '
                                f'Dishes D ON (D.Dish_id = appo.Dish_id AND D.Price= appo.dish_price) '
                                f'WHERE D.Is_active=true) AS curr '
                        f'ON(ap.Dish_id = curr.dish_id) '
                        f'WHERE curr.Price > ap.dish_price AND curr.val < ap.val '
                        f'ORDER BY curr.dish_id ASC')

    query = sql.SQL(query_string)
    retVal, rowsAmount, resultRows, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('get_non_worth_price_increase')
        print(exp)

    for i in range(rowsAmount):
        resultList.append(resultRows[i]['Dish_id'])

    return resultList


# A View that holds all the profit in each month per years
# And each month will be the sum of itself and the month before them in the same year
def get_cumulative_profit_per_month(year: int) -> List[Tuple[int, float]]:
    """
    Calculates the cumulative profit per month for a given year.
    The profit for each month is the sum of the total prices of all orders placed in that month, in case there was no orders, the profit is 0.
    The results are returned as a list of tuples, where each tuple contains the month number (1-12) in descending order and the cumulative profit
    up to that month.

    :param year: The year for which to calculate the cumulative profit.
    :return: A list of tuples (month, cumulative_profit).
    """
    resultList = []

    query_string = (f'''
        SELECT
            months_series.MonthNum AS month,
            COALESCE(
                (
                    SELECT SUM(COALESCE(mpv.Monthly_Profit, 0))
                    FROM Monthly_Profit_View mpv
                    WHERE mpv.Year = {year}
                      AND mpv.Month <= months_series.MonthNum
                ),
                0
            ) AS cumulative_profit
        FROM
            (
                SELECT 1 AS MonthNum UNION ALL
                SELECT 2 UNION ALL
                SELECT 3 UNION ALL
                SELECT 4 UNION ALL
                SELECT 5 UNION ALL
                SELECT 6 UNION ALL
                SELECT 7 UNION ALL
                SELECT 8 UNION ALL
                SELECT 9 UNION ALL
                SELECT 10 UNION ALL
                SELECT 11 UNION ALL
                SELECT 12
            ) AS months_series
        ORDER BY
            months_series.MonthNum DESC;
        ''')

    query = sql.SQL(query_string)
    retVal, rowsAmount, resultRows, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('get_cumulative_profit_per_month')
        print(exp)

    for i in range(rowsAmount):
        resultList.append((resultRows[i]['Month'], float(resultRows[i]['Cumulative_Profit'])))

    return resultList


#
def get_potential_dish_recommendations(cust_id: int) -> List[int]:
    """
    Retrieves potential dish recommendations for a given customer.
    Recommendations are dishes ordered by other customers who have also ordered dishes rated highly (4 or 5) by the given customer, but which the given customer has not yet ordered.
    The results are ordered by dish ID in ascending order.

    :param cust_id: The ID of the customer.
    :return: A list of dish IDs. Returns an empty list if no recommendations are found or an error occurs.
    """
    resultList = []

    query_string = (
        f'SELECT * FROM '
        f'( '
            f'(SELECT Dish_id FROM '
            f'( '
                f'(SELECT b FROM '
                    f'(WITH RECURSIVE a_similar_b AS ( '
                        f'SELECT * FROM SimilarRelation '
                        f'UNION SELECT a_to_b.a, b_to_c.b '
                        f'FROM a_similar_b a_to_b JOIN SimilarRelation b_to_c ON a_to_b.b = b_to_c.a '
                    f') SELECT * FROM a_similar_b where a != b) '
                f'WHERE a = {cust_id}) '
            f') rs JOIN Customer_Ratings dr ON dr.Cust_id = rs.b '
            f'WHERE dr.Rating >= 4) '
            f'EXCEPT (SELECT Dish_id FROM Customer_Ordered_Dishes_View WHERE Cust_id = {cust_id}) '
        f') ORDER BY dish_id ASC;')

    query = sql.SQL(query_string)
    _, rowsAmount, resultRows, exp = handle_query(query)

    if (DEBUG_FLAG and None != exp):
        print('get_cumulative_profit_per_month')
        print(exp)

    for i in range(rowsAmount):
        resultList.append(resultRows[i]['Dish_id'])

    return resultList
