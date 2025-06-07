# File: test_streamlit.py
import streamlit as st
import pandas as pd
from streamlit import columns

from Solution import *
from Business.Customer import Customer
from datetime import datetime
import psycopg2

DB_PARAMS = {
    "dbname": "Yummy",
    "user": "DB_Test_User",
    "password": "Qwerty-123456",
    "host": "localhost",
    "port": 5432
}


def main():
    st.title("Yummify")

    if "db_initialized" not in st.session_state:
        st.session_state.db_initialized = False

    if st.button("Initialize Database (Drop/Create)"):
        drop_tables()
        create_tables()
        st.session_state.db_initialized = True
        st.success("Database initialized!")

    if not st.session_state.db_initialized:
        st.warning("Please initialize the database first.")
        return

    action = st.selectbox("Choose Action", [
        "Add Customer",
        "Add Dish",
        "Add Order",
        "Place Order",
        "Add Dish to Order",
        "Visualize Tables",
        "Total Price of Every Order",
        "Max Avg Spending",
        "Dishes ordered"
    ])

    if action == "Add Customer":
        with st.form("Add Customer"):
            cust_id = st.number_input("Customer ID", min_value=1, step=1, format="%d")
            name = st.text_input("Full Name")
            age = st.number_input("Age", min_value=18, max_value=120, step=1, format="%d")
            phone = st.text_input("Phone (10 digits)")
            submitted = st.form_submit_button("Add Customer")
            if submitted:
                result = add_customer(Customer(cust_id, name, age, phone))
                if result == ReturnValue.OK:
                    st.success("Customer added successfully.")
                elif result == ReturnValue.ALREADY_EXISTS:
                    st.warning("Customer already exists.")
                elif result == ReturnValue.BAD_PARAMS:
                    st.error("Invalid input. Make sure all fields are valid (including a 10-digit phone number).")
                else:
                    st.error("An unexpected error occurred.")

    elif action == "Add Dish":
        with st.form("Add Dish"):
            dish_id = st.number_input("Dish ID", min_value=1, step=1, format="%d")
            name = st.text_input("Dish Name")
            price = st.number_input("Dish Price", min_value=0.0, step=0.1, format="%f")
            is_active = st.checkbox("Is Active", value=True)
            submitted = st.form_submit_button("Add Dish")
            if submitted:
                result = add_dish(Dish(dish_id, name, price, is_active))
                if result == ReturnValue.OK:
                    st.success("Dish added successfully.")
                elif result == ReturnValue.ALREADY_EXISTS:
                    st.warning("Dish already exists.")
                elif result == ReturnValue.BAD_PARAMS:
                    st.error("Invalid input parameters.")
                else:
                    st.error("An unexpected error occurred.")

    elif action == "Add Order":
        with st.form("Add Order"):
            order_id = st.number_input("Order ID", min_value=1, step=1, format="%d")
            date = st.date_input("Date")
            time = st.time_input("Time")
            address = st.text_input("Address")
            delivery_fee = st.number_input("Delivery Fee", min_value=0.0, step=0.1, format="%f")
            submitted = st.form_submit_button("Add Order")
            if submitted:
                result = add_order(Order(order_id, datetime.combine(date, time), delivery_fee, address))
                if result == ReturnValue.OK:
                    st.success("Order added successfully.")
                elif result == ReturnValue.ALREADY_EXISTS:
                    st.warning("Order already exists.")
                elif result == ReturnValue.BAD_PARAMS:
                    st.error("Invalid input parameters.")
                else:
                    st.error("An unexpected error occurred.")

    elif action == "Place Order":
        with st.form("Place Order"):
            order_id = st.number_input("Order ID", min_value=1, step=1, format="%d")
            customer_id = st.number_input("Customer ID", min_value=1, step=1, format="%d")
            submitted = st.form_submit_button("Place Order")
            if submitted:
                result = customer_placed_order(customer_id, order_id)
                if result == ReturnValue.OK:
                    st.success("Order placed successfully.")
                elif result == ReturnValue.NOT_EXISTS:
                    st.warning("Order or customer does not exist.")
                elif result == ReturnValue.ALREADY_EXISTS:
                    st.warning("Order is already placed.")
                elif result == ReturnValue.BAD_PARAMS:
                    st.error("Invalid input parameters.")
                else:
                    st.error("An unexpected error occurred.")

    elif action == "Add Dish to Order":
        with st.form("Add Dish to Order"):
            order_id = st.number_input("Order ID", min_value=1, step=1, format="%d")
            dish_id = st.number_input("Dish ID", min_value=1, step=1, format="%d")
            amount = st.number_input("Amount", min_value=1, step=1, format="%d")
            submitted = st.form_submit_button("Add Dish to Order")
            if submitted:
                result = order_contains_dish(order_id, dish_id, amount)
                if result == ReturnValue.OK:
                    st.success("Dish added to order successfully.")
                elif result == ReturnValue.NOT_EXISTS:
                    st.warning("Order or dish does not exist.")
                elif result == ReturnValue.ALREADY_EXISTS:
                    st.warning("Dish is already in the order.")
                elif result == ReturnValue.BAD_PARAMS:
                    st.error("Invalid input parameters.")
                else:
                    st.error("An unexpected error occurred.")

    elif action == "Visualize Tables":
        st.subheader("Customers")
        res = Connector.DBConnector().execute("SELECT * FROM customers")[1]
        st.dataframe(pd.DataFrame(res.rows, columns=['Customer ID', 'Customer Name', 'Customer Age', 'Customer Phone Number']))

        st.subheader("Orders")
        res = Connector.DBConnector().execute("SELECT * FROM orders")[1]
        st.dataframe(pd.DataFrame(res.rows, columns=['Order ID', 'Order Date', 'Order Address', 'Order Delivery Fee']))

        st.subheader("Dishes")
        res = Connector.DBConnector().execute("SELECT * FROM dishes")[1]
        st.dataframe(pd.DataFrame(res.rows, columns=['Dish ID', 'Dish Name', 'Dish Price', 'Is Active?']))


    elif action == "Total Price of Every Order": 
        res = Connector.DBConnector().execute("SELECT * FROM orders")[1]
        df = []
        for row in res.rows:
            currOrderID = row[0]
            df.append((currOrderID, get_order_total_price(currOrderID)))

        st.dataframe(pd.DataFrame(df, columns=["Order ID", "Total Price"]))



    elif action == "Max Avg Spending":
        result_dict = []
        res = get_customers_spent_max_avg_amount_money()
        for i in res:
            currCustomer = get_customer(i)
            result_dict.append((i, currCustomer.get_full_name()))

        st.dataframe(pd.DataFrame(result_dict, columns=['Customer ID', 'Customer Name']))




    elif action == "Dishes ordered":
        res = Connector.DBConnector().execute("SELECT * FROM orders")[1]
        allOrderedDishes = []
        for row in res.rows:
            currOrderDishList = get_all_order_items(row[0])
            allOrderedDishes += [[currDish.get_dish_id() for currDish in currOrderDishList]]

        st.dataframe(pd.DataFrame(allOrderedDishes))





if __name__ == "__main__":
    main()