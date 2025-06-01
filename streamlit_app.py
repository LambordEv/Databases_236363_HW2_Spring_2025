# File: test_streamlit.py
import streamlit as st
import pandas as pd
from Solution import *
from Business.Customer import Customer
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

    # TODO Check if we should implement any other function in this streamlit shit
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
            age = st.number_input("Age", min_value=0, step=1, format="%d")
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
            price = st.number_input("Dish Price", format="%d")
            is_active = st.checkbox("Is Active?")
            submitted = st.form_submit_button("Add Dish")
            if submitted:
                result = add_dish(Dish(dish_id, name, price, is_active))
                if result == ReturnValue.OK:
                    st.success("Customer added successfully.")
                elif result == ReturnValue.ALREADY_EXISTS:
                    st.warning("Customer already exists.")
                elif result == ReturnValue.BAD_PARAMS:
                    st.error("Invalid input. Make sure all fields are valid (including a 10-digit phone number).")
                else:
                    st.error("An unexpected error occurred.")

    elif action == "Add Order":
        # TODO: implement
        pass

    elif action == "Place Order":
        # TODO: implement
        pass

    elif action == "Add Dish to Order":
        # TODO: implement
        pass

    elif action == "Visualize Tables":
        st.subheader("Customers")
        res = Connector.DBConnector().execute("SELECT * FROM Customers")[1]
        st.dataframe(pd.DataFrame(res.rows))

        st.subheader("Orders")
        res = Connector.DBConnector().execute("SELECT * FROM orders")[1]
        st.dataframe(pd.DataFrame(res.rows))

        st.subheader("Dishes")
        res = Connector.DBConnector().execute("SELECT * FROM dishes")[1]
        st.dataframe(pd.DataFrame(res.rows))


    elif action == "Total Price of Every Order": 
        # TODO: implement
        pass

    elif action == "Max Avg Spending":
        # TODO: implement
        pass


    elif action == "Dishes ordered":
        # TODO: implement
        pass


if __name__ == "__main__":
    main()