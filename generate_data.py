from faker import Faker
import pandas as pd
import random
from uuid import uuid4
from datetime import datetime

# Create an instance of Faker
faker_instance = Faker()

# Generate fake customer data
def generate_customers(num_customers=100):
    customer_data = []
    for _ in range(num_customers):
        customer = {
            'customer_id': str(uuid4()),
            'first_name': faker_instance.first_name(),
            'last_name': faker_instance.last_name(),
            'email': faker_instance.email(),
            'address': faker_instance.address(),
            'city': faker_instance.city(),
            'state': faker_instance.state(),
            'country': faker_instance.country(),
            'postal_code': faker_instance.postcode(),
            'date_of_birth': faker_instance.date_of_birth(),
            'account_balance': round(random.uniform(100, 10000), 2),
            'created_at': faker_instance.date_time_this_year()
        }
        customer_data.append(customer)
    return pd.DataFrame(customer_data)

# Generate fake product data
def generate_products(num_products=50):
    product_data = []
    for i in range(num_products):
        product = {
            'product_id': i + 1,  # Start product IDs from 1
            'product_name': faker_instance.word(),
            'category': random.choice(['Electronics', 'Clothing', 'Books', 'Food']),
            'price': round(random.uniform(5, 500), 2),
            'stock_quantity': random.randint(1, 100)
        }
        product_data.append(product)
    return pd.DataFrame(product_data).reset_index(drop=True)

# Generate fake store data
def generate_stores(num_stores=10):
    store_data = []
    for i in range(num_stores):
        store = {
            'store_id': i + 1,
            'store_name': faker_instance.company(),
            'location': faker_instance.city(),
            'store_type': random.choice(['Online', 'Retail'])
        }
        store_data.append(store)
    return pd.DataFrame(store_data).reset_index(drop=True)

# Generate fake transaction data
def generate_transactions(customers, products, stores, num_transactions=1000):
    transaction_data = []
    for _ in range(num_transactions):
        transaction = {
            'transaction_id': str(uuid4()),
            'customer_id': random.choice(customers['customer_id']),
            'transaction_date': faker_instance.date_time_this_year(),
            'transaction_amount': round(random.uniform(20, 1000), 2),
            'store_id': random.choice(stores['store_id']),
            'product_id': random.choice(products['product_id']),
            'quantity': random.randint(1, 10),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'Cash', 'PayPal'])
        }
        transaction_data.append(transaction)
    return pd.DataFrame(transaction_data).reset_index(drop=True)

if __name__ == "__main__":
    # Generate fake data
    df_customers = generate_customers(1000)
    df_products = generate_products(50)
    df_stores = generate_stores(10)
    df_transactions = generate_transactions(df_customers, df_products, df_stores, 5000)
    
    # Save to CSV for later use by Airflow
    df_customers.to_csv('customers.csv', index=False)
    df_products.to_csv('products.csv', index=False)
    df_stores.to_csv('stores.csv', index=False)
    df_transactions.to_csv('transactions.csv', index=False)
