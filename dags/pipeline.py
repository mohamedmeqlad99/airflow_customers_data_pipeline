import os
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from faker import Faker
import pandas as pd
import random
from uuid import uuid4
import psycopg2

# Create an instance of Faker
faker_instance = Faker()

# SQL to create tables
def create_tables():
    conn = psycopg2.connect("dbname=airflow user=airflow password=airflow host=postgres")
    cursor = conn.cursor()
    
    # Create customers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id UUID PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            address TEXT,
            city VARCHAR(50),
            state VARCHAR(50),
            country VARCHAR(50),
            postal_code VARCHAR(20),
            date_of_birth DATE,
            account_balance NUMERIC(10, 2),
            created_at TIMESTAMP
        );
    """)
    
    # Create products table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id SERIAL PRIMARY KEY,
            product_name VARCHAR(100),
            category VARCHAR(50),
            price NUMERIC(10, 2),
            stock_quantity INT
        );
    """)
    
    # Create stores table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            store_id SERIAL PRIMARY KEY,
            store_name VARCHAR(100),
            location VARCHAR(100),
            store_type VARCHAR(50)
        );
    """)
    
    # Create transactions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id UUID PRIMARY KEY,
            customer_id UUID REFERENCES customers(customer_id),
            transaction_date TIMESTAMP,
            transaction_amount NUMERIC(10, 2),
            store_id INT REFERENCES stores(store_id),
            product_id INT REFERENCES products(product_id),
            quantity INT,
            payment_method VARCHAR(50)
        );
    """)
    
    conn.commit()
    cursor.close()
    conn.close()

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
    df_customers = pd.DataFrame(customer_data)
    df_customers.to_csv('/home/meqlad/airflow_customers_data_pipeline/data/customers.csv', index=False)

# Generate fake product data
def generate_products(num_products=50):
    product_data = []
    for i in range(num_products):
        product = {
            'product_id': i + 1,
            'product_name': faker_instance.word(),
            'category': random.choice(['Electronics', 'Clothing', 'Books', 'Food']),
            'price': round(random.uniform(5, 500), 2),
            'stock_quantity': random.randint(1, 100)
        }
        product_data.append(product)
    df_products = pd.DataFrame(product_data)
    df_products.to_csv('/home/meqlad/airflow_customers_data_pipeline/data/products.csv', index=False)

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
    df_stores = pd.DataFrame(store_data)
    df_stores.to_csv('/home/meqlad/airflow_customers_data_pipeline/data/stores.csv', index=False)

# Generate fake transaction data
def generate_transactions(num_transactions=1000):
    df_customers = pd.read_csv('/home/meqlad/airflow_customers_data_pipeline/data/customers.csv')
    df_products = pd.read_csv('/home/meqlad/airflow_customers_data_pipeline/data/products.csv')
    df_stores = pd.read_csv('/home/meqlad/airflow_customers_data_pipeline/data/stores.csv')
    
    transaction_data = []
    for _ in range(num_transactions):
        transaction = {
            'transaction_id': str(uuid4()),
            'customer_id': random.choice(df_customers['customer_id']),
            'transaction_date': faker_instance.date_time_this_year(),
            'transaction_amount': round(random.uniform(20, 1000), 2),
            'store_id': random.choice(df_stores['store_id']),
            'product_id': random.choice(df_products['product_id']),
            'quantity': random.randint(1, 10),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'Cash', 'PayPal'])
        }
        transaction_data.append(transaction)
    df_transactions = pd.DataFrame(transaction_data)
    df_transactions.to_csv('/home/meqlad/airflow_customers_data_pipeline/data/transactions.csv', index=False)

# Calculate metrics and generate report
def calculate_metrics(**kwargs):
    data_folder = '/home/meqlad/airflow_customers_data_pipeline/data/'
    transactions_file = os.path.join(data_folder, 'transactions.csv')
    df_transactions = pd.read_csv(transactions_file)

    if df_transactions.empty:
        return "No transactions available to generate metrics."

    total_transaction_amount = df_transactions['transaction_amount'].sum()
    top_customer_df = df_transactions.groupby('customer_id')['transaction_amount'].sum()
    top_customer = top_customer_df.idxmax() if not top_customer_df.empty else 'N/A'
    top_customer_amount = top_customer_df.max() if not top_customer_df.empty else 0
    average_transaction_amount = df_transactions['transaction_amount'].mean()
    total_number_of_transactions = df_transactions['transaction_id'].nunique()
    min_transaction_amount = df_transactions['transaction_amount'].min()
    max_transaction_amount = df_transactions['transaction_amount'].max()

    # Generate the report
    report = f"""
    Transaction Metrics Report
    ==========================
    Total Transaction Amount: {total_transaction_amount:.2f}
    Top Customer ID: {top_customer} (Total: {top_customer_amount:.2f})
    Average Transaction Amount: {average_transaction_amount:.2f}
    Total Number of Transactions: {total_number_of_transactions}
    Minimum Transaction Amount: {min_transaction_amount:.2f}
    Maximum Transaction Amount: {max_transaction_amount:.2f}
    """

    # Save the report to a text file
    report_file = os.path.join(data_folder, 'metrics_report.txt')
    with open(report_file, 'w') as f:
        f.write(report)

    return report_file

# Delete files from the data folder
def delete_files():
    data_folder = '/home/meqlad/airflow_customers_data_pipeline/data/'
    for filename in os.listdir(data_folder):
        file_path = os.path.join(data_folder, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)

# Default args for DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',  
    catchup=False
) as dag:

    create_tables_task = PythonOperator(
        task_id='create_tables_task',
        python_callable=create_tables
    )

    generate_customers_task = PythonOperator(
        task_id='generate_customers_task',
        python_callable=generate_customers
    )

    generate_products_task = PythonOperator(
        task_id='generate_products_task',
        python_callable=generate_products
    )

    generate_stores_task = PythonOperator(
        task_id='generate_stores_task',
        python_callable=generate_stores
    )

    generate_transactions_task = PythonOperator(
        task_id='generate_transactions_task',
        python_callable=generate_transactions
    )

    calculate_metrics_task = PythonOperator(
        task_id='calculate_metrics_task',
        python_callable=calculate_metrics,
        provide_context=True
    )

    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='recipient@example.com',  # Change this to your recipient email
        subject='Daily Metrics Report',
        html_content="Please find the daily metrics report attached.",
        files=[os.path.join('/home/meqlad/airflow_customers_data_pipeline/data/', 'metrics_report.txt')]
    )

    delete_files_task = PythonOperator(
        task_id='delete_files_task',
        python_callable=delete_files
    )

    # Define the task order
    create_tables_task >> generate_customers_task
    create_tables_task >> generate_products_task
    create_tables_task >> generate_transactions_task
    create_tables_task >> generate_stores_task
    generate_stores_task >> calculate_metrics_task
    calculate_metrics_task >> send_email_task
    send_email_task >> delete_files_task
