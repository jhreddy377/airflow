import pandas as pd
import yfinance as yf
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import os
import glob

#  DAG Workflow configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now().replace(hour=18, minute=0, second=0, microsecond=0), # Start at 6 PM current date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2, # Retry twice
    'retry_delay': timedelta(minutes=5), # 5-minute interval for retries
}

# The schedule runs at 6 PM (0 minutes, 18 hours) on every weekday (Mon-Fri, 1-5)
with DAG(
    'marketvol',
    default_args=default_args,
    description='A simple DAG for stock market data analysis',
    schedule='0 18 * * 1-5',
    catchup=False,
) as dag:

    # Define functions for PythonOperators
    def download_stock_data(symbol, run_date=None, output_dir=None, **context):
        """
        Download 1-minute interval market data for the given symbol and save to CSV.
        The function uses the provided run_date (YYYY-MM-DD) or today's date as start_date and start_date + 1 day as end_date.
        """
        # Determine run date (use provided run_date string YYYY-MM-DD or today's date)
        if run_date:
            try:
                start_date = date.fromisoformat(run_date)
            except Exception:
                start_date = date.today()
        else:
            start_date = date.today()

        end_date = start_date + timedelta(days=1)

        print(f"Downloading {symbol} from {start_date} to {end_date}")
        df = yf.download(symbol, start=start_date, end=end_date, interval='1m', progress=False)

        # output directory 
        if output_dir is None:
            output_dir = f"/tmp/data/{start_date.isoformat()}"
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{symbol}_{timestamp}.csv"
        file_path = os.path.join(output_dir, filename)

        # Save CSV without header 
        df.to_csv(file_path, header=False)
        print(f"Downloaded and saved data for {symbol} to {file_path}")

        # Push path to XCom for downstream tasks
        try:
            ti = context.get('ti') or context.get('task_instance')
            if ti:
                ti.xcom_push(key=f"{symbol}_file", value=file_path)
        except Exception:
            pass

        return file_path

    def run_custom_query(run_date=None, **context):
        # run_date is expected as 'YYYY-MM-DD'
        if run_date:
            date_str = run_date
        else:
            # fallback to execution_date in context
            exec_dt = context.get('execution_date') or context.get('ds')
            if hasattr(exec_dt, 'strftime'):
                date_str = exec_dt.strftime('%Y-%m-%d')
            else:
                date_str = str(exec_dt)

        data_dir = f"/opt/airflow/data/{date_str}"

        # Find latest files for each symbol
        aapl_files = sorted(glob.glob(os.path.join(data_dir, 'AAPL_*.csv')))
        tsla_files = sorted(glob.glob(os.path.join(data_dir, 'TSLA_*.csv')))

        if not aapl_files or not tsla_files:
            raise ValueError("Stock data files not found for query execution.")

        aapl_file = aapl_files[-1]
        tsla_file = tsla_files[-1]

        # Define columns based on project schema
        columns = ['date_time', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
        aapl_df = pd.read_csv(aapl_file, header=None, names=columns)
        tsla_df = pd.read_csv(tsla_file, header=None, names=columns)
        
        # query: Calculate and print the average closing price for both
        avg_aapl = aapl_df['close'].mean()
        avg_tsla = tsla_df['close'].mean()
        
        print(f"Average AAPL closing price for {date_str}: {avg_aapl}")
        print(f"Average TSLA closing price for {date_str}: {avg_tsla}")
       

    # 1. Create a BashOperator to initialize a temporary directory (t0)
    # Use Jinja templating `{{ ds }}` for the execution date in YYYY-MM-DD format
    t0_create_dir = BashOperator(
        task_id='t0',
        bash_command='mkdir -p /tmp/data/{{ ds }}',
    )

    # 2. Create PythonOperators to download the market data (t1, t2)
    # Pass the execution date to the function
    t1 = PythonOperator(
        task_id='t1',
        python_callable=download_stock_data,
        op_kwargs={'symbol': 'AAPL', 'run_date': '{{ ds }}', 'output_dir': '/tmp/data/{{ ds }}'},
    )

    t2 = PythonOperator(
        task_id='t2',
        python_callable=download_stock_data,
        op_kwargs={'symbol': 'TSLA', 'run_date': '{{ ds }}', 'output_dir': '/tmp/data/{{ ds }}'},
    )

    # 3. Create BashOperators to move the downloaded file to a data location (t3, t4)
    # Files are already saved in the correct /tmp/data/{{ ds }} directory in the download function, 
    # so these tasks can be simple placeholder BashOperators or use a more explicit move command if preferred.
    t3_load_aapl_hdfs = BashOperator(
        task_id='t3',
        bash_command=(
            "mkdir -p /opt/airflow/data/{{ ds }} && "
            "mv {{ ti.xcom_pull(task_ids='t1', key='AAPL_file') }} /opt/airflow/data/{{ ds }}/ || true"
        ),
        dag=dag,
    )

    t4_load_tsla_hdfs = BashOperator(
        task_id='t4',
        bash_command=(
            "mkdir -p /opt/airflow/data/{{ ds }} && "
            "mv {{ ti.xcom_pull(task_ids='t2', key='TSLA_file') }} /opt/airflow/data/{{ ds }}/ || true"
        ),
        dag=dag,
    )
    
    # 4. Create a PythonOperator to run a query on both data files (t5)
    t5 = PythonOperator(
        task_id='t5',
        python_callable=run_custom_query,
        op_kwargs={'run_date': '{{ ds }}'},
    )

    # Set job dependencies
    # t1 and t2 must run only after t0
    t0_create_dir >> [t1, t2]

    # t3 must run after t1; t4 must run after t2
    t1 >> t3_load_aapl_hdfs
    t2 >> t4_load_tsla_hdfs

    # t5 must run after both t3 and t4 are complete
    [t3_load_aapl_hdfs, t4_load_tsla_hdfs] >> t5