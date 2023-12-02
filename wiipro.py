import pandas as pd
import sqlite3
import time
import psutil
from datetime import datetime
import threading

class wiipro:

    def __init__(self):
        self.data = None
        self.database_name = "db"
        self.modifier_dict = {}
        self.result = {}
        self.instrument_list = []

    def calculate_performance(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            start_resources = psutil.cpu_percent(), psutil.virtual_memory().percent

            result = func(*args, **kwargs)

            end_time = time.time()
            end_resources = psutil.cpu_percent(), psutil.virtual_memory().percent

            execution_time = end_time - start_time
            cpu_percent_change = end_resources[0] - start_resources[0]
            memory_percent_change = end_resources[1] - start_resources[1]

            print(f"--------------------------------------------------")
            print(f"Function: {func.__name__}")
            print(f"Execution Time: {execution_time:.6f} seconds")
            print(f"CPU Usage Change: {cpu_percent_change:.2f}%")
            print(f"Memory Usage Change: {memory_percent_change:.2f}%")


            return result

        return wrapper
    
    @calculate_performance
    def read_data(self, path):
        print("Getting Data...")
        df = pd.read_csv(path, header=None, names=["INSTRUMENT_NAME", "DATE", "VALUE"])
        # Convert Data String into DataTime format
        print("Transforming DateTime...")
        df["DATE"] = pd.to_datetime(df["DATE"], format='%d-%b-%Y')
        print("Saving Data...")
        self.data = df
        print("Defining Instruments...")
        self.instrument_list = list(self.data["INSTRUMENT_NAME"].unique())    # Speedup engine workload caching the instrument list in a variable
        return True

    @calculate_performance
    def inizialize_sqlite3(self, database_name = "db", mokeup_data = False):
        # Inizialize the db and table

        database_name = f'{database_name}.db'
        self.database_name = database_name

        connection = sqlite3.connect(database_name)
        cursor = connection.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS INSTRUMENT_PRICE_MODIFIER (
                ID INTEGER PRIMARY KEY,
                NAME TEXT,
                MULTIPLIER REAL
            )
        ''')

        if mokeup_data:
            cursor.execute(f"INSERT INTO INSTRUMENT_PRICE_MODIFIER (NAME, MULTIPLIER) VALUES (?, ?)", ("INSTRUMENT1", 1.5))  
            cursor.execute(f"INSERT INTO INSTRUMENT_PRICE_MODIFIER (NAME, MULTIPLIER) VALUES (?, ?)", ("INSTRUMENT2", 1.2))

        connection.commit()
        connection.close()

        return True

    @calculate_performance
    def update_price_modifier(self):
        connection = sqlite3.connect(self.database_name)
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM INSTRUMENT_PRICE_MODIFIER")
        rows = cursor.fetchall()
        connection.close()
        self.modifier_dict = {row[1]: row[2] for row in rows}
        return True

    @calculate_performance
    def run_engine(self, func = None):
        threads = []

        # Order by Data
        self.__sort()

        self.__custom_instr3(func)                                                     # Custom Function for the Instrument 3

        threads.append(threading.Thread(target=self.__mean_instr1))                  # Mean for Instrument 1
        threads.append(threading.Thread(target=self.__mean_instr2_nov2014))                  # Mean for Instrument 2 (only Novembre 2014)
        threads.append(threading.Thread(target=self.__instrN_sum_last_10))                  # Sum last 10 elements of every Instrument except Instrument1, Instrument2, Instrument3
        
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        return True

    @calculate_performance
    def __sort(self):
        print("Starting Sorting...")
        self.data = self.data.sort_values(by="DATE")
        print("Sorting Ended.")
    
    @calculate_performance
    def __mean_instr1(self):
        print("Starting Thread 1...")
        self.result["INSTRUMENT1_MEAN"] = self.data[self.data["INSTRUMENT_NAME"] == "INSTRUMENT1"]["VALUE"].mean()
        print("Thread 1 Ended.")

    @calculate_performance
    def __mean_instr2_nov2014(self):
        print("Starting Thread 2...")
        target_date = pd.to_datetime('Nov-2014', format='%b-%Y').to_period('M')
        self.result["INSTRUMENT2_NOV2014_MEAN"] = self.data[(self.data["INSTRUMENT_NAME"] == "INSTRUMENT2") & (self.data["DATE"].dt.to_period('M') == target_date)]["VALUE"].mean()
        print("Thread 2 Ended.")

    @calculate_performance
    def __custom_instr3(self, func):
        print("Starting Thread 3...")
        self.result["INSTRUMENT3_CUSTOM"] = func(self.data[self.data["INSTRUMENT_NAME"] == "INSTRUMENT3"]["VALUE"]) if func is not None else None
        print("Thread 3 Ended.")

    @calculate_performance
    def __instrN_sum_last_10(self):
        print(f"Starting Thread 4...")
        # Calculate the sum of the last 10 values for each instrument
        sum_last_10_values_df = self.data.groupby("INSTRUMENT_NAME").tail(10).groupby("INSTRUMENT_NAME")["VALUE"].sum().reset_index()
        for idx, row in sum_last_10_values_df.iterrows():
            if row["INSTRUMENT_NAME"] not in ["INSTRUMENT1", "INSTRUMENT2", "INSTRUMENT3"]:
                self.result[row["INSTRUMENT_NAME"]] = row["VALUE"]
        print(f"Thread 4 Ended.")

    @calculate_performance
    def calculate(self, target_instrument, target_date):
        target_date = pd.to_datetime(target_date, format='%d-%b-%Y')
        if 0 <= target_date.weekday() <= 4:
            value = self.data[(self.data["INSTRUMENT_NAME"] == target_instrument) & (self.data["DATE"] == target_date)]["VALUE"]    # Filter required on millions of rows
            if target_instrument in self.modifier_dict.keys():
                return value * self.modifier_dict[target_instrument]
            else:
                return value
        else:
            print("The selected date is not a business day (Mon-Fri)")
            return None