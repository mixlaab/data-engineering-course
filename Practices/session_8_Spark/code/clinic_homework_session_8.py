from pyspark.sql import SparkSession
from functools import reduce

# PostgreSQL connection parameters
conn_params = {
    "host": "postgres_db",
    "port": 5432,
    "database": "clinic_db",
    "user": "myuser",
    "password": "mypassword"
}

# Initialize Spark
spark = SparkSession.builder \
    .appName("ClinicApp") \
    .master("local[*]") \
    .config('spark.jars', './postgresql-42.5.4.jar') \
    .getOrCreate()

# Read CSV files and declare a Temp View clinic
print("Reading and loading a Temp View from csv files.")
csv_files = ["./clinic_1.csv", "./clinic_2.csv", "./clinic_3.csv"]
views = []

for index, file in enumerate(csv_files):
    df = spark.read.csv(file, header=True, inferSchema=True)
    view_name = "clinic_" + str(index + 1)
    df.createOrReplaceTempView(view_name)
    views.append(view_name)

# Create DataFrames for each table with SparkSQL
print("Initializing Dataframes with SparkSQL.")
patient_dfs = []
clinical_specialization_dfs = []
doctor_dfs = []
appointment_dfs = []

for view in views:
    patient_df = spark.sql(f"select distinct patient_name as name, patient_last_name as last_name, patient_address as address from {view}")
    clinical_specialization_df = spark.sql(f"select distinct doctor_clinical_specialization as name from {view}")
    doctor_df = spark.sql(f"select distinct doctor_name as name, doctor_last_name as last_name, doctor_clinical_specialization as specialization from {view}")
    appointment_df = spark.sql(f"select distinct to_date(appointment_date, 'yyyy-MM-dd') as date, to_timestamp(appointment_time, 'HH:mm a') as time, patient_name, patient_last_name, patient_address, doctor_name, doctor_last_name from {view}")

    patient_dfs.append(patient_df)
    clinical_specialization_dfs.append(clinical_specialization_df)
    doctor_dfs.append(doctor_df)
    appointment_dfs.append(appointment_df)

# Union all DataFrames
patient_df = reduce(lambda df1, df2: df1.union(df2).distinct(), patient_dfs)
clinical_specialization_df = reduce(lambda df1, df2: df1.union(df2).distinct(), clinical_specialization_dfs)
doctor_df = reduce(lambda df1, df2: df1.union(df2).distinct(), doctor_dfs)
appointment_df = reduce(lambda df1, df2: df1.union(df2).distinct(), appointment_dfs)

# Define save functions
def read_from_postgres(table_name, conn_params):
    jdbc_url = f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", conn_params["user"]) \
        .option("password", conn_params["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df

def save_to_postgres(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{conn_params['host']}:{conn_params['port']}/{conn_params['database']}") \
        .option("dbtable", table_name) \
        .option("user", conn_params["user"]) \
        .option("password", conn_params["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Save DataFrames to PostgreSQL
print('Saving to postgres...')
save_to_postgres(patient_df, "patient")
save_to_postgres(clinical_specialization_df, "clinical_specialization")

# Read from postgres to get patient and specialization dataframes but now with ids included
patient_df = read_from_postgres("patient", conn_params)
clinical_specialization_df = read_from_postgres("clinical_specialization", conn_params)

# Create views to be used in SQL queries
patient_df.createOrReplaceTempView("patient")
appointment_df.createOrReplaceTempView("appointment")
doctor_df.createOrReplaceTempView("doctor")
clinical_specialization_df.createOrReplaceTempView("specialization")

# Join the doctor_df with the clinical_specialization_df to get the corresponding id for each clinical specialization
doctor_df = spark.sql("""
    SELECT d.name, d.last_name, s.id AS clinical_specialization_id
    FROM doctor d
    JOIN specialization s
    ON d.specialization = s.name
""")
save_to_postgres(doctor_df, "doctor")

# Read from postgres to get doctor dataframes but now with ids included
doctor_df = read_from_postgres("doctor", conn_params)
doctor_df.createOrReplaceTempView("doctor")

appointment_df = spark.sql("""
    SELECT a.date, a.time, p.id AS patient_id, d.id AS doctor_id
    FROM appointment a
    JOIN patient p
    ON a.patient_name = p.name
    AND a.patient_last_name = p.last_name
    AND a.patient_address = p.address
    JOIN doctor d
    ON a.doctor_name = d.name
    AND a.doctor_last_name = d.last_name
""")
save_to_postgres(appointment_df, "appointment")
print('Save to postgres completed!')