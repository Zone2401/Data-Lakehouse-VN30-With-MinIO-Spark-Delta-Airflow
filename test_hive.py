
from spark_minio import create_spark_session

def test_hive_connection():
    spark = create_spark_session("HiveTest")
    
    print("\n [HIVE VERIFICATION]")
    try:
        # Create a test database
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
        print(" Successfully created/verified database 'test_db'")
        
        # Create a test table
        spark.sql("CREATE TABLE IF NOT EXISTS test_db.test_table (id INT, name STRING) USING PARQUET")
        print(" Successfully created/verified table 'test_db.test_table'")
        
        # List databases
        dbs = spark.sql("SHOW DATABASES").collect()
        print(f" Datasets in Hive: {[db[0] for db in dbs]}")
        
        # Insert data
        spark.sql("INSERT INTO test_db.test_table VALUES (1, 'Hello Hive')")
        print(" Successfully inserted data into Hive table")
        
        # Query data
        df = spark.sql("SELECT * FROM test_db.test_table")
        df.show()
        
        print("\n [SUCCESS] Hive Metastore is working correctly with Spark!")
        
    except Exception as e:
        print(f"\n [ERROR] Hive verification failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    test_hive_connection()
