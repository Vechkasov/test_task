from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, array, struct, explode, lit, create_map
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import socket
import sys
import re

def get_local_ip():
    """Получаем локальный IP адрес"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except:
        return "127.0.0.1"

def get_postgres_host():
    """Определяем правильный хост для PostgreSQL"""
    docker_hosts = ['postgres', 'db', 'host.docker.internal', 'localhost']
    
    for host in docker_hosts:
        try:
            socket.gethostbyname(host)
            print(f"PostgreSQL host found: {host}")
            return host
        except socket.gaierror:
            continue
    
    return "localhost"

def melt_dataframe(df, id_vars, var_name="date", value_name="cases"):
    """
    Трансформирует DataFrame из широкого формата в длинный (melt/unpivot)
    
    Parameters:
    - df: исходный DataFrame
    - id_vars: список колонок, которые остаются как идентификаторы
    - var_name: название новой колонки для дат
    - value_name: название новой колонки для значений
    """
    # Получаем все колонки, которые не являются id_vars (это колонки с датами)
    value_vars = [c for c in df.columns if c not in id_vars]
    
    print(f"ID columns: {id_vars}")
    print(f"Date columns (first 5): {value_vars[:5]}")
    print(f"Total date columns: {len(value_vars)}")
    
    if not value_vars:
        print("WARNING: No date columns found!")
        return df
    
    # Способ 1: Используем stack (более производительный)
    try:
        # Создаем выражение для stack
        # stack(n, col1, 'val1', col2, 'val2', ...) as (date, cases)
        stack_items = []
        for date_col in value_vars:
            stack_items.append(f"'{date_col}'")  # Название колонки как строка
            stack_items.append(f"`{date_col}`")  # Значение колонки
        
        stack_expr = f"stack({len(value_vars)}, {', '.join(stack_items)}) as ({var_name}, {value_name})"
        
        print(f"Stack expression created")
        
        # Применяем stack
        melted_df = df.select(
            *id_vars,
            expr(stack_expr)
        )
        
        return melted_df
        
    except Exception as e:
        print(f"Stack method failed: {e}")
        print("Trying alternative method...")
        
        # Способ 2: Используем explode + array + struct (более гибкий)
        try:
            # Создаем массив структур для каждой колонки с датой
            date_structs = []
            for date_col in value_vars:
                date_structs.append(
                    struct(
                        lit(date_col).alias(var_name),
                        col(date_col).alias(value_name)
                    )
                )
            
            # Создаем массив всех структур и взрываем его
            melted_df = df.select(
                *id_vars,
                explode(array(*date_structs)).alias("tmp")
            ).select(
                *id_vars,
                col("tmp.date").alias(var_name),
                col("tmp.cases").alias(value_name)
            )
            
            return melted_df
            
        except Exception as e2:
            print(f"Alternative method also failed: {e2}")
            raise

def extract_date_from_colname(date_col):
    """
    Извлекает дату из названия колонки формата DD_MM_YYYY
    """
    parts = date_col.split('_')
    if len(parts) == 3 and len(parts[2]) == 4:
        day, month, year = parts
        return f"{year}-{month}-{day}"  # YYYY-MM-DD для SQL
    return date_col

def truncate_postgres_table(host, dbname, user, password, schema, table):
    """Напрямую выполняет TRUNCATE таблицы в PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        # Формируем полное имя таблицы с учетом схемы
        full_table_name = sql.Identifier(schema, table)
        
        # Выполняем TRUNCATE
        cur.execute(sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE;").format(full_table_name))
        
        cur.close()
        conn.close()
        print(f"✓ Table {schema}.{table} truncated successfully")
        return True
    except Exception as e:
        print(f"TRUNCATE failed for {schema}.{table}: {e}")
        return False
    
def main():
    local_ip = get_local_ip()
    print(f"Local IP: {local_ip}")
    
    pg_host = get_postgres_host()
    print(f"Using PostgreSQL host: {pg_host}")
    
    # Параметры для прямого подключения (для TRUNCATE)
    pg_params = {
        'host': pg_host,
        'dbname': 'airflow',
        'user': 'airflow',
        'password': 'airflow'
        }
		
    # Пытаемся TRUNCATE таблицу
    truncate_success = truncate_postgres_table(
        schema='stage',
        table='covid_data',
        **pg_params
    )
    
    # Создаем SparkSession
    spark = SparkSession.builder \
        .appName("TransformStage") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.host", local_ip) \
        .config("spark.driver.port", "0") \
        .config("spark.driver.blockManager.port", "0") \
        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.0.jar") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.debug.maxToStringFields", "1000") \
        .getOrCreate()
    
    try:
        # Подключение к PostgreSQL
        url = f"jdbc:postgresql://{pg_host}:5432/airflow"
        properties = {
            'driver': 'org.postgresql.Driver',
            'user': 'airflow',
            'password': 'airflow'
        }
        
        # Создаем схемы если не существуют     
        spark.sql("CREATE DATABASE IF NOT EXISTS stage")              
        
        # Чтение и трансформация данных для каждого года
        for year in range(2020, 2024):
            source_table = f"raw.covid_data_{year}"
            target_table = f"stage.covid_data"
            
            print(f"\n{'='*60}")
            print(f"Transforming {source_table} -> {target_table}")
            
            try:
                # Проверяем существование таблицы
                try:
                    df_wide = spark.read.jdbc(
                        url=url, 
                        table=source_table, 
                        properties=properties
                    )
                except Exception as e:
                    print(f"Table {source_table} does not exist: {e}")
                    continue
                
                print(f"Read {df_wide.count()} rows from {source_table}")
                print(f"Columns in source table: {len(df_wide.columns)}")
                
                # Показываем структуру данных
                print(f"\nSource schema:")
                df_wide.printSchema()
                
                print(f"\nFirst few rows from source:")
                df_wide.show(3, truncate=False)
                
                # Определяем ID колонки (колонки, которые не являются датами)
                id_vars = []
                potential_id_cols = ['country_region', 'province_state']
                
                for id_col in potential_id_cols:
                    if id_col in df_wide.columns:
                        id_vars.append(id_col)
                
                # Если не нашли стандартные ID, используем все строковые колонки с малым количеством уникальных значений
                if not id_vars:
                    for col_name in df_wide.columns:
                        # Проверяем, не является ли колонка датой
                        if not re.match(r'\d{2}_\d{2}_\d{4}', col_name):
                            # Проверяем количество уникальных значений
                            unique_count = df_wide.select(col_name).distinct().count()
                            if unique_count < df_wide.count() * 0.5:  # Если уникальных значений меньше половины
                                id_vars.append(col_name)
                    
                    # Если все еще пусто, используем первую колонку
                    if not id_vars and len(df_wide.columns) > 0:
                        id_vars = [df_wide.columns[0]]
                
                print(f"\nIdentified ID columns: {id_vars}")
                
                # Трансформируем из широкого формата в длинный
                df_long = melt_dataframe(df_wide, id_vars)
                
                print(f"\nstage data shape: {df_long.count()} rows")
                print(f"stage schema:")
                df_long.printSchema()
                
                # Очищаем и преобразуем даты
                print(f"\nCleaning date column...")
                
                # Извлекаем дату из названия колонки
                from pyspark.sql.functions import udf
                from pyspark.sql.types import StringType
                
                extract_date_udf = udf(extract_date_from_colname, StringType())
                
                df_long = df_long.withColumn(
                    'date_formatted', 
                    extract_date_udf(col('date'))
                )
                
                # Пробуем преобразовать в правильный формат даты
                try:
                    df_long = df_long.withColumn(
                        'date_clean',
                        expr("to_date(date_formatted, 'yyyy-MM-dd')")
                    )
                except:
                    df_long = df_long.withColumn(
                        'date_clean',
                        col('date_formatted')
                    )
                
                # Конвертируем cases в integer
                df_long = df_long.withColumn(
                    'cases_int',
                    col('cases').cast('int')
                )
                
                # Выбираем финальные колонки
                final_columns = id_vars + ['date_clean', 'cases_int']
                df_final = df_long.select(*final_columns)
                
                print(f"\nFinal schema:")
                df_final.printSchema()
                
                print(f"\nSample stage data:")
                df_final.show(20, truncate=False)
                
                # Проверяем наличие данных
                total_cases = df_final.select('cases_int').groupBy().sum().collect()[0][0]
                print(f"\nTotal cases: {total_cases}")
                
                # Записываем трансформированные данные
                print(f"\nWriting to {target_table}...")
                
                df_final.write.jdbc(
                    url=url,
                    table=target_table,
                    mode='append',  #mode='overwrite',
                    properties=properties
                )
                
                print(f"✓ Successfully stage and wrote to {target_table}")
                
                # Проверка результата
                df_check = spark.read.jdbc(
                    url=url, 
                    table=target_table, 
                    properties=properties
                )
                print(f"✓ Verification: {df_check.count()} rows in {target_table}")
                df_check.printSchema()
                df_check.show(5, truncate=False)
                
            except Exception as e:
                print(f"Error processing year {year}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        spark.stop()
        print("\n✓ Transform stage completed successfully")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()