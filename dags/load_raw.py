from pyspark.sql import SparkSession
import socket
import sys
import os
import re
from functools import reduce

class DateColumnNormalizer:
    """Класс для нормализации колонок с датами и приведения к формату DD_MM_YYYY"""
    
    @staticmethod
    def standardize_to_dd_mm_yyyy(col_name):
        """
        Приводит название колонки с датой к единому формату DD_MM_YYYY
        
        Поддерживаемые входные форматы:
        - MM/DD/YY (1/22/20) -> 22_01_2020
        - DD.MM.YYYY (02.01.2020) -> 02_01_2020
        - MM/DD/YYYY (12/31/2020) -> 31_12_2020
        - DD/MM/YY (22/01/20) -> 22_01_2020
        - MM.DD.YY (01.22.20) -> 22_01_2020
        """
        # Основные колонки оставляем как есть
        if col_name == 'Country/Region':
            return 'country_region'
        if col_name == 'Province/State':
            return 'province_state'
        
        # Очищаем от лишних пробелов
        col_name = col_name.strip()
        
        # Разбиваем по разделителям (/ или .)
        parts = re.split(r'[/.]', col_name)
        
        if len(parts) == 3:
            p1, p2, p3 = parts
            
            # Определяем формат и преобразуем
            if len(p3) == 4:  # Год в формате YYYY
                if '.' in col_name and int(p1) <= 31:  # DD.MM.YYYY
                    day, month, year = p1, p2, p3
                elif '/' in col_name and int(p1) <= 12:  # MM/DD/YYYY
                    month, day, year = p1, p2, p3
                else:  # Пытаемся угадать
                    if int(p1) <= 31 and int(p2) <= 12:
                        day, month, year = p1, p2, p3
                    else:
                        month, day, year = p1, p2, p3
            else:  # Год в формате YY
                year_full = f"20{p3}"
                
                # Определяем по разделителю и значениям
                if '/' in col_name:
                    if int(p1) > 12 and int(p2) <= 12:  # DD/MM/YY
                        day, month = p1, p2
                    elif int(p1) <= 12 and int(p2) <= 31:  # MM/DD/YY
                        month, day = p1, p2
                    else:  # Не можем определить, используем разумное предположение
                        if int(p1) <= 31 and int(p2) <= 12:
                            day, month = p1, p2
                        else:
                            month, day = p1, p2
                elif '.' in col_name:
                    if int(p1) <= 31 and int(p2) <= 12:  # DD.MM.YY
                        day, month = p1, p2
                    elif int(p1) <= 12 and int(p2) <= 31:  # MM.DD.YY
                        month, day = p1, p2
                    else:
                        if int(p1) <= 31 and int(p2) <= 12:
                            day, month = p1, p2
                        else:
                            month, day = p1, p2
                else:
                    # Если нет разделителей (маловероятно)
                    month, day = p1, p2
                
                year = year_full
            
            # Форматируем с ведущими нулями (DD_MM_YYYY)
            try:
                return f"{int(day):02d}_{int(month):02d}_{year}"
            except:
                # В случае ошибки возвращаем нормализованную версию
                return col_name.replace('/', '_').replace('.', '_').lower()
        
        # Если не удалось распарсить, возвращаем простую нормализацию
        normalized = col_name.replace('/', '_').replace('.', '_')
        normalized = re.sub(r'_+', '_', normalized).strip('_').lower()
        return normalized
    
    @staticmethod
    def get_date_parts(standardized_col):
        """
        Извлекает день, месяц и год из стандартизированного названия колонки
        """
        if standardized_col in ['country_region', 'province_state']:
            return None
        
        parts = standardized_col.split('_')
        if len(parts) == 3 and len(parts[2]) == 4:
            return {
                'day': parts[0],
                'month': parts[1],
                'year': parts[2],
                'date': f"{parts[2]}-{parts[1]}-{parts[0]}"  # YYYY-MM-DD для SQL
            }
        return None

def load_and_standardize_csv(spark, file_path, year):
    """
    Загружает CSV и приводит все колонки с датами к формату DD_MM_YYYY
    """
    print(f"\n{'='*60}")
    print(f"Processing year {year}")
    print(f"File: {file_path}")
    
    # Читаем CSV
    df = spark.read.csv(
        file_path,
        header=True,
        sep=';',
        inferSchema=True
    )
    
    print(f"Original columns: {len(df.columns)}")
    print(f"Sample original columns: {df.columns[:10]}")
    
    # Анализируем исходные форматы
    date_columns_original = [col for col in df.columns 
                            if col not in ['Country/Region', 'Province/State']]
    print(f"\nOriginal date columns: {len(date_columns_original)}")
    print(f"Sample original date columns: {date_columns_original[:5]}")
    
    # Стандартизируем названия колонок к формату DD_MM_YYYY
    normalizer = DateColumnNormalizer()
    rename_map = {}
    
    for old_col in df.columns:
        new_col = normalizer.standardize_to_dd_mm_yyyy(old_col)
        rename_map[old_col] = new_col
    
    # Применяем переименование
    for old, new in rename_map.items():
        df = df.withColumnRenamed(old, new)
    
    print(f"\nStandardized columns: {len(df.columns)}")
    print(f"Sample standardized columns: {df.columns[:10]}")
    
    # Анализируем результат стандартизации
    date_columns = [col for col in df.columns 
                   if col not in ['country_region', 'province_state']]
    
    print(f"\nDate columns after standardization (DD_MM_YYYY):")
    print(f"Total: {len(date_columns)}")
    if date_columns:
        print(f"Sample (first 10): {date_columns[:10]}")
        
        # Проверяем формат нескольких колонок
        print(f"\nFormat verification:")
        for col in date_columns[:5]:
            parts = normalizer.get_date_parts(col)
            if parts:
                print(f"  {col:15} -> year: {parts['year']}, month: {parts['month']}, day: {parts['day']}")
            else:
                print(f"  {col:15} -> UNKNOWN FORMAT")
    
    return df, rename_map

def test_standardization():
    """
    Тестирует функцию стандартизации на различных форматах дат
    """
    test_cases = [
        ('Country/Region', 'country_region'),
        ('Province/State', 'province_state'),
        ('1/22/20', '22_01_2020'),
        ('02.01.2020', '02_01_2020'),
        ('12/31/20', '31_12_2020'),
        ('12.31.2020', '31_12_2020'),
        ('1/1/21', '01_01_2021'),
        ('01.01.2021', '01_01_2021'),
        ('12/13/21', '13_12_2021'),
        ('12.13.2021', '13_12_2021'),
        ('2/13/20', '13_02_2020'),
        ('3/14/20', '14_03_2020'),
        ('4/15/21', '15_04_2021'),
        ('22.01.2020', '22_01_2020'),  # DD.MM.YYYY
        ('22/01/20', '22_01_2020'),     # DD/MM/YY
    ]
    
    print("\n" + "="*60)
    print("TESTING DATE STANDARDIZATION")
    print("="*60)
    print(f"{'Input':<25} {'Expected':<20} {'Result':<20} {'Status'}")
    print("-"*70)
    
    normalizer = DateColumnNormalizer()
    success_count = 0
    
    for input_col, expected in test_cases:
        result = normalizer.standardize_to_dd_mm_yyyy(input_col)
        status = "✓" if result == expected else "✗"
        if result == expected:
            success_count += 1
        print(f"{input_col:<25} {expected:<20} {result:<20} {status}")
    
    print("-"*70)
    print(f"Success rate: {success_count}/{len(test_cases)}")
    print("="*60)

def main():
    # Запускаем тесты
    test_standardization()
    
    # Инициализация Spark
    spark = SparkSession.builder \
        .appName("COVID Data Loader") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.host", socket.gethostbyname(socket.gethostname())) \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.0.jar") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.debug.maxToStringFields", "1000") \
        .getOrCreate()
    
    try:
        base_path = "/usr/local/airflow/data"
        
        for year in range(2020, 2024):
            file_path = f"file://{base_path}/raw_{year}.csv"
            
            # Проверяем существование файла
            local_path = file_path.replace('file://', '')
            if not os.path.exists(local_path):
                print(f"\nFile not found: {local_path}")
                continue
            
            # Загружаем и стандартизируем
            df, rename_map = load_and_standardize_csv(spark, file_path, year)
            
            # Показываем результат
            print(f"\nFinal schema for {year}:")
            df.printSchema()
            
            print(f"\nSample data (first 5 rows, first 10 columns):")
            # Показываем первые 5 строк и первые 10 колонок
            df.select(df.columns[:10]).show(5, truncate=False)
            
            # Записываем в PostgreSQL
            print(f"\nWriting to PostgreSQL...")
            url = "jdbc:postgresql://postgres:5432/airflow"
            properties = {
                'user': 'airflow', 
                'password': 'airflow', 
                'driver': 'org.postgresql.Driver'
            }
            
            # Создаем схему raw если не существует
            spark.sql("CREATE DATABASE IF NOT EXISTS raw")
            
            table_name = f"raw.covid_data_{year}"
            df.write.jdbc(
                url=url, 
                table=table_name, 
                mode='overwrite', 
                properties=properties
            )
            
            print(f"✓ Successfully wrote {df.count()} rows to {table_name}")
            
            # Проверка записи
            df_check = spark.read.jdbc(url=url, table=table_name, properties=properties)
            print(f"✓ Verification: read {df_check.count()} rows from {table_name}")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()
        print("\n✓ Job completed successfully")

if __name__ == "__main__":
    main()