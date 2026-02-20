from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, sum as sql_sum, avg, count as sql_count, max as sql_max, min as sql_min, lit, when, countDistinct, datediff
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import socket
import sys

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

def clean_date_column(df, date_col='date_clean'):
    """Очищает колонку с датами от null значений"""
    return df.filter(col(date_col).isNotNull())

def main():
    local_ip = get_local_ip()
    print(f"Local IP: {local_ip}")
    
    pg_host = get_postgres_host()
    print(f"Using PostgreSQL host: {pg_host}")
    
    # Создаем SparkSession
    spark = SparkSession.builder \
        .appName("CreateMart") \
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
        
        # Создаем схему mart если не существует
        spark.sql("CREATE DATABASE IF NOT EXISTS mart")
        
        # Читаем единую таблицу с данными за все годы
        source_table = "stage.covid_data"
        
        print(f"\n{'='*60}")
        print(f"Reading data from {source_table}")
        
        df = spark.read.jdbc(
                url=url, 
                table=source_table, 
                properties=properties)
        
        # Очищаем данные от null значений в датах
        df_clean = clean_date_column(df)
        
        row_count = df_clean.count()
        print(f"Read {row_count} rows from {source_table} (after cleaning null dates)")
        
        if row_count == 0:
            print(f"Table {source_table} is empty, exiting")
            sys.exit(1)
        
        print(f"\nSource schema:")
        df_clean.printSchema()
        
        print(f"\nSample data:")
        df_clean.show(5, truncate=False)
        
        # Определяем диапазон лет в данных (исключая null)
        years_df = df_clean.select(year(col('date_clean')).alias('year')) \
            .filter(col('year').isNotNull()) \
            .distinct() \
            .orderBy('year')
        
        years = [row.year for row in years_df.collect()]
        print(f"\nYears found in data: {years}")
        
        if not years:
            print("No valid years found in data, exiting")
            sys.exit(1)
        
        # 1. ЕЖЕМЕСЯЧНАЯ СТАТИСТИКА ПО СТРАНАМ
        print(f"\n📊 Creating monthly statistics by country...")
        
        monthly_by_country = df_clean.groupBy(
            col('country_region'),
            year(col('date_clean')).alias('year'),
            month(col('date_clean')).alias('month')
        ).agg(
            sql_sum('cases_int').alias('total_cases'),
            avg('cases_int').alias('avg_daily_cases'),
            sql_max('cases_int').alias('max_daily_cases'),
            sql_count('date_clean').alias('days_with_data')
        ).orderBy('country_region', 'year', 'month')
        
        print(f"Monthly by country statistics schema:")
        monthly_by_country.printSchema()
        print(f"Sample monthly by country statistics:")
        monthly_by_country.show(10, truncate=False)
        
        # Сохраняем ежемесячную статистику по странам
        monthly_by_country.write.jdbc(
            url=url,
            table='mart.covid_monthly_by_country',
            mode='overwrite',
            properties=properties
        )
        print(f"✓ Saved monthly by country statistics to mart.covid_monthly_by_country")
        print(f"  Rows: {monthly_by_country.count()}")
        
        # 2. ГОДОВАЯ СТАТИСТИКА ПО СТРАНАМ
        print(f"\n📊 Creating yearly country statistics...")
        
        yearly_country_stats = df_clean.groupBy(
            col('country_region'),
            year(col('date_clean')).alias('year')
        ).agg(
            sql_sum('cases_int').alias('total_cases'),
            avg('cases_int').alias('avg_daily_cases'),
            sql_max('cases_int').alias('max_daily_cases'),
            sql_count('date_clean').alias('days_with_data')
        ).orderBy('country_region', 'year')
        
        yearly_country_stats.write.jdbc(
            url=url,
            table='mart.covid_yearly_by_country',
            mode='overwrite',
            properties=properties
        )
        print(f"✓ Saved yearly country statistics to mart.covid_yearly_by_country")
        print(f"  Rows: {yearly_country_stats.count()}")
        yearly_country_stats.show(10, truncate=False)
        
        # 3. ГЛОБАЛЬНАЯ ЕЖЕМЕСЯЧНАЯ СТАТИСТИКА
        print(f"\n📊 Creating global monthly statistics...")
        
        global_monthly = df_clean.groupBy(
            year(col('date_clean')).alias('year'),
            month(col('date_clean')).alias('month')
        ).agg(
            sql_sum('cases_int').alias('global_total_cases'),
            avg('cases_int').alias('global_avg_daily_cases'),
            sql_max('cases_int').alias('global_max_daily_cases'),
            sql_count('date_clean').alias('days_with_data')
        ).orderBy('year', 'month')
        
        global_monthly.write.jdbc(
            url=url,
            table='mart.covid_global_monthly',
            mode='overwrite',
            properties=properties
        )
        print(f"✓ Saved global monthly statistics to mart.covid_global_monthly")
        print(f"  Rows: {global_monthly.count()}")
        global_monthly.show(10, truncate=False)
        
        # 4. ТОП-10 СТРАН ПО ОБЩЕМУ КОЛИЧЕСТВУ СЛУЧАЕВ
        print(f"\n📊 Creating top 10 countries by total cases...")
        
        top_countries = df_clean.groupBy('country_region').agg(
            sql_sum('cases_int').alias('total_cases_all_time')
        ).orderBy(col('total_cases_all_time').desc()).limit(10)
        
        top_countries.write.jdbc(
            url=url,
            table='mart.covid_top_10_countries',
            mode='overwrite',
            properties=properties
        )
        print(f"✓ Saved top 10 countries to mart.covid_top_10_countries")
        print(f"  Rows: {top_countries.count()}")
        top_countries.show(truncate=False)
        
        # 5. ГЛОБАЛЬНАЯ СТАТИСТИКА ПО ГОДАМ
        print(f"\n📊 Creating global statistics by year...")
        
        global_by_year = df_clean.groupBy(
            year(col('date_clean')).alias('year')
        ).agg(
            sql_sum('cases_int').alias('total_cases'),
            avg('cases_int').alias('avg_daily_cases'),
            sql_max('cases_int').alias('max_daily_cases'),
            sql_count('date_clean').alias('total_days'),
            countDistinct('country_region').alias('countries_with_data')
        ).orderBy('year')
        
        global_by_year.write.jdbc(
            url=url,
            table='mart.covid_global_by_year',
            mode='overwrite',
            properties=properties
        )
        print(f"✓ Saved global by year to mart.covid_global_by_year")
        print(f"  Rows: {global_by_year.count()}")
        global_by_year.show(truncate=False)
        
        # 6. СТАТИСТИКА ПО ПРОВИНЦИЯМ/ШТАТАМ
        print(f"\n📊 Creating province/state statistics...")
        
        if 'province_state' in df_clean.columns:
            province_stats = df_clean.filter(
                col('province_state').isNotNull() & 
                (col('province_state') != '')
            ).groupBy(
                col('country_region'),
                col('province_state'),
                year(col('date_clean')).alias('year')
            ).agg(
                sql_sum('cases_int').alias('total_cases'),
                avg('cases_int').alias('avg_daily_cases'),
                sql_max('cases_int').alias('max_cases'),
                sql_count('date_clean').alias('days_with_data')
            ).orderBy('country_region', 'province_state', 'year')
            
            province_count = province_stats.count()
            if province_count > 0:
                province_stats.write.jdbc(
                    url=url,
                    table='mart.covid_province_stats',
                    mode='overwrite',
                    properties=properties
                )
                print(f"✓ Saved province statistics to mart.covid_province_stats")
                print(f"  Rows: {province_count}")
                province_stats.show(10, truncate=False)
            else:
                print("No province/state data found, skipping province statistics")
        else:
            print("No province_state column found, skipping province statistics")
        
        # 7. ПИКОВЫЕ ДНИ ПО КАЖДОЙ СТРАНЕ
        print(f"\n📊 Finding peak days by country...")
        
        window_spec = Window.partitionBy('country_region').orderBy(col('cases_int').desc())
        
        peak_days = df_clean.withColumn('rank', row_number().over(window_spec)) \
            .filter(col('rank') == 1) \
            .select(
                col('country_region'),
                col('date_clean').alias('peak_date'),
                col('cases_int').alias('peak_cases')
            ).orderBy(col('peak_cases').desc())
        
        peak_days.write.jdbc(
            url=url,
            table='mart.covid_peak_days',
            mode='overwrite',
            properties=properties
        )
        print(f"✓ Saved peak days to mart.covid_peak_days")
        print(f"  Rows: {peak_days.count()}")
        peak_days.show(10, truncate=False)
        
        # 8. СВОДНАЯ СТАТИСТИКА ПО ВСЕМ ДАННЫМ
        print(f"\n📊 Creating overall summary statistics...")
        
        # Получаем min и max дат
        date_stats = df_clean.agg(
            sql_min('date_clean').alias('first_date'),
            sql_max('date_clean').alias('last_date')
        ).collect()[0]
        
        first_date = date_stats['first_date']
        last_date = date_stats['last_date']
        
        # Основные агрегации
        overall_stats = df_clean.agg(
            sql_sum('cases_int').alias('total_cases_worldwide'),
            avg('cases_int').alias('global_avg_daily_cases'),
            sql_max('cases_int').alias('highest_daily_cases'),
            countDistinct('country_region').alias('total_countries'),
            countDistinct('date_clean').alias('total_days')
        )
        
        # Добавляем информацию о датах
        from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
        from pyspark.sql import Row
        
        # Создаем новый DataFrame с дополнительными колонками
        additional_data = spark.createDataFrame([
            Row(
                first_date=first_date,
                last_date=last_date,
                days_span=(last_date - first_date).days if first_date and last_date else None
            )
        ])
        
        # Объединяем с основными статистиками
        from pyspark.sql.functions import lit
        final_stats = overall_stats.crossJoin(additional_data)
        
        final_stats.write.jdbc(
            url=url,
            table='mart.covid_overall_summary',
            mode='overwrite',
            properties=properties
        )
        print(f"✓ Saved overall summary to mart.covid_overall_summary")
        print(f"  Rows: {final_stats.count()}")
        final_stats.show(truncate=False)
        
        # Выводим статистику по созданным мартам
        print(f"\n{'='*60}")
        print("✅ MART CREATION SUMMARY")
        print('='*60)
        
        marts_to_check = [
            'mart.covid_monthly_by_country',
            'mart.covid_yearly_by_country',
            'mart.covid_global_monthly',
            'mart.covid_top_10_countries',
            'mart.covid_global_by_year',
            'mart.covid_province_stats',
            'mart.covid_peak_days',
            'mart.covid_overall_summary'
        ]
        
        for mart_table in marts_to_check:
            try:
                count_rows = spark.read.jdbc(url=url, table=mart_table, properties=properties).count()
                print(f"  ✓ {mart_table}: {count_rows} rows")
            except Exception as e:
                print(f"  ✗ {mart_table}: not created ({str(e)[:50]}...)")
        
        spark.stop()
        print("\n✓ Create mart stage completed successfully")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()