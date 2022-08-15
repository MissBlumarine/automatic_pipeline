import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions
from pyspark.sql.functions import col

def main():

    #====================================================================
    # инициируем сессию spark
    conf = SparkConf().setAppName('dedublicate_raw_data').setMaster("local")
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession.builder.config("spark.jars", "/opt/spark/postgresql-42.4.1.jar").getOrCreate()
    #====================================================================


    #====================================================================
    # задаем адрес БД и аутентификационные данные
    url_stg = ("jdbc:postgresql://c-c9q7fedn3itbd408mpmm.rw.mdb.yandexcloud.net:6432/stg?targetServerType=master&ssl=false")
    user_stg = "user1"
    password_stg = "Galateya_224"

    url_dds = ("jdbc:postgresql://c-c9q7fedn3itbd408mpmm.rw.mdb.yandexcloud.net:6432/dds?targetServerType=master&ssl=false")
    user_dds = "user1"
    password_dds = "Galateya_224"
    #====================================================================


    #====================================================================
    from pyspark.sql.functions import col
    from pyspark.sql.functions import concat
    from pyspark.sql.functions import md5
    from pyspark.sql.functions import max
    #====================================================================


    #====================================================================
    # Зачитываем сырые данные из слоя stg
    stg_books = spark.read.format("jdbc").option("url", url_stg).option("dbtable", "stg_books").option("user", user_stg).option("password", password_stg).load().alias("stg_books")

    # Зачитываем справочник из слоя dds
    dds_dict_author = spark.read.format("jdbc").option("url", url_dds).option("dbtable", "dict_author").option("user", user_dds).option("password", password_dds).load().alias("dds_dict_author")
    #====================================================================
    

    #====================================================================
    # Готовим хэш: сначала объединяем все колонки в df кроме processed_time
    # Дальше по объединенным колонкам считаем хэш (получаем краткий идентификатор построеный почти по всем атрибутам
    # После разделим на две таблицы нашу таблицу с хэшэм
    # - первая таблица будет содержать все атрибуты, кроме processed_time + хэш
    # - вторая таблица processed_time + хэш
    # Первую таблицу мы дедублицируем
    # В случаях когда по 1 global_id будет изменение какого то атрибута у нас в таблице сохраниться обе версии строки для этого global_id
    # Во второй таблице мы оставим уникальные строки хэша и максимальную дату каждому хэшу т.е. если у нас будет на 1 значение хэша приходится несколько дат то это 
    # значит что мы несколько раз считали одни и теже данные и в них не было изменения и наша операция позволит сделать справочник с уникальным хэш значением строки 
    # последней датой скачивания
    # Следующим шагом мы сделаем JOIN дедублицированной таблицы с атрибутами и таблицы с последней актуальной датой скачивания строки через хэш
    # Такой подход реализует сохранение версионности строк и позволит в дальнейшем отслеживать изменения по global_id

    # Собираем и считаем хэш
    md5_df = stg_books.select(concat("global_id", "number", "nomitation_year", "name", "author", "pub_year", "age_limit", "publishing_house", "lit_prize_name", "nomination").alias("conc"),"processed_time", "global_id", "number", "nomitation_year", "name", "author", "pub_year", "age_limit", "publishing_house", "lit_prize_name", "nomination").select(md5("conc").alias("hash"),"processed_time", "global_id", "number", "nomitation_year", "name", "author", "pub_year", "age_limit", "publishing_house", "lit_prize_name", "nomination").alias("md5_df")

    # Создание первой таблицы почти со всеми атрибутами
    all_attr_md5 = md5_df.select("hash", "global_id", "number", "nomitation_year", "name", "author", "pub_year", "age_limit", "publishing_house", "lit_prize_name", "nomination").distinct().alias("all_attr_md5")

    # Создание второй таблицы со хэшэм и последней датой скачивания строки
    max_date_hash = md5_df.select("hash","processed_time").alias("max_date_hash")
    max_date_hash.createOrReplaceTempView("max_date_hash")
    max_date_hash_2 = spark.sql("SELECT hash, processed_time,  ROW_NUMBER() OVER(PARTITION BY hash ORDER BY processed_time DESC) AS rn FROM max_date_hash").alias("max_date_hash_2")
    max_date_hash_3 = max_date_hash_2.select("*").where("rn =1").alias("max_date_hash_3")
    
    # Записываем в БД    
    books = all_attr_md5.join(max_date_hash_3, col("all_attr_md5.hash") == col("max_date_hash_3.hash"), "left").select(col("all_attr_md5.*"),col("max_date_hash_3.processed_time")).alias("books")
    #====================================================================
    print("LOAD_DDS")



if __name__ == "__main__":
    main()
