import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions
from pyspark.sql.functions import col

def main():

    #====================================================================
    # инициируем сессию spark
    conf = SparkConf().setAppName('make_data_mart').setMaster("local")
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession.builder.config("spark.jars", "/opt/spark/postgresql-42.4.1.jar").getOrCreate()
    #====================================================================


    #====================================================================
    # задаем адрес БД и аутентификационные данные
    url_dds = ("jdbc:postgresql://c-c9q7fedn3itbd408mpmm.rw.mdb.yandexcloud.net:6432/dds?targetServerType=master&ssl=false")
    user_dds = "user1"
    password_dds = "Galateya_224"

    url_data_mart = ("jdbc:postgresql://c-c9q7fedn3itbd408mpmm.rw.mdb.yandexcloud.net:6432/data_mart?targetServerType=master&ssl=false")
    user_data_mart = "user1"
    password_data_mart = "Galateya_224"
    #====================================================================


    #====================================================================
    from pyspark.sql.functions import col
    from pyspark.sql.functions import concat
    from pyspark.sql.functions import md5
    from pyspark.sql.functions import max
    #====================================================================


    #====================================================================
    # Зачитываем детализированную таблицу из слоя dds чтобы из нее собирать витрины для BI
    full_data = spark.read.format("jdbc").option("url", url_dds).option("dbtable", "full_data").option("user", user_dds).option("password", password_dds).load().alias("full_data")
    #====================================================================
    

    #====================================================================
    #ЗОНА РАСЧЕТА ВИТРИН:

    #******************************
    # Строим витрину с агрегатом литературных призов в групировке по регионам, городам
    qnt_lit_prize_in_city = full_data.select("region","city","lit_prize_name").groupBy("region","city","lit_prize_name").count().alias("qnt_lit_prize_in_city")
    # записываем витрину в БД
    qnt_lit_prize_in_city.write.format("jdbc").option("url", url_data_mart).option("dbtable", "qnt_lit_prize_in_city").option("user", user_data_mart).option("password", password_data_mart).mode("overwrite").save()

    #******************************
    # Строим агрегат по подсчету кол-ва авторов в разрезе регионов
    qnt_author_by_region = full_data.select("region","author").distinct().groupBy("region").count().alias("qnt_author_by_region")
    # записываем витрину в БД
    qnt_author_by_region.write.format("jdbc").option("url", url_data_mart).option("dbtable", "qnt_author_by_region").option("user", user_data_mart).option("password", password_data_mart).mode("overwrite").save()

    #******************************
    # Строим агрегат в разрезе года публикации, и возрастных ограничений
    age_limit_by_pub_year = full_data.select("pub_year","age_limit").groupBy("pub_year","age_limit").count().alias("age_limit_by_pub_year")
    # записываем витрину в БД
    age_limit_by_pub_year.write.format("jdbc").option("url", url_data_mart).option("dbtable", "age_limit_by_pub_year").option("user", user_data_mart).option("password", password_data_mart).mode("overwrite").save()

    #******************************
    # Список авторов и их книг с возврастным ограничением 18+
    author_list_with_18_agel_limit_books = full_data.select("author","name","age_limit").where("age_limit = '18+'").alias("author_list_with_18_agel_limit_books")

    author_list_with_18_agel_limit_books.write.format("jdbc").option("url", url_data_mart).option("dbtable", "author_list_with_18_agel_limit_books").option("user", user_data_mart).option("password", password_data_mart).mode("overwrite").save()
    #******************************
    #====================================================================
    print("LOAD_DATA_MART")


if __name__ == "__main__":
    main()
