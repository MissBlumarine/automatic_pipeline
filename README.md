# automatic_pipeline
Выпускной проект для курса data engineer OTUS

Создана БД Postgresql на Yandex Cloud.  
Архитектура хранилища предусматривает создание 3-х слоев данных:
- stg (для загрузки "сырых" данных из источника)
- dds (для хранения денормализованных детализированных таблиц с данными для сохранения историчности данных)
- data_mart (для хранения предварительно рассчитанных витрин с различными агрегациями и расчетами для последующего использования этих витрин BI системой)

На Yandex Cloud поднята виртуальная машина с Airflow

Автоматический pipeline сбора данных по API c сайта mos.ru: 

1. Этап Extract+Load
   Данный этап реализован с помощью оркестратора Airflow, в котором создан DAG c кодом на python. Сначала через API mos.ru скачивается целевой датасет (Книги-номинанты и победители литературных премий (по годам), находящиеся в каждой государственной публичной библиотеке города Москвы). Данные скачиваются в виде строки, затем на этапе парсинга разбиваются сначала на отдельные строки по ключевым символам, а затем складываются в список. Затем Airflow подключается к БД Postgresql, расположенной на Yandex Cloud, и в БД записываются "сырые" данные в слой stg. На этом этапе к данным добавляется уникальный ключ, созданный для каждой строки, с помощью комбинации global_id (изначально данная колонка присутствует в датасете) и python-функции "now".

2. Этап Transform
   Трансформации данных выполняются с помощью фреймворка Apache Spark, который локально запущен на виртуальной машине на компьютере. На данном этапе выполняется дедубликация полученных данных и их обогащение с помощью заранее созданной таблицы (справочника), в которой устанавливается связь между автором, регионом и городом его проживания. Производится хэширование детализированных данных с целью обеспечения версионности получаемых данных (чтобы можно было сохранить все изменения в атрибутах) с помощью MD5 hash generator. Этап трансформации данных (запуск Spark Submit) происходит по расписанию с помощью Cron. Затем происходит расчет и построение витрин, которые будут использованы BI системой Data Lens, и их размещение в БД в слое data_mart. 
   
3. Этап настройки dasboard в BI системе Data Lens
   На данном этапе осуществляется подключение Data Lens к БД Postgresql  и непосредственно настройка дашбордов.
