# -----Импорт пакетов-------------------------------------------------------------
import pprint
from pymongo import MongoClient
import json

# -----Подключение----------------------------------------------------------------
host = 'localhost'
port = 27017
# установить соединение с MongoClient
client = MongoClient(f'mongodb://{host}:{port}/')
# удалить БД (каждый раз начинаем "с чистого листа")
client.drop_database('habr_news')
# создать БД
db = client['habr_news']
# создать коллекцию
collection = db.habr_news_collection
# ------Добавление------------------------------------------------------------------------------------------------------

# загрузить json полученный в результате работы scrapy
with open('./harb_news/habr_news.json') as json_file:
    habr_news = json.load(json_file)
# добавить все данные из json в mongodb
insert_results = db.habr_news_collection.insert_many(habr_news)

# ------Запросы---------------------------------------------------------------------------------------------------------

# количество документов в коллекции
docs_count = db.habr_news_collection.count_documents({})
print(f"{docs_count} документов")
# получить имена коллекций из БД
print(db.collection_names())
# получить один любой документ из коллекции
print(pprint.pformat(db.habr_news_collection.find_one()))
# получить один документ из коллекции удовлетворяющий условию {'news_id': 529690}
print(pprint.pformat(db.habr_news_collection.find_one({'news_id': 529690})))
# получить все документы из коллекции удовлетворяющие условию {'comments_counter': 3} + сортировка по 'news_id'
for document in db.habr_news_collection.find({'comments_counter': 3}).sort('news_id'):
    print(pprint.pformat(document))
# получить все документы из коллекции удовлетворяющие условию {'author': 'avouner'}
for document in db.habr_news_collection.find({'author': 'avouner'}):
    print(pprint.pformat(document))
# получить количество документов из коллекции поле tags которых содержит `Научно-популярное` (другие теги тоже допустимы)
print(db.habr_news_collection.count_documents({'tags': {'$all': ['МВД']}}))
# ------Обновление------------------------------------------------------------------------------------------------------

# установить в качестве `author` имя `MONGO`
# во всех документах удовлетворяющие условию {'hubs': {'$all': ['Астрономия']}}
# и получить количество обновленных
update_author_res = \
    db.habr_news_collection.update_many({'hubs': {'$all': ['Астрономия']}}, {'$set': {'author': 'MONGO'}})
print(update_author_res.matched_count)
# получить количество документов из коллекции, в которых 'author' = 'MONGO'
print(db.habr_news_collection.count_documents({'author': 'MONGO'}))
# ------Удаление--------------------------------------------------------------------------------------------------------

# удалить все документы, у которых 'comments_counter' равен 0
# и получить количество удаленных
delete_docs = db.habr_news_collection.delete_many({'comments_counter': 0})
print(delete_docs.deleted_count)
