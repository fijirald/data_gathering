"""
Зачем нужны __init__.py файлы
https://stackoverflow.com/questions/448271/what-is-init-py-for

Про документирование в Python проекте
https://www.python.org/dev/peps/pep-0257/

Про оформление Python кода
https://www.python.org/dev/peps/pep-0008/


Примеры сбора данных:
https://habrahabr.ru/post/280238/

Для запуска тестов в корне проекта:
python3 -m unittest discover

Для запуска проекта из корня проекта:
python3 -m gathering gather
или
python3 -m gathering transform
или
python3 -m gathering stats


Для проверки стиля кода всех файлов проекта из корня проекта
pep8 .


ЗАДАНИЕ

Выбрать источник данных и собрать данные по некоторой предметной области.

Цель задания - отработать навык написания программ на Python.
В процессе выполнения задания затронем области:
- организация кода в виде проекта, импортирование модулей внутри проекта
- unit тестирование
- работа с файлами
- работа с протоколом http
- работа с pandas
- логирование

Требования к выполнению задания:

- собрать не менее 1000 объектов

- в каждом объекте должно быть не менее 5 атрибутов
(иначе просто будет не с чем работать.
исключение - вы абсолютно уверены что 4 атрибута в ваших данных
невероятно интересны)

- сохранить объекты в виде csv файла

- считать статистику по собранным объектам


Этапы:

1. Выбрать источник данных.

Это может быть любой сайт или любое API

Примеры:
- Пользователи vk.com (API)
- Посты любой популярной группы vk.com (API)
- Фильмы с Кинопоиска
(см. ссылку на статью выше)
- Отзывы с Кинопоиска
- Статьи Википедии
(довольно сложная задача,
можно скачать дамп википедии и распарсить его,
можно найти упрощенные дампы)
- Статьи на habrahabr.ru
- Объекты на внутриигровом рынке на каком-нибудь сервере WOW (API)
(желательно англоязычном, иначе будет сложно разобраться)
- Матчи в DOTA (API)
- Сайт с кулинарными рецептами
- Ebay (API)
- Amazon (API)
...

Не ограничивайте свою фантазию. Это могут быть любые данные,
связанные с вашим хобби, работой, данные любой тематики.
Задание специально ставится в открытой форме.
У такого подхода две цели -
развить способность смотреть на задачу широко,
пополнить ваше портфолио (вы вполне можете в какой-то момент
развить этот проект в стартап, почему бы и нет,
а так же написать статью на хабр(!) или в личный блог.
Чем больше у вас таких активностей, тем ценнее ваша кандидатура на рынке)

2. Собрать данные из источника и сохранить себе в любом виде,
который потом сможете преобразовать

Можно сохранять страницы сайта в виде отдельных файлов.
Можно сразу доставать нужную информацию.
Главное - постараться не обращаться по http за одними и теми же данными много раз.
Суть в том, чтобы скачать данные себе, чтобы потом их можно было как угодно обработать.
В случае, если обработать захочется иначе - данные не надо собирать заново.
Нужно соблюдать "этикет", не пытаться заддосить сайт собирая данные в несколько потоков,
иногда может понадобиться дополнительная авторизация.

В случае с ограничениями api можно использовать time.sleep(seconds),
чтобы сделать задержку между запросами

3. Преобразовать данные из собранного вида в табличный вид.

Нужно достать из сырых данных ту самую информацию, которую считаете ценной
и сохранить в табличном формате - csv отлично для этого подходит

4. Посчитать статистики в данных
Требование - использовать pandas (мы ведь еще отрабатываем навык использования инструментария)
То, что считаете важным и хотели бы о данных узнать.

Критерий сдачи задания - собраны данные по не менее чем 1000 объектам (больше - лучше),
при запуске кода командой "python3 -m gathering stats" из собранных данных
считается и печатается в консоль некоторая статистика

Код можно менять любым удобным образом
Можно использовать и Python 2.7, и 3

"""

import logging

import sys

from scrappers.scrapper import Scrapper
from storages.file_storage import FileStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SCRAPPED_FILE = 'scrapped_data.txt'
TABLE_FORMAT_FILE = 'data.csv'


def gather_process():
    logger.info("gather")
    # storage = FileStorage(SCRAPPED_FILE)

    # You can also pass a storage
    # scrapper = Scrapper()
    # scrapper.scrap_process(storage)
    import vk
    import pandas as pd
    import time

    total_users = 1500

    # ограничение api
    batch_size = 100
    delay = .3

    # access_token = ""
    # session = vk.Session(access_token=access_token)
    session = vk.Session()
    api = vk.API(session)

    # https://vk.com/wall-460389_2711177

    # получаем первую партию вне цикла, чтобы было из чего создать первичный датафрейм, с которым мы будем далее конкатенировать новые батчи с пользователями
    response = api.likes.getList(type="post", owner_id="-460389", item_id="2711177", extended=0)
    userIds = response['users']
    print("Загружено юзеров: 100")

    usersList = api.users.get(user_ids=userIds, fields=['bdate', 'sex', 'city'])
    users = pd.DataFrame(usersList)

    # идем циклом пока не наберем достаточное количество пользователей
    for i in range(1, int(total_users / batch_size)):
        time.sleep(delay)
        offset = i * batch_size
        usersList = api.users.get(user_ids=userIds, fields=['bdate', 'sex', 'city'], offset=offset)
        print("Загружено юзеров: {}".format(offset + 100))
        tempDataframe = pd.DataFrame(usersList)
        users = pd.concat([users, tempDataframe])

    users = users.reset_index()
    print("Количество скачанных юзеров: ", len(users.index))

    users['friends'] = 0
    users['friends'].astype(int)
    users['followers'] = 0
    users['followers'].astype(int)

    # вытягиваем количество друзей для каждого из пользователей
    for i in range(len(users.index)):
        time.sleep(delay)
        response = api.users.get(user_ids=[users.iloc[i]['uid']], fields=['counters'])
        friends = response[0]['counters']['friends']
        followers = response[0]['counters']['followers']
        print("load user #{}, friends = {}, followers = {}".format(i, friends, followers))
        users.ix[i, 'friends'] = friends
        users.ix[i, 'followers'] = followers

    print("download is done succesfull")

    users.to_csv(SCRAPPED_FILE, encoding="utf8", index=False)
    users.to_csv(TABLE_FORMAT_FILE, encoding="utf8", index=False)


def convert_data_to_table_format():
    logger.info("transform")

    # Your code here
    # transform gathered data from txt file to pandas DataFrame and save as csv
    pass


def stats_of_data():
    logger.info("stats")

    # Your code here
    # Load pandas DataFrame and print to stdout different statistics about the data.
    # Try to think about the data and use not only describe and info.
    # Ask yourself what would you like to know about this data (most frequent word, or something else)
    import pandas as pd

    users = pd.read_csv("data.csv");

    print("Количество мужчин, которым понравился пост: {}\n".format(len(users[users.sex == 2].index)))
    print("Количество женщин, которым понравился пост: {}\n".format(len(users[users.sex == 1].index)))
    print("Top5 городов среди лайкнувших: \n{}\n".format(
        users[["city_id", "city"]].groupby("city")['city_id'].count().reset_index(name="count").sort_values(['count'],ascending=False).head().reset_index().drop(["index"], axis=1))
    )


if __name__ == '__main__':
    """
    why main is so...?
    https://stackoverflow.com/questions/419163/what-does-if-name-main-do
    """
    logger.info("Work started")

    if sys.argv[1] == 'gather':
        gather_process()

    elif sys.argv[1] == 'transform':
        convert_data_to_table_format()

    elif sys.argv[1] == 'stats':
        stats_of_data()

    logger.info("work ended")
