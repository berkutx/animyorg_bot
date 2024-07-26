import hashlib

import requests
import threading
import time
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryResultArticle, \
    InputTextMessageContent, InlineQueryResultPhoto
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes, InlineQueryHandler, \
    ChosenInlineResultHandler
import sqlite3
import os
import logging
import asyncio
import re

# Elasticsearch
from elasticsearch import Elasticsearch

es_url = "http://localhost:9200"

# Укажите имя пользователя и пароль
username = "stastest"
password = "test123"

# Создайте объект клиента Elasticsearch
es = Elasticsearch(
    [es_url],
    basic_auth=(username, password)
)
# Настройка логирования
logging.basicConfig(filename='anime_bot.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    encoding='utf-8')

# Инициализация базы данных
conn = sqlite3.connect('anime_bot.db', check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY
                )''')
cursor.execute('''CREATE TABLE IF NOT EXISTS anime (
                    anime_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    anime_title TEXT,
                    anime_image TEXT,
                    anime_url TEXT UNIQUE
                )''')
cursor.execute('''CREATE TABLE IF NOT EXISTS episodes (
                    episode_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    episode_hash TEXT UNIQUE,
                    anime_id INTEGER,
                    episode_url TEXT UNIQUE,
                    FOREIGN KEY (anime_id) REFERENCES anime(anime_id)
                )''')
cursor.execute('''CREATE TABLE IF NOT EXISTS subscriptions (
                    subscription_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    anime_id INTEGER,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (anime_id) REFERENCES anime(anime_id)
                )''')
conn.commit()

# Инициализация бота
bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
if not bot_token:
    print("Ошибка: переменная окружения TELEGRAM_BOT_TOKEN не установлена.")
    exit(1)
application = ApplicationBuilder().token(bot_token).build()


# --- Функции бота ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start. Показывает первую страницу аниме."""
    user_id = update.effective_user.id
    cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
    conn.commit()

    logging.info("Пользователь %s запустил бота", user_id)
    context.user_data['current_page'] = 1
    anime_data, has_next_page = get_anime_data(cursor, 1)
    await show_anime_options(update, context, anime_data, has_next_page)

    keyboard = [[InlineKeyboardButton("Мои подписки", callback_data="show_subscriptions")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.effective_message.reply_text("Управление подписками:", reply_markup=reply_markup)


async def show_anime_options(update: Update, context: ContextTypes.DEFAULT_TYPE, anime_data, has_next_page):
    """Показывает кнопки с аниме."""
    keyboard = []
    current_page = context.user_data.get('current_page', 1)

    for i in range(0, len(anime_data), 3):
        row = []
        # Изменено: убран anime_hash из цикла
        for title, image, anime_id in anime_data[i:i + 3]:
            callback_data = f"show_anime_{anime_id}"  # Используем anime_id в callback_data
            logging.info("Кнопка для аниме %s с ID %s", title, anime_id)
            title_button = title[:45] + '...' if len(title) > 45 else title
            row.append(InlineKeyboardButton(title_button, callback_data=callback_data))
        keyboard.append(row)

    keyboard.append([
        InlineKeyboardButton("⬅️ Назад", callback_data="prev_page" if current_page > 1 else "disabled"),
        InlineKeyboardButton(f"Страница {current_page}", callback_data="disabled"),
        InlineKeyboardButton("➡️ Вперед", callback_data="next_page" if has_next_page else "disabled")
    ])

    reply_markup = InlineKeyboardMarkup(keyboard)

    message = update.message or update.callback_query.message
    if update.message is not None:
        await message.reply_text("Выберите аниме для подписки:", reply_markup=reply_markup)
    else:
        await message.edit_text("Выберите аниме для подписки:", reply_markup=reply_markup)


async def button_clicked(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатия на кнопку."""
    query = update.callback_query
    try:
        await query.answer()
    except Exception as e:
        logging.error(f"Ошибка при ответе на callback-запрос: {e}")
        return  # Прерываем обработку, если не удалось ответить на запрос

    user_id = update.effective_user.id
    data = query.data

    if data.startswith("subscribe_"):
        anime_id = int(data[len("subscribe_"):])  # Получаем anime_id из callback_data
        logging.info("Пользователь %s нажал кнопку подписки с ID %s", user_id, anime_id)

        try:
            cursor.execute("INSERT INTO subscriptions (user_id, anime_id) VALUES (?, ?)", (user_id, anime_id))
            conn.commit()
            anime_title = get_anime_title_by_id(anime_id)
            await context.bot.send_message(chat_id=update.effective_chat.id,
                                           text=f"Вы успешно подписались на {anime_title}!")
        except sqlite3.IntegrityError:
            await context.bot.send_message(chat_id=update.effective_chat.id,
                                           text="Вы уже подписаны на это аниме.")
        except Exception as e:
            logging.error(f"Ошибка при подписке пользователя {user_id}: {e}")
            await context.bot.send_message(chat_id=update.effective_chat.id,
                                           text="Произошла ошибка при подписке.")

    elif data == "show_subscriptions":
        await show_subscriptions(update, context)
    elif data == "back_to_list":
        context.user_data['current_page'] = context.user_data.get('current_page', 1)
        anime_data, has_next_page = get_anime_data(cursor, context.user_data['current_page'])
        await query.message.delete()
        await show_anime_options(update, context, anime_data, has_next_page)
    elif data == "prev_page":
        context.user_data['current_page'] = max(1, context.user_data.get('current_page', 1) - 1)
        anime_data, has_next_page = get_anime_data(cursor, context.user_data['current_page'])
        await show_anime_options(update, context, anime_data, has_next_page)
    elif data == "next_page":
        context.user_data['current_page'] = context.user_data.get('current_page', 1) + 1
        anime_data, has_next_page = get_anime_data(cursor, context.user_data['current_page'])
        await show_anime_options(update, context, anime_data, has_next_page)
    elif data.startswith("show_anime_"):
        anime_id = int(data[len("show_anime_"):])  # Используем anime_id
        await show_anime_details(update, context, anime_id)


async def show_anime_details(update: Update, context: ContextTypes.DEFAULT_TYPE, anime_id):  # Изменено: anime_id вместо anime_hash
    """Показывает детали аниме: картинку, название и кнопку подписки."""
    anime_title = get_anime_title_by_id(anime_id)  # Используем anime_id
    anime_image = get_anime_image_by_id(anime_id)  # Используем anime_id
    bot = context.bot

    if anime_title and anime_image:
        keyboard = [
            [InlineKeyboardButton("Подписаться", callback_data=f"subscribe_{anime_id}")],  # Используем anime_id
            [InlineKeyboardButton("Назад", callback_data="back_to_list")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await bot.send_photo(
            chat_id=update.effective_chat.id,
            photo=anime_image,
            caption=anime_title,
            reply_markup=reply_markup
        )
    else:
        await update.callback_query.message.edit_text("Ошибка: не удалось найти аниме.")


async def show_subscriptions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список подписок пользователя."""
    user_id = update.effective_user.id
    logging.info("Пользователь %s запросил список подписок", user_id)

    cursor.execute('''SELECT anime.anime_title FROM anime
                      INNER JOIN subscriptions ON anime.anime_id = subscriptions.anime_id
                      WHERE subscriptions.user_id=?''', (user_id,))

    subscriptions = cursor.fetchall()

    if subscriptions:
        subscriptions_text = "\n".join([f"- {row[0]}" for row in subscriptions])
        await update.effective_message.reply_text(f"Ваши подписки:\n{subscriptions_text}")
    else:
        await update.effective_message.reply_text("У вас нет активных подписок.")


def get_anime_data(cursorThread, page_num=1):
    """Получает данные об аниме с указанной страницы."""
    base_url = f"https://animy.org/releases/page/{page_num}"
    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')

    anime_list = []
    for anime_div in soup.find_all('div', class_='releases-main'):
        for anime_data in anime_div.find_all('a'):
            anime_title = anime_data.find('h2').text.strip()
            anime_image = anime_data.find('img')['src']
            anime_url = anime_data["href"]

            try:
                # Вставляем данные в таблицу anime
                cursorThread.execute("INSERT OR IGNORE INTO anime (anime_title, anime_image, anime_url) VALUES (?, ?, ?)",
                                     (anime_title, anime_image, anime_url))
                conn.commit()

                if cursorThread.rowcount > 0: # Проверяем, была ли выполнена вставка
                    # Вставка произошла, получаем last_insert_rowid()
                    cursorThread.execute("SELECT last_insert_rowid()")
                    anime_id = cursorThread.fetchone()[0]
                else:
                    # Вставка не произошла, получаем anime_id по другим данным
                    cursorThread.execute("SELECT anime_id FROM anime WHERE anime_title=? AND anime_url=?", (anime_title, anime_url))
                    anime_id = cursorThread.fetchone()[0]

                anime_list.append((anime_title, anime_image, anime_id))

            except Exception as e:
                logging.error(f"Ошибка при добавлении аниме в базу данных: {e}")

    has_next_page = soup.find('span', class_='num_right') is not None
    return anime_list, has_next_page


# Удалены функции get_anime_title_by_hash и get_anime_image_by_hash

def get_anime_title_by_id(anime_id):
    """Получает название аниме по id."""
    cursor.execute("SELECT anime_title FROM anime WHERE anime_id=?", (anime_id,))
    result = cursor.fetchone()
    return result[0] if result else None


def get_anime_image_by_id(anime_id):
    """Получает изображение аниме по ID."""
    cursor.execute("SELECT anime_image FROM anime WHERE anime_id=?", (anime_id,))
    result = cursor.fetchone()
    return result[0] if result else None


def get_latest_anime():
    """Получает последние аниме серии с главной страницы сайта."""
    base_url = "https://animy.org"
    response = requests.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')

    latest_anime_list = []
    seen_hashes = set()
    releases_section = soup.find('div', class_='list_main_update')
    if releases_section:
        for anime_data in releases_section.find_all('li'):
            anime_url = anime_data.find('a')['href']
            anime_title = anime_data.find('h2').text.strip()
            anime_image = anime_data.find('img')['src']
            episode_hash = hashlib.md5(anime_url.encode()).hexdigest()

            # Проверяем, был ли уже добавлен такой эпизод по хешу
            if episode_hash not in seen_hashes:
                latest_anime_list.append((anime_title, anime_image, episode_hash, anime_url))
                seen_hashes.add(episode_hash)

    return latest_anime_list


def extract_anime_root_url(episode_url):
    """Извлекает корень URL аниме из ссылки на эпизод."""
    match = re.match(r'(https://animy\.org/releases/item/[^/]+)', episode_url)
    return match.group(0) if match else None


def check_updates_and_notify():
    cursorThread = conn.cursor()
    """Проверяет обновления на сайте и отправляет уведомления подписчикам."""
    while True:
        logging.info("Проверка новых эпизодов на сайте...")
        latest_anime = get_latest_anime()
        new_episodes = []

        for title, image, episode_hash, episode_url in latest_anime:
            anime_root_url = extract_anime_root_url(episode_url)

            if not anime_root_url:
                continue

            cursorThread.execute("SELECT 1 FROM episodes WHERE episode_hash=?", (episode_hash,))
            if cursorThread.fetchone() is None:
                cursorThread.execute("SELECT anime_id FROM anime WHERE anime_url=?", (anime_root_url,))
                result = cursorThread.fetchone()
                if result:
                    anime_id = result[0]
                    new_episodes.append((episode_hash, anime_id, episode_url))

        if new_episodes:
            logging.info(f"Новые эпизоды найдены: {len(new_episodes)} эпизодов.")
            cursorThread.executemany(
                "INSERT INTO episodes (episode_hash, anime_id, episode_url) VALUES (?, ?, ?)",
                new_episodes)
            conn.commit()

            for episode_hash, anime_id, url in new_episodes:
                cursorThread.execute('''SELECT user_id FROM subscriptions
                                  WHERE anime_id=?''', (anime_id,))
                subscribers = cursorThread.fetchall()
                for (user_id,) in subscribers:
                    anime_title = get_anime_title_by_id(anime_id)
                    try:
                        asyncio.run(application.bot.send_message(chat_id=user_id,
                                                                text=f"Новая серия: {anime_title}\n{url}"))
                    except Exception as e:
                        logging.error(e)

        time.sleep(3600)  # ждем 1 час


def update_anime_database():
    cursorThread = conn.cursor()
    """Обновляет базу данных аниме раз в час."""
    while True:
        logging.info("Обновление базы данных аниме...")
        page_num = 1
        has_next_page = True

        while has_next_page:
            anime_data, has_next_page = get_anime_data(cursorThread, page_num)
            page_num += 1

        logging.info("Updating ElasticSearch...")
        index_anime_data()
        logging.info("Finished update of ElasticSearch...")

        logging.info("База данных аниме обновлена.")
        time.sleep(3500)  # ждем 1 час


async def send_notification(user_id, title, url):
    """Отправляет уведомление пользователю о новом аниме."""
    async with application:  # Используем контекстный менеджер для доступа к боту
        await application.bot.send_message(
            chat_id=user_id,
            text=f"Новая серия: {title}\n{url}"
        )


# --- Elasticsearch ---

def index_anime_data():
    """Индексирует данные аниме в Elasticsearch."""
    cursor.execute("SELECT * FROM anime")
    anime_list = cursor.fetchall()

    for anime in anime_list:
        # Изменено: убран anime_hash из doc
        doc = {
            'anime_id': anime[0],
            'anime_title': anime[1],
            'anime_image': anime[2],
        }
        es.index(index='anime', id=anime[0], document=doc)


async def search_anime(query: str):
    """Выполняет нечеткий поиск аниме в Elasticsearch."""
    try:
        search_results = es.search(
            index="anime",
            body={
                "query": {
                    "bool": {  # Используем bool для комбинирования запросов
                        "must": [  # Обязательное условие: совпадение по префиксу
                            {
                                "match_phrase_prefix": {
                                    "anime_title": {
                                        "query": query.lower(),
                                        "slop": 2  # Допускаем до 2 перестановок слов
                                    }
                                }
                            }
                        ],
                        "should": [  # Желательное условие: нечеткое совпадение первых символов
                            {
                                "fuzzy": {
                                    "anime_title": {
                                        "value": query.lower(),
                                        "fuzziness": 2,  # Максимальное количество опечаток: 1
                                        "prefix_length": 3  # Не допускаем опечатки в первых 2 символах
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        )
        return [
            {
                "id": hit["_source"]["anime_id"],
                "title": hit["_source"]["anime_title"],
                "image_url": hit["_source"].get("anime_image", "")  # Получаем URL картинки
            }
            for hit in search_results["hits"]["hits"]
        ]
    except Exception as e:
        logging.error(f"Ошибка при поиске в Elasticsearch: {e}")
        return []  # Возвращаем пустой результат в случае ошибки


# --- Обработчик инлайн-запросов ---

async def inline_search_anime(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик инлайн-запросов."""
    query = update.inline_query.query

    if not query:
        return

    search_results = await search_anime(query)
    if search_results:
        results = [
            InlineQueryResultArticle(
                id=result['id'],
                title=result['title'],
                thumbnail_url=result.get('image_url', ''),  # URL маленькой картинки (можно тот же)
                description=f"Нажмите, чтобы подписаться на {result['title']}",
                input_message_content=InputTextMessageContent(
                    message_text=f"/anime {result['id']}"  # Используем команду /anime
                ),
            )
            for result in search_results
        ]
        await update.inline_query.answer(results)


# Новый обработчик команды /anime
async def handle_anime_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает команду /anime <anime_id>."""
    anime_id = context.args[0]  # Получаем anime_id из аргументов команды

    # Получаем информацию об аниме
    anime_title = get_anime_title_by_id(anime_id)
    anime_image = get_anime_image_by_id(anime_id)

    # Создаем кнопку "Подписаться"
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Подписаться", callback_data=f"subscribe_{anime_id}")]]
    )
    str = f"Выбранное аниме: {anime_title}"
    # Отправляем сообщение с картинкой и кнопкой
    await context.bot.send_photo(
        chat_id=update.effective_user.id,
        photo=anime_image,
        caption=str,
        reply_markup=keyboard,
    )


# --- Запуск бота ---

if __name__ == '__main__':
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_clicked))
    application.add_handler(InlineQueryHandler(inline_search_anime))  # <--  Обработчик инлайн-режима
    application.add_handler(CommandHandler("anime", handle_anime_command))

    logging.info("Запуск бота...")
    # Запуск проверки обновлений в отдельном потоке
    threading.Thread(target=check_updates_and_notify, daemon=True).start()
    # Запуск обновления базы данных в отдельном потоке
    threading.Thread(target=update_anime_database, daemon=True).start()

    application.run_polling()