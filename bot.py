# -*- coding: utf-8 -*-

import logging
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, Router, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO)
TOKEN = "7766027837:AAFFORwPFg_CCZ5iEx0saTzCQL-ihXoHvNA"
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()

# URL для скачивания расписания
SCHEDULE_URL = "https://guppros.ru/ru/rubric/students/shedule1/rapisanietabl"
FILE_PATH = "44.03.01 Информатика.xlsx"

# Функция для скачивания файла
async def download_schedule():
    async with aiohttp.ClientSession() as session:
        async with session.get(SCHEDULE_URL) as response:
            if response.status == 200:
                with open(FILE_PATH, "wb") as file:
                    file.write(await response.read())
                logging.info("Файл с расписанием успешно скачан.")
            else:
                logging.error("Ошибка при скачивании файла.")

# Функция для загрузки данных из Excel
async def load_schedule():
    if os.path.exists(FILE_PATH):
        return pd.read_excel(FILE_PATH, sheet_name='Лист1', header=None)
    return None

df = None  # Глобальная переменная для хранения данных

# Функция для получения расписания
async def get_schedule(group, date):
    global df
    if df is None:
        df = await load_schedule()
    
    if df is None:
        return "Расписание не загружено."
    
    schedule = ""
    date_str = date.strftime("%Y-%m-%d")
    found_date = False
    
    for i in range(len(df)):
        cell_date = pd.to_datetime(df.iloc[i, 0], errors='coerce')
        if pd.notna(cell_date) and cell_date.strftime("%Y-%m-%d") == date_str:
            found_date = True
            for j in range(7):
                time_info = df.iloc[i + j, 1]
                group_schedule = df.iloc[i + j, 4 if group == 1 else 5]
                if pd.notna(group_schedule):
                    schedule += f"\n📚 Пара {j+1}\n⏰ {time_info}\n{group_schedule}\n"
            break
    
    return schedule if found_date and schedule else "Нет занятий"

# Планировщик для обновления файла раз в 2 дня
async def schedule_updater():
    while True:
        await download_schedule()
        global df
        df = await load_schedule()
        await asyncio.sleep(172800)  # 2 дня в секундах

# Обработчики команд и кнопок
@router.message(Command(commands=['start', 'help']))
async def send_welcome(message: types.Message):
    markup = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="1 группа"), KeyboardButton(text="2 группа")]
        ],
        resize_keyboard=True
    )
    await message.answer("Привет! Выбери свою группу:", reply_markup=markup)

@router.message(lambda message: message.text in ["1 группа", "2 группа"])
async def choose_group(message: types.Message, state: FSMContext):
    group = 1 if message.text == "1 группа" else 2
    await state.update_data(group=group)
    markup = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Сегодня"), KeyboardButton(text="Завтра")],
            [KeyboardButton(text="Неделя"), KeyboardButton(text="Выбрать дату")]
        ],
        resize_keyboard=True
    )
    await message.answer(f"Вы выбрали {message.text}. Что показать?", reply_markup=markup)

@router.message(lambda message: message.text in ["Сегодня", "Завтра", "Неделя", "Выбрать дату"])
async def show_schedule(message: types.Message, state: FSMContext):
    data = await state.get_data()
    group = data.get("group", 1)
    today = datetime.now().date()
    
    if message.text == "Сегодня":
        date = today
    elif message.text == "Завтра":
        date = today + timedelta(days=1)
    else:
        await message.answer("Введите дату в формате ДД.ММ.ГГГГ:")
        return
    
    schedule = await get_schedule(group, date)
    await message.answer(f"📅 Расписание на {date.strftime('%d.%m.%Y')}:{schedule}")

async def main():
    logging.info("Бот запущен и готов к работе!")
    dp.include_router(router)
    asyncio.create_task(schedule_updater())  # Запускаем обновление расписания
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
