# -*- coding: utf-8 -*-

import pandas as pd
import datetime
import re
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# Укажите путь к файлу расписания
EXCEL_FILE = "44.03.01 Информатика.xlsx"
GROUPS = {
    "Группа 1": "09.ПООБ.22.И.1*1",
    "Группа 2": "09.ПООБ.22.И.1*2"
}

# Загружаем данные
xls = pd.ExcelFile(EXCEL_FILE)
df = pd.read_excel(xls, sheet_name="Лист1")

# Определяем названия колонок
df.columns = df.iloc[1]  # Берем строку с заголовками
df = df[2:].reset_index(drop=True)  # Удаляем лишние строки

# Приводим дату в правильный формат
df[df.columns[0]] = pd.to_datetime(df[df.columns[0]], errors="coerce").dt.date

# Функция поиска расписания по дате и группе
def get_schedule(date, group):
    day_schedule = df[df[df.columns[0]] == date][[df.columns[1], group]].dropna()
    
    if day_schedule.empty:
        return "Занятий нет."

    schedule_text = f"📅 {date}:\n\n👥 {group}:\n"
    for _, row in day_schedule.iterrows():
        schedule_text += f"{row[df.columns[1]]}\n{row[group]}\n\n"

    return schedule_text.strip()

# Функция запроса подгруппы
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [["Группа 1"], ["Группа 2"]]
    reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("Выберите свою подгруппу:", reply_markup=reply_markup)

# Функция обработки выбора подгруппы
async def choose_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    group = update.message.text.strip()
    if group in GROUPS:
        context.user_data["group"] = GROUPS[group]
        keyboard = [["Расписание на сегодня"], ["Расписание на завтра"], ["Расписание на неделю"]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(f"Вы выбрали {group}. Теперь выберите расписание на какой день вы хотите увидеть или введите дату (ДД.ММ.ГГГГ) самостоятельно:", reply_markup=reply_markup)
    else:
        return  # Если введено что-то другое — ничего не делаем

# Функции получения расписания
async def schedule_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "group" not in context.user_data:
        await update.message.reply_text("Сначала выберите подгруппу с помощью команды /start.")
        return
    today = datetime.date.today()
    await update.message.reply_text(get_schedule(today, context.user_data["group"]))

async def schedule_tomorrow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "group" not in context.user_data:
        await update.message.reply_text("Сначала выберите подгруппу с помощью команды /start.")
        return
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    await update.message.reply_text(get_schedule(tomorrow, context.user_data["group"]))

async def schedule_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "group" not in context.user_data:
        await update.message.reply_text("Сначала выберите подгруппу с помощью команды /start.")
        return
    today = datetime.date.today()
    next_week = df[(df[df.columns[0]] >= today) & (df[df.columns[0]] < today + datetime.timedelta(days=7))]

    schedule_text = ""
    for date in next_week[df.columns[0]].unique():
        day_schedule = df[df[df.columns[0]] == date][[df.columns[1], context.user_data["group"]]].dropna()
        if not day_schedule.empty:
            schedule_text += f"📅 {date}:\n\n👥 {context.user_data['group']}:\n"
            for _, row in day_schedule.iterrows():
                schedule_text += f"{row[df.columns[1]]}\n{row[context.user_data['group']]}\n\n"

    await update.message.reply_text(schedule_text if schedule_text else "Занятий нет.")

# Обработчик сообщений с датой
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "group" not in context.user_data:
        await update.message.reply_text("Сначала выберите подгруппу с помощью команды /start.")
        return
    
    user_message = update.message.text.strip()
    if re.match(r"\d{2}\.\d{2}\.\d{4}", user_message):
        try:
            date = datetime.datetime.strptime(user_message, "%d.%m.%Y").date()
            response = get_schedule(date, context.user_data["group"])
        except ValueError:
            response = "Некорректный формат даты. Используйте ДД.ММ.ГГГГ."
    else:
        return  # Не обрабатываем случайные сообщения

    await update.message.reply_text(response)

# Основной запуск бота
def main():
    TOKEN = "7766027837:AAFFORwPFg_CCZ5iEx0saTzCQL-ihXoHvNA"  # Замените на свой токен
    app = Application.builder().token(TOKEN).build()

    # Добавляем обработчики команд
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Regex("Группа 1|Группа 2"), choose_group))
    app.add_handler(MessageHandler(filters.Regex("Расписание на сегодня"), schedule_today))
    app.add_handler(MessageHandler(filters.Regex("Расписание на завтра"), schedule_tomorrow))
    app.add_handler(MessageHandler(filters.Regex("Расписание на неделю"), schedule_week))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    print("Бот запущен...")

    app.run_polling()

if __name__ == "__main__":
    main()
