import asyncio
import logging
import json
import multiprocessing

import aiohttp

import aiogram
import psycopg2
from html import escape
from typing import Union

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramUnauthorizedError, TelegramBadRequest # Added for API key validation


# --- Predefined list of exchanges for the new filter ---
PREDEFINED_EXCHANGES = [
    "binance", "mexc", "gate", "kucoin", "bybit", "lbank",
    "htx", "bitmart", "coinex", "okx", "bingx"
]

# --- PostgreSQL Database Configuration ---
# Replace with your actual database credentials, consider using environment variables
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "admin"
DB_HOST = "localhost"  # Or your DB host
DB_PORT = "5432"  # Default PostgreSQL port

# --- Logging Setup ---
full_path_tg_log = 'logging/telegram_bot.log'
logger = logging.getLogger('TelegramBot')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(full_path_tg_log)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False

sent_message_logger_path = 'logging/sent_message.log'
sent_message_logger = logging.getLogger('SENT MESSAGE')
sent_message_logger.setLevel(logging.INFO)
sent_message_handler = logging.FileHandler(sent_message_logger_path)
sent_message_handler.setFormatter(formatter)
sent_message_logger.addHandler(sent_message_handler)
sent_message_logger.propagate = False

# --- Bot Token ---
BOT_TOKEN = "7673553106:AAFwdxsLR8s1BILe-xm1fFO52Ac9SHq7FqY"
if not BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN environment variable not set!")
    exit("Telegram Bot Token not found.")

# --- Initialize Bot and Dispatcher ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


# --- Database Functions ---
def get_db_connection():
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        return conn
    except psycopg2.Error as e:
        logger.error(f"Error connecting to PostgreSQL database: {e}")
        raise


def initialize_db():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS users (user_id BIGINT PRIMARY KEY, username VARCHAR(255), created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);")
            cur.execute(
                "CREATE TABLE IF NOT EXISTS user_settings (user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE, user_api_key TEXT, filters JSONB, last_updated TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);")
            conn.commit()
        logger.info("Database initialized/checked successfully.")
    except psycopg2.Error as e:
        logger.error(f"Database initialization error: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()


def get_default_filter():
    """UPDATED: Contains the full set of new and old filters."""
    return {
        "mod": ["DEX/CEX"],
        "exception_token_pair": [],
        "exception_networks": [],
        "exchange_settings": {exchange: {"buy": True, "sell": True} for exchange in PREDEFINED_EXCHANGES},
        "in_one_network": True,
        "min_profit_percentage": 1.0,
        "max_profit_percentage": None,
        "min_volume": 0.0,
        "max_volume": None,
    }


def get_user_config_sync(user_id: int, username: str | None) -> dict:
    """UPDATED: Robust migration for all new keys."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (user_id, username) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, updated_at = NOW() WHERE users.username IS DISTINCT FROM EXCLUDED.username;",
                (user_id, username))
            cur.execute("SELECT user_api_key, filters FROM user_settings WHERE user_id = %s;", (user_id,))
            settings = cur.fetchone()
            config = {}
            default_filters = get_default_filter()

            if settings:
                user_api_key, filters_json = settings
                config = {"user_id": user_id, "username": username, "user_API": user_api_key,
                          "filter": filters_json if filters_json else default_filters}
            else:
                logger.info(f"New user settings for DB: {user_id} ({username}). Creating default.")
                cur.execute("INSERT INTO user_settings (user_id, user_api_key, filters) VALUES (%s, %s, %s);",
                            (user_id, None, json.dumps(default_filters)))
                conn.commit()
                config = {"user_id": user_id, "username": username, "user_API": None, "filter": default_filters}

            # --- MIGRATION/VALIDATION LOGIC ---
            # Rename old max_profit key if it exists
            if 'max_profit' in config['filter']:
                config['filter']['max_profit_percentage'] = config['filter'].pop('max_profit')

            # Ensure all default keys exist
            for key, value in default_filters.items():
                if key not in config['filter']:
                    config['filter'][key] = value

            # Remove the very old exception_exchanges list if it exists
            if "exception_exchanges" in config['filter']:
                del config['filter']['exception_exchanges']

            return config
    except psycopg2.Error as e:
        logger.error(f"Error getting user config for {user_id} from DB: {e}")
        if conn: conn.rollback()
        return {"user_id": user_id, "username": username, "user_API": None, "filter": get_default_filter()}
    finally:
        if conn: conn.close()


def save_user_config_sync(user_id: int, config: dict):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (user_id, username) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, updated_at = NOW() WHERE users.username IS DISTINCT FROM EXCLUDED.username;",
                (user_id, config.get("username")))
            cur.execute(
                "INSERT INTO user_settings (user_id, user_api_key, filters, last_updated) VALUES (%s, %s, %s, NOW()) ON CONFLICT (user_id) DO UPDATE SET user_api_key = EXCLUDED.user_api_key, filters = EXCLUDED.filters, last_updated = NOW();",
                (user_id, config.get("user_API"), json.dumps(config.get("filter"))))
            conn.commit()
        logger.info(f"Configuration saved to DB for user {user_id}.")
    except psycopg2.Error as e:
        logger.error(f"Error saving user config for {user_id} to DB: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()


# --- FSM States ---
class ConfigStates(StatesGroup):
    MAIN_MENU = State()
    ASK_API_KEY = State()
    FILTER_MENU = State()
    MANAGE_EXCHANGES = State()  # New state for the grid
    ASK_MODS = State()
    MANAGE_LIST_CHOICE = State()
    ASK_ADD_ITEM = State()
    ASK_REMOVE_ITEM_SELECT = State()
    ASK_REMOVE_ITEM_INPUT = State()
    ASK_BOOL_VALUE = State()
    ASK_FLOAT_VALUE = State()


# --- Keyboard Helpers ---
def main_menu_keyboard_aiogram() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="📊 Показать текущую конфигурацию", callback_data="main_view")],
        [InlineKeyboardButton(text="🔑 Установить API-ключ", callback_data="main_set_api")],
        [InlineKeyboardButton(text="⚙️ Изменить фильтры", callback_data="main_modify_filters")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_config")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def filter_menu_keyboard_aiogram() -> InlineKeyboardMarkup:
    """UPDATED: Full menu with all new options."""
    buttons = [
        [InlineKeyboardButton(text="🏦 Настроить биржи", callback_data="filter_manage_exchanges")],
        [InlineKeyboardButton(text="🔄 Установить режимы (mod)", callback_data="filter_set_mod")],
        [InlineKeyboardButton(text="📈 Управлять парами-исключениями",
                              callback_data="filter_manage_list_exception_token_pair")],
        [InlineKeyboardButton(text="🔗 Управлять сетями-исключениями",
                              callback_data="filter_manage_list_exception_networks")],
        [InlineKeyboardButton(text="↔️ В одной сети", callback_data="filter_set_bool_in_one_network")],
        [InlineKeyboardButton(text="💲 Установить мин. % прибыли",
                              callback_data="filter_set_float_min_profit_percentage")],
        [InlineKeyboardButton(text="📈 Установить макс. % прибыли",
                              callback_data="filter_set_float_max_profit_percentage")],
        [InlineKeyboardButton(text="💰 Установить мин. объем ($)", callback_data="filter_set_float_min_volume")],
        [InlineKeyboardButton(text="💸 Установить макс. объем ($)", callback_data="filter_set_float_max_volume")],
        [InlineKeyboardButton(text="⬅️ Назад в главное меню", callback_data="back_to_main_menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def exchange_management_keyboard(settings: dict) -> InlineKeyboardMarkup:
    """NEW: Keyboard for the exchange grid."""
    buttons = [[InlineKeyboardButton(text="Покупка 🔴", callback_data="noop"),
                InlineKeyboardButton(text="Продажа 🟢", callback_data="noop")]]
    for exchange in PREDEFINED_EXCHANGES:
        buy_status = settings.get(exchange, {}).get("buy", True)
        sell_status = settings.get(exchange, {}).get("sell", True)
        buy_emoji, sell_emoji = ("✅" if buy_status else "❌"), ("✅" if sell_status else "❌")
        buy_button = InlineKeyboardButton(text=f"{buy_emoji} {exchange}",
                                          callback_data=f"exchange_toggle:{exchange}:buy")
        sell_button = InlineKeyboardButton(text=f"{sell_emoji} {exchange}",
                                           callback_data=f"exchange_toggle:{exchange}:sell")
        buttons.append([buy_button, sell_button])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад к фильтрам", callback_data="back_to_filters_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def list_management_keyboard_aiogram(filter_key: str) -> InlineKeyboardMarkup:
    filter_key_display = {"exception_token_pair": "пары-исключения", "exception_networks": "сети-исключения"}.get(
        filter_key, filter_key)
    buttons = [
        [InlineKeyboardButton(text=f"➕ Добавить в {escape(filter_key_display)}",
                              callback_data=f"list_add_{filter_key}")],
        [InlineKeyboardButton(text=f"➖ Удалить из {escape(filter_key_display)}",
                              callback_data=f"list_remove_{filter_key}")],
        [InlineKeyboardButton(text="⬅️ Назад к фильтрам", callback_data="back_to_filters_menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def boolean_keyboard_aiogram(filter_key: str) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="✅ Да", callback_data=f"bool_set_{filter_key}_True"),
         InlineKeyboardButton(text="❌ Нет", callback_data=f"bool_set_{filter_key}_False")],
        [InlineKeyboardButton(text="⬅️ Назад к фильтрам", callback_data="back_to_filters_menu")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def removal_keyboard_aiogram(items: list, filter_key: str) -> InlineKeyboardMarkup:
    # This function is unchanged from your stable script
    max_buttons_per_row = 3
    keyboard_buttons = []
    row = []
    for i, item in enumerate(items):
        display_item = escape(item[:20] + '...' if len(item) > 23 else item)
        row.append(InlineKeyboardButton(text=f"➖ {display_item}", callback_data=f"list_remove_item_{filter_key}_{i}"))
        if len(row) == max_buttons_per_row:
            keyboard_buttons.append(row)
            row = []
    if row: keyboard_buttons.append(row)
    keyboard_buttons.append(
        [InlineKeyboardButton(text="✨ Удалить несколько (ввести)", callback_data=f"list_remove_input_{filter_key}")])
    keyboard_buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"list_manage_back_{filter_key}")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)


# --- Conversation Entry Point & Global Commands ---
@dp.message(CommandStart())
@dp.message(Command("config"))
async def cmd_start(message: Message, state: FSMContext):
    user = message.from_user
    logger.info(f"User {user.id} ({user.username}) started config conversation.")
    await state.clear()
    await asyncio.to_thread(get_user_config_sync, user.id, user.username)
    await message.answer(f"Привет, {user.mention_html()}! Добро пожаловать в конфигуратор.\nЧто вы хотите сделать?",
                         reply_markup=main_menu_keyboard_aiogram())
    await state.set_state(ConfigStates.MAIN_MENU)


async def common_cancel_logic(event_obj: Union[Message, CallbackQuery], state: FSMContext):
    user_id = event_obj.from_user.id
    current_state = await state.get_state()
    logger.info(f"User {user_id} cancelled configuration from state {current_state}.")
    await state.clear()
    message_text = "Настройка отменена."
    if isinstance(event_obj, Message):
        await event_obj.answer(message_text, reply_markup=ReplyKeyboardRemove())
    elif isinstance(event_obj, CallbackQuery):
        await event_obj.answer()
        try:
            await event_obj.message.edit_text(message_text, reply_markup=None)
        except Exception as e:
            logger.warning(f"Could not edit message on cancel for user {user_id}: {e}")
            await bot.send_message(chat_id=user_id, text=message_text, reply_markup=ReplyKeyboardRemove())


@dp.message(Command("cancel_config"))
async def cmd_cancel_config_msg(message: Message, state: FSMContext):
    await common_cancel_logic(message, state)


@dp.callback_query(F.data == "cancel_config")
async def cq_cancel_config(callback_query: CallbackQuery, state: FSMContext):
    await common_cancel_logic(callback_query, state)


# --- Navigation Handlers ---
@dp.callback_query(F.data == "back_to_filters_menu")
async def cq_back_to_filters_menu(callback: CallbackQuery, state: FSMContext):
    # This handler now serves multiple "back" buttons
    await callback.answer()
    await state.update_data(current_list_key=None, current_bool_key=None, current_float_key=None)
    logger.debug(f"User {callback.from_user.id} returning to filter menu.")
    await callback.message.edit_text("Выберите категорию фильтра:", reply_markup=filter_menu_keyboard_aiogram())
    await state.set_state(ConfigStates.FILTER_MENU)


@dp.callback_query(ConfigStates.FILTER_MENU, F.data == "back_to_main_menu")
async def cq_filter_back_to_main(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_text("Возвращаемся в главное меню.", reply_markup=main_menu_keyboard_aiogram())
    await state.set_state(ConfigStates.MAIN_MENU)


# --- Main Menu Handlers (TRANSLATED) ---
@dp.callback_query(ConfigStates.MAIN_MENU, F.data.startswith("main_"))
async def cq_main_menu(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user = callback.from_user
    query_data = callback.data

    if query_data == "main_view":
        config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)
        config_json = json.dumps(config, indent=4, ensure_ascii=False)
        message_text = f"Ваша текущая конфигурация:\n<pre><code class=\"language-json\">{escape(config_json)}</code></pre>"
        try:
            if len(message_text) > 4096:
                await callback.message.edit_text("Конфигурация слишком длинная. Отправляю по частям:")
                for i in range(0, len(config_json), 4000):
                    chunk = escape(config_json[i:i + 4000])
                    await callback.message.answer(f"<pre><code class=\"language-json\">{chunk}</code></pre>")
                await callback.message.answer("--- Конец конфигурации ---\nЧто дальше?",
                                              reply_markup=main_menu_keyboard_aiogram())
            else:
                await callback.message.edit_text(message_text, reply_markup=main_menu_keyboard_aiogram())
        except Exception as e:
            logger.error(f"Error sending/editing config view for user {user.id}: {e}")
            await callback.message.answer("Ошибка отображения конфигурации. Пожалуйста, попробуйте снова.")
        await state.set_state(ConfigStates.MAIN_MENU)

    elif query_data == "main_set_api":
        await callback.message.edit_text(
            "Пожалуйста, отправьте мне API-ключ для <b>вашего бота</b>.\n"
            "Это токен, который вы получили от @BotFather для бота, который будет получать арбитражные сигналы.\n\n"
            "<i>Пример: 123456:ABC-DEF1234ghIkl-zyx57W2v1uTxs_000</i>\n\n"
            "Используйте /cancel_config для отмены.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_config")]])
        )
        await state.set_state(ConfigStates.ASK_API_KEY)

    elif query_data == "main_modify_filters":
        await callback.message.edit_text(
            "Выберите категорию фильтра, которую вы хотите изменить:",
            reply_markup=filter_menu_keyboard_aiogram()
        )
        await state.set_state(ConfigStates.FILTER_MENU)


@dp.message(ConfigStates.ASK_API_KEY, F.text)
async def msg_ask_api_key(message: Message, state: FSMContext):
    user = message.from_user
    provided_api_key = message.text.strip()

    if not provided_api_key:
        await message.reply("API-ключ не может быть пустым. Пожалуйста, отправьте действительный ключ или используйте /cancel_config.")
        return

    if provided_api_key == BOT_TOKEN:
        await message.reply(
            "Вы ввели API-ключ этого бота-конфигуратора. "
            "Пожалуйста, предоставьте API-ключ для <b>вашего собственного бота</b> (который будет получать сигналы), полученный от @BotFather."
        )
        return

    temp_bot_instance = None
    try:
        logger.info(f"User {user.id} attempting to verify API key: {provided_api_key[:10]}...")
        temp_bot_instance = Bot(token=provided_api_key, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        bot_info = await temp_bot_instance.get_me()
        bot_username = bot_info.username

        if not bot_username:
            logger.warning(f"API key {provided_api_key[:10]}... is valid but bot has no username. User: {user.id}")
            await message.reply(
                "API-ключ действителен, но у связанного с ним бота нет имени пользователя. "
                "Пожалуйста, убедитесь, что у вашего бота установлено имя пользователя в @BotFather.\n"
                "Ключ пока не будет сохранен. Пожалуйста, попробуйте снова после установки имени пользователя или используйте /cancel_config."
            )
            return

        logger.info(f"API key successfully verified for bot @{bot_username} by user {user.id}.")

        config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)
        config["user_API"] = provided_api_key
        await asyncio.to_thread(save_user_config_sync, user.id, config)

        success_message = (
            f"✅ API-ключ подтвержден для бота: @{bot_username}\n\n"
            f"<b>ВАЖНО:</b> Чтобы получать сигналы, убедитесь, что вы запустили вашего бота "
            f"(@{bot_username}), отправив ему команду /start."
        )
        await message.reply(success_message, reply_markup=main_menu_keyboard_aiogram())
        await state.set_state(ConfigStates.MAIN_MENU)

    except TelegramUnauthorizedError:
        logger.warning(f"Invalid API key provided by user {user.id} ({provided_api_key[:10]}...): Unauthorized.")
        await message.reply(
            "Предоставленный вами API-ключ недействителен (Unauthorized).\n"
            "Пожалуйста, перепроверьте ключ, полученный от @BotFather, и попробуйте снова, или используйте /cancel_config."
        )
    except TelegramBadRequest as e:
        logger.warning(f"Invalid API key provided by user {user.id} ({provided_api_key[:10]}...): Bad Request ({e}).")
        await message.reply(
            f"Предоставленный вами API-ключ выглядит некорректным или недействительным (Ошибка: {escape(str(e))}).\n"
            "Пожалуйста, проверьте ключ и попробуйте снова, или используйте /cancel_config."
        )
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during API key verification for user {user.id} ({provided_api_key[:10]}...): {e}",
            exc_info=True)
        await message.reply(
            "Произошла непредвиденная ошибка при проверке вашего API-ключа. Пожалуйста, попробуйте снова.\n"
            "Если проблема сохранится, вы можете использовать /cancel_config для возврата."
        )
    finally:
        if temp_bot_instance:
            await temp_bot_instance.session.close()
            logger.info(f"Temporary bot session closed for user {user.id}'s API key check.")

@dp.callback_query(ConfigStates.FILTER_MENU, F.data.startswith("filter_"))
async def cq_filter_menu(callback: CallbackQuery, state: FSMContext):
    """UPDATED: This handler now routes to all new and old filter types."""
    await callback.answer()
    user = callback.from_user
    config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)
    query_data = callback.data

    if query_data == "filter_manage_exchanges":
        await callback.message.edit_text(
            "Выберите биржи:\nНажмите на биржу, чтобы включить/выключить (✅/❌) ее для покупки или продажи.",
            reply_markup=exchange_management_keyboard(config["filter"]["exchange_settings"]))
        await state.set_state(ConfigStates.MANAGE_EXCHANGES)

    elif query_data.startswith("filter_manage_list_"):
        # This branch handles the OLD list-based exception_exchanges as well as pairs/networks
        filter_key = query_data.replace("filter_manage_list_", "")
        if filter_key == "exception_exchanges":
            # Redirect old button to new interface
            await callback.message.edit_text("Управление биржами теперь в новом формате.\nВыберите биржи:",
                                             reply_markup=exchange_management_keyboard(
                                                 config["filter"]["exchange_settings"]))
            await state.set_state(ConfigStates.MANAGE_EXCHANGES)
            return

        await state.update_data(current_list_key=filter_key)
        current_list = config["filter"].get(filter_key, [])
        list_str = "\n- ".join(map(escape, current_list)) if current_list else "Пусто"
        await callback.message.edit_text(
            f"Управление: <b>{escape(filter_key)}</b>\n\nТекущие элементы:\n- {list_str}\n\nЧто вы хотите сделать?",
            reply_markup=list_management_keyboard_aiogram(filter_key))
        await state.set_state(ConfigStates.MANAGE_LIST_CHOICE)

    elif query_data == "filter_set_mod":
        current_mods_list = config["filter"].get("mod", [])
        text_to_send = f"Текущие режимы: <code>{escape(', '.join(current_mods_list))}</code>\n\nВведите новые режимы через пробел (например, <code>DEX DEX/CEX CEX</code>). Это <b>заменит</b> текущий список.\nИспользуйте /cancel_config для отмены."
        await callback.message.edit_text(text_to_send, reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_filters_menu")]]))
        await state.set_state(ConfigStates.ASK_MODS)

    elif query_data.startswith("filter_set_bool_"):
        filter_key = query_data.replace("filter_set_bool_", "")
        await state.update_data(current_bool_key=filter_key)
        current_value = config["filter"].get(filter_key, "Не установлено")
        await callback.message.edit_text(
            f"Установить <b>{escape(filter_key)}</b>\nТекущее значение: <code>{escape(str(current_value))}</code>\nВыберите новое значение:",
            reply_markup=boolean_keyboard_aiogram(filter_key))
        await state.set_state(ConfigStates.ASK_BOOL_VALUE)

    elif query_data.startswith("filter_set_float_"):
        filter_key = query_data.replace("filter_set_float_", "")
        await state.update_data(current_float_key=filter_key)
        current_value_raw = config["filter"].get(filter_key)
        current_value_display = escape(str(current_value_raw)) if current_value_raw is not None else "Не установлено"
        key_display_names = {"min_profit_percentage": "Мин. % прибыли", "max_profit_percentage": "Макс. % прибыли",
                             "min_volume": "Мин. объем ($)", "max_volume": "Макс. объем ($)"}
        display_name = key_display_names.get(filter_key, filter_key)

        text_to_send = f"Установить <b>{display_name}</b>\nТекущее значение: <code>{current_value_display}</code>\n\nПожалуйста, отправьте новое числовое значение (например, <code>1000</code> или <code>1.5</code>).\nИспользуйте /cancel_config для отмены."

        if filter_key in ["max_volume", "max_profit_percentage"]:
            text_to_send = f"Установить <b>{display_name}</b>\nТекущее значение: <code>{current_value_display}</code>\n\nПожалуйста, отправьте новое числовое значение. Отправьте '0' или 'нет', чтобы убрать ограничение.\nИспользуйте /cancel_config для отмены."

        await callback.message.edit_text(text_to_send, reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_filters_menu")]]))
        await state.set_state(ConfigStates.ASK_FLOAT_VALUE)


# --- Exchange Grid Handlers ---
@dp.callback_query(ConfigStates.MANAGE_EXCHANGES, F.data.startswith("exchange_toggle:"))
async def cq_toggle_exchange(callback: CallbackQuery, state: FSMContext):
    user = callback.from_user
    try:
        _, exchange_name, action_type = callback.data.split(":")
        config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)
        current_status = config["filter"]["exchange_settings"][exchange_name][action_type]
        config["filter"]["exchange_settings"][exchange_name][action_type] = not current_status
        await asyncio.to_thread(save_user_config_sync, user.id, config)
        logger.info(f"User {user.id} toggled {exchange_name}/{action_type} to {not current_status}")
        await callback.message.edit_reply_markup(
            reply_markup=exchange_management_keyboard(config["filter"]["exchange_settings"]))
        await callback.answer()
    except (ValueError, KeyError) as e:
        logger.warning(f"Error in exchange toggle: {e} for data {callback.data}")
        await callback.answer("Ошибка. Попробуйте снова.")


@dp.callback_query(ConfigStates.MANAGE_EXCHANGES, F.data == "noop")
async def cq_noop(callback: CallbackQuery):
    await callback.answer()


# --- All other message and callback handlers from your stable script ---
# These handlers are now complete and functional.
@dp.message(ConfigStates.ASK_MODS, F.text)
async def msg_ask_mods(message: Message, state: FSMContext):
    user = message.from_user
    new_mods_str = message.text.strip()
    config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)

    if not new_mods_str:
        current_mods = ", ".join(config["filter"].get("mod", []))
        await message.reply(
            f"Режимы не могут быть пустыми. Пожалуйста, укажите режимы через пробел или используйте кнопку 'Назад' / /cancel_config.\n"
            f"Текущие режимы: <code>{escape(current_mods)}</code>",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_filters_menu")]])
        )
        return
    new_mods = [mod.strip().upper() for mod in new_mods_str.split()]
    config["filter"]["mod"] = new_mods
    await asyncio.to_thread(save_user_config_sync, user.id, config)
    logger.info(f"Modes set to {new_mods} for user {user.id} ({user.username}).")
    await message.reply(
        f"✅ Фильтр 'mod' установлен на: {escape(', '.join(new_mods))}",
        reply_markup=filter_menu_keyboard_aiogram()
    )
    await state.set_state(ConfigStates.FILTER_MENU)


# --- List Management Handlers (TRANSLATED) ---
@dp.callback_query(ConfigStates.MANAGE_LIST_CHOICE,
                   F.data.startswith("list_add_") | F.data.startswith("list_remove_") | F.data.startswith("list_manage_"))
async def cq_manage_list_choice(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user = callback.from_user
    fsm_data = await state.get_data()
    filter_key = fsm_data.get('current_list_key')

    if not filter_key:
        logger.warning(f"manage_list_choice_handler called without 'current_list_key' for user {user.id}")
        await callback.message.edit_text("Ошибка: Не удалось определить, какой список редактировать. Возврат в меню фильтров.",
                                         reply_markup=filter_menu_keyboard_aiogram())
        await state.set_state(ConfigStates.FILTER_MENU)
        return

    query_data = callback.data
    config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)

    if query_data == f"list_add_{filter_key}":
        await callback.message.edit_text(
            f"Введите элементы для <b>добавления</b> в <b>{escape(filter_key)}</b>, разделяя их пробелами (например, если пара то <code>MONETA/USDT PEPE/USDT</code>, если есть то <code>ARBITRUM ETHEREUM </code>).\n"
            "Используйте /cancel_config для отмены.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"list_manage_back_{filter_key}")]])
        )
        await state.set_state(ConfigStates.ASK_ADD_ITEM)
    elif query_data == f"list_remove_{filter_key}":
        current_list = config["filter"].get(filter_key, [])
        if not current_list:
            await callback.message.edit_text(
                f"Список <b>{escape(filter_key)}</b> уже пуст. Нечего удалять.",
                reply_markup=list_management_keyboard_aiogram(filter_key)
            )
            return
        await callback.message.edit_text(
            f"Выберите элементы для <b>удаления</b> из <b>{escape(filter_key)}</b>:",
            reply_markup=removal_keyboard_aiogram(current_list, filter_key)
        )
        await state.set_state(ConfigStates.ASK_REMOVE_ITEM_SELECT)
    elif query_data == f"list_manage_back_{filter_key}":
        current_list = config["filter"].get(filter_key, [])
        list_str = "\n- ".join(map(escape, current_list)) if current_list else "Пусто"
        await callback.message.edit_text(
            f"Управление: <b>{escape(filter_key)}</b>\n\nТекущие элементы:\n- {list_str}\n\nЧто вы хотите сделать?",
            reply_markup=list_management_keyboard_aiogram(filter_key)
        )
        await state.set_state(ConfigStates.MANAGE_LIST_CHOICE)


@dp.callback_query(ConfigStates.ASK_ADD_ITEM, F.data.startswith("list_manage_back_"))
async def cq_add_item_back_to_manage(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user = callback.from_user
    fsm_data = await state.get_data()
    filter_key = fsm_data.get('current_list_key')

    if not filter_key:
        logger.warning(f"Back button pressed in ASK_ADD_ITEM but no 'current_list_key' for user {user.id}")
        await callback.message.edit_text("Ошибка: Не удалось определить, какой список редактировать. Возврат в меню фильтров.",
                                         reply_markup=filter_menu_keyboard_aiogram())
        await state.set_state(ConfigStates.FILTER_MENU)
        return

    config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)
    current_list = config["filter"].get(filter_key, [])
    list_str = "\n- ".join(map(escape, current_list)) if current_list else "Пусто"

    await callback.message.edit_text(
        f"Управление: <b>{escape(filter_key)}</b>\n\nТекущие элементы:\n- {list_str}\n\nЧто вы хотите сделать?",
        reply_markup=list_management_keyboard_aiogram(filter_key)
    )
    await state.set_state(ConfigStates.MANAGE_LIST_CHOICE)


@dp.message(ConfigStates.ASK_ADD_ITEM, F.text)
async def msg_ask_add_item(message: Message, state: FSMContext):
    user = message.from_user
    input_text = message.text.strip()
    fsm_data = await state.get_data()
    filter_key = fsm_data.get('current_list_key')

    if not filter_key:
        await message.reply("Ошибка: Состояние утеряно. Возврат в меню фильтров.", reply_markup=filter_menu_keyboard_aiogram())
        await state.set_state(ConfigStates.FILTER_MENU)
        return

    if not input_text:
        await message.reply(
            "Ввод не может быть пустым. Пожалуйста, укажите элементы через пробел или используйте кнопку 'Назад' / /cancel_config.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"list_manage_back_{filter_key}")]])
        )
        return

    config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)
    items_to_add_raw = [item.strip() for item in input_text.split()]
    items_to_add = []
    if filter_key in ["exception_token_pair", "exception_exchanges"]:
        items_to_add = [item.upper() for item in items_to_add_raw if item]
    elif filter_key == "exception_networks":
        items_to_add = [item.lower() for item in items_to_add_raw if item]
    else:
        items_to_add = [item for item in items_to_add_raw if item]

    current_list = config["filter"].get(filter_key, [])
    added_count = 0
    actually_added = []
    for item in items_to_add:
        if item and item not in current_list:
            current_list.append(item)
            actually_added.append(item)
            added_count += 1

    if added_count > 0:
        config["filter"][filter_key] = current_list
        await asyncio.to_thread(save_user_config_sync, user.id, config)
        logger.info(f"Added {added_count} items to '{filter_key}' for user {user.id}.")
        await message.reply(f"✅ Добавлено: {escape(', '.join(actually_added))}")
    else:
        await message.reply("Новые элементы не были добавлены (дубликаты или пустой ввод).")

    updated_config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)
    current_list_updated = updated_config["filter"].get(filter_key, [])
    list_str = "\n- ".join(map(escape, current_list_updated)) if current_list_updated else "Пусто"
    await message.answer(
        f"Управление: <b>{escape(filter_key)}</b>\n\nТекущие элементы:\n- {list_str}\n\nДобавить еще или удалить элементы?",
        reply_markup=list_management_keyboard_aiogram(filter_key)
    )
    await state.set_state(ConfigStates.MANAGE_LIST_CHOICE)

@dp.callback_query(ConfigStates.ASK_REMOVE_ITEM_SELECT,
                   F.data.startswith("list_remove_item_") | F.data.startswith("list_remove_input_") | F.data.startswith("list_manage_back_"))
async def cq_ask_remove_item_select(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user = callback.from_user
    fsm_data = await state.get_data()
    filter_key = fsm_data.get('current_list_key')

    if not filter_key:
        await callback.message.edit_text("Ошибка: Состояние утеряно. Возврат в меню фильтров.",
                                         reply_markup=filter_menu_keyboard_aiogram())
        await state.set_state(ConfigStates.FILTER_MENU)
        return

    query_data = callback.data
    config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)

    if query_data == f"list_manage_back_{filter_key}":
        current_list = config["filter"].get(filter_key, [])
        list_str = "\n- ".join(map(escape, current_list)) if current_list else "Пусто"
        await callback.message.edit_text(
            f"Управление: <b>{escape(filter_key)}</b>\n\nТекущие элементы:\n- {list_str}\n\nЧто вы хотите сделать?",
            reply_markup=list_management_keyboard_aiogram(filter_key)
        )
        await state.set_state(ConfigStates.MANAGE_LIST_CHOICE)
        return

    elif query_data == f"list_remove_input_{filter_key}":
        await callback.message.edit_text(
            f"Введите элементы для <b>удаления</b> из <b>{escape(filter_key)}</b>, разделяя их пробелами.\nРегистр символов может не учитываться в зависимости от типа списка.\n"
            "Используйте /cancel_config для отмены.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"list_remove_back_to_select_{filter_key}")]])
        )
        await state.set_state(ConfigStates.ASK_REMOVE_ITEM_INPUT)
        return

    elif query_data.startswith(f"list_remove_item_{filter_key}_"):
        try:
            item_index = int(query_data.split('_')[-1])
            current_list = config["filter"].get(filter_key, [])

            if 0 <= item_index < len(current_list):
                removed_item = current_list.pop(item_index)
                config["filter"][filter_key] = current_list
                await asyncio.to_thread(save_user_config_sync, user.id, config)
                logger.info(f"Removed item '{removed_item}' from '{filter_key}' for user {user.id}.")
                await callback.answer(f"✅ Удалено: {escape(removed_item)}", show_alert=False)

                updated_config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)
                current_list_updated = updated_config["filter"].get(filter_key, [])
                if not current_list_updated:
                    await callback.message.edit_text(
                        f"Список <b>{escape(filter_key)}</b> теперь пуст.",
                        reply_markup=list_management_keyboard_aiogram(filter_key)
                    )
                    await state.set_state(ConfigStates.MANAGE_LIST_CHOICE)
                else:
                    await callback.message.edit_text(
                        f"Выберите элементы для <b>удаления</b> из <b>{escape(filter_key)}</b>:",
                        reply_markup=removal_keyboard_aiogram(current_list_updated, filter_key)
                    )
                    await state.set_state(ConfigStates.ASK_REMOVE_ITEM_SELECT)
            else:
                logger.warning(f"Invalid item index {item_index} for removal from {filter_key} for user {user.id}")
                await callback.answer("Ошибка: Выбран неверный элемент.", show_alert=True)
                await callback.message.edit_text(
                    f"Выберите элементы для <b>удаления</b> из <b>{escape(filter_key)}</b> (Произошла ошибка, пожалуйста, повторите):",
                    reply_markup=removal_keyboard_aiogram(config["filter"].get(filter_key, []), filter_key),
                )
                await state.set_state(ConfigStates.ASK_REMOVE_ITEM_SELECT)
        except (ValueError, IndexError) as e:
            logger.error(f"Error processing removal callback '{query_data}': {e}")
            await callback.answer("Ошибка обработки запроса на удаление.", show_alert=True)
            await state.set_state(ConfigStates.ASK_REMOVE_ITEM_SELECT)

@dp.callback_query(ConfigStates.ASK_REMOVE_ITEM_INPUT, F.data.startswith("list_remove_back_to_select_"))
async def cq_remove_input_back_to_select(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user = callback.from_user
    fsm_data = await state.get_data()
    filter_key = fsm_data.get('current_list_key')
    if not filter_key:
        await callback.message.edit_text("Ошибка: Состояние утеряно.", reply_markup=filter_menu_keyboard_aiogram())
        await state.set_state(ConfigStates.FILTER_MENU)
        return

    config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)
    current_list = config["filter"].get(filter_key, [])
    if not current_list:
        await callback.message.edit_text(
            f"Список <b>{escape(filter_key)}</b> пуст.",
            reply_markup=list_management_keyboard_aiogram(filter_key)
        )
        await state.set_state(ConfigStates.MANAGE_LIST_CHOICE)
    else:
        await callback.message.edit_text(
            f"Выберите элементы для <b>удаления</b> из <b>{escape(filter_key)}</b>:",
            reply_markup=removal_keyboard_aiogram(current_list, filter_key)
        )
        await state.set_state(ConfigStates.ASK_REMOVE_ITEM_SELECT)


@dp.message(ConfigStates.ASK_REMOVE_ITEM_INPUT, F.text)
async def msg_ask_remove_item_input(message: Message, state: FSMContext):
    user = message.from_user
    input_text = message.text.strip()
    fsm_data = await state.get_data()
    filter_key = fsm_data.get('current_list_key')

    if not filter_key:
        await message.reply("Ошибка: Состояние утеряно. Возврат в меню фильтров.", reply_markup=filter_menu_keyboard_aiogram())
        await state.set_state(ConfigStates.FILTER_MENU)
        return

    if not input_text:
        await message.reply(
            "Ввод не может быть пустым. Пожалуйста, укажите элементы через пробел или используйте кнопку 'Назад' / /cancel_config.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"list_remove_back_to_select_{filter_key}")]])
        )
        return

    config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)
    items_to_remove_input_raw = [item.strip() for item in input_text.split()]
    current_list = config["filter"].get(filter_key, [])
    original_length = len(current_list)
    removed_items_display = []
    new_list = []

    if filter_key in ["exception_token_pair", "exception_exchanges", "exception_networks"]:
        items_to_remove_lower = {item.lower() for item in items_to_remove_input_raw if item}
        for item_in_list in current_list:
            if item_in_list.lower() not in items_to_remove_lower: new_list.append(item_in_list)
            else: removed_items_display.append(item_in_list)
    else:
        items_to_remove_set = {item for item in items_to_remove_input_raw if item}
        for item_in_list in current_list:
            if item_in_list not in items_to_remove_set: new_list.append(item_in_list)
            else: removed_items_display.append(item_in_list)

    config["filter"][filter_key] = new_list
    removed_count = original_length - len(new_list)

    if removed_count > 0:
        await asyncio.to_thread(save_user_config_sync, user.id, config)
        logger.info(f"Removed {removed_count} items from '{filter_key}' for user {user.id}.")
        await message.reply(f"✅ Удалено: {escape(', '.join(removed_items_display))}")
    else:
        await message.reply("Элементы не были удалены (не найдены или пустой ввод).")

    updated_config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)
    current_list_updated = updated_config["filter"].get(filter_key, [])
    list_str = "\n- ".join(map(escape, current_list_updated)) if current_list_updated else "Пусто"
    await message.answer(
        f"Управление: <b>{escape(filter_key)}</b>\n\nТекущие элементы:\n- {list_str}\n\nДобавить еще или удалить элементы?",
        reply_markup=list_management_keyboard_aiogram(filter_key)
    )
    await state.set_state(ConfigStates.MANAGE_LIST_CHOICE)


# --- Boolean Value Handlers (TRANSLATED) ---
@dp.callback_query(ConfigStates.ASK_BOOL_VALUE, F.data.startswith("bool_set_"))
async def cq_ask_bool_value(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user = callback.from_user
    fsm_data = await state.get_data()
    filter_key = fsm_data.get('current_bool_key')

    if not filter_key:
        await callback.message.edit_text("Ошибка: Состояние утеряно. Возврат в меню фильтров.",
                                         reply_markup=filter_menu_keyboard_aiogram())
        await state.set_state(ConfigStates.FILTER_MENU)
        return

    value_str = callback.data.split('_')[-1]
    new_value = value_str.lower() == 'true'

    config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)
    config["filter"][filter_key] = new_value
    await asyncio.to_thread(save_user_config_sync, user.id, config)
    logger.info(f"Filter '{filter_key}' set to {new_value} for user {user.id}.")
    await callback.message.edit_text(
        f"✅ Фильтр '<b>{escape(filter_key)}</b>' установлен на: <code>{new_value}</code>",
        reply_markup=filter_menu_keyboard_aiogram()
    )
    await state.update_data(current_bool_key=None)
    await state.set_state(ConfigStates.FILTER_MENU)



@dp.message(ConfigStates.ASK_FLOAT_VALUE, F.text)
async def msg_ask_float_value(message: Message, state: FSMContext):
    user = message.from_user
    input_text = message.text.replace(',', '.').strip()
    fsm_data = await state.get_data()
    filter_key = fsm_data.get('current_float_key')
    if not filter_key:
        await message.reply("Ошибка: Состояние утеряно. Возврат в меню фильтров.",
                            reply_markup=filter_menu_keyboard_aiogram())
        await state.set_state(ConfigStates.FILTER_MENU)
        return
    config = await asyncio.to_thread(get_user_config_sync, user.id, user.username)
    new_value = None
    if filter_key in ["max_volume", "max_profit_percentage"] and input_text.lower() in ["0", "нет", "none", "сброс"]:
        new_value = None
    else:
        try:
            new_value = float(input_text)
            if new_value < 0:
                await message.reply("Значение не может быть отрицательным.")
                return
        except ValueError:
            await message.reply("Неверный ввод. Пожалуйста, введите числовое значение.")
            return
    config["filter"][filter_key] = new_value
    await asyncio.to_thread(save_user_config_sync, user.id, config)
    new_value_display = escape(str(new_value)) if new_value is not None else "Не установлено"
    await message.reply(f"✅ Фильтр '<b>{escape(filter_key)}</b>' установлен на: <code>{new_value_display}</code>",
                        reply_markup=filter_menu_keyboard_aiogram())
    await state.update_data(current_float_key=None)
    await state.set_state(ConfigStates.FILTER_MENU)


# --- Fallback Handlers (TRANSLATED) ---
@dp.message(ConfigStates.MAIN_MENU, ConfigStates.FILTER_MENU, ConfigStates.MANAGE_LIST_CHOICE,
            ConfigStates.ASK_REMOVE_ITEM_SELECT, ConfigStates.ASK_BOOL_VALUE)
async def msg_fsm_unknown(message: Message, state: FSMContext):
    current_state = await state.get_state()
    if message.text and message.text.startswith("/"):
        await message.reply(
            "Команды не обрабатываются в этом меню. Пожалуйста, используйте кнопки или отправьте запрошенную информацию. "
            "Используйте /cancel_config для выхода из режима настройки."
        )
    else:
        logger.debug(f"User {message.from_user.id} sent unexpected text '{message.text}' in state {current_state}")
        await message.reply(
            "Пожалуйста, используйте кнопки или отправьте конкретно запрошенную информацию. "
            "Используйте /cancel_config для выхода из режима настройки."
        )


# --- General Help Command (TRANSLATED) ---
@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer("Используйте /start или /config, чтобы управлять настройками вашего арбитражного бота с помощью интерактивных кнопок.")


# --- Main Bot Execution ---
async def main_aiogram():
    initialize_db()
    logger.info("Starting bot polling with aiogram...")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()

# --- Sender and filter ---
async def send_signal(message: dict):
    mod = message.get('mod')
    cex = message.get('CEX')
    _is_it_in_one_network = message.get('is_it_in_one_network')
    profit_percentage = message.get('profit_in_percentage')

    symbol_raw = message.get('symbol')
    if not symbol_raw:
        logger.warning(f"Signal message received without a 'symbol'. Skipping. Message: {message}")
        return
    symbol = symbol_raw.upper()

    vol_dex = message.get('volume_dex', 0)
    vol_cex = message.get('volume_cex', 0)
    min_volume = min(vol_dex, vol_cex)
    max_volume = max(vol_dex, vol_cex)

    conn = None
    tasks = []
    task_user_ids = []

    try:
        conn = get_db_connection()
        async with aiohttp.ClientSession() as session:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM user_settings;")

                for user_row in cur:
                    user_id = user_row[0]
                    user_api_key = user_row[1]
                    user_filter = user_row[2] or {}

                    if not user_api_key:
                        continue

                    if user_filter.get('in_one_network') and not _is_it_in_one_network:
                        continue

                    if symbol in user_filter.get('exception_token_pair', []):
                        continue

                    if profit_percentage is None or profit_percentage < user_filter.get('min_profit_percentage', 0):
                        continue

                    max_profit_req = user_filter.get('max_profit_percentage')
                    if max_profit_req is not None and profit_percentage is not None and profit_percentage > max_profit_req:
                        continue

                    min_vol_req = user_filter.get('min_volume')
                    if min_vol_req is not None and min_volume < min_vol_req:
                        continue

                    max_vol_req = user_filter.get('max_volume')
                    if max_vol_req is not None and max_volume > max_vol_req:
                        continue

                    exchange_settings = user_filter.get('exchange_settings', {}).get(cex)
                    if not exchange_settings or \
                       (mod == "DEX->CEX" and not exchange_settings.get('sell')) or \
                       (mod != "DEX->CEX" and not exchange_settings.get('buy')):
                        continue

                    tasks.append(__send_to_user(session=session, user_id=user_id, user_api=user_api_key, text=message.get('signal')))
                    task_user_ids.append(user_id)

                if tasks:
                    logger.info(f"Sending signal to {len(tasks)} users.")
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    for i, result in enumerate(results):
                        if isinstance(result, Exception):
                            failed_user_id = task_user_ids[i]
                            sent_message_logger.error(
                                f"Failed to send message to user {failed_user_id}. Exception: {result}")
                else:
                    logger.info(f"No users matched the filter criteria for signal: {symbol}")

    except Exception as e:
        logger.exception(f"An unexpected error occurred in send_signal: {e}")
    finally:
        if conn:
            conn.close()

async def __send_to_user(session: aiohttp.ClientSession, user_id: int, text: str, user_api: str):
    url = f"https://api.telegram.org/bot{user_api}/sendMessage"
    payload = {
        "chat_id": user_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "protect_content": True
    }
    try:
        async with session.post(url, json=payload) as resp:
            resp.raise_for_status()
            data = await resp.json()
            sent_message_logger.info(f"User {user_id} sent signal, data:\n '{data}'")
    except Exception as e:
        sent_message_logger.error(f"Failed to send message to {user_id}\n Exception: {e}")
        raise

def main_entry_pint_TGbot():
    if not BOT_TOKEN:
        print("Critical: TELEGRAM_BOT_TOKEN is not set. Exiting.")
    else:
        asyncio.run(main_aiogram())
