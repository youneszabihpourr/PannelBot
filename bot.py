import os
import time
import requests
import urllib3
import psycopg2
import psycopg2.extras
from datetime import datetime
from flask import Flask
from threading import Thread

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, ContextTypes, filters,
)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ===================================================================
# تنظیمات پایه — توکن‌ها رو حتماً توی Render → Environment ست کن
# ===================================================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DATABASE_URL = os.environ.get("DATABASE_URL")

MARZBAN_URL = "https://speedest.sbs:8000"
MARZBAN_API_KEY_MULTI = os.environ.get("MARZBAN_API_KEY_MULTI", "pg_key_7eccb866-3158-477a-a53a-d930dcd1ac4a")
MARZBAN_API_KEY_TUNNEL_FI = os.environ.get("MARZBAN_API_KEY_TUNNEL_FI", "pg_key_f0674003-addf-40b5-9167-4c2b713ade2f")
MARZBAN_API_KEY_TUNNEL_DE = os.environ.get("MARZBAN_API_KEY_TUNNEL_DE", "pg_key_370bca05-0fe5-4187-aa85-204638d11e0c")

PANEL_TYPES = ["multi", "tunnel_de", "tunnel_fi"]
PANELS = {
    "multi": {"url": MARZBAN_URL, "api_key": MARZBAN_API_KEY_MULTI},
    "tunnel_de": {"url": MARZBAN_URL, "api_key": MARZBAN_API_KEY_TUNNEL_DE},
    "tunnel_fi": {"url": MARZBAN_URL, "api_key": MARZBAN_API_KEY_TUNNEL_FI},
}
PANEL_DEFAULT_LABELS = {
    "multi": "🌐 پنل مولتی لوکیشن CDN + Direct",
    "tunnel_de": "🇩🇪 پنل تک‌لوکیشن Tunnel آلمان",
    "tunnel_fi": "🇫🇮 پنل تک‌لوکیشن Tunnel فنلاند",
}

ADMIN_ID = 8141379807
REQUIRED_CHANNEL = "BlueConnection"
BOT_TAG = "BluePannel"

RECEIPT_CHANNEL_ID = int(os.environ["RECEIPT_CHANNEL_ID"]) if os.environ.get("RECEIPT_CHANNEL_ID") else None

MIN_BALANCE_FIRST_PANEL = 500_000
USAGE_CHECK_INTERVAL_SECONDS = 300  # هر ۵ دقیقه مصرف پنل‌ها چک میشه
BYTES_100MB = 100 * 1024 * 1024
BYTES_GB = 1024 ** 3

_marzban_session = requests.Session()

(
    WAITING_CUSTOM_AMOUNT, WAITING_RECEIPT,
    WAITING_BROADCAST_ALL_MSG, WAITING_BROADCAST_TARGET_ID, WAITING_BROADCAST_TARGET_MSG,
    WAITING_GIFT_ALL_AMOUNT, WAITING_GIFT_ONE_ID, WAITING_GIFT_ONE_AMOUNT,
    WAITING_ADD_TARGET_ID, WAITING_ADD_AMOUNT,
    WAITING_DEDUCT_TARGET_ID, WAITING_DEDUCT_AMOUNT,
    WAITING_VIEW_USER_ID,
    WAITING_RATE_VALUE,
    WAITING_REG_USER_ID, WAITING_REG_USERNAME, WAITING_REG_PASSWORD,
    WAITING_RENAME_TEXT,
    WAITING_SUPPORT_VALUE,
) = range(19)

# ===================================================================
# دیتابیس (Supabase Postgres) — تنها منبع واقعی داده، با هر ری‌استارت پاک نمیشه
# ===================================================================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL تنظیم نشده! توی Render → Environment اضافه‌اش کن.")
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def init_db():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY, username TEXT, first_seen TIMESTAMP NOT NULL DEFAULT NOW());""")
        cur.execute("""CREATE TABLE IF NOT EXISTS wallets (
            user_id BIGINT PRIMARY KEY, balance BIGINT NOT NULL DEFAULT 0);""")
        cur.execute("""CREATE TABLE IF NOT EXISTS panel_rates (
            panel_type TEXT PRIMARY KEY, rate_per_gb BIGINT NOT NULL DEFAULT 0);""")
        cur.execute("""CREATE TABLE IF NOT EXISTS user_panels (
            id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, panel_type TEXT NOT NULL,
            panel_username TEXT NOT NULL, panel_password TEXT NOT NULL,
            registered_at TIMESTAMP NOT NULL DEFAULT NOW(),
            last_used_traffic_bytes BIGINT NOT NULL DEFAULT 0,
            pending_remainder_bytes BIGINT NOT NULL DEFAULT 0,
            UNIQUE(user_id, panel_type));""")
        cur.execute("""CREATE TABLE IF NOT EXISTS panel_requests (
            id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, panel_type TEXT NOT NULL,
            wallet_balance_at_request BIGINT NOT NULL, requested_at TIMESTAMP NOT NULL DEFAULT NOW());""")
        cur.execute("""CREATE TABLE IF NOT EXISTS button_labels (
            button_key TEXT PRIMARY KEY, label TEXT NOT NULL);""")
        cur.execute("""CREATE TABLE IF NOT EXISTS bot_settings (
            key TEXT PRIMARY KEY, value TEXT);""")
        for pt in PANEL_TYPES:
            cur.execute("INSERT INTO panel_rates (panel_type, rate_per_gb) VALUES (%s, 0) ON CONFLICT DO NOTHING;", (pt,))
        conn.commit()
        cur.close()
        print("✅ اتصال به Supabase برقرار شد و جدول‌ها آماده‌ان.")
    finally:
        conn.close()

def get_balance(user_id):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT balance FROM wallets WHERE user_id=%s;", (user_id,))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else 0
    finally:
        conn.close()

def update_balance(user_id, amount):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO wallets (user_id, balance) VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET balance = wallets.balance + EXCLUDED.balance
            RETURNING balance;""", (user_id, amount))
        new_bal = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return new_bal
    finally:
        conn.close()

def register_user(user_id, username):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO users (user_id, username) VALUES (%s, %s)
            ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username;""", (user_id, username))
        conn.commit()
        cur.close()
    finally:
        conn.close()

def get_user_count():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users;")
        n = cur.fetchone()[0]
        cur.close()
        return n
    finally:
        conn.close()

def get_all_user_ids():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users;")
        rows = [r[0] for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()

def get_user_full_info(user_id):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM users WHERE user_id=%s;", (user_id,))
        u = cur.fetchone()
        cur.close()
        return u
    finally:
        conn.close()

def get_panel_rate(panel_type):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT rate_per_gb FROM panel_rates WHERE panel_type=%s;", (panel_type,))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else 0
    finally:
        conn.close()

def set_panel_rate(panel_type, rate):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO panel_rates (panel_type, rate_per_gb) VALUES (%s, %s)
            ON CONFLICT (panel_type) DO UPDATE SET rate_per_gb = EXCLUDED.rate_per_gb;""", (panel_type, rate))
        conn.commit()
        cur.close()
    finally:
        conn.close()

def user_has_any_panel(user_id):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM user_panels WHERE user_id=%s LIMIT 1;", (user_id,))
        exists = cur.fetchone() is not None
        cur.close()
        return exists
    finally:
        conn.close()

def get_user_panels(user_id):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM user_panels WHERE user_id=%s ORDER BY registered_at DESC;", (user_id,))
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()

def get_user_panel_by_id(panel_id, user_id):
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM user_panels WHERE id=%s AND user_id=%s;", (panel_id, user_id))
        row = cur.fetchone()
        cur.close()
        return row
    finally:
        conn.close()

def get_all_user_panels():
    conn = get_conn()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM user_panels;")
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()

def add_user_panel(user_id, panel_type, username, password):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO user_panels (user_id, panel_type, panel_username, panel_password)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, panel_type) DO UPDATE SET
                panel_username = EXCLUDED.panel_username, panel_password = EXCLUDED.panel_password,
                last_used_traffic_bytes = 0, pending_remainder_bytes = 0;""",
            (user_id, panel_type, username, password))
        conn.commit()
        cur.close()
    finally:
        conn.close()

def update_panel_usage_tracking(panel_id, new_last_used, new_remainder):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""UPDATE user_panels SET last_used_traffic_bytes=%s, pending_remainder_bytes=%s
            WHERE id=%s;""", (new_last_used, new_remainder, panel_id))
        conn.commit()
        cur.close()
    finally:
        conn.close()

def log_panel_request(user_id, panel_type, wallet_balance):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO panel_requests (user_id, panel_type, wallet_balance_at_request)
            VALUES (%s, %s, %s);""", (user_id, panel_type, wallet_balance))
        conn.commit()
        cur.close()
    finally:
        conn.close()

_BUTTON_LABELS_CACHE = None

def _load_button_labels():
    global _BUTTON_LABELS_CACHE
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT button_key, label FROM button_labels;")
        _BUTTON_LABELS_CACHE = dict(cur.fetchall())
        cur.close()
    finally:
        conn.close()

def get_button_label(key, default_label):
    global _BUTTON_LABELS_CACHE
    if _BUTTON_LABELS_CACHE is None:
        _load_button_labels()
    return _BUTTON_LABELS_CACHE.get(key, default_label)

def set_button_label(key, label):
    global _BUTTON_LABELS_CACHE
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO button_labels (button_key, label) VALUES (%s, %s)
            ON CONFLICT (button_key) DO UPDATE SET label = EXCLUDED.label;""", (key, label))
        conn.commit()
        cur.close()
    finally:
        conn.close()
    _BUTTON_LABELS_CACHE = None

def get_setting(key, default=None):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM bot_settings WHERE key=%s;", (key,))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else default
    finally:
        conn.close()

def set_setting(key, value):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO bot_settings (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;""", (key, value))
        conn.commit()
        cur.close()
    finally:
        conn.close()

# ===================================================================
# ارتباط با پنل‌های مرزبان — فقط خواندن اطلاعات ادمین (مصرف/تعداد کاربر)
# چون یوزر/پسورد پنل نمایندگی رو خودت دستی روی پنل می‌سازی، بات فقط
# مصرفش رو با API Key سودو می‌خونه.
# ===================================================================
def get_marzban_admin_info(panel_key, admin_username):
    """اطلاعات ادمین (پنل نمایندگی) رو از مرزبان می‌گیره: users_usage (بایت
    مصرف‌شده‌ی کل کاربرای زیرمجموعه‌ش). اگه نسخه‌ی پنلت اسم فیلد رو فرق
    گذاشته بود، همینجا باید اصلاح بشه."""
    panel = PANELS[panel_key]
    headers = {'Authorization': f'ApiKey {panel["api_key"]}', 'Accept': 'application/json'}
    try:
        res = _marzban_session.get(f"{panel['url']}/api/admins", headers=headers, verify=False, timeout=10)
        if res.status_code != 200:
            print(f"MARZBAN ADMINS ERROR panel={panel_key}: {res.status_code} - {res.text}")
            return None
        data = res.json()
        admins = data if isinstance(data, list) else (data.get("admins") or data.get("items") or [])
        for a in admins:
            if a.get("username") == admin_username:
                return a
        return None
    except Exception as e:
        print(f"MARZBAN ADMINS EXCEPTION panel={panel_key}: {e}")
        return None

def get_marzban_admin_user_count(panel_key, admin_username):
    """تعداد کاربرهای ساخته‌شده زیر این ادمین رو best-effort می‌گیره."""
    panel = PANELS[panel_key]
    headers = {'Authorization': f'ApiKey {panel["api_key"]}', 'Accept': 'application/json'}
    try:
        res = _marzban_session.get(
            f"{panel['url']}/api/users", headers=headers, verify=False, timeout=10,
            params={"admin": admin_username, "limit": 1}
        )
        if res.status_code == 200:
            data = res.json()
            if isinstance(data, dict) and "total" in data:
                return data["total"]
    except Exception as e:
        print(f"MARZBAN USER COUNT EXCEPTION panel={panel_key}: {e}")
    return None

# ===================================================================
# فلاسک برای پایداری روی رندر (UptimeRobot به این آدرس پینگ می‌زنه)
# ===================================================================
app = Flask(__name__)

@app.route('/')
def home():
    return f"{BOT_TAG} Reseller Bot is running."

def run_flask():
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

# ===================================================================
# بررسی عضویت اجباری کانال
# ===================================================================
async def check_membership(user_id, context: ContextTypes.DEFAULT_TYPE):
    try:
        member = await context.bot.get_chat_member(chat_id=f"@{REQUIRED_CHANNEL}", user_id=user_id)
        return member.status in ['member', 'administrator', 'creator']
    except Exception:
        return False

def is_admin_user(user_id):
    return user_id == ADMIN_ID

# ===================================================================
# منوها
# ===================================================================
def main_menu_keyboard(is_admin=False):
    kb = [
        [InlineKeyboardButton(get_button_label("btn_request_panel", "🔷 درخواست پنل نمایندگی"), callback_data="req_panel_menu", style="primary")],
        [InlineKeyboardButton(get_button_label("btn_my_panels", "🟣 پنل‌های من"), callback_data="my_panels_menu", style="success")],
        [InlineKeyboardButton(get_button_label("btn_wallet", "💰 کیف پول + شارژ"), callback_data="wallet_menu", style="primary")],
        [InlineKeyboardButton(get_button_label("btn_support", "🎧 تماس با پشتیبانی"), callback_data="support_menu", style="success")],
    ]
    if is_admin:
        kb.append([InlineKeyboardButton("🛠 پنل مدیریت", callback_data="admin_panel", style="danger")])
    return InlineKeyboardMarkup(kb)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await check_membership(user.id, context):
        join_kb = [
            [InlineKeyboardButton("📢 عضويت در کانال", url=f"https://t.me/{REQUIRED_CHANNEL}", style="success")],
            [InlineKeyboardButton("🔄 عضو شدم، بررسی مجدد", callback_data="main_menu", style="primary")]
        ]
        text = "❌ برای استفاده از ربات باید ابتدا در کانال ما عضو شوید:"
        if update.message:
            await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(join_kb))
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(join_kb))
        return ConversationHandler.END

    register_user(user.id, user.username)
    is_admin = is_admin_user(user.id)
    welcome = f"سلام {user.first_name} عزیز 👋\n\nبه ربات نمایندگی **{BOT_TAG}** خوش آمدید.\nاز طریق منوی زیر ادامه بدید:"
    if update.message:
        await update.message.reply_text(welcome, parse_mode='Markdown', reply_markup=main_menu_keyboard(is_admin))
    elif update.callback_query:
        q = update.callback_query
        await q.answer()
        await q.edit_message_text(welcome, parse_mode='Markdown', reply_markup=main_menu_keyboard(is_admin))
    return ConversationHandler.END

# ===================================================================
# دکمه‌ها
# ===================================================================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id
    is_admin = is_admin_user(user_id)

    if data == "main_menu":
        return await start(update, context)

    # ---------------- درخواست پنل نمایندگی ----------------
    elif data == "req_panel_menu":
        kb = []
        styles = ["primary", "success", "primary"]
        for i, pt in enumerate(PANEL_TYPES):
            kb.append([InlineKeyboardButton(get_button_label(f"btn_req_{pt}", PANEL_DEFAULT_LABELS[pt]), callback_data=f"req_type_{pt}", style=styles[i])])
        kb.append([InlineKeyboardButton("🔙 بازگشت", callback_data="main_menu", style="danger")])
        await query.edit_message_text("🔷 **درخواست پنل نمایندگی**\n\nکدوم پنل رو می‌خواید درخواست بدید؟", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith("req_type_"):
        pt = data.replace("req_type_", "")
        rate = get_panel_rate(pt)
        balance = get_balance(user_id)
        has_panel = user_has_any_panel(user_id)
        label = get_button_label(f"btn_req_{pt}", PANEL_DEFAULT_LABELS[pt])
        if not has_panel and balance < MIN_BALANCE_FIRST_PANEL:
            text = (
                f"❌ **موجودی کافی نیست**\n\n"
                f"برای اولین درخواست پنل نمایندگی، کیف پول شما باید حداقل "
                f"`{MIN_BALANCE_FIRST_PANEL:,}` تومان موجودی داشته باشه.\n\n"
                f"💳 موجودی فعلی شما: `{balance:,}` تومان\n\n"
                "لطفاً ابتدا کیف پول خود را شارژ کنید."
            )
            kb = [
                [InlineKeyboardButton("💰 شارژ کیف پول", callback_data="wallet_menu", style="success")],
                [InlineKeyboardButton("🔙 بازگشت", callback_data="req_panel_menu", style="danger")]
            ]
            await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
            return ConversationHandler.END

        rate_text = f"`{rate:,}` تومان به ازای هر گیگابایت مصرف" if rate else "⚠️ نرخ این پنل هنوز تنظیم نشده، از پشتیبانی بپرسید"
        text = (
            f"📦 **{label}**\n\n"
            f"💹 نرخ مصرف: {rate_text}\n\n"
            "پرداخت این پنل به‌صورت «پرداخت به ازای مصرف» است: هر مقدار مصرف کنید، از کیف پولتان به‌طور خودکار کسر می‌شود.\n\n"
            "برای ثبت درخواست مطمئن هستید؟"
        )
        kb = [
            [InlineKeyboardButton("✅ بله، درخواست بده", callback_data=f"req_confirm_{pt}", style="success")],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="req_panel_menu", style="danger")]
        ]
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith("req_confirm_"):
        pt = data.replace("req_confirm_", "")
        balance = get_balance(user_id)
        log_panel_request(user_id, pt, balance)

        user = query.from_user
        username_str = f"@{user.username}" if user.username else "ندارد"
        label = get_button_label(f"btn_req_{pt}", PANEL_DEFAULT_LABELS[pt])
        admin_text = (
            f"📥 **درخواست پنل نمایندگی جدید!**\n\n"
            f"👤 نام: [{user.first_name}](tg://user?id={user.id})\n"
            f"🆔 یوزرنیم: {username_str}\n"
            f"🔢 آیدی عددی: `{user.id}`\n"
            f"💳 موجودی کیف پول: `{balance:,}` تومان\n"
            f"📦 پنل درخواستی: {label}"
        )
        admin_kb = [[InlineKeyboardButton("📝 ثبت پنل برای این کاربر", callback_data=f"quickreg_{user.id}_{pt}", style="success")]]
        try:
            await context.bot.send_message(chat_id=ADMIN_ID, text=admin_text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(admin_kb))
        except Exception as e:
            print(f"ADMIN NOTIFY ERROR: {e}")

        text = "✅ درخواست شما با موفقیت ثبت و برای ادمین ارسال شد.\nپس از تایید، اطلاعات پنل به‌صورت خودکار برایتان ارسال می‌شود."
        kb = [[InlineKeyboardButton("🔙 بازگشت به منوی اصلی", callback_data="main_menu", style="primary")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb))

    # ---------------- پنل‌های من ----------------
    elif data == "my_panels_menu":
        panels = get_user_panels(user_id)
        if not panels:
            text = "❌ شما هنوز هیچ پنلی ندارید."
            kb = [
                [InlineKeyboardButton("🔷 درخواست پنل نمایندگی", callback_data="req_panel_menu", style="success")],
                [InlineKeyboardButton("🔙 بازگشت", callback_data="main_menu", style="danger")]
            ]
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb))
            return ConversationHandler.END

        kb = []
        styles = ["primary", "success"]
        for i, p in enumerate(panels):
            label = PANEL_DEFAULT_LABELS.get(p["panel_type"], p["panel_type"])
            kb.append([InlineKeyboardButton(f"📦 {label}", callback_data=f"panel_detail_{p['id']}", style=styles[i % 2])])
        kb.append([InlineKeyboardButton("🔙 بازگشت", callback_data="main_menu", style="danger")])
        await query.edit_message_text("🟣 **پنل‌های من**\n\nبرای دیدن جزئیات هر پنل روش بزنید:", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith("panel_detail_"):
        panel_id = int(data.replace("panel_detail_", ""))
        p = get_user_panel_by_id(panel_id, user_id)
        if not p:
            await query.edit_message_text("❌ این پنل پیدا نشد.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 بازگشت", callback_data="my_panels_menu", style="danger")]]))
            return ConversationHandler.END

        await query.edit_message_text("⏳ در حال دریافت اطلاعات پنل...")
        pt = p["panel_type"]
        label = PANEL_DEFAULT_LABELS.get(pt, pt)
        rate = get_panel_rate(pt)
        balance = get_balance(user_id)
        info = get_marzban_admin_info(pt, p["panel_username"])
        used_bytes = info.get("users_usage", 0) if info else None
        used_gb = round(used_bytes / BYTES_GB, 2) if used_bytes is not None else None
        user_count = get_marzban_admin_user_count(pt, p["panel_username"])
        remaining_gb_est = round(balance / rate, 2) if rate else None
        reg_date = p["registered_at"].strftime("%Y-%m-%d")

        text = (
            f"📦 **{label}**\n\n"
            f"👤 یوزر پنل: `{p['panel_username']}`\n"
            f"🔑 پسورد پنل: `{p['panel_password']}`\n"
            f"🌐 آدرس پنل: `{MARZBAN_URL}`\n\n"
            f"📊 حجم مصرف‌شده: {f'{used_gb} گیگابایت' if used_gb is not None else '⚠️ در دسترس نیست'}\n"
            f"💰 حجم باقیمانده (بر اساس موجودی کیف پول): {f'~{remaining_gb_est} گیگابایت' if remaining_gb_est is not None else '⚠️ نرخ تنظیم نشده'}\n"
            f"💹 نرخ هر گیگابایت: `{rate:,}` تومان\n"
            f"👥 تعداد کاربران ساخته‌شده: {user_count if user_count is not None else '⚠️ در دسترس نیست'}\n"
            f"🗓 تاریخ ثبت: {reg_date}"
        )
        kb = [[InlineKeyboardButton("🔙 بازگشت به لیست پنل‌ها", callback_data="my_panels_menu", style="danger")]]
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    # ---------------- کیف پول ----------------
    elif data == "wallet_menu":
        balance = get_balance(user_id)
        text = f"💰 **کیف پول شما**\n\nموجودی فعلی: `{balance:,}` تومان"
        kb = [
            [InlineKeyboardButton(get_button_label("btn_topup", "➕ افزایش موجودی کیف پول"), callback_data="wallet_topup", style="primary")],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="main_menu", style="danger")]
        ]
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    elif data == "wallet_topup":
        kb = [
            [InlineKeyboardButton(get_button_label("btn_card", "💳 کارت به کارت"), callback_data="topup_card", style="primary")],
            [InlineKeyboardButton("🔙 بازگشت", callback_data="wallet_menu", style="danger")]
        ]
        await query.edit_message_text("لطفاً روش افزایش موجودی را انتخاب کنید:", reply_markup=InlineKeyboardMarkup(kb))

    elif data == "topup_card":
        text = "💳 لطفاً مبلغ مورد نظر برای شارژ کیف پول (به تومان) را وارد کنید:\n\n(مثلاً: `50000`)"
        kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
        return WAITING_CUSTOM_AMOUNT

    # ---------------- پشتیبانی ----------------
    elif data == "support_menu":
        support_val = get_setting("support_contact", str(ADMIN_ID))
        if support_val.lstrip("-").isdigit():
            contact_text = f"[ادمین پشتیبانی](tg://user?id={support_val})"
        else:
            contact_text = f"@{support_val.lstrip('@')}"
        text = f"🎧 **پشتیبانی**\n\nبرای ارتباط با پشتیبانی به آیدی زیر پیام دهید:\n\n{contact_text}"
        kb = [[InlineKeyboardButton("🔙 بازگشت", callback_data="main_menu", style="danger")]]
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    # ---------------- پنل مدیریت ----------------
    elif data == "admin_panel":
        if not is_admin:
            await query.answer("⛔️ دسترسی ندارید.", show_alert=True)
            return ConversationHandler.END
        kb = [
            [InlineKeyboardButton("👤 مشاهده اطلاعات کاربر", callback_data="admin_view_user", style="primary")],
            [InlineKeyboardButton("📊 تعداد کاربران", callback_data="admin_user_count", style="success")],
            [InlineKeyboardButton("📢 پیام همگانی", callback_data="admin_broadcast_all", style="primary")],
            [InlineKeyboardButton("✉️ پیام به یک کاربر", callback_data="admin_broadcast_one", style="success")],
            [InlineKeyboardButton("🎁 هدیه همگانی", callback_data="admin_gift_all", style="primary")],
            [InlineKeyboardButton("🎁 هدیه به یک کاربر", callback_data="admin_gift_one", style="success")],
            [InlineKeyboardButton("➕ افزایش موجودی یک کاربر", callback_data="admin_add_balance", style="primary")],
            [InlineKeyboardButton("➖ کاهش موجودی یک کاربر", callback_data="admin_deduct_balance", style="success")],
            [InlineKeyboardButton("🗂 ثبت پنل برای کاربر", callback_data="admin_register_panel", style="primary")],
            [InlineKeyboardButton("💹 تنظیم نرخ پنل‌ها", callback_data="admin_set_rates", style="success")],
            [InlineKeyboardButton("✏️ تغییر اسم دکمه‌ها", callback_data="admin_rename_buttons", style="primary")],
            [InlineKeyboardButton("🎧 تنظیم پشتیبانی", callback_data="admin_set_support", style="success")],
            [InlineKeyboardButton("🔙 بازگشت به منوی اصلی", callback_data="main_menu", style="danger")],
        ]
        await query.edit_message_text("🛠 **پنل مدیریت**", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    elif data == "admin_view_user":
        if not is_admin: return ConversationHandler.END
        text = "🔢 آیدی عددی کاربر مورد نظر را وارد کنید:"
        kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb))
        return WAITING_VIEW_USER_ID

    elif data == "admin_user_count":
        if not is_admin: return ConversationHandler.END
        n = get_user_count()
        text = f"📊 تعداد کل کاربران ربات: `{n}`"
        kb = [[InlineKeyboardButton("🔙 بازگشت", callback_data="admin_panel", style="danger")]]
        await query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))

    elif data == "admin_broadcast_all":
        if not is_admin: return ConversationHandler.END
        kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
        await query.edit_message_text("📢 متن پیام همگانی را بنویسید:", reply_markup=InlineKeyboardMarkup(kb))
        return WAITING_BROADCAST_ALL_MSG

    elif data == "admin_broadcast_one":
        if not is_admin: return ConversationHandler.END
        kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
        await query.edit_message_text("🔢 آیدی عددی کاربر مقصد را وارد کنید:", reply_markup=InlineKeyboardMarkup(kb))
        return WAITING_BROADCAST_TARGET_ID

    elif data == "admin_gift_all":
        if not is_admin: return ConversationHandler.END
        kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
        await query.edit_message_text("🎁 مبلغ هدیه برای همه‌ی کاربران را وارد کنید (تومان):", reply_markup=InlineKeyboardMarkup(kb))
        return WAITING_GIFT_ALL_AMOUNT

    elif data == "admin_gift_one":
        if not is_admin: return ConversationHandler.END
        kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
        await query.edit_message_text("🔢 آیدی عددی کاربر مورد نظر را وارد کنید:", reply_markup=InlineKeyboardMarkup(kb))
        return WAITING_GIFT_ONE_ID

    elif data == "admin_add_balance":
        if not is_admin: return ConversationHandler.END
        kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
        await query.edit_message_text("🔢 آیدی عددی کاربر برای افزایش موجودی را وارد کنید:", reply_markup=InlineKeyboardMarkup(kb))
        return WAITING_ADD_TARGET_ID

    elif data == "admin_deduct_balance":
        if not is_admin: return ConversationHandler.END
        kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
        await query.edit_message_text("🔢 آیدی عددی کاربر برای کاهش موجودی را وارد کنید:", reply_markup=InlineKeyboardMarkup(kb))
        return WAITING_DEDUCT_TARGET_ID

    elif data == "admin_register_panel" or data.startswith("quickreg_"):
        if not is_admin: return ConversationHandler.END
        if data.startswith("quickreg_"):
            _, uid, pt = data.split("_")
            context.user_data['reg_user_id'] = int(uid)
            context.user_data['reg_panel_type'] = pt
            kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
            await query.edit_message_text(f"👤 یوزرنیم پنل برای کاربر `{uid}` (نوع: {PANEL_DEFAULT_LABELS.get(pt, pt)}) را وارد کنید:", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
            return WAITING_REG_USERNAME
        kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
        await query.edit_message_text("🔢 آیدی عددی کاربری که می‌خواهید پنل براش ثبت کنید را وارد کنید:", reply_markup=InlineKeyboardMarkup(kb))
        return WAITING_REG_USER_ID

    elif data == "admin_set_rates":
        if not is_admin: return ConversationHandler.END
        kb = []
        for pt in PANEL_TYPES:
            rate = get_panel_rate(pt)
            kb.append([InlineKeyboardButton(f"{PANEL_DEFAULT_LABELS[pt]} — {rate:,} ت", callback_data=f"setrate_{pt}", style="primary")])
        kb.append([InlineKeyboardButton("🔙 بازگشت", callback_data="admin_panel", style="danger")])
        await query.edit_message_text("💹 نرخ کدوم پنل رو می‌خواید تغییر بدید؟", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith("setrate_"):
        if not is_admin: return ConversationHandler.END
        pt = data.replace("setrate_", "")
        context.user_data['rate_panel_type'] = pt
        kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
        await query.edit_message_text(f"💹 نرخ جدید هر گیگابایت برای «{PANEL_DEFAULT_LABELS[pt]}» را به تومان وارد کنید:", reply_markup=InlineKeyboardMarkup(kb))
        return WAITING_RATE_VALUE

    elif data == "admin_rename_buttons":
        if not is_admin: return ConversationHandler.END
        keys = [
            ("btn_request_panel", "🔷 درخواست پنل نمایندگی"), ("btn_my_panels", "🟣 پنل‌های من"),
            ("btn_wallet", "💰 کیف پول + شارژ"), ("btn_support", "🎧 تماس با پشتیبانی"),
            ("btn_topup", "➕ افزایش موجودی کیف پول"), ("btn_card", "💳 کارت به کارت"),
            ("btn_req_multi", PANEL_DEFAULT_LABELS["multi"]), ("btn_req_tunnel_de", PANEL_DEFAULT_LABELS["tunnel_de"]),
            ("btn_req_tunnel_fi", PANEL_DEFAULT_LABELS["tunnel_fi"]),
        ]
        kb = []
        styles = ["primary", "success"]
        for i, (k, d) in enumerate(keys):
            current = get_button_label(k, d)
            kb.append([InlineKeyboardButton(current, callback_data=f"rename_{k}", style=styles[i % 2])])
        kb.append([InlineKeyboardButton("🔙 بازگشت", callback_data="admin_panel", style="danger")])
        await query.edit_message_text("✏️ روی دکمه‌ای که می‌خواید اسمش رو عوض کنید بزنید:", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith("rename_"):
        if not is_admin: return ConversationHandler.END
        key = data.replace("rename_", "")
        context.user_data['rename_key'] = key
        kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
        await query.edit_message_text("✏️ اسم جدید این دکمه را وارد کنید:", reply_markup=InlineKeyboardMarkup(kb))
        return WAITING_RENAME_TEXT

    elif data == "admin_set_support":
        if not is_admin: return ConversationHandler.END
        kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
        await query.edit_message_text("🎧 آیدی عددی یا یوزرنیم (@) ادمین پشتیبانی جدید را وارد کنید:", reply_markup=InlineKeyboardMarkup(kb))
        return WAITING_SUPPORT_VALUE

    elif data.startswith("accept_topup_") or data.startswith("reject_topup_"):
        return await admin_topup_callback(update, context)

    return ConversationHandler.END

# ===================================================================
# مراحل متنی مکالمه
# ===================================================================
async def handle_custom_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
    if not text.isdigit() or int(text) < 1000:
        await update.message.reply_text("❌ لطفاً یک عدد معتبر (حداقل ۱۰۰۰ تومان) وارد کنید:", reply_markup=InlineKeyboardMarkup(kb))
        return WAITING_CUSTOM_AMOUNT
    amount = int(text)
    context.user_data['pending_topup_amount'] = amount
    card_info = (
        f"💳 **اطلاعات کارت جهت واریز مبلغ {amount:,} تومان:**\n\n"
        "<code>6219861471267970</code>\n"
        "به نام: **ذبیح پور**\n\n"
        "👇 پس از واریز وجه، **رسید بانکی (عکس فیش)** را همینجا ارسال کنید."
    )
    await update.message.reply_text(card_info, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))
    return WAITING_RECEIPT

async def handle_receipt_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    amount = context.user_data.get('pending_topup_amount')
    if not amount:
        await update.message.reply_text("❌ خطا در فرآیند شارژ. لطفاً دوباره از منو اقدام کنید.")
        return ConversationHandler.END

    photo = update.message.photo[-1]
    username_str = f"@{user.username}" if user.username else "ندارد"
    caption = (
        f"📥 **درخواست شارژ کیف پول جدید!**\n\n"
        f"👤 کاربر: [{user.first_name}](tg://user?id={user.id})\n"
        f"🆔 یوزرنیم: {username_str}\n"
        f"🔢 آیدی عددی: `{user.id}`\n"
        f"💰 مبلغ درخواستی: `{amount:,}` تومان\n\n"
        f"⚠️ این رسید از سمت ربات {BOT_TAG} ارسال شده است."
    )
    kb = [[
        InlineKeyboardButton("✅ تایید و شارژ", callback_data=f"accept_topup_{user.id}_{amount}", style="success"),
        InlineKeyboardButton("❌ رد درخواست", callback_data=f"reject_topup_{user.id}", style="danger")
    ]]
    target = RECEIPT_CHANNEL_ID if RECEIPT_CHANNEL_ID else ADMIN_ID
    try:
        await context.bot.send_photo(chat_id=target, photo=photo.file_id, caption=caption, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(kb))
        await update.message.reply_text("✅ رسید شما با موفقیت ارسال شد. پس از بررسی، حساب شما شارژ خواهد شد.")
    except Exception as e:
        await update.message.reply_text("❌ خطا در ارسال رسید. لطفاً به پشتیبانی پیام دهید.")
        print(f"SEND RECEIPT ERROR: {e}")
    context.user_data.pop('pending_topup_amount', None)
    return ConversationHandler.END

async def admin_topup_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query.from_user.id != ADMIN_ID:
        await query.answer("⛔️ فقط ادمین.", show_alert=True)
        return ConversationHandler.END
    parts = query.data.split("_")
    action, target_user_id = parts[0], int(parts[2])
    if action == "accept":
        amount = int(parts[3])
        new_bal = update_balance(target_user_id, amount)
        try:
            cap = query.message.caption or ""
            await query.edit_message_caption(caption=cap + "\n\n✅ **تایید و شارژ شد**", parse_mode='Markdown')
        except Exception:
            pass
        try:
            await context.bot.send_message(chat_id=target_user_id, text=f"🎉 کیف پول شما به مبلغ `{amount:,}` تومان شارژ شد!\n💳 موجودی جدید: `{new_bal:,}` تومان", parse_mode='Markdown')
        except Exception:
            pass
    else:
        try:
            cap = query.message.caption or ""
            await query.edit_message_caption(caption=cap + "\n\n❌ **رد شد**", parse_mode='Markdown')
        except Exception:
            pass
        try:
            await context.bot.send_message(chat_id=target_user_id, text="❌ درخواست شارژ کیف پول شما رد شد.")
        except Exception:
            pass
    return ConversationHandler.END

async def handle_view_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ فقط آیدی عددی معتبر وارد کنید:")
        return WAITING_VIEW_USER_ID
    uid = int(text)
    u = get_user_full_info(uid)
    if not u:
        await update.message.reply_text("❌ این کاربر در دیتابیس ربات پیدا نشد.")
        return ConversationHandler.END
    balance = get_balance(uid)
    panels = get_user_panels(uid)
    panels_text = "\n".join([f"  • {PANEL_DEFAULT_LABELS.get(p['panel_type'], p['panel_type'])} (ثبت: {p['registered_at'].strftime('%Y-%m-%d')})" for p in panels]) or "  هیچ پنلی ندارد"
    text = (
        f"👤 **اطلاعات کاربر**\n\n"
        f"🆔 آیدی: `{uid}`\n"
        f"یوزرنیم: @{u['username'] if u['username'] else 'ندارد'}\n"
        f"🗓 اولین بازدید: {u['first_seen'].strftime('%Y-%m-%d %H:%M')}\n"
        f"💳 موجودی: `{balance:,}` تومان\n\n"
        f"📦 پنل‌ها:\n{panels_text}"
    )
    await update.message.reply_text(text, parse_mode='Markdown')
    return ConversationHandler.END

async def handle_broadcast_all_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message.text
    ids = get_all_user_ids()
    status = await update.message.reply_text(f"⏳ در حال ارسال به {len(ids)} کاربر...")
    ok, fail = 0, 0
    for uid in ids:
        try:
            await context.bot.send_message(chat_id=uid, text=msg)
            ok += 1
        except Exception:
            fail += 1
    await status.edit_text(f"📢 تمام شد.\n✅ موفق: {ok}\n❌ ناموفق: {fail}")
    return ConversationHandler.END

async def handle_broadcast_target_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ فقط آیدی عددی معتبر وارد کنید:")
        return WAITING_BROADCAST_TARGET_ID
    context.user_data['bc_target'] = int(text)
    await update.message.reply_text("✉️ متن پیام را بنویسید:")
    return WAITING_BROADCAST_TARGET_MSG

async def handle_broadcast_target_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target = context.user_data.get('bc_target')
    try:
        await context.bot.send_message(chat_id=target, text=update.message.text)
        await update.message.reply_text(f"✅ پیام برای `{target}` ارسال شد.", parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text(f"❌ ارسال ناموفق بود: {e}")
    context.user_data.pop('bc_target', None)
    return ConversationHandler.END

async def handle_gift_all_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ فقط عدد وارد کنید:")
        return WAITING_GIFT_ALL_AMOUNT
    amount = int(text)
    ids = get_all_user_ids()
    status = await update.message.reply_text(f"⏳ در حال هدیه دادن به {len(ids)} کاربر...")
    for uid in ids:
        update_balance(uid, amount)
        try:
            await context.bot.send_message(chat_id=uid, text=f"🎁 مبلغ `{amount:,}` تومان به کیف پول شما هدیه داده شد!", parse_mode='Markdown')
        except Exception:
            pass
    await status.edit_text(f"🎁 مبلغ `{amount:,}` تومان به {len(ids)} کاربر هدیه داده شد.", parse_mode='Markdown')
    return ConversationHandler.END

async def handle_gift_one_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ فقط آیدی عددی معتبر وارد کنید:")
        return WAITING_GIFT_ONE_ID
    context.user_data['gift_target'] = int(text)
    await update.message.reply_text("🎁 مبلغ هدیه را وارد کنید (تومان):")
    return WAITING_GIFT_ONE_AMOUNT

async def handle_gift_one_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ فقط عدد وارد کنید:")
        return WAITING_GIFT_ONE_AMOUNT
    amount = int(text)
    target = context.user_data.get('gift_target')
    new_bal = update_balance(target, amount)
    await update.message.reply_text(f"✅ مبلغ `{amount:,}` تومان به کاربر `{target}` هدیه داده شد. موجودی جدید: `{new_bal:,}`", parse_mode='Markdown')
    try:
        await context.bot.send_message(chat_id=target, text=f"🎁 مبلغ `{amount:,}` تومان به کیف پول شما هدیه داده شد!", parse_mode='Markdown')
    except Exception:
        pass
    context.user_data.pop('gift_target', None)
    return ConversationHandler.END

async def handle_add_target_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ فقط آیدی عددی معتبر وارد کنید:")
        return WAITING_ADD_TARGET_ID
    context.user_data['add_target'] = int(text)
    await update.message.reply_text("➕ مبلغ افزایش موجودی را وارد کنید (تومان):")
    return WAITING_ADD_AMOUNT

async def handle_add_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ فقط عدد وارد کنید:")
        return WAITING_ADD_AMOUNT
    amount = int(text)
    target = context.user_data.get('add_target')
    new_bal = update_balance(target, amount)
    await update.message.reply_text(f"✅ موجودی کاربر `{target}` به اندازه‌ی `{amount:,}` تومان افزایش یافت. موجودی جدید: `{new_bal:,}`", parse_mode='Markdown')
    try:
        await context.bot.send_message(chat_id=target, text=f"✅ کیف پول شما به مبلغ `{amount:,}` تومان توسط ادمین شارژ شد!", parse_mode='Markdown')
    except Exception:
        pass
    context.user_data.pop('add_target', None)
    return ConversationHandler.END

async def handle_deduct_target_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ فقط آیدی عددی معتبر وارد کنید:")
        return WAITING_DEDUCT_TARGET_ID
    context.user_data['deduct_target'] = int(text)
    await update.message.reply_text("➖ مبلغ کاهش موجودی را وارد کنید (تومان):")
    return WAITING_DEDUCT_AMOUNT

async def handle_deduct_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ فقط عدد وارد کنید:")
        return WAITING_DEDUCT_AMOUNT
    amount = int(text)
    target = context.user_data.get('deduct_target')
    new_bal = update_balance(target, -amount)
    await update.message.reply_text(f"✅ موجودی کاربر `{target}` به اندازه‌ی `{amount:,}` تومان کاهش یافت. موجودی جدید: `{new_bal:,}`", parse_mode='Markdown')
    try:
        await context.bot.send_message(chat_id=target, text=f"ℹ️ مبلغ `{amount:,}` تومان از کیف پول شما کسر شد.", parse_mode='Markdown')
    except Exception:
        pass
    context.user_data.pop('deduct_target', None)
    return ConversationHandler.END

async def handle_reg_user_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ فقط آیدی عددی معتبر وارد کنید:")
        return WAITING_REG_USER_ID
    context.user_data['reg_user_id'] = int(text)
    kb = [
        [InlineKeyboardButton(PANEL_DEFAULT_LABELS["multi"], callback_data="regtype_multi", style="primary")],
        [InlineKeyboardButton(PANEL_DEFAULT_LABELS["tunnel_de"], callback_data="regtype_tunnel_de", style="success")],
        [InlineKeyboardButton(PANEL_DEFAULT_LABELS["tunnel_fi"], callback_data="regtype_tunnel_fi", style="primary")],
    ]
    await update.message.reply_text("نوع پنل را انتخاب کنید:", reply_markup=InlineKeyboardMarkup(kb))
    return WAITING_REG_USER_ID  # همچنان همین state می‌مونه؛ انتخاب پنل با callback زیر مدیریت میشه

async def handle_reg_panel_type_choice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    pt = query.data.replace("regtype_", "")
    context.user_data['reg_panel_type'] = pt
    kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
    await query.edit_message_text(f"👤 یوزرنیم پنل ({PANEL_DEFAULT_LABELS[pt]}) را وارد کنید:", reply_markup=InlineKeyboardMarkup(kb))
    return WAITING_REG_USERNAME

async def handle_reg_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['reg_username'] = update.message.text.strip()
    kb = [[InlineKeyboardButton("❌ لغو عملیات", callback_data="cancel_conv", style="danger")]]
    await update.message.reply_text("🔑 پسورد پنل را وارد کنید:", reply_markup=InlineKeyboardMarkup(kb))
    return WAITING_REG_PASSWORD

async def handle_reg_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    password = update.message.text.strip()
    uid = context.user_data.get('reg_user_id')
    pt = context.user_data.get('reg_panel_type')
    username = context.user_data.get('reg_username')

    add_user_panel(uid, pt, username, password)
    rate = get_panel_rate(pt)
    label = PANEL_DEFAULT_LABELS[pt]

    await update.message.reply_text(f"✅ پنل «{label}» برای کاربر `{uid}` ثبت شد.", parse_mode='Markdown')
    try:
        user_text = (
            f"🎉 **پنل نمایندگی شما فعال شد!**\n\n"
            f"📦 نوع پنل: {label}\n"
            f"🌐 آدرس پنل: `{MARZBAN_URL}`\n"
            f"👤 یوزرنیم: `{username}`\n"
            f"🔑 پسورد: `{password}`\n"
            f"💹 نرخ هر گیگابایت مصرف: `{rate:,}` تومان\n\n"
            "⚠️ این پنل به‌صورت «پرداخت به ازای مصرف» است: به ازای هر ۱۰۰ مگابایت مصرف، مبلغ متناسب از کیف پول شما به‌صورت خودکار کسر می‌شود. لطفاً موجودی کیف پول خود را شارژ نگه دارید تا سرویستان قطع نشود."
        )
        await context.bot.send_message(chat_id=uid, text=user_text, parse_mode='Markdown')
    except Exception as e:
        print(f"NOTIFY USER PANEL REG ERROR: {e}")

    for k in ('reg_user_id', 'reg_panel_type', 'reg_username'):
        context.user_data.pop(k, None)
    return ConversationHandler.END

async def handle_rate_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("❌ فقط عدد وارد کنید:")
        return WAITING_RATE_VALUE
    pt = context.user_data.get('rate_panel_type')
    set_panel_rate(pt, int(text))
    await update.message.reply_text(f"✅ نرخ «{PANEL_DEFAULT_LABELS[pt]}» به `{int(text):,}` تومان به ازای هر گیگابایت تنظیم شد.", parse_mode='Markdown')
    context.user_data.pop('rate_panel_type', None)
    return ConversationHandler.END

async def handle_rename_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_label = update.message.text.strip()
    key = context.user_data.get('rename_key')
    if key:
        set_button_label(key, new_label)
        await update.message.reply_text(f"✅ اسم دکمه به «{new_label}» تغییر کرد.")
    context.user_data.pop('rename_key', None)
    return ConversationHandler.END

async def handle_support_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    val = update.message.text.strip()
    set_setting("support_contact", val)
    await update.message.reply_text(f"✅ پشتیبانی روی `{val}` تنظیم شد.", parse_mode='Markdown')
    return ConversationHandler.END

async def cancel_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data.clear()
    is_admin = is_admin_user(query.from_user.id)
    await query.edit_message_text("❌ عملیات لغو شد.\n\nاز طریق منوی زیر می‌توانید ادامه دهید:", reply_markup=main_menu_keyboard(is_admin))
    return ConversationHandler.END

# ===================================================================
# جاب دوره‌ای: چک مصرف پنل‌ها و کسر خودکار کیف پول
# ===================================================================
async def check_usage_job(context: ContextTypes.DEFAULT_TYPE):
    try:
        panels = get_all_user_panels()
    except Exception as e:
        print(f"USAGE JOB DB ERROR: {e}")
        return

    for p in panels:
        try:
            info = get_marzban_admin_info(p["panel_type"], p["panel_username"])
            if not info:
                continue
            current_usage = info.get("users_usage", 0) or 0
            last_used = p["last_used_traffic_bytes"] or 0
            remainder = p["pending_remainder_bytes"] or 0

            delta = current_usage - last_used
            if delta < 0:
                # احتمالاً مصرف روی پنل ریست شده — نقطه‌ی صفر جدید رو ذخیره کن
                update_panel_usage_tracking(p["id"], current_usage, 0)
                continue
            if delta == 0:
                continue

            total = remainder + delta
            units = total // BYTES_100MB
            new_remainder = total % BYTES_100MB

            if units > 0:
                rate = get_panel_rate(p["panel_type"])
                deduct = int(units * (rate * 0.1))
                if deduct > 0:
                    update_balance(p["user_id"], -deduct)
                    try:
                        await context.bot.send_message(
                            chat_id=p["user_id"],
                            text=f"ℹ️ به‌خاطر مصرف پنل «{PANEL_DEFAULT_LABELS.get(p['panel_type'], p['panel_type'])}»، مبلغ `{deduct:,}` تومان از کیف پول شما کسر شد.",
                            parse_mode='Markdown'
                        )
                    except Exception:
                        pass

            update_panel_usage_tracking(p["id"], current_usage, new_remainder)
        except Exception as e:
            print(f"USAGE JOB PANEL ERROR (id={p.get('id')}): {e}")

# ===================================================================
# main
# ===================================================================
def main():
    init_db()
    t = Thread(target=run_flask)
    t.daemon = True
    t.start()

    application = ApplicationBuilder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(button_handler, pattern="^topup_card$"),
            CallbackQueryHandler(button_handler, pattern="^admin_view_user$"),
            CallbackQueryHandler(button_handler, pattern="^admin_broadcast_all$"),
            CallbackQueryHandler(button_handler, pattern="^admin_broadcast_one$"),
            CallbackQueryHandler(button_handler, pattern="^admin_gift_all$"),
            CallbackQueryHandler(button_handler, pattern="^admin_gift_one$"),
            CallbackQueryHandler(button_handler, pattern="^admin_add_balance$"),
            CallbackQueryHandler(button_handler, pattern="^admin_deduct_balance$"),
            CallbackQueryHandler(button_handler, pattern="^(admin_register_panel|quickreg_)"),
            CallbackQueryHandler(button_handler, pattern="^setrate_"),
            CallbackQueryHandler(button_handler, pattern="^rename_"),
            CallbackQueryHandler(button_handler, pattern="^admin_set_support$"),
        ],
        states={
            WAITING_CUSTOM_AMOUNT: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_amount)],
            WAITING_RECEIPT: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.PHOTO & ~filters.COMMAND, handle_receipt_photo)],
            WAITING_BROADCAST_ALL_MSG: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_broadcast_all_msg)],
            WAITING_BROADCAST_TARGET_ID: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_broadcast_target_id)],
            WAITING_BROADCAST_TARGET_MSG: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_broadcast_target_msg)],
            WAITING_GIFT_ALL_AMOUNT: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_gift_all_amount)],
            WAITING_GIFT_ONE_ID: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_gift_one_id)],
            WAITING_GIFT_ONE_AMOUNT: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_gift_one_amount)],
            WAITING_ADD_TARGET_ID: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_target_id)],
            WAITING_ADD_AMOUNT: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add_amount)],
            WAITING_DEDUCT_TARGET_ID: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_deduct_target_id)],
            WAITING_DEDUCT_AMOUNT: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_deduct_amount)],
            WAITING_VIEW_USER_ID: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_view_user_id)],
            WAITING_RATE_VALUE: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_rate_value)],
            WAITING_REG_USER_ID: [
                CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"),
                CallbackQueryHandler(handle_reg_panel_type_choice, pattern="^regtype_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_reg_user_id)
            ],
            WAITING_REG_USERNAME: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_reg_username)],
            WAITING_REG_PASSWORD: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_reg_password)],
            WAITING_RENAME_TEXT: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_rename_text)],
            WAITING_SUPPORT_VALUE: [CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_support_value)],
        },
        fallbacks=[
            CommandHandler("start", start),
            CallbackQueryHandler(cancel_operation, pattern="^cancel_conv$"),
            CallbackQueryHandler(button_handler),
        ]
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(conv_handler)
    application.add_handler(CallbackQueryHandler(button_handler))

    if application.job_queue:
        application.job_queue.run_repeating(check_usage_job, interval=USAGE_CHECK_INTERVAL_SECONDS, first=15)
    else:
        print("⚠️ JobQueue فعال نیست — requirements.txt باید python-telegram-bot[job-queue] باشه، وگرنه کسر خودکار کار نمی‌کنه.")

    application.run_polling()

if __name__ == "__main__":
    main()
