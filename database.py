import logging
import sqlite3
from datetime import datetime


logger = logging.getLogger(__name__)


class DatabaseManager:
    DB_NAME = "neo_link_bot.db"

    @staticmethod
    def init():
        conn = sqlite3.connect(DatabaseManager.DB_NAME)
        c = conn.cursor()

        c.execute(
            """CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                balance REAL DEFAULT 0,
                ref_level_1 INTEGER DEFAULT 0,
                ref_level_2 INTEGER DEFAULT 0,
                earnings_ref REAL DEFAULT 0,
                earnings_tasks REAL DEFAULT 0,
                total_earnings REAL DEFAULT 0,
                joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                referrer_id INTEGER,
                is_banned INTEGER DEFAULT 0,
                last_notified TIMESTAMP
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT,
                link TEXT,
                name TEXT,
                reward REAL DEFAULT 0.3,
                is_request INTEGER DEFAULT 0,
                created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS required_channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT,
                link TEXT,
                name TEXT,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS completed_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                task_id TEXT,
                task_type TEXT,
                completed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reward REAL,
                UNIQUE(user_id, task_id)
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount REAL,
                card TEXT,
                name TEXT,
                status TEXT DEFAULT 'pending',
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                new_users INTEGER DEFAULT 0,
                total_users INTEGER DEFAULT 0,
                subgram_earnings REAL DEFAULT 0,
                flyer_earnings REAL DEFAULT 0
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS subgram_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                subscriptions INTEGER DEFAULT 0,
                earnings REAL DEFAULT 0
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS traffic_users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                balance REAL DEFAULT 0,
                deposit_balance REAL DEFAULT 0,
                earned_balance REAL DEFAULT 0,
                total_spent REAL DEFAULT 0,
                total_earned REAL DEFAULT 0,
                total_withdrawn REAL DEFAULT 0,
                referrer_id INTEGER,
                ref_count INTEGER DEFAULT 0,
                last_cryptobot_user_id INTEGER,
                joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                link TEXT,
                order_type TEXT DEFAULT 'channel',
                amount INTEGER,
                done INTEGER DEFAULT 0,
                price REAL,
                status TEXT DEFAULT 'active',
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                channel_id TEXT,
                invite_link TEXT
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                amount REAL,
                type TEXT,
                description TEXT,
                date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                invoice_id TEXT
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                invoice_id TEXT UNIQUE,
                amount REAL,
                status TEXT DEFAULT 'active',
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS traffic_withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                rub_amount REAL NOT NULL,
                asset TEXT NOT NULL,
                asset_amount REAL NOT NULL,
                cryptobot_user_id INTEGER NOT NULL,
                spend_id TEXT NOT NULL UNIQUE,
                transfer_id TEXT,
                status TEXT DEFAULT 'processing',
                error TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS traffic_bots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_user_id INTEGER NOT NULL,
                bot_id INTEGER,
                bot_username TEXT,
                bot_name TEXT,
                bot_token TEXT NOT NULL,
                api_key TEXT NOT NULL UNIQUE,
                theme TEXT DEFAULT 'Другое',
                status TEXT DEFAULT 'pending',
                is_enabled INTEGER DEFAULT 1,
                rejection_reason TEXT,
                display_bots INTEGER DEFAULT 1,
                display_resources INTEGER DEFAULT 1,
                max_sponsors INTEGER DEFAULT 1,
                reset_hours INTEGER DEFAULT 3,
                new_sponsors_after_hours INTEGER DEFAULT 3,
                price_per_subscription REAL DEFAULT 1,
                anti_scam_enabled INTEGER DEFAULT 1,
                suspicious_limit INTEGER DEFAULT 25,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                moderated_at TIMESTAMP
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS traffic_bot_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                traffic_bot_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                user_id INTEGER,
                sponsor_chat_id TEXT,
                order_id INTEGER,
                amount REAL DEFAULT 0,
                payload TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS traffic_bot_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                traffic_bot_id INTEGER NOT NULL,
                external_user_id INTEGER NOT NULL,
                sponsor_chat_id TEXT NOT NULL,
                sponsor_name TEXT,
                order_id INTEGER,
                status TEXT DEFAULT 'active',
                charge_amount REAL DEFAULT 1,
                rewarded INTEGER DEFAULT 0,
                charged INTEGER DEFAULT 0,
                first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rewarded_at TIMESTAMP,
                last_checked_at TIMESTAMP,
                unsubscribed_at TIMESTAMP,
                UNIQUE(traffic_bot_id, external_user_id, sponsor_chat_id)
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS traffic_bot_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                traffic_bot_id INTEGER,
                level TEXT,
                action TEXT,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS traffic_order_conversions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                external_user_id INTEGER NOT NULL,
                traffic_bot_id INTEGER NOT NULL,
                sponsor_chat_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(order_id, external_user_id)
            )"""
        )
        c.execute(
            """CREATE TABLE IF NOT EXISTS traffic_sponsor_assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                traffic_bot_id INTEGER NOT NULL,
                external_user_id INTEGER NOT NULL,
                order_id INTEGER NOT NULL,
                sponsor_chat_id TEXT NOT NULL,
                sponsor_link TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                UNIQUE(traffic_bot_id, external_user_id, order_id)
            )"""
        )
        for statement in [
            "ALTER TABLE traffic_bots ADD COLUMN is_enabled INTEGER DEFAULT 1",
            "ALTER TABLE traffic_users ADD COLUMN deposit_balance REAL DEFAULT 0",
            "ALTER TABLE traffic_users ADD COLUMN earned_balance REAL DEFAULT 0",
            "ALTER TABLE traffic_users ADD COLUMN total_earned REAL DEFAULT 0",
            "ALTER TABLE traffic_users ADD COLUMN total_withdrawn REAL DEFAULT 0",
            "ALTER TABLE traffic_users ADD COLUMN last_cryptobot_user_id INTEGER",
            "ALTER TABLE traffic_bot_subscriptions ADD COLUMN rewarded INTEGER DEFAULT 0",
            "ALTER TABLE traffic_bot_subscriptions ADD COLUMN rewarded_at TIMESTAMP",
        ]:
            try:
                c.execute(statement)
            except sqlite3.OperationalError:
                pass

        conn.commit()
        conn.close()
        logger.info("Database initialized")

    @staticmethod
    def execute_query(query: str, params: tuple = None, fetch: str = None):
        try:
            conn = sqlite3.connect(DatabaseManager.DB_NAME)
            c = conn.cursor()
            if params:
                c.execute(query, params)
            else:
                c.execute(query)
            if fetch == "one":
                result = c.fetchone()
            elif fetch == "all":
                result = c.fetchall()
            else:
                result = None
            conn.commit()
            conn.close()
            return result if fetch else True
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            return None

    @staticmethod
    def create_user(user_id, username, first_name, last_name, referrer_id=None):
        existing = DatabaseManager.execute_query(
            "SELECT user_id FROM users WHERE user_id = ?",
            (user_id,),
            "one",
        )
        if existing:
            return False
        DatabaseManager.execute_query(
            "INSERT INTO users (user_id, username, first_name, last_name, referrer_id) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, first_name, last_name, referrer_id),
        )
        today = datetime.now().strftime("%Y-%m-%d")
        stats = DatabaseManager.execute_query(
            "SELECT * FROM stats WHERE date = ?",
            (today,),
            "one",
        )
        if stats:
            DatabaseManager.execute_query(
                "UPDATE stats SET new_users = new_users + 1, total_users = total_users + 1 WHERE date = ?",
                (today,),
            )
        else:
            total = DatabaseManager.execute_query("SELECT COUNT(*) FROM users", fetch="one")[0]
            DatabaseManager.execute_query(
                "INSERT INTO stats (date, new_users, total_users) VALUES (?, ?, ?)",
                (today, 1, total),
            )
        return True

    @staticmethod
    def create_traffic_user(user_id, username, first_name, referrer_id=None):
        existing = DatabaseManager.execute_query(
            "SELECT user_id FROM traffic_users WHERE user_id = ?",
            (user_id,),
            "one",
        )
        if existing:
            return False
        DatabaseManager.execute_query(
            "INSERT INTO traffic_users (user_id, username, first_name, referrer_id) VALUES (?, ?, ?, ?)",
            (user_id, username, first_name, referrer_id),
        )
        if referrer_id:
            DatabaseManager.execute_query(
                "UPDATE traffic_users SET ref_count = ref_count + 1 WHERE user_id = ?",
                (referrer_id,),
            )
        return True

    @staticmethod
    def get_traffic_user_finances(user_id):
        return DatabaseManager.execute_query(
            """SELECT balance, deposit_balance, earned_balance, total_spent, total_earned, total_withdrawn,
                      last_cryptobot_user_id
               FROM traffic_users
               WHERE user_id = ?""",
            (user_id,),
            "one",
        )

    @staticmethod
    def credit_traffic_user_deposit(user_id, amount, description="Пополнение через CryptoBot", invoice_id=None):
        updated = DatabaseManager.execute_query(
            """UPDATE traffic_users
               SET balance = balance + ?, deposit_balance = deposit_balance + ?
               WHERE user_id = ?""",
            (amount, amount, user_id),
        )
        if not updated:
            return False
        return DatabaseManager.execute_query(
            "INSERT INTO transactions (user_id, amount, type, description, invoice_id) VALUES (?, ?, ?, ?, ?)",
            (user_id, amount, "deposit", description, invoice_id),
        )

    @staticmethod
    def set_traffic_user_cryptobot_id(user_id, cryptobot_user_id):
        return DatabaseManager.execute_query(
            "UPDATE traffic_users SET last_cryptobot_user_id = ? WHERE user_id = ?",
            (cryptobot_user_id, user_id),
        )

    @staticmethod
    def debit_traffic_user_balance(user_id, amount, description, tx_type, invoice_id=None, allow_negative=False):
        row = DatabaseManager.execute_query(
            "SELECT balance, deposit_balance, earned_balance FROM traffic_users WHERE user_id = ?",
            (user_id,),
            "one",
        )
        if not row:
            return {"ok": False, "reason": "user_not_found"}

        balance, deposit_balance, earned_balance = row
        if balance < amount and not allow_negative:
            return {"ok": False, "reason": "insufficient_balance", "balance": balance}

        debit_from_deposit = min(deposit_balance or 0, amount)
        remaining = max(amount - debit_from_deposit, 0)
        debit_from_earned = min(earned_balance or 0, remaining)

        updated = DatabaseManager.execute_query(
            """UPDATE traffic_users
               SET balance = balance - ?,
                   deposit_balance = deposit_balance - ?,
                   earned_balance = earned_balance - ?,
                   total_spent = total_spent + ?
               WHERE user_id = ?""",
            (amount, debit_from_deposit, debit_from_earned, amount, user_id),
        )
        if not updated:
            return {"ok": False, "reason": "update_failed"}

        inserted = DatabaseManager.execute_query(
            "INSERT INTO transactions (user_id, amount, type, description, invoice_id) VALUES (?, ?, ?, ?, ?)",
            (user_id, -amount, tx_type, description, invoice_id),
        )
        if not inserted:
            return {"ok": False, "reason": "transaction_insert_failed"}

        return {
            "ok": True,
            "amount": amount,
            "debit_from_deposit": debit_from_deposit,
            "debit_from_earned": debit_from_earned,
            "result_balance": (balance or 0) - amount,
        }

    @staticmethod
    def get_user_rank(user_id):
        earn = DatabaseManager.execute_query(
            "SELECT COUNT(*) + 1 FROM users WHERE total_earnings > (SELECT total_earnings FROM users WHERE user_id = ?)",
            (user_id,),
            "one",
        )[0]
        ref_c = DatabaseManager.execute_query(
            "SELECT COUNT(*) + 1 FROM users WHERE (ref_level_1 + ref_level_2) > (SELECT (ref_level_1 + ref_level_2) FROM users WHERE user_id = ?)",
            (user_id,),
            "one",
        )[0]
        ref_e = DatabaseManager.execute_query(
            "SELECT COUNT(*) + 1 FROM users WHERE earnings_ref > (SELECT earnings_ref FROM users WHERE user_id = ?)",
            (user_id,),
            "one",
        )[0]
        return earn, ref_c, ref_e

    @staticmethod
    def add_subgram_stat(date, count, earnings):
        existing = DatabaseManager.execute_query(
            "SELECT id FROM subgram_stats WHERE date = ?",
            (date,),
            "one",
        )
        if existing:
            DatabaseManager.execute_query(
                "UPDATE subgram_stats SET subscriptions = subscriptions + ?, earnings = earnings + ? WHERE date = ?",
                (count, earnings, date),
            )
        else:
            DatabaseManager.execute_query(
                "INSERT INTO subgram_stats (date, subscriptions, earnings) VALUES (?, ?, ?)",
                (date, count, earnings),
            )

    @staticmethod
    def create_traffic_bot(owner_user_id, bot_id, bot_username, bot_name, bot_token, api_key):
        return DatabaseManager.execute_query(
            """INSERT INTO traffic_bots (
                owner_user_id, bot_id, bot_username, bot_name, bot_token, api_key
            ) VALUES (?, ?, ?, ?, ?, ?)""",
            (owner_user_id, bot_id, bot_username, bot_name, bot_token, api_key),
        )

    @staticmethod
    def get_traffic_bot_by_token(bot_token):
        return DatabaseManager.execute_query(
            "SELECT id, owner_user_id, bot_id, bot_username, bot_name, api_key, status FROM traffic_bots WHERE bot_token = ?",
            (bot_token,),
            "one",
        )

    @staticmethod
    def get_traffic_bot(bot_id):
        return DatabaseManager.execute_query(
            """SELECT id, owner_user_id, bot_id, bot_username, bot_name, bot_token, api_key, theme, status,
               is_enabled, rejection_reason, display_bots, display_resources, max_sponsors, reset_hours,
               new_sponsors_after_hours, price_per_subscription, anti_scam_enabled, suspicious_limit
               FROM traffic_bots WHERE id = ?""",
            (bot_id,),
            "one",
        )

    @staticmethod
    def get_traffic_bot_by_api_key(api_key):
        return DatabaseManager.execute_query(
            """SELECT id, owner_user_id, bot_id, bot_username, bot_name, bot_token, api_key, theme, status,
                      is_enabled, rejection_reason, display_bots, display_resources, max_sponsors, reset_hours,
                      new_sponsors_after_hours, price_per_subscription, anti_scam_enabled, suspicious_limit
               FROM traffic_bots
               WHERE api_key = ?""",
            (api_key,),
            "one",
        )

    @staticmethod
    def list_traffic_bots(owner_user_id):
        return DatabaseManager.execute_query(
            """SELECT
                tb.id,
                tb.bot_name,
                tb.bot_username,
                tb.theme,
                tb.status,
                tb.rejection_reason,
                COALESCE(SUM(CASE WHEN s.status = 'active' THEN 1 ELSE 0 END), 0) AS sold_count,
                COALESCE(SUM(CASE WHEN s.rewarded = 1 THEN s.charge_amount ELSE 0 END), 0) AS charged_total
               FROM traffic_bots tb
               LEFT JOIN traffic_bot_subscriptions s ON s.traffic_bot_id = tb.id
               WHERE tb.owner_user_id = ?
               GROUP BY tb.id, tb.bot_name, tb.bot_username, tb.theme, tb.status, tb.rejection_reason
               ORDER BY tb.created_at DESC""",
            (owner_user_id,),
            "all",
        ) or []

    @staticmethod
    def update_traffic_bot_token(bot_row_id, bot_token, bot_id, bot_username, bot_name):
        return DatabaseManager.execute_query(
            """UPDATE traffic_bots
               SET bot_token = ?, bot_id = ?, bot_username = ?, bot_name = ?, updated_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (bot_token, bot_id, bot_username, bot_name, bot_row_id),
        )

    @staticmethod
    def update_traffic_bot_settings(bot_row_id, **kwargs):
        allowed = {
            "display_bots",
            "display_resources",
            "max_sponsors",
            "reset_hours",
            "new_sponsors_after_hours",
            "price_per_subscription",
            "anti_scam_enabled",
            "suspicious_limit",
            "theme",
        }
        updates = []
        values = []
        for key, value in kwargs.items():
            if key in allowed:
                updates.append(f"{key} = ?")
                values.append(value)
        if not updates:
            return False
        updates.append("updated_at = CURRENT_TIMESTAMP")
        values.append(bot_row_id)
        return DatabaseManager.execute_query(
            f"UPDATE traffic_bots SET {', '.join(updates)} WHERE id = ?",
            tuple(values),
        )

    @staticmethod
    def set_traffic_bot_status(bot_row_id, status, rejection_reason=None):
        return DatabaseManager.execute_query(
            """UPDATE traffic_bots
               SET status = ?, rejection_reason = ?, moderated_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (status, rejection_reason, bot_row_id),
        )

    @staticmethod
    def set_traffic_bot_enabled(bot_row_id, is_enabled):
        return DatabaseManager.execute_query(
            "UPDATE traffic_bots SET is_enabled = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (1 if is_enabled else 0, bot_row_id),
        )

    @staticmethod
    def add_traffic_bot_log(bot_row_id, level, action, details):
        return DatabaseManager.execute_query(
            "INSERT INTO traffic_bot_logs (traffic_bot_id, level, action, details) VALUES (?, ?, ?, ?)",
            (bot_row_id, level, action, details),
        )

    @staticmethod
    def add_traffic_bot_event(bot_row_id, event_type, user_id=None, sponsor_chat_id=None, order_id=None, amount=0, payload=None):
        return DatabaseManager.execute_query(
            """INSERT INTO traffic_bot_events (
                traffic_bot_id, event_type, user_id, sponsor_chat_id, order_id, amount, payload
               ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (bot_row_id, event_type, user_id, sponsor_chat_id, order_id, amount, payload),
        )

    @staticmethod
    def upsert_traffic_subscription(bot_row_id, external_user_id, sponsor_chat_id, sponsor_name=None, order_id=None, charge_amount=1):
        existing = DatabaseManager.execute_query(
            """SELECT id, rewarded FROM traffic_bot_subscriptions
               WHERE traffic_bot_id = ? AND external_user_id = ? AND sponsor_chat_id = ?""",
            (bot_row_id, external_user_id, sponsor_chat_id),
            "one",
        )
        if existing:
            return DatabaseManager.execute_query(
                """UPDATE traffic_bot_subscriptions
                   SET status = 'active', sponsor_name = ?, order_id = ?, charge_amount = ?, charged = 0,
                       unsubscribed_at = NULL, last_checked_at = CURRENT_TIMESTAMP
                   WHERE id = ?""",
                (sponsor_name, order_id, charge_amount, existing[0]),
            )
        return DatabaseManager.execute_query(
            """INSERT INTO traffic_bot_subscriptions (
                traffic_bot_id, external_user_id, sponsor_chat_id, sponsor_name, order_id, charge_amount, last_checked_at
               ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
            (bot_row_id, external_user_id, sponsor_chat_id, sponsor_name, order_id, charge_amount),
        )

    @staticmethod
    def register_traffic_subscription_reward(api_key, external_user_id, sponsor_chat_id, sponsor_name=None, order_id=None, charge_amount=1):
        bot_data = DatabaseManager.get_traffic_bot_by_api_key(api_key)
        if not bot_data:
            return {"ok": False, "reason": "invalid_api_key"}

        bot_row_id = bot_data[0]
        owner_user_id = bot_data[1]
        status = bot_data[8]
        is_enabled = bot_data[9]

        if status != "approved":
            return {"ok": False, "reason": "bot_not_approved"}
        if not is_enabled:
            return {"ok": False, "reason": "bot_disabled"}

        existing = DatabaseManager.execute_query(
            """SELECT id, rewarded
               FROM traffic_bot_subscriptions
               WHERE traffic_bot_id = ? AND external_user_id = ? AND sponsor_chat_id = ?""",
            (bot_row_id, external_user_id, sponsor_chat_id),
            "one",
        )

        if existing:
            subscription_id, rewarded = existing
            DatabaseManager.execute_query(
                """UPDATE traffic_bot_subscriptions
                   SET status = 'active',
                       sponsor_name = ?,
                       order_id = ?,
                       charge_amount = ?,
                       charged = 0,
                       unsubscribed_at = NULL,
                       last_checked_at = CURRENT_TIMESTAMP
                   WHERE id = ?""",
                (sponsor_name, order_id, charge_amount, subscription_id),
            )
            if rewarded:
                return {"ok": True, "credited": False, "subscription_id": subscription_id, "bot_id": bot_row_id}
            DatabaseManager.execute_query(
                "UPDATE traffic_bot_subscriptions SET rewarded = 1, rewarded_at = CURRENT_TIMESTAMP WHERE id = ?",
                (subscription_id,),
            )
        else:
            DatabaseManager.execute_query(
                """INSERT INTO traffic_bot_subscriptions (
                    traffic_bot_id, external_user_id, sponsor_chat_id, sponsor_name, order_id,
                    charge_amount, rewarded, rewarded_at, last_checked_at
                   ) VALUES (?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
                (bot_row_id, external_user_id, sponsor_chat_id, sponsor_name, order_id, charge_amount),
            )
            subscription_id = DatabaseManager.execute_query(
                """SELECT id
                   FROM traffic_bot_subscriptions
                   WHERE traffic_bot_id = ? AND external_user_id = ? AND sponsor_chat_id = ?""",
                (bot_row_id, external_user_id, sponsor_chat_id),
                "one",
            )[0]

        DatabaseManager.execute_query(
            """UPDATE traffic_users
               SET balance = balance + ?,
                   earned_balance = earned_balance + ?,
                   total_earned = total_earned + ?
               WHERE user_id = ?""",
            (charge_amount, charge_amount, charge_amount, owner_user_id),
        )
        DatabaseManager.execute_query(
            "INSERT INTO transactions (user_id, amount, type, description) VALUES (?, ?, ?, ?)",
            (
                owner_user_id,
                charge_amount,
                "traffic_reward",
                f"Доход за подписку: {sponsor_name or sponsor_chat_id or 'Neo Link'}",
            ),
        )
        DatabaseManager.add_traffic_bot_event(
            bot_row_id,
            "subscription_reward",
            user_id=external_user_id,
            sponsor_chat_id=sponsor_chat_id,
            order_id=order_id,
            amount=charge_amount,
            payload=sponsor_name,
        )
        return {
            "ok": True,
            "credited": True,
            "subscription_id": subscription_id,
            "bot_id": bot_row_id,
            "owner_user_id": owner_user_id,
            "amount": charge_amount,
        }

    @staticmethod
    def get_active_sponsor_orders(include_channel=True, include_resource=True, limit=1):
        order_types = []
        if include_channel:
            order_types.append("channel")
        if include_resource:
            order_types.append("resource")
        if not order_types:
            return []

        placeholders = ",".join("?" for _ in order_types)
        return DatabaseManager.execute_query(
            f"""SELECT id, user_id, link, order_type, amount, done, channel_id, invite_link
                FROM orders
                WHERE status = 'active' AND done < amount AND order_type IN ({placeholders})
                ORDER BY created ASC
                LIMIT ?""",
            tuple(order_types + [limit]),
            "all",
        ) or []

    @staticmethod
    def get_active_traffic_sponsor_assignments(bot_row_id, external_user_id):
        return DatabaseManager.execute_query(
            """SELECT
                   a.order_id,
                   a.sponsor_chat_id,
                   a.sponsor_link,
                   o.order_type,
                   o.amount,
                   o.done,
                   o.channel_id,
                   o.invite_link,
                   o.link
               FROM traffic_sponsor_assignments a
               JOIN orders o ON o.id = a.order_id
               WHERE a.traffic_bot_id = ?
                 AND a.external_user_id = ?
                 AND a.expires_at > CURRENT_TIMESTAMP
                 AND o.status = 'active'
                 AND o.done < o.amount
               ORDER BY a.created_at ASC""",
            (bot_row_id, external_user_id),
            "all",
        ) or []

    @staticmethod
    def replace_traffic_sponsor_assignments(bot_row_id, external_user_id, assignments, ttl_minutes=15):
        conn = sqlite3.connect(DatabaseManager.DB_NAME)
        try:
            c = conn.cursor()
            c.execute(
                """DELETE FROM traffic_sponsor_assignments
                   WHERE traffic_bot_id = ? AND external_user_id = ?""",
                (bot_row_id, external_user_id),
            )
            for assignment in assignments:
                c.execute(
                    """INSERT INTO traffic_sponsor_assignments (
                        traffic_bot_id, external_user_id, order_id, sponsor_chat_id, sponsor_link, expires_at
                       ) VALUES (?, ?, ?, ?, ?, DATETIME('now', ?))""",
                    (
                        bot_row_id,
                        external_user_id,
                        assignment["order_id"],
                        assignment["sponsor_chat_id"],
                        assignment.get("sponsor_link"),
                        f"+{int(ttl_minutes)} minutes",
                    ),
                )
            conn.commit()
            return True
        except Exception as exc:
            conn.rollback()
            logger.error(f"Traffic sponsor assignment replace error: {exc}")
            return False
        finally:
            conn.close()

    @staticmethod
    def get_valid_traffic_sponsor_assignment(bot_row_id, external_user_id, sponsor_chat_id=None, order_id=None):
        conditions = [
            "traffic_bot_id = ?",
            "external_user_id = ?",
            "expires_at > CURRENT_TIMESTAMP",
        ]
        params = [bot_row_id, external_user_id]
        if order_id is not None:
            conditions.append("order_id = ?")
            params.append(order_id)
        if sponsor_chat_id:
            conditions.append("sponsor_chat_id = ?")
            params.append(sponsor_chat_id)
        return DatabaseManager.execute_query(
            f"""SELECT order_id, sponsor_chat_id, sponsor_link
                FROM traffic_sponsor_assignments
                WHERE {' AND '.join(conditions)}
                LIMIT 1""",
            tuple(params),
            "one",
        )

    @staticmethod
    def delete_traffic_sponsor_assignment(bot_row_id, external_user_id, order_id):
        return DatabaseManager.execute_query(
            """DELETE FROM traffic_sponsor_assignments
               WHERE traffic_bot_id = ? AND external_user_id = ? AND order_id = ?""",
            (bot_row_id, external_user_id, order_id),
        )

    @staticmethod
    def register_traffic_order_conversion(api_key, external_user_id, sponsor_chat_id, sponsor_name=None, order_id=None, charge_amount=1):
        bot_data = DatabaseManager.get_traffic_bot_by_api_key(api_key)
        if not bot_data:
            return {"ok": False, "reason": "invalid_api_key"}

        bot_row_id = bot_data[0]
        owner_user_id = bot_data[1]
        status = bot_data[8]
        is_enabled = bot_data[9]
        if status != "approved":
            return {"ok": False, "reason": "bot_not_approved"}
        if not is_enabled:
            return {"ok": False, "reason": "bot_disabled"}

        assignment = DatabaseManager.get_valid_traffic_sponsor_assignment(
            bot_row_id,
            external_user_id,
            sponsor_chat_id=sponsor_chat_id or None,
            order_id=order_id,
        )
        if not assignment:
            return {"ok": False, "reason": "assignment_not_found"}

        conn = sqlite3.connect(DatabaseManager.DB_NAME)
        try:
            c = conn.cursor()

            if order_id:
                c.execute(
                    """SELECT id, done, amount, status
                       FROM orders
                       WHERE id = ? AND status = 'active' AND done < amount""",
                    (order_id,),
                )
            else:
                c.execute(
                    """SELECT id, done, amount, status
                       FROM orders
                       WHERE status = 'active' AND done < amount AND (channel_id = ? OR link = ? OR invite_link = ?)
                       ORDER BY created ASC
                       LIMIT 1""",
                    (sponsor_chat_id, sponsor_chat_id, sponsor_chat_id),
                )
            order_row = c.fetchone()
            if not order_row:
                return {"ok": False, "reason": "order_not_found"}

            resolved_order_id, done, amount, _order_status = order_row

            c.execute(
                """SELECT id
                   FROM traffic_order_conversions
                   WHERE order_id = ? AND external_user_id = ?""",
                (resolved_order_id, external_user_id),
            )
            if c.fetchone():
                return {"ok": True, "credited": False, "duplicate": True, "order_id": resolved_order_id}

            c.execute(
                """SELECT id, rewarded
                   FROM traffic_bot_subscriptions
                   WHERE traffic_bot_id = ? AND external_user_id = ? AND sponsor_chat_id = ?""",
                (bot_row_id, external_user_id, sponsor_chat_id),
            )
            existing = c.fetchone()
            if existing:
                subscription_id, rewarded = existing
                if rewarded:
                    return {"ok": True, "credited": False, "duplicate": True, "order_id": resolved_order_id}
                c.execute(
                    """UPDATE traffic_bot_subscriptions
                       SET status = 'active',
                           sponsor_name = ?,
                           order_id = ?,
                           charge_amount = ?,
                           charged = 0,
                           rewarded = 1,
                           rewarded_at = CURRENT_TIMESTAMP,
                           unsubscribed_at = NULL,
                           last_checked_at = CURRENT_TIMESTAMP
                       WHERE id = ?""",
                    (sponsor_name, resolved_order_id, charge_amount, subscription_id),
                )
            else:
                c.execute(
                    """INSERT INTO traffic_bot_subscriptions (
                        traffic_bot_id, external_user_id, sponsor_chat_id, sponsor_name, order_id, charge_amount,
                        rewarded, rewarded_at, last_checked_at
                       ) VALUES (?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
                    (bot_row_id, external_user_id, sponsor_chat_id, sponsor_name, resolved_order_id, charge_amount),
                )
                subscription_id = c.lastrowid

            c.execute(
                """INSERT INTO traffic_order_conversions (
                    order_id, external_user_id, traffic_bot_id, sponsor_chat_id
                   ) VALUES (?, ?, ?, ?)""",
                (resolved_order_id, external_user_id, bot_row_id, sponsor_chat_id),
            )
            c.execute(
                """UPDATE traffic_users
                   SET balance = balance + ?,
                       earned_balance = earned_balance + ?,
                       total_earned = total_earned + ?
                   WHERE user_id = ?""",
                (charge_amount, charge_amount, charge_amount, owner_user_id),
            )
            c.execute(
                "INSERT INTO transactions (user_id, amount, type, description) VALUES (?, ?, ?, ?)",
                (
                    owner_user_id,
                    charge_amount,
                    "traffic_reward",
                    f"Доход за подписку: {sponsor_name or sponsor_chat_id or 'Neo Link'}",
                ),
            )
            new_done = done + 1
            new_status = "completed" if new_done >= amount else "active"
            c.execute(
                "UPDATE orders SET done = ?, status = ? WHERE id = ?",
                (new_done, new_status, resolved_order_id),
            )
            c.execute(
                """INSERT INTO traffic_bot_events (
                    traffic_bot_id, event_type, user_id, sponsor_chat_id, order_id, amount, payload
                   ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (bot_row_id, "subscription_reward", external_user_id, sponsor_chat_id, resolved_order_id, charge_amount, sponsor_name),
            )
            c.execute(
                """DELETE FROM traffic_sponsor_assignments
                   WHERE traffic_bot_id = ? AND external_user_id = ? AND order_id = ?""",
                (bot_row_id, external_user_id, resolved_order_id),
            )
            conn.commit()
            return {
                "ok": True,
                "credited": True,
                "subscription_id": subscription_id,
                "bot_id": bot_row_id,
                "owner_user_id": owner_user_id,
                "amount": charge_amount,
                "order_id": resolved_order_id,
                "order_done": new_done,
                "order_amount": amount,
                "order_completed": new_status == "completed",
            }
        except sqlite3.IntegrityError:
            conn.rollback()
            return {"ok": True, "credited": False, "duplicate": True, "order_id": order_id}
        except Exception as exc:
            conn.rollback()
            logger.error(f"Traffic order conversion error: {exc}")
            return {"ok": False, "reason": "db_error", "details": str(exc)}
        finally:
            conn.close()

    @staticmethod
    def list_active_traffic_subscriptions():
        return DatabaseManager.execute_query(
            """SELECT s.id, s.traffic_bot_id, s.external_user_id, s.sponsor_chat_id, s.sponsor_name, s.charge_amount,
                      tb.owner_user_id, tb.bot_token, tb.bot_name
               FROM traffic_bot_subscriptions s
               JOIN traffic_bots tb ON tb.id = s.traffic_bot_id
               WHERE s.status = 'active' AND tb.status = 'approved' AND COALESCE(tb.is_enabled, 1) = 1""",
            fetch="all",
        ) or []

    @staticmethod
    def mark_traffic_subscription_checked(subscription_id):
        return DatabaseManager.execute_query(
            "UPDATE traffic_bot_subscriptions SET last_checked_at = CURRENT_TIMESTAMP WHERE id = ?",
            (subscription_id,),
        )

    @staticmethod
    def charge_for_unsubscribe(subscription_id):
        row = DatabaseManager.execute_query(
            """SELECT s.traffic_bot_id, s.charge_amount, s.charged, tb.owner_user_id, tb.bot_name, s.sponsor_name
               FROM traffic_bot_subscriptions s
               JOIN traffic_bots tb ON tb.id = s.traffic_bot_id
               WHERE s.id = ?""",
            (subscription_id,),
            "one",
        )
        if not row:
            return None
        bot_row_id, amount, charged, owner_user_id, bot_name, sponsor_name = row
        if charged:
            return {"charged": False, "reason": "already"}
        DatabaseManager.execute_query(
            """UPDATE traffic_bot_subscriptions
               SET status = 'unsubscribed', charged = 1, unsubscribed_at = CURRENT_TIMESTAMP, last_checked_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (subscription_id,),
        )
        DatabaseManager.debit_traffic_user_balance(
            owner_user_id,
            amount,
            "unsubscribe charge",
            "unsubscribe_charge",
            allow_negative=True,
        )
        description = f"Списание за отписку от спонсора: {sponsor_name or bot_name or 'Neo Link'}"
        DatabaseManager.add_traffic_bot_event(
            bot_row_id,
            "unsubscribe_charge",
            amount=amount,
            payload=description,
        )
        return {"charged": True, "amount": amount, "owner_user_id": owner_user_id, "description": description}

    @staticmethod
    def create_traffic_withdrawal_request(user_id, rub_amount, asset, asset_amount, cryptobot_user_id, spend_id):
        return DatabaseManager.execute_query(
            """INSERT INTO traffic_withdrawals (
                user_id, rub_amount, asset, asset_amount, cryptobot_user_id, spend_id
               ) VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, rub_amount, asset, asset_amount, cryptobot_user_id, spend_id),
        )

    @staticmethod
    def fail_traffic_withdrawal(spend_id, error):
        return DatabaseManager.execute_query(
            "UPDATE traffic_withdrawals SET status = 'failed', error = ? WHERE spend_id = ?",
            (error, spend_id),
        )

    @staticmethod
    def complete_traffic_withdrawal(user_id, rub_amount, asset, asset_amount, cryptobot_user_id, spend_id, transfer_id):
        conn = sqlite3.connect(DatabaseManager.DB_NAME)
        try:
            c = conn.cursor()
            c.execute(
                "SELECT balance, earned_balance FROM traffic_users WHERE user_id = ?",
                (user_id,),
            )
            row = c.fetchone()
            if not row:
                raise ValueError("user_not_found")

            _balance, earned_balance = row
            if (earned_balance or 0) < rub_amount:
                raise ValueError("insufficient_earned_balance")

            c.execute(
                """UPDATE traffic_users
                   SET balance = balance - ?,
                       earned_balance = earned_balance - ?,
                       total_withdrawn = total_withdrawn + ?,
                       last_cryptobot_user_id = ?
                   WHERE user_id = ?""",
                (rub_amount, rub_amount, rub_amount, cryptobot_user_id, user_id),
            )
            c.execute(
                """UPDATE traffic_withdrawals
                   SET transfer_id = ?, status = 'completed', error = NULL, completed_at = CURRENT_TIMESTAMP
                   WHERE spend_id = ?""",
                (str(transfer_id), spend_id),
            )
            c.execute(
                "INSERT INTO transactions (user_id, amount, type, description, invoice_id) VALUES (?, ?, ?, ?, ?)",
                (
                    user_id,
                    -rub_amount,
                    "withdraw",
                    f"Вывод через CryptoBot: {asset_amount:.6f} {asset} на user id {cryptobot_user_id}",
                    spend_id,
                ),
            )
            conn.commit()
            return True
        except Exception as exc:
            conn.rollback()
            logger.error(f"Traffic withdrawal completion error: {exc}")
            return False
        finally:
            conn.close()
