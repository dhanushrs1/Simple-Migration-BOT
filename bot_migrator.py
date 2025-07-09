# bot_migrator.py
import logging
import asyncio
import traceback
import sys
import time
from datetime import datetime, UTC, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum
import gc

# --- Configuration and Imports ---
try:
    from config import BOT_TOKEN, MONGO_URI, ADMIN_USER_ID, SOURCE_CHANNEL_ID, DESTINATION_CHANNEL_ID
except ImportError:
    print("FATAL: config.py not found or variables are missing. Please create and configure it.")
    sys.exit(1)

from pymongo import MongoClient
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegram.ext import Application, CommandHandler, filters, ContextTypes, CallbackQueryHandler
from telegram.error import TelegramError, RetryAfter, TimedOut, NetworkError, BadRequest, Forbidden
from telegram.constants import ParseMode

# --- Enhanced Logging Setup ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("bot_migrator.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- Constants and Configuration ---
class TransferStatus(Enum):
    STOPPED = "stopped"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    ERROR = "error"
    WAITING = "waiting"
    INITIALIZING = "initializing"

@dataclass
class TransferConfig:
    """Configuration for transfer parameters"""
    base_delay: float = 1.5
    max_delay: float = 45.0
    max_retries: int = 5
    requests_per_minute: int = 25
    checkpoint_interval: int = 100
    memory_cleanup_interval: int = 1000
    max_copy_size: int = 1.9 * 1024 * 1024 * 1024
    max_file_size: int = 3.9 * 1024 * 1024 * 1024

# --- Enhanced TransferManager Class ---
class TransferManager:
    """Enhanced transfer manager with robust error handling and rate limiting"""

    def __init__(self, db_client: MongoClient, config: TransferConfig = None):
        self.db = db_client.telegram_transfer_bot
        self.progress_collection = self.db.migration_progress
        self.error_logs_collection = self.db.migration_errors
        self.config = config or TransferConfig()
        self.status = TransferStatus.STOPPED
        self.task: Optional[asyncio.Task] = None
        self.request_times: List[float] = []
        self.wait_until: Optional[datetime] = None
        self.wait_message_id: Optional[int] = None
        self.wait_task: Optional[asyncio.Task] = None
        self._create_indexes()

    def _create_indexes(self):
        try:
            self.progress_collection.create_index([("_id", 1)])
            self.error_logs_collection.create_index([("message_id", 1), ("timestamp", -1)])
            logger.info("Database indexes created successfully.")
        except Exception as e:
            logger.warning(f"Could not create indexes: {e}")

    def _ensure_utc(self, dt: Any) -> Optional[datetime]:
        """Ensures a datetime object is timezone-aware (UTC)."""
        if not dt or not isinstance(dt, datetime): return None
        return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)

    def get_progress(self) -> Dict[str, Any]:
        """Fetches the current progress state from MongoDB."""
        try:
            state = self.progress_collection.find_one({'_id': 'main_state'})
            if state:
                for key in ['start_time', 'last_update', 'estimated_completion']:
                    state[key] = self._ensure_utc(state.get(key))
                return state
        except Exception as e:
            logger.error(f"Error fetching progress: {e}")
        
        return {
            '_id': 'main_state', 'first_message_id': 1, 'last_message_id': 0,
            'last_processed_id': 0, 'transferred_count': 0, 'skipped_count': 0,
            'error_count': 0, 'large_files_count': 0, 'start_time': None,
            'last_update': None, 'estimated_completion': None,
            'status': TransferStatus.STOPPED.value, 'average_speed': 0.0
        }

    def save_progress(self, state: Dict[str, Any]):
        """Saves progress with enhanced error handling and speed calculations."""
        try:
            state['last_update'] = datetime.now(UTC)
            state['status'] = self.status.value
            
            # --- Corrected Speed and ETA Calculation ---
            start_time = self._ensure_utc(state.get('start_time'))
            transferred = state.get('transferred_count', 0)
            
            if start_time and transferred > 0:
                # Calculate runtime, excluding paused time, if available.
                total_runtime_seconds = (datetime.now(UTC) - start_time).total_seconds()
                if total_runtime_seconds > 10:
                    avg_speed = (transferred / total_runtime_seconds) * 60
                    state['average_speed'] = avg_speed
                    
                    total_messages = (state.get('last_message_id', 0) - state.get('first_message_id', 1)) + 1
                    remaining_messages = total_messages - transferred
                    
                    if avg_speed > 0 and remaining_messages > 0:
                        eta_seconds = remaining_messages * (total_runtime_seconds / transferred)
                        state['estimated_completion'] = datetime.now(UTC) + timedelta(seconds=eta_seconds)

            self.progress_collection.replace_one({'_id': 'main_state'}, state, upsert=True)
        except Exception as e:
            logger.error(f"Error saving progress: {e}")

    def log_error_db(self, message_id: int, error: str, error_type: str):
        """Logs an error to the database for later review."""
        try:
            self.error_logs_collection.insert_one({
                'message_id': message_id, 'error': str(error), 'error_type': error_type,
                'timestamp': datetime.now(UTC), 'traceback': traceback.format_exc()
            })
        except Exception as e:
            logger.error(f"FATAL: Could not log error to database: {e}")

    async def _rate_limit_check(self):
        """Prevents hitting Telegram's API limits by self-regulating request speed."""
        now = time.time()
        self.request_times = [t for t in self.request_times if now - t < 60]
        if len(self.request_times) >= self.config.requests_per_minute:
            sleep_time = 60 - (now - self.request_times[0])
            if sleep_time > 0:
                logger.info(f"Rate limit enforced. Sleeping for {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)
        self.request_times.append(time.time())

    async def _handle_rate_limit_wait(self, bot: Bot, retry_after: int, message_id: int):
        """Manages the waiting period for a RetryAfter exception."""
        self.status = TransferStatus.WAITING
        self.wait_until = datetime.now(UTC) + timedelta(seconds=retry_after)
        try:
            wait_msg = await bot.send_message(
                ADMIN_USER_ID,
                f"⏳ **Rate Limited**\nBot will pause for `{retry_after}s` and resume automatically.",
                parse_mode=ParseMode.MARKDOWN
            )
            self.wait_message_id = wait_msg.message_id
            self.wait_task = asyncio.create_task(self._wait_timer(bot, retry_after, message_id))
            await self.wait_task
        except asyncio.CancelledError: logger.info("Wait task was cancelled.")
        except Exception as e:
            logger.error(f"Error during rate limit wait handling: {e}")
            if self.status == TransferStatus.WAITING: self.status = TransferStatus.PAUSED
        finally:
            self.wait_task = None
            self.wait_until = None
            if self.wait_message_id:
                try: await bot.delete_message(ADMIN_USER_ID, self.wait_message_id)
                except Exception: pass
                self.wait_message_id = None

    async def _wait_timer(self, bot: Bot, total_wait: int, message_id: int):
        """The countdown timer task for rate-limiting."""
        try:
            await asyncio.sleep(total_wait)
            if self.status == TransferStatus.WAITING:
                self.status = TransferStatus.RUNNING
                await bot.send_message(ADMIN_USER_ID, f"✅ **Resuming transfer** from message `{message_id}`.")
        except asyncio.CancelledError:
            logger.info("Wait timer cancelled.")
            raise

    async def _transfer_single_message(self, bot: Bot, message_id: int, state: Dict[str, Any]) -> bool:
        """Main logic to transfer one message with fallbacks and error handling."""
        for attempt in range(self.config.max_retries):
            try:
                await self._rate_limit_check()
                await bot.copy_message(
                    chat_id=DESTINATION_CHANNEL_ID, from_chat_id=SOURCE_CHANNEL_ID,
                    message_id=message_id, disable_notification=True
                )
                state['transferred_count'] += 1
                return True

            except BadRequest as e:
                error_text = str(e).lower()
                # This error means the message ID does not exist. We skip it and move on.
                if "message to copy not found" in error_text:
                    logger.warning(f"Message {message_id} not found in source channel, skipping.")
                    state['skipped_count'] += 1
                    return True # Return True to continue to the next ID
                
                logger.error(f"BadRequest on message {message_id}: {e}")
                self.log_error_db(message_id, str(e), "BadRequest")
                state['error_count'] += 1
                return True

            except RetryAfter as e:
                logger.warning(f"Rate limit hit at message {message_id}. Waiting {e.retry_after}s.")
                await self._handle_rate_limit_wait(bot, e.retry_after, message_id)
                return self.status == TransferStatus.RUNNING

            except (TimedOut, NetworkError) as e:
                delay = min(self.config.base_delay * (2 ** attempt), self.config.max_delay)
                logger.warning(f"Network error on msg {message_id}: {e}. Retrying in {delay:.1f}s.")
                await asyncio.sleep(delay)

            except Forbidden as e:
                logger.critical(f"Permission error: {e}. Bot might be banned or not an admin.")
                self.log_error_db(message_id, str(e), "PermissionError")
                self.status = TransferStatus.ERROR
                return False

            except Exception as e:
                logger.error(f"Unexpected error on msg {message_id}: {e}", exc_info=True)
                self.log_error_db(message_id, str(e), "UnexpectedError")
                state['error_count'] += 1
                return True

        logger.error(f"Max retries reached for message {message_id}. Skipping.")
        self.log_error_db(message_id, "Max retries reached", "MaxRetriesExceeded")
        state['error_count'] += 1
        return True

    async def _get_channel_message_range(self, bot: Bot) -> Optional[tuple[int, int]]:
        """Gets the first and last message IDs of the source channel."""
        logger.info("Attempting to determine message range in source channel...")
        try:
            # Find the last message ID by sending and deleting a message
            msg = await bot.send_message(chat_id=SOURCE_CHANNEL_ID, text="_")
            last_id = msg.message_id
            await bot.delete_message(chat_id=SOURCE_CHANNEL_ID, message_id=last_id)
            
            # Find the first message ID (usually 1 for channels, but we can search)
            first_id = 1 # Assume 1, common for channels
            
            logger.info(f"Detected message range: {first_id} to {last_id}")
            return first_id, last_id
        except Exception as e:
            logger.critical(f"Could not determine message range in source channel: {e}")
            await bot.send_message(ADMIN_USER_ID, f"❌ **Error:** Could not access source channel to determine message range. Please ensure the bot is an admin with permission to send/delete messages.\n\n`{e}`")
            return None

    async def start(self, bot: Bot):
        """Starts or resumes the transfer process."""
        if self.status == TransferStatus.RUNNING: return

        self.status = TransferStatus.INITIALIZING
        state = self.get_progress()

        # Get the message range if we haven't already
        if state.get('last_message_id', 0) == 0:
            message_range = await self._get_channel_message_range(bot)
            if not message_range:
                self.status = TransferStatus.ERROR
                self.save_progress(state)
                return
            state['first_message_id'], state['last_message_id'] = message_range
            # If starting from scratch, set the starting point
            if state.get('last_processed_id', 0) == 0:
                state['last_processed_id'] = state['first_message_id'] - 1
            self.save_progress(state)

        # Set start time only if it's the very first run
        if not self._ensure_utc(state.get('start_time')):
            state['start_time'] = datetime.now(UTC)

        self.status = TransferStatus.RUNNING
        self.task = asyncio.create_task(self._transfer_loop(bot))
        logger.info("Transfer process started/resumed.")

    def pause(self):
        if self.status == TransferStatus.RUNNING:
            self.status = TransferStatus.PAUSED
            logger.info("Transfer paused.")

    async def stop(self):
        """Stops the transfer process cleanly."""
        if self.status in [TransferStatus.RUNNING, TransferStatus.PAUSED, TransferStatus.WAITING]:
            self.status = TransferStatus.STOPPED
            if self.wait_task: self.wait_task.cancel()
            if self.task: self.task.cancel()
            logger.info("Transfer process stopped.")

    async def _transfer_loop(self, bot: Bot):
        """The main asynchronous loop that processes messages."""
        state = self.get_progress()
        message_id = state.get('last_processed_id', 0) + 1
        
        try:
            while self.status == TransferStatus.RUNNING:
                # Check for completion
                if message_id > state.get('last_message_id', 0):
                    self.status = TransferStatus.COMPLETED
                    logger.info("All messages in the detected range have been processed.")
                    break

                if not await self._transfer_single_message(bot, message_id, state):
                    break
                
                state['last_processed_id'] = message_id
                message_id += 1
                
                if message_id % 10 == 0: self.save_progress(state)
                if message_id % self.config.memory_cleanup_interval == 0:
                    gc.collect()
                
                await asyncio.sleep(self.config.base_delay)

        except asyncio.CancelledError: logger.info("Transfer task was cancelled.")
        except Exception as e:
            logger.critical(f"Critical error in transfer loop: {e}", exc_info=True)
            self.status = TransferStatus.ERROR
            self.log_error_db(message_id, str(e), "CriticalLoopError")
        finally:
            self.save_progress(state)
            logger.info(f"Transfer loop finished. Final status: {self.status.value}")
            
            final_message = ""
            if self.status == TransferStatus.COMPLETED: final_message = "🎉 **Transfer Complete!**"
            elif self.status == TransferStatus.ERROR: final_message = "❌ **Transfer Halted Due to Error!**"
            elif self.status == TransferStatus.STOPPED: final_message = "⏹️ **Transfer Stopped.**"
            if final_message:
                await bot.send_message(ADMIN_USER_ID, f"{final_message}\nCheck `/status` for the final report.", parse_mode=ParseMode.MARKDOWN)

    def reset_all_data(self) -> bool:
        """Completely wipes all progress and logs from the database."""
        try:
            self.progress_collection.delete_many({})
            self.error_logs_collection.delete_many({})
            self.status = TransferStatus.STOPPED
            logger.info("All bot data has been reset.")
            return True
        except Exception as e:
            logger.error(f"Error resetting data: {e}")
            return False

# --- UI and Command Handlers ---
def get_status_keyboard(manager: TransferManager) -> InlineKeyboardMarkup:
    buttons = []
    if manager.status == TransferStatus.RUNNING:
        buttons.append([InlineKeyboardButton("⏸️ Pause", callback_data="pause"), InlineKeyboardButton("⏹️ Stop", callback_data="stop")])
    elif manager.status == TransferStatus.WAITING:
        buttons.append([InlineKeyboardButton("⏹️ Stop", callback_data="stop")])
    elif manager.status in [TransferStatus.STOPPED, TransferStatus.PAUSED, TransferStatus.ERROR, TransferStatus.INITIALIZING, TransferStatus.COMPLETED]:
        if manager.status not in [TransferStatus.COMPLETED, TransferStatus.INITIALIZING]:
            buttons.append([InlineKeyboardButton("▶️ Resume / Start", callback_data="resume")])
        buttons.append([InlineKeyboardButton("⚠️ Reset All Data", callback_data="reset_confirm")])
    buttons.append([InlineKeyboardButton("🔄 Refresh Status", callback_data="status")])
    return InlineKeyboardMarkup(buttons)

async def send_status_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends or edits the main status message panel."""
    manager: TransferManager = context.application.bot_data['manager']
    state = manager.get_progress()
    
    status_emoji = {"running": "🟢", "stopped": "🔴", "paused": "🟡", "completed": "✅", "error": "❌", "waiting": "⏳", "initializing": "⚙️"}
    
    total_messages = max(0, (state.get('last_message_id', 0) - state.get('first_message_id', 1)) + 1)
    transferred = state.get('transferred_count', 0)
    progress_bar = "N/A"
    if total_messages > 0:
        percentage = (transferred / total_messages) * 100
        filled_blocks = int(percentage // 10)
        empty_blocks = 10 - filled_blocks
        progress_bar = f"[{'■' * filled_blocks}{'□' * empty_blocks}] {percentage:.1f}%"

    eta_str = "N/A"
    if manager.status == TransferStatus.RUNNING:
        eta_time = manager._ensure_utc(state.get('estimated_completion'))
        if eta_time and eta_time > datetime.now(UTC):
            eta_str = str(timedelta(seconds=int((eta_time - datetime.now(UTC)).total_seconds())))
        elif state.get('average_speed', 0) > 0:
            eta_str = "Calculating..."

    status_text = (
        f"📊 **Migration Bot Status**\n"
        f"`{progress_bar}`\n\n"
        f"**Status:** {status_emoji.get(manager.status.value, '❓')} `{manager.status.value.title()}`\n"
        f"**ETA:** `{eta_str}`\n\n"
        f"--- **Progress** ---\n"
        f"**Processed:** `{transferred:,} / {total_messages:,}` messages\n"
        f"**Last ID:** `{state.get('last_processed_id', 0):,}` (Range: `{state.get('first_message_id', 1):,}`-`{state.get('last_message_id', 0):,}`)\n"
        f"**Skipped/Empty:** `{state.get('skipped_count', 0):,}` | **Errors:** `{state.get('error_count', 0):,}`\n\n"
        f"--- **Performance** ---\n"
        f"**Average Speed:** `{state.get('average_speed', 0):.2f} msg/min`\n\n"
        f"🕒 _Last update: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}_"
    )
    
    keyboard = get_status_keyboard(manager)
    try:
        if update.callback_query:
            await update.callback_query.edit_message_text(text=status_text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
        else:
            await update.message.reply_text(text=status_text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
    except BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Error updating status message: {e}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Welcome, Admin! Use `/status` to control the migration bot.")

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_status_message(update, context)

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles all inline button presses from the status panel."""
    query = update.callback_query
    await query.answer()
    action = query.data
    manager: TransferManager = context.application.bot_data['manager']
    
    if action == "resume":
        await query.message.reply_text("▶️ Starting transfer... This may take a moment to determine the message range.")
        await manager.start(context.bot)
    elif action == "pause":
        manager.pause()
        await query.message.reply_text("⏸️ Pausing transfer.")
    elif action == "stop":
        await query.message.reply_text("⏹️ Stopping transfer...")
        await manager.stop()
    elif action == "reset_confirm":
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ YES, DELETE EVERYTHING ❌", callback_data="reset_execute"), InlineKeyboardButton("Cancel", callback_data="status")]])
        await query.edit_message_text("**ARE YOU SURE?**\n\nThis will delete all progress from the database. This cannot be undone.", reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)
        return
    elif action == "reset_execute":
        await manager.stop()
        await asyncio.sleep(1)
        if manager.reset_all_data():
            await query.edit_message_text("🔄 All bot data has been successfully reset.")
        else:
            await query.edit_message_text("❌ Error resetting data. Check logs.")
        return

    await send_status_message(update, context)

async def post_init(application: Application):
    """Tasks to run once after the bot connects."""
    manager: TransferManager = application.bot_data['manager']
    state = manager.get_progress()
    logger.info("Bot started successfully.")
    await application.bot.send_message(ADMIN_USER_ID, f"🤖 **Bot is online!** Last known status: `{state.get('status', 'stopped').title()}`.")
    
    if state.get('status') in [TransferStatus.RUNNING.value, TransferStatus.WAITING.value]:
        logger.info("Previous state was running/waiting. Auto-resuming transfer.")
        await application.bot.send_message(ADMIN_USER_ID, "🔄 Auto-resuming previous transfer...")
        await manager.start(application.bot)

async def on_shutdown(application: Application):
    """Handles graceful shutdown of the bot."""
    logger.info("Shutdown signal received. Stopping services...")
    manager: TransferManager = application.bot_data['manager']
    db_client: MongoClient = application.bot_data['db_client']
    await manager.stop()
    await asyncio.sleep(2)
    db_client.close()
    logger.info("Database connection closed. Shutdown complete.")

def main():
    """Main function to initialize and run the bot."""
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        logger.info("MongoDB connection successful.")
    except Exception as e:
        logger.critical(f"FATAL: Could not connect to MongoDB. {e}")
        sys.exit(1)

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(on_shutdown)
        .build()
    )
    
    manager = TransferManager(client)
    application.bot_data['manager'] = manager
    application.bot_data['db_client'] = client

    admin_filter = filters.User(user_id=ADMIN_USER_ID)
    application.add_handler(CommandHandler("start", start_command, filters=admin_filter))
    application.add_handler(CommandHandler("status", status_command, filters=admin_filter))
    application.add_handler(CallbackQueryHandler(button_callback))
    
    application.run_polling()

if __name__ == '__main__':
    main()