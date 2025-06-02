import os
import asyncio
import subprocess
import sys
import shutil
import logging
import time
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import tempfile
import psutil
import html

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters, ConversationHandler
)

# Configure logging for worker environment
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),  # Output to stdout for Render logs
        logging.FileHandler('bot.log')     # Also save to file
    ]
)
logger = logging.getLogger(__name__)

# Bot configuration from environment variables
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_USER_IDS = [int(x) for x in os.getenv('ADMIN_USER_IDS', '').split(',') if x]
LOG_CHANNEL_ID = os.getenv('LOG_CHANNEL_ID')

# Validate required environment variables
if not BOT_TOKEN:
    logger.error("❌ BOT_TOKEN environment variable not set!")
    sys.exit(1)

logger.info("🤖 Bot configuration loaded successfully")

# Bot start time for uptime calculation
BOT_START_TIME = time.time()

# User data storage
USER_DATA_DIR = Path('user_data')
USER_DATA_DIR.mkdir(exist_ok=True)

# Running processes
RUNNING_PROCESSES: Dict[str, subprocess.Popen] = {}

# Conversation states
UPLOAD_REQUIREMENTS, UPLOAD_FILES, UPLOAD_MAIN_SCRIPT = range(3)

class UserManager:
    """Manage user data and file isolation"""

    @staticmethod
    def get_user_dir(user_id: int) -> Path:
        """Get user's isolated directory"""
        user_dir = USER_DATA_DIR / str(user_id)
        user_dir.mkdir(exist_ok=True)
        return user_dir

    @staticmethod
    def get_user_scripts_dir(user_id: int) -> Path:
        """Get user's scripts directory"""
        scripts_dir = UserManager.get_user_dir(user_id) / 'scripts'
        scripts_dir.mkdir(exist_ok=True)
        return scripts_dir

    @staticmethod
    def get_user_logs_dir(user_id: int) -> Path:
        """Get user's logs directory"""
        logs_dir = UserManager.get_user_dir(user_id) / 'logs'
        logs_dir.mkdir(exist_ok=True)
        return logs_dir

class ScriptManager:
    """Manage script execution and logging"""

    @staticmethod
    async def install_requirements(requirements_content: str) -> tuple[bool, str]:
        """Install requirements globally (not per-user)"""
        try:
            # Write requirements.txt to a temp file
            with tempfile.NamedTemporaryFile('w+', delete=False) as tmp_req:
                tmp_req.write(requirements_content)
                tmp_req_path = tmp_req.name

            # Install requirements globally (or in the main venv)
            result = subprocess.run([
                sys.executable, '-m', 'pip', 'install', '-r', tmp_req_path
            ], capture_output=True, text=True, timeout=600)

            os.unlink(tmp_req_path)

            if result.returncode != 0:
                logger.error(f"Failed to install requirements: {result.stderr}")
                return False, f"Failed to install requirements: {result.stderr}"

            logger.info("Requirements installed successfully!")
            return True, "Requirements installed successfully!"

        except subprocess.TimeoutExpired:
            logger.error("Requirements installation timeout")
            return False, "Installation timed out (10 minutes limit)"
        except Exception as e:
            logger.error(f"Error installing requirements: {str(e)}")
            return False, f"Error during installation: {str(e)}"

    @staticmethod
    async def run_script(user_id: int, script_name: str) -> tuple[bool, str]:
        """Run a user's script using system Python"""
        try:
            scripts_dir = UserManager.get_user_scripts_dir(user_id)
            logs_dir = UserManager.get_user_logs_dir(user_id)

            script_path = scripts_dir / script_name
            if not script_path.exists():
                return False, "Script not found"

            python_path = sys.executable  # Always use system Python

            # Create log file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = logs_dir / f"{script_name}_{timestamp}.log"

            process_key = f"{user_id}_{script_name}"

            logger.info(f"Starting script {script_name} for user {user_id}")

            with open(log_file, 'w') as log:
                process = subprocess.Popen([
                    python_path, str(script_path)
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
                cwd=str(scripts_dir),
                text=True
                )

                RUNNING_PROCESSES[process_key] = process

            return True, f"Script {script_name} started successfully!"

        except Exception as e:
            logger.error(f"Error running script {script_name} for user {user_id}: {str(e)}")
            return False, f"Error running script: {str(e)}"

    @staticmethod
    def get_running_scripts(user_id: int) -> List[str]:
        """Get list of running scripts for a user"""
        running = []
        user_prefix = f"{user_id}_"

        for key, process in list(RUNNING_PROCESSES.items()):
            if key.startswith(user_prefix):
                if process.poll() is None:  # Still running
                    script_name = key.replace(user_prefix, "")
                    running.append(script_name)
                else:  # Process finished
                    del RUNNING_PROCESSES[key]

        return running

    @staticmethod
    def stop_script(user_id: int, script_name: str) -> tuple[bool, str]:
        """Stop a running script"""
        process_key = f"{user_id}_{script_name}"

        if process_key in RUNNING_PROCESSES:
            process = RUNNING_PROCESSES[process_key]
            try:
                logger.info(f"Stopping script {script_name} for user {user_id}")
                process.terminate()
                process.wait(timeout=10)
                del RUNNING_PROCESSES[process_key]
                return True, f"Script {script_name} stopped successfully!"
            except subprocess.TimeoutExpired:
                process.kill()
                del RUNNING_PROCESSES[process_key]
                return True, f"Script {script_name} force-killed!"
            except Exception as e:
                logger.error(f"Error stopping script {script_name} for user {user_id}: {str(e)}")
                return False, f"Error stopping script: {str(e)}"
        else:
            return False, "Script not running or not found"

# Enhanced logging functions with HTML parsing to avoid markdown issues
async def log_user_action(context: ContextTypes.DEFAULT_TYPE, user_id: int, username: str, action: str, details: str = "", file_path: str = None):
    """Log user action to the log channel with optional file forwarding"""
    if not LOG_CHANNEL_ID:
        return

    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")

        # Escape HTML characters to prevent parsing issues
        safe_username = html.escape(username)
        safe_action = html.escape(action)
        safe_details = html.escape(details)

        log_message = f"""
🔍 <b>User Action Log</b>
👤 User: {safe_username} (<code>{user_id}</code>)
⚡ Action: {safe_action}
📝 Details: {safe_details}
🕐 Time: {timestamp}
        """

        # Send text log first using HTML parse mode
        await context.bot.send_message(
            chat_id=LOG_CHANNEL_ID,
            text=log_message,
            parse_mode='HTML'
        )

        # If there's a file to forward, send it too
        if file_path and Path(file_path).exists():
            try:
                await context.bot.send_document(
                    chat_id=LOG_CHANNEL_ID,
                    document=open(file_path, 'rb'),
                    filename=Path(file_path).name,
                    caption=f"📎 <b>File from user {safe_username}</b> (<code>{user_id}</code>)\n",
                    parse_mode='HTML'
                )
            except Exception as e:
                logger.error(f"Failed to send document to log channel: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to log user action: {str(e)}")

# --- Conversation Handlers ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Welcome! Send /upload to upload your Python script and requirements."
    )

async def upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Please send your requirements.txt file (as text or file). If your script doesn't need extra packages, send 'skip'."
    )
    return UPLOAD_REQUIREMENTS

async def upload_requirements(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.full_name

    if update.message.text and update.message.text.strip().lower() == "skip":
        context.user_data['requirements'] = None
        await update.message.reply_text("Now send your Python script files (as documents). Send /done when finished.")
        return UPLOAD_FILES

    # If requirements.txt is sent as a file
    if update.message.document:
        file = await update.message.document.get_file()
        requirements_content = await file.download_as_bytearray()
        requirements_content = requirements_content.decode('utf-8')
    else:
        requirements_content = update.message.text

    context.user_data['requirements'] = requirements_content
    await update.message.reply_text("Now send your Python script files (as documents). Send /done when finished.")
    return UPLOAD_FILES

async def upload_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    scripts_dir = UserManager.get_user_scripts_dir(user_id)
    if 'uploaded_files' not in context.user_data:
        context.user_data['uploaded_files'] = []

    if update.message.document:
        file = await update.message.document.get_file()
        filename = update.message.document.file_name
        file_path = scripts_dir / filename
        await file.download_to_drive(str(file_path))
        context.user_data['uploaded_files'].append(filename)
        await update.message.reply_text(f"Uploaded: {filename}\nSend more files or /done if finished.")
    else:
        await update.message.reply_text("Please send your script as a document (not as text).")
    return UPLOAD_FILES

async def upload_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    uploaded_files = context.user_data.get('uploaded_files', [])
    if not uploaded_files:
        await update.message.reply_text("You haven't uploaded any files. Please send at least one Python script.")
        return UPLOAD_FILES

    keyboard = [[InlineKeyboardButton(f"{fname}", callback_data=fname)] for fname in uploaded_files]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Which file is your main script (the one to run)?",
        reply_markup=reply_markup
    )
    return UPLOAD_MAIN_SCRIPT

async def select_main_script(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    main_script = query.data
    context.user_data['main_script'] = main_script

    user_id = query.from_user.id
    username = query.from_user.username or query.from_user.full_name

    # Install requirements if provided
    requirements = context.user_data.get('requirements')
    if requirements:
        await query.edit_message_text("Installing requirements, please wait...")
        ok, msg = await ScriptManager.install_requirements(requirements)
        if not ok:
            await query.edit_message_text(f"❌ {msg}")
            return ConversationHandler.END

    await query.edit_message_text("Starting your script...")
    ok, msg = await ScriptManager.run_script(user_id, main_script)
    if ok:
        await query.edit_message_text(f"✅ {msg}\nUse /status to check running scripts or /logs to get logs.")
    else:
        await query.edit_message_text(f"❌ {msg}")

    # Log the action
    await log_user_action(context, user_id, username, "Uploaded and started script", f"Main: {main_script}")

    return ConversationHandler.END

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    running = ScriptManager.get_running_scripts(user_id)
    if running:
        await update.message.reply_text("🟢 Running scripts:\n" + "\n".join(running))
    else:
        await update.message.reply_text("No scripts are currently running.")

async def logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    logs_dir = UserManager.get_user_logs_dir(user_id)
    log_files = sorted(logs_dir.glob("*.log"), reverse=True)
    if not log_files:
        await update.message.reply_text("No logs found.")
        return

    latest_log = log_files[0]
    await update.message.reply_document(document=InputFile(str(latest_log)), filename=latest_log.name)

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    running = ScriptManager.get_running_scripts(user_id)
    if not running:
        await update.message.reply_text("No running scripts to stop.")
        return

    keyboard = [[InlineKeyboardButton(f"{fname}", callback_data=fname)] for fname in running]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "Which script do you want to stop?",
        reply_markup=reply_markup
    )

async def stop_script_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    script_name = query.data
    user_id = query.from_user.id
    ok, msg = ScriptManager.stop_script(user_id, script_name)
    if ok:
        await query.edit_message_text(f"🛑 {msg}")
    else:
        await query.edit_message_text(f"❌ {msg}")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Operation cancelled.")
    return ConversationHandler.END

def main():
    application = Application.builder().token(BOT_TOKEN).build()

    upload_conv = ConversationHandler(
        entry_points=[CommandHandler("upload", upload_command)],
        states={
            UPLOAD_REQUIREMENTS: [MessageHandler(filters.TEXT | filters.Document.ALL, upload_requirements)],
            UPLOAD_FILES: [
                MessageHandler(filters.Document.ALL, upload_files),
                CommandHandler("done", upload_done)
            ],
            UPLOAD_MAIN_SCRIPT: [CallbackQueryHandler(select_main_script)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(upload_conv)
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("logs", logs_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CallbackQueryHandler(stop_script_callback, pattern=".*"))

    logger.info("Bot started. Waiting for events...")
    application.run_polling()

if __name__ == "__main__":
    main()
