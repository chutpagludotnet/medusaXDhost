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

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
    def get_user_venv_dir(user_id: int) -> Path:
        """Get user's virtual environment directory"""
        venv_dir = UserManager.get_user_dir(user_id) / 'venv'
        return venv_dir

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
    async def install_requirements(user_id: int, requirements_content: str) -> tuple[bool, str]:
        """Install requirements for a user"""
        try:
            user_dir = UserManager.get_user_dir(user_id)
            venv_dir = UserManager.get_user_venv_dir(user_id)

            logger.info(f"Installing requirements for user {user_id}")

            # Create virtual environment
            if not venv_dir.exists():
                result = subprocess.run([
                    sys.executable, '-m', 'venv', str(venv_dir)
                ], capture_output=True, text=True, timeout=300)

                if result.returncode != 0:
                    logger.error(f"Failed to create venv for user {user_id}: {result.stderr}")
                    return False, f"Failed to create virtual environment: {result.stderr}"

            # Write requirements.txt
            req_file = user_dir / 'requirements.txt'
            req_file.write_text(requirements_content)

            # Install requirements
            pip_path = venv_dir / ('Scripts' if os.name == 'nt' else 'bin') / 'pip'
            result = subprocess.run([
                str(pip_path), 'install', '-r', str(req_file)
            ], capture_output=True, text=True, timeout=600)

            if result.returncode != 0:
                logger.error(f"Failed to install requirements for user {user_id}: {result.stderr}")
                return False, f"Failed to install requirements: {result.stderr}"

            logger.info(f"Requirements installed successfully for user {user_id}")
            return True, "Requirements installed successfully!"

        except subprocess.TimeoutExpired:
            logger.error(f"Requirements installation timeout for user {user_id}")
            return False, "Installation timed out (10 minutes limit)"
        except Exception as e:
            logger.error(f"Error installing requirements for user {user_id}: {str(e)}")
            return False, f"Error during installation: {str(e)}"

    @staticmethod
    async def run_script(user_id: int, script_name: str) -> tuple[bool, str]:
        """Run a user's script"""
        try:
            scripts_dir = UserManager.get_user_scripts_dir(user_id)
            logs_dir = UserManager.get_user_logs_dir(user_id)
            venv_dir = UserManager.get_user_venv_dir(user_id)

            script_path = scripts_dir / script_name
            if not script_path.exists():
                return False, "Script not found"

            # Use virtual environment Python if available
            python_path = venv_dir / ('Scripts' if os.name == 'nt' else 'bin') / 'python'
            if not python_path.exists():
                python_path = sys.executable

            # Create log file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = logs_dir / f"{script_name}_{timestamp}.log"

            # Start process
            process_key = f"{user_id}_{script_name}"

            logger.info(f"Starting script {script_name} for user {user_id}")

            with open(log_file, 'w') as log:
                process = subprocess.Popen([
                    str(python_path), str(script_path)
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

async def log_user_action(context: ContextTypes.DEFAULT_TYPE, user_id: int, username: str, action: str, details: str = ""):
    """Log user action to the log channel"""
    if not LOG_CHANNEL_ID:
        return

    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        log_message = f"""
🔍 **User Action Log**
👤 User: {username} (`{user_id}`)
⚡ Action: {action}
📝 Details: {details}
🕐 Time: {timestamp}
        """

        await context.bot.send_message(
            chat_id=LOG_CHANNEL_ID,
            text=log_message,
            parse_mode='Markdown'
        )
        logger.info(f"Logged action: {action} for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to log user action: {e}")

# [Include all the same command handlers from the previous code]
# ... (keeping the rest of the handlers the same as in the original code)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /start command"""
    user = update.effective_user

    welcome_text = """
🚀 **Welcome to MedusaXD HOSTING!**
*The Ultimate Python Hosting Bot*

**What can this bot do?**
🔹 Host, run, and manage Python scripts right from Telegram!
🔹 Secure file storage (only you can access your scripts)
🔹 Auto-install dependencies from requirements.txt
🔹 Run scripts and track execution logs
🔹 Admin panel for user & system management

**🎯 Admin:** @medusaXD

**Get Started:**
Use /upload to begin hosting your Python script!

**Need help?** Use /help for more information.
    """

    await update.message.reply_text(welcome_text, parse_mode='Markdown')
    await log_user_action(context, user.id, user.username or "Unknown", "/start")

# ... [Include all other handlers from the original code] ...

# Error handler with better logging for worker environment
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors"""
    logger.error(f"Exception while handling an update: {context.error}", exc_info=True)

def main():
    """Main function to run the bot as a worker"""
    logger.info("🚀 Starting MedusaXD HOSTING Bot as background worker...")

    # Create application with optimized settings for worker deployment
    application = Application.builder().token(BOT_TOKEN).build()

    # Add conversation handler for upload
    upload_conv_handler = ConversationHandler(
        entry_points=[CommandHandler('upload', upload_start)],
        states={
            UPLOAD_REQUIREMENTS: [
                MessageHandler(filters.Document.ALL | filters.TEXT, handle_requirements)
            ],
            UPLOAD_FILES: [
                MessageHandler(filters.Document.ALL | filters.TEXT, handle_files)
            ],
            UPLOAD_MAIN_SCRIPT: [
                MessageHandler(filters.Document.ALL, handle_main_script)
            ],
        },
        fallbacks=[CommandHandler('cancel', cancel_upload)],
    )

    # Add all handlers
    application.add_handler(CommandHandler('start', start_command))
    application.add_handler(CommandHandler('help', help_command))
    application.add_handler(CommandHandler('ping', ping_command))
    application.add_handler(CommandHandler('info', info_command))
    application.add_handler(CommandHandler('run', run_command))
    application.add_handler(CommandHandler('stop', stop_command))
    application.add_handler(CommandHandler('delete', delete_command))
    application.add_handler(CommandHandler('logs', logs_command))
    application.add_handler(CommandHandler('edit', edit_command))
    application.add_handler(CommandHandler('admin', admin_command))
    application.add_handler(upload_conv_handler)
    application.add_handler(CallbackQueryHandler(button_callback))

    # Add error handler
    application.add_error_handler(error_handler)

    # Start the bot with polling (perfect for worker deployment)
    logger.info("✅ Bot worker is running and polling for updates!")
    application.run_polling(
        poll_interval=1.0,  # Check for updates every second
        timeout=10,         # Timeout for long polling
        bootstrap_retries=-1,  # Infinite retries on startup
        read_timeout=10,
        write_timeout=10,
        connect_timeout=10,
        pool_timeout=10
    )

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Bot stopped by user")
    except Exception as e:
        logger.error(f"💥 Bot crashed: {e}", exc_info=True)
        sys.exit(1)
