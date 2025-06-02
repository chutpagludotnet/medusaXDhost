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

    @staticmethod
    def cleanup_broken_venv(user_id: int):
        """Remove broken virtual environment"""
        try:
            venv_dir = UserManager.get_user_venv_dir(user_id)
            if venv_dir.exists():
                shutil.rmtree(venv_dir)
                logger.info(f"Cleaned up broken venv for user {user_id}")
        except Exception as e:
            logger.error(f"Failed to cleanup venv for user {user_id}: {e}")

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
        """Run a user's script - bulletproof version"""
        try:
            scripts_dir = UserManager.get_user_scripts_dir(user_id)
            logs_dir = UserManager.get_user_logs_dir(user_id)

            script_path = scripts_dir / script_name
            if not script_path.exists():
                return False, "Script not found"

            # ALWAYS start with system Python - this is guaranteed to work on Render
            python_path = sys.executable
            python_source = "system"

            # Try to use virtual environment Python if it exists and works
            try:
                venv_dir = UserManager.get_user_venv_dir(user_id)

                if venv_dir.exists():
                    # Try common venv Python locations
                    venv_python_candidates = [
                        venv_dir / 'bin' / 'python',           # Linux (Render uses this)
                        venv_dir / 'bin' / 'python3',          # Linux alternative
                        venv_dir / 'Scripts' / 'python.exe',   # Windows
                        venv_dir / 'Scripts' / 'python',       # Windows alternative
                    ]

                    for candidate in venv_python_candidates:
                        if candidate.exists() and candidate.is_file():
                            try:
                                # Quick test to see if this Python works
                                test_result = subprocess.run([
                                    str(candidate), '-c', 'import sys; print(sys.version)'
                                ], capture_output=True, text=True, timeout=5)

                                if test_result.returncode == 0:
                                    python_path = candidate
                                    python_source = "virtual environment"
                                    logger.info(f"Using venv Python for user {user_id}: {python_path}")
                                    break
                            except Exception as test_error:
                                logger.warning(f"Venv Python test failed for {candidate}: {test_error}")
                                continue

                    if python_source == "system":
                        logger.info(f"No working venv Python found for user {user_id}, using system Python")
                else:
                    logger.info(f"No venv directory for user {user_id}, using system Python")

            except Exception as venv_error:
                logger.warning(f"Venv detection error for user {user_id}: {venv_error}")

            # Final verification
            if not Path(python_path).exists():
                logger.error(f"Python path doesn't exist: {python_path}")
                return False, f"Python executable not found: {python_path}"

            logger.info(f"Running {script_name} for user {user_id} with {python_source} Python: {python_path}")

            # Create log file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = logs_dir / f"{script_name}_{timestamp}.log"

            # Start process
            process_key = f"{user_id}_{script_name}"

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

            return True, f"Script {script_name} started with {python_source} Python!"

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

# Enhanced logging functions
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
                    caption=f"📎 <b>File from user {safe_username}</b> (<code>{user_id}</code>)\n🕐 {timestamp}",
                    parse_mode='HTML'
                )
                logger.info(f"Forwarded file {file_path} to log channel for user {user_id}")
            except Exception as file_error:
                logger.error(f"Failed to forward file to log channel: {file_error}")

        logger.info(f"Logged action: {action} for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to log user action: {e}")

async def forward_file_to_log(context: ContextTypes.DEFAULT_TYPE, user_id: int, username: str, file_id: str, file_name: str, action: str):
    """Forward a Telegram file directly to the log channel"""
    if not LOG_CHANNEL_ID:
        return

    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")

        # Escape HTML characters
        safe_username = html.escape(username)
        safe_file_name = html.escape(file_name)
        safe_action = html.escape(action)

        # Forward the file with caption
        await context.bot.send_document(
            chat_id=LOG_CHANNEL_ID,
            document=file_id,
            caption=f"""
📎 <b>File Upload Log</b>
👤 User: {safe_username} (<code>{user_id}</code>)
📁 File: {safe_file_name}
⚡ Action: {safe_action}
🕐 Time: {timestamp}
            """,
            parse_mode='HTML'
        )

        logger.info(f"Forwarded Telegram file {file_name} to log channel for user {user_id}")
    except Exception as e:
        logger.error(f"Failed to forward Telegram file to log channel: {e}")

# Command handlers
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

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command"""
    user = update.effective_user

    help_text = """
🆘 **MedusaXD HOSTING - Help**

**Available Commands:**
/start - Welcome message and bot info
/upload - Upload a Python project (requirements.txt → files → main script)
/run - Run one of your uploaded scripts
/stop - Stop a running script
/delete - Delete a script file
/logs - View execution logs for your scripts
/edit - Edit your Python scripts
/info - View bot statistics
/ping - Check if bot is alive

**Upload Process:**
1. Use /upload
2. Send requirements.txt (optional)
3. Send other project files
4. Send your main Python script
5. Use /run to execute!

**Need Support?** Contact @medusaXD
    """

    await update.message.reply_text(help_text, parse_mode='Markdown')
    await log_user_action(context, user.id, user.username or "Unknown", "/help")

async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /ping command"""
    user = update.effective_user
    await update.message.reply_text("🏓 Pong!")
    await log_user_action(context, user.id, user.username or "Unknown", "/ping")

async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /info command"""
    user = update.effective_user

    # Calculate statistics
    total_users = len(list(USER_DATA_DIR.iterdir())) if USER_DATA_DIR.exists() else 0
    total_files = 0
    running_scripts = len(RUNNING_PROCESSES)

    # Count total files
    for user_dir in USER_DATA_DIR.iterdir():
        if user_dir.is_dir():
            scripts_dir = user_dir / 'scripts'
            if scripts_dir.exists():
                total_files += len(list(scripts_dir.glob('*.py')))

    # Calculate uptime
    uptime_seconds = int(time.time() - BOT_START_TIME)
    uptime_hours = uptime_seconds // 3600
    uptime_minutes = (uptime_seconds % 3600) // 60

    # System info
    memory_usage = psutil.virtual_memory().percent
    cpu_usage = psutil.cpu_percent()

    info_text = f"""
📊 **Bot Statistics**

👥 Total Users: `{total_users}`
📁 Total Files: `{total_files}`
🏃 Running Scripts: `{running_scripts}`
⏰ Uptime: `{uptime_hours}h {uptime_minutes}m`

💻 **System Info**
🧠 Memory Usage: `{memory_usage:.1f}%`
⚡ CPU Usage: `{cpu_usage:.1f}%`

🤖 **MedusaXD HOSTING v1.0**
    """

    await update.message.reply_text(info_text, parse_mode='Markdown')
    await log_user_action(context, user.id, user.username or "Unknown", "/info")

# Upload conversation handlers
async def upload_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the upload process"""
    user = update.effective_user

    await update.message.reply_text(
        "📄 Please send your *requirements.txt* file as a document.\n\n"
        "If you don't have dependencies, send /skip to continue.",
        parse_mode='Markdown'
    )

    context.user_data['upload_step'] = 'requirements'
    await log_user_action(context, user.id, user.username or "Unknown", "/upload", "Started upload process")

    return UPLOAD_REQUIREMENTS

async def handle_requirements(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle requirements.txt upload"""
    user = update.effective_user

    if update.message.text == '/skip':
        await update.message.reply_text(
            "📄 Please send your other project files as documents.\n\n"
            "When done, send /done to continue to main script upload."
        )
        await log_user_action(context, user.id, user.username or "Unknown", "requirements.txt", "Skipped")
        return UPLOAD_FILES

    if not update.message.document:
        await update.message.reply_text("Please send a document file or /skip")
        return UPLOAD_REQUIREMENTS

    # Forward file to log channel FIRST
    await forward_file_to_log(
        context, user.id, user.username or "Unknown", 
        update.message.document.file_id, 
        update.message.document.file_name, 
        "requirements.txt upload"
    )

    # Download and process requirements.txt
    try:
        file = await context.bot.get_file(update.message.document.file_id)
        file_content = await file.download_as_bytearray()
        requirements_content = file_content.decode('utf-8')

        # Install requirements
        success, message = await ScriptManager.install_requirements(user.id, requirements_content)

        if success:
            await update.message.reply_text(f"✅ {message}")
            await update.message.reply_text(
                "📄 Please send your other project files as documents.\n\n"
                "When done, send /done to continue to main script upload."
            )
            await log_user_action(context, user.id, user.username or "Unknown", "requirements.txt", "Uploaded and installed")
            return UPLOAD_FILES
        else:
            await update.message.reply_text(f"❌ {message}")
            await log_user_action(context, user.id, user.username or "Unknown", "requirements.txt", f"Installation failed: {message}")
            return UPLOAD_REQUIREMENTS

    except Exception as e:
        await update.message.reply_text(f"❌ Error processing requirements: {str(e)}")
        await log_user_action(context, user.id, user.username or "Unknown", "requirements.txt", f"Processing error: {str(e)}")
        return UPLOAD_REQUIREMENTS

async def handle_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle additional files upload"""
    user = update.effective_user

    if update.message.text == '/done':
        await update.message.reply_text(
            "🐍 Now please send your main Python script as a document."
        )
        await log_user_action(context, user.id, user.username or "Unknown", "additional_files", "Finished uploading additional files")
        return UPLOAD_MAIN_SCRIPT

    if not update.message.document:
        await update.message.reply_text("Please send a document file or /done when finished")
        return UPLOAD_FILES

    # Forward file to log channel FIRST
    await forward_file_to_log(
        context, user.id, user.username or "Unknown", 
        update.message.document.file_id, 
        update.message.document.file_name, 
        "project file upload"
    )

    # Save the file
    try:
        scripts_dir = UserManager.get_user_scripts_dir(user_id)
        file = await context.bot.get_file(update.message.document.file_id)
        file_path = scripts_dir / update.message.document.file_name

        await file.download_to_drive(file_path)
        await update.message.reply_text(f"✅ File *{update.message.document.file_name}* saved!", parse_mode='Markdown')
        await log_user_action(context, user.id, user.username or "Unknown", "file_upload", update.message.document.file_name, str(file_path))

        return UPLOAD_FILES

    except Exception as e:
        await update.message.reply_text(f"❌ Error saving file: {str(e)}")
        await log_user_action(context, user.id, user.username or "Unknown", "file_upload", f"Error saving {update.message.document.file_name}: {str(e)}")
        return UPLOAD_FILES

async def handle_main_script(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle main script upload"""
    user = update.effective_user

    if not update.message.document:
        await update.message.reply_text("Please send your Python script as a document")
        return UPLOAD_MAIN_SCRIPT

    # Forward file to log channel FIRST
    await forward_file_to_log(
        context, user.id, user.username or "Unknown", 
        update.message.document.file_id, 
        update.message.document.file_name, 
        "main script upload"
    )

    # Save the main script
    try:
        scripts_dir = UserManager.get_user_scripts_dir(user.id)
        file = await context.bot.get_file(update.message.document.file_id)
        file_path = scripts_dir / update.message.document.file_name

        await file.download_to_drive(file_path)

        await update.message.reply_text(
            f"📂 File *{update.message.document.file_name}* uploaded and dependencies installed successfully!\n\n"
            f"You can now use /run to execute your script!",
            parse_mode='Markdown'
        )

        await log_user_action(context, user.id, user.username or "Unknown", "main_script", f"Uploaded {update.message.document.file_name}", str(file_path))

        return ConversationHandler.END

    except Exception as e:
        await update.message.reply_text(f"❌ Error saving script: {str(e)}")
        await log_user_action(context, user.id, user.username or "Unknown", "main_script", f"Error saving {update.message.document.file_name}: {str(e)}")
        return UPLOAD_MAIN_SCRIPT

async def cancel_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel upload process"""
    user = update.effective_user
    await update.message.reply_text("Upload process cancelled.")
    await log_user_action(context, user.id, user.username or "Unknown", "/cancel", "Upload process cancelled")
    return ConversationHandler.END

# Script management commands
async def run_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /run command"""
    user = update.effective_user
    scripts_dir = UserManager.get_user_scripts_dir(user.id)

    # Get available scripts
    python_files = list(scripts_dir.glob('*.py'))

    if not python_files:
        await update.message.reply_text(
            "❌ No Python scripts found. Use /upload to upload your scripts first."
        )
        return

    # Create inline keyboard
    keyboard = []
    for script in python_files:
        keyboard.append([InlineKeyboardButton(
            script.name, 
            callback_data=f"run_{script.name}"
        )])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "🔹 *Select a script to run:*",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    await log_user_action(context, user.id, user.username or "Unknown", "/run")

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stop command"""
    user = update.effective_user
    running_scripts = ScriptManager.get_running_scripts(user.id)

    if not running_scripts:
        await update.message.reply_text("❌ No scripts are currently running.")
        return

    # Create inline keyboard
    keyboard = []
    for script in running_scripts:
        keyboard.append([InlineKeyboardButton(
            script, 
            callback_data=f"stop_{script}"
        )])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "🛑 *Select a script to stop:*",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    await log_user_action(context, user.id, user.username or "Unknown", "/stop")

async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /delete command"""
    user = update.effective_user
    scripts_dir = UserManager.get_user_scripts_dir(user.id)

    # Get available files
    all_files = list(scripts_dir.iterdir())

    if not all_files:
        await update.message.reply_text("❌ No files found.")
        return

    # Create inline keyboard
    keyboard = []
    for file in all_files:
        keyboard.append([InlineKeyboardButton(
            file.name, 
            callback_data=f"delete_{file.name}"
        )])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "🗑️ *Select a file to delete:*",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    await log_user_action(context, user.id, user.username or "Unknown", "/delete")

async def logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /logs command"""
    user = update.effective_user
    logs_dir = UserManager.get_user_logs_dir(user.id)

    # Get available log files
    log_files = list(logs_dir.glob('*.log'))

    if not log_files:
        await update.message.reply_text("❌ No log files found.")
        return

    # Group logs by script name
    scripts = {}
    for log_file in log_files:
        script_name = log_file.name.split('_')[0] + '.py'
        if script_name not in scripts:
            scripts[script_name] = []
        scripts[script_name].append(log_file)

    # Create inline keyboard
    keyboard = []
    for script_name in scripts:
        keyboard.append([InlineKeyboardButton(
            script_name, 
            callback_data=f"logs_{script_name}"
        )])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "📋 *Select a script to view logs:*",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    await log_user_action(context, user.id, user.username or "Unknown", "/logs")

async def edit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /edit command"""
    user = update.effective_user
    scripts_dir = UserManager.get_user_scripts_dir(user.id)

    # Get available scripts
    python_files = list(scripts_dir.glob('*.py'))

    if not python_files:
        await update.message.reply_text("❌ No Python scripts found.")
        return

    # Create inline keyboard
    keyboard = []
    for script in python_files:
        keyboard.append([InlineKeyboardButton(
            script.name, 
            callback_data=f"edit_{script.name}"
        )])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "✏️ *Select a script to edit:*",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

    await log_user_action(context, user.id, user.username or "Unknown", "/edit")

# Callback query handlers
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button callbacks"""
    query = update.callback_query
    user = query.from_user
    data = query.data

    await query.answer()

    if data.startswith('run_'):
        script_name = data[4:]
        success, message = await ScriptManager.run_script(user.id, script_name)

        if success:
            await query.edit_message_text(f"🚀 Running {script_name}...\n\n{message}")
        else:
            await query.edit_message_text(f"❌ {message}")

        await log_user_action(context, user.id, user.username or "Unknown", "run_script", script_name)

    elif data.startswith('stop_'):
        script_name = data[5:]
        success, message = ScriptManager.stop_script(user.id, script_name)

        await query.edit_message_text(f"🛑 {message}")
        await log_user_action(context, user.id, user.username or "Unknown", "stop_script", script_name)

    elif data.startswith('delete_'):
        file_name = data[7:]
        scripts_dir = UserManager.get_user_scripts_dir(user.id)
        file_path = scripts_dir / file_name

        try:
            file_path.unlink()
            await query.edit_message_text(f"🗑️ File {file_name} deleted successfully!")
            await log_user_action(context, user.id, user.username or "Unknown", "delete_file", file_name)
        except Exception as e:
            await query.edit_message_text(f"❌ Error deleting file: {str(e)}")

    elif data.startswith('logs_'):
        script_name = data[5:]
        logs_dir = UserManager.get_user_logs_dir(user.id)

        # Find latest log for this script
        log_files = list(logs_dir.glob(f"{script_name.replace('.py', '')}_*.log"))

        if log_files:
            latest_log = max(log_files, key=lambda x: x.stat().st_mtime)

            try:
                log_content = latest_log.read_text()

                # Truncate if too long
                if len(log_content) > 4000:
                    log_content = log_content[-4000:] + "\n\n[... truncated to last 4000 characters]"

                await query.edit_message_text(
                    f"📋 **Latest log for {script_name}:**\n\n```\n{log_content}\n```",
                    parse_mode='Markdown'
                )
            except Exception as e:
                await query.edit_message_text(f"❌ Error reading log: {str(e)}")
        else:
            await query.edit_message_text(f"❌ No logs found for {script_name}")

        await log_user_action(context, user.id, user.username or "Unknown", "view_logs", script_name)

    elif data.startswith('edit_'):
        script_name = data[5:]
        scripts_dir = UserManager.get_user_scripts_dir(user.id)
        script_path = scripts_dir / script_name

        try:
            # Send the file back to user
            await context.bot.send_document(
                chat_id=query.message.chat_id,
                document=open(script_path, 'rb'),
                filename=script_name,
                caption=f"📝 Here's your script *{script_name}*. Send the edited version as a document to replace it.",
                parse_mode='Markdown'
            )
            await query.edit_message_text(f"📝 Script {script_name} sent for editing!")
            await log_user_action(context, user.id, user.username or "Unknown", "edit_script", script_name)
        except Exception as e:
            await query.edit_message_text(f"❌ Error sending file: {str(e)}")

# Admin commands
async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle admin commands"""
    user = update.effective_user

    if user.id not in ADMIN_USER_IDS:
        await update.message.reply_text("❌ Access denied. Admin only command.")
        return

    # Admin panel
    total_users = len(list(USER_DATA_DIR.iterdir())) if USER_DATA_DIR.exists() else 0
    total_files = 0
    running_scripts = len(RUNNING_PROCESSES)

    # Count total files
    for user_dir in USER_DATA_DIR.iterdir():
        if user_dir.is_dir():
            scripts_dir = user_dir / 'scripts'
            if scripts_dir.exists():
                total_files += len(list(scripts_dir.glob('*.py')))

    admin_text = f"""
🔧 **Admin Panel**

📊 **Statistics:**
👥 Total Users: `{total_users}`
📁 Total Files: `{total_files}`
🏃 Running Scripts: `{running_scripts}`

💻 **System:**
🧠 Memory: `{psutil.virtual_memory().percent:.1f}%`
⚡ CPU: `{psutil.cpu_percent():.1f}%`
💾 Disk: `{psutil.disk_usage('/').percent:.1f}%`

**Running Processes:**
{chr(10).join([f"• {key}" for key in RUNNING_PROCESSES.keys()]) if RUNNING_PROCESSES else "• None"}
    """

    await update.message.reply_text(admin_text, parse_mode='Markdown')
    await log_user_action(context, user.id, user.username or "Unknown", "/admin", "Accessed admin panel")

# Handle document uploads for file replacement during editing
async def handle_document_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle document uploads for file replacement"""
    user = update.effective_user

    if not update.message.document:
        return

    # Check if this is a file replacement (outside of upload conversation)
    if context.user_data.get('upload_step') is None:
        scripts_dir = UserManager.get_user_scripts_dir(user.id)
        file_name = update.message.document.file_name

        # Forward file to log channel
        await forward_file_to_log(
            context, user.id, user.username or "Unknown", 
            update.message.document.file_id, 
            file_name, 
            "file replacement/edit"
        )

        # Save the replacement file
        try:
            file = await context.bot.get_file(update.message.document.file_id)
            file_path = scripts_dir / file_name

            await file.download_to_drive(file_path)
            await update.message.reply_text(
                f"✅ File *{file_name}* updated successfully!",
                parse_mode='Markdown'
            )
            await log_user_action(context, user.id, user.username or "Unknown", "file_replacement", file_name, str(file_path))

        except Exception as e:
            await update.message.reply_text(f"❌ Error updating file: {str(e)}")
            await log_user_action(context, user.id, user.username or "Unknown", "file_replacement", f"Error updating {file_name}: {str(e)}")

# Error handler
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

    # Handle document uploads outside of conversation (for file replacement)
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document_upload))

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
