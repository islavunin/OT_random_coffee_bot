"""
OT_random_coffee_bot
"""

from time import time, strftime, localtime
import logging
#import pathlib
#import os
from pathlib import Path
from datetime import time as dtime
from pytz import timezone

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    PollAnswerHandler,
    filters,
)

from tinydb_utils import (
    read_config,
    update_tinydb,
    update_poll_chat_id,
    update_last_poll,
    get_last_poll,
    remove_answer,
    parse_pair,
    update_match_status,
    main_message,
    add_test_cands
    #make_stat_plot
)

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

THIS_FOLDER = Path(__file__).parent.resolve()
CONFIG_PATH =  THIS_FOLDER / 'config.ini'  #os.path.join(os.getcwd(), 'config.ini')
config = read_config(CONFIG_PATH)
BOT_TOKEN = config.get('tgbot', 'TOKEN')
#FILE_NAME = config.get('gsheet', 'FILE_NAME')
CLOSE_TIME_SEC = int(config.get('tgbot', 'CLOSE_TIME_SEC'))
DB_NAME = config.get('tgbot', 'DB_NAME')
ADMIN_CHAT_ID = config.get('tgbot', 'ADMIN_CHAT_ID')
POll_IMG_URL = THIS_FOLDER / config.get('tgbot', 'POll_IMG_URL')
#EXTRA_CAND = config.get('tgbot', 'EXTRA_CANDIDATE')


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Inform user about what this bot can do"""
    await update.message.reply_text(
        "If you want to start random coffee activity you should first"
        "select /add_chat to add chat you want to your config"
        "after that you will receive a message with comand and chat id"
        "if you commit the command you you will be able to start random coffee poll"
        "you could do it at the choosen chat with /poll command"
        "after /poll you can specify the time for which this poll will be opened"
        "when the poll will be closed bot send message to the chat with random pairs"
    )


#async def test(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
#    """Inform user about what this bot can do"""
#    make_stat_plot(DB_NAME)
#    await update.message.reply_photo(
#        "out.png",
#        "<b>Статистика встреч Random Coffee</b>",
#        parse_mode='html'
#    )
#    pathlib.Path("out.png").unlink(missing_ok=False)


async def add_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Save data from admin chat with /start """
    #add topic info: "message_thread_id"
    #print(update)
    chat_id = update.message.chat.id
    try:
        msg_thread_id = update.message.reply_to_message.message_thread_id
    except AttributeError:
        msg_thread_id = "" #"General"
    #print(msg_thread_id)
    if str(chat_id) == ADMIN_CHAT_ID:
        await update.message.reply_text("Welcome in admin chat!")
    else:
        #message to admin chat
        await context.bot.send_message(
            ADMIN_CHAT_ID,
            "Someone is trying to update poll chat id!\n"
            f"If it is OK type command /update_chat_id {chat_id} {msg_thread_id}",
            parse_mode=ParseMode.HTML,
    )


async def update_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Save data from admin chat with /start """
    poll_chat_id = context.args[0]
    msg_thread_id = context.args[1]
    chat_id = update.message.chat.id
    if str(chat_id) == ADMIN_CHAT_ID:
        update_poll_chat_id(CONFIG_PATH, poll_chat_id, msg_thread_id)
        msg = f"Poll chat id was updated to {poll_chat_id} thread {msg_thread_id}!"
        await update.message.reply_text(msg)


async def post_daily_message(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Save data from admin chat with /start """
    await context.bot.send_message(
        ADMIN_CHAT_ID,
        "Good morning, I'm on duty!",
        parse_mode=ParseMode.HTML,
    )


def remove_job_if_exists(name: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Remove job with given name. Returns whether job was removed."""
    current_jobs = context.job_queue.get_jobs_by_name(name)
    if not current_jobs:
        return False
    for job in current_jobs:
        job.schedule_removal()
    return True


async def set_timer(context: ContextTypes.DEFAULT_TYPE, poll_chat_id) -> None:
    """Add a close poll job to the queue."""
    admin_chat_id = ADMIN_CHAT_ID
    chat_id = poll_chat_id
    close_datetime = CLOSE_TIME_SEC
    job_removed = remove_job_if_exists(str(chat_id), context)
    context.job_queue.run_once(close_poll_sch, close_datetime, chat_id=chat_id, name=str(chat_id))
    text = "Close poll timer successfully set!"
    if job_removed:
        text += " Old one was removed."
    #correct to admin chat
    timer_time = strftime('%d.%m.%Y %H:%m', localtime(time() + close_datetime))
    await context.bot.send_message(
        admin_chat_id,
        f"Timer successfuly set on {timer_time}!",
        parse_mode=ParseMode.HTML,
    )


async def poll(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a predefined poll"""
    if str(update.message.chat.id) == ADMIN_CHAT_ID:
        conf = read_config(CONFIG_PATH)
        chat_id = conf.get('tgbot', 'POLL_CHAT_ID')
        msg_thread_id = conf.get('tgbot', 'msg_thread_id')
        questions = ["Да", "Не в этот раз"]
        try:
            last_poll = get_last_poll(DB_NAME)
            if not last_poll['poll']['is_closed']:
                await update.message.reply_text(
                "Please finish previous poll!"
            )
                return
        except (KeyError, AttributeError):
            pass
        #send poll to chat
        with open(THIS_FOLDER / 'message.txt', 'r', encoding='UTF-8') as f:
            start_message = f.read()
        await context.bot.sendPhoto(chat_id=chat_id, photo=
            POll_IMG_URL, caption=start_message, message_thread_id=msg_thread_id, parse_mode='html')
        message = await context.bot.send_poll(
            chat_id,
            #update.effective_chat.id,
            "Привет, будешь участвовать во встречах Random Coffee в ближайшие 2 недели? ☕️",
            questions,
            is_anonymous=False,
            allows_multiple_answers=False,
            message_thread_id=msg_thread_id
        )
        update_tinydb(DB_NAME, 'polls_data', [message.to_dict()])
        #correct timer message
        try:
            if context.args[0] == 'test':
                add_test_cands(DB_NAME)
        except IndexError:
            pass
        await set_timer(context, chat_id)
        #add cands for test


async def receive_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Receive users poll vote"""
    answer = update.poll_answer
    #answered_poll = answer.poll_id
    if answer["option_ids"]:
        update_tinydb(DB_NAME, 'answers_table', [answer.to_dict()])
    else:
        #to admin chat
        await context.bot.send_message(
            ADMIN_CHAT_ID,
            "Someone retract his voice!",
            parse_mode=ParseMode.HTML,
            )
        #delete answer
        remove_answer(DB_NAME, answer['user']['id'], answer['poll_id'])


async def close_poll_sch(context: ContextTypes.DEFAULT_TYPE):
    """Close the poll by job_query callback"""
    await close_poll(update=False, context=context)


async def close_poll(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Close the poll"""
    #set ids
    msg_thread_id = read_config(CONFIG_PATH).get('tgbot', 'msg_thread_id')
    last_poll = get_last_poll(DB_NAME)
    chat_id = last_poll['chat']['id']
    message_id = last_poll['message_id']
    message = await context.bot.stop_poll(chat_id, message_id)
    #update poll status in db
    update_last_poll(DB_NAME, message.to_dict())
    #print message with pairs
    chat_message = main_message(DB_NAME)
    await context.bot.sendPhoto(chat_id=chat_id, photo=
        POll_IMG_URL, caption=chat_message, message_thread_id=msg_thread_id, parse_mode='html')


async def receive_meet_result(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Receive meeting result"""
    #try: if not username:
    user = '@' + update.effective_user.username
    message = update.effective_message
    pair = parse_pair(user, str(message))
    if pair[1]:
        update_match_status(DB_NAME, pair)
    else:
        await update.message.reply_text(
            f"Будет замечательно, {user}, если ты уточнишь, с кем прошла встреча!")


def main() -> None:
    """Run bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(BOT_TOKEN).build()
    #application.add_handler(CommandHandler("test", test))
    application.add_handler(CommandHandler("add_chat", add_chat))
    application.add_handler(CommandHandler("update_chat_id", update_chat_id))
    application.add_handler(CommandHandler("poll", poll))
    application.add_handler(CommandHandler("close_poll", close_poll))
    hashtag_filter = filters.Regex('#random')|filters.Regex('#rc')
    application.add_handler(MessageHandler(hashtag_filter, receive_meet_result))
    application.add_handler(PollAnswerHandler(receive_poll_answer))
    #post daily message to admin chat if bot will be still running
    dt = dtime(hour=10, tzinfo=timezone('Europe/Moscow'))
    application.job_queue.run_daily(post_daily_message, dt, name=str(ADMIN_CHAT_ID))
    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
