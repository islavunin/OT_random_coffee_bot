"""
OT_random_coffee_bot
"""
#from rcbot_pygsheets import *
from tinydb_utils import *

import logging

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

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


async def add_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Save data from admin chat with /start """
    chat_id = update.message.chat.id
    if str(chat_id) == ADMIN_CHAT_ID:
        await update.message.reply_text("Welcome in admin chat!")
    else:
        #message to admin chat
        #message = f'/start message was recieved from chat {chat_id}'
        write_json('add_chat_data.json', update.to_dict())


def remove_job_if_exists(name: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Remove job with given name. Returns whether job was removed."""
    current_jobs = context.job_queue.get_jobs_by_name(name)
    if not current_jobs:
        return False
    for job in current_jobs:
        job.schedule_removal()
    return True


async def set_timer(update: Update, context: ContextTypes.DEFAULT_TYPE, poll_chat_id) -> None:
    """Add a close poll job to the queue."""
    #last_poll = get_last_poll(DB_NAME)
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
    #await update.effective_message.reply_text(text)


async def poll(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a predefined poll"""
    #chat_id = update.message.chat.id
    if str(update.message.chat.id) == ADMIN_CHAT_ID:
        chat_id = POLL_CHAT_ID
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
        with open('message.txt', 'r', encoding='UTF-8') as f:
            start_message = f.read()
        await context.bot.sendPhoto(chat_id=chat_id, photo=
            POll_IMG_URL, caption=start_message)
        message = await context.bot.send_poll(
            chat_id,
            #update.effective_chat.id,
            "Привет, будешь участвовать во встречах Random Coffee в ближайшие 2 недели? ☕️",
            questions,
            is_anonymous=False,
            allows_multiple_answers=False,
        )
        update_tinydb(DB_NAME, 'polls_data', [message.to_dict()])

        #correct timer message
        await set_timer(update, context, chat_id)


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
    #add cands for test
    add_test_cands(DB_NAME)
    #set ids
    #print(context)
    last_poll = get_last_poll(DB_NAME)
    chat_id = last_poll['chat']['id']
    message_id = last_poll['message_id']
    message = await context.bot.stop_poll(chat_id, message_id)
    #update poll status in db
    update_last_poll(DB_NAME, message.to_dict())
    #print message with pairs
    chat_message = main_message(DB_NAME)
    #await context.bot.send_message(
    #        chat_id,
    #        chat_message,
    #        #'Poll is closed!',
    #        parse_mode=ParseMode.HTML,
    #    )
    await context.bot.sendPhoto(chat_id=chat_id, photo=
        POll_IMG_URL, caption=chat_message)


async def receive_meet_result(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Receive meeting result"""
    #try: if not username:
    user = '@' + update.effective_user.username
    message = update.effective_message
    pair = parse_pair(user, str(message))
    print(pair)
    if pair:
        update_match_status(DB_NAME, pair)
    else:
        await update.message.reply_text(
            f"Будет замечательно, {user}\n, если ты уточнишь с кем прошла встреча!")


def main() -> None:
    """Run bot."""
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", add_chat))
    application.add_handler(CommandHandler("poll", poll))
    application.add_handler(CommandHandler("close_poll", close_poll))
    application.add_handler(MessageHandler(filters.Regex('#random')|filters.Regex('#rc'), receive_meet_result))
    application.add_handler(PollAnswerHandler(receive_poll_answer))

    # Run the bot until the user presses Ctrl-C
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    config = read_config('config.ini')
    BOT_TOKEN = config.get('tgbot', 'TOKEN')
    #FILE_NAME = config.get('gsheet', 'FILE_NAME')
    CLOSE_TIME_SEC = int(config.get('tgbot', 'CLOSE_TIME_SEC'))
    DB_NAME = config.get('tgbot', 'DB_NAME')
    ADMIN_CHAT_ID = config.get('tgbot', 'ADMIN_CHAT_ID')
    POLL_CHAT_ID = config.get('tgbot', 'POLL_CHAT_ID')
    POll_IMG_URL = config.get('tgbot', 'POll_IMG_URL')
    main()