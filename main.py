import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters, CallbackQueryHandler
from sqlalchemy import create_engine, text
import pandas as pd

from dags.load_news_data import TELEGRAM_TOKEN

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Database connection
DB_URI = 'postgresql://airflow:airflow@scrape-postgres-1:5432/airflow'
engine = create_engine(DB_URI)


# Bot functions
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        ['🔍 Latest Insights', '📊 Trending Topics'],
        ['📚 Scholarly Articles', '💬 Reddit Highlights'],
        ['🗞️ News Summary', '🤖 About']
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

    await update.message.reply_text(
        "Hello! I'm your personal content intelligence assistant. I analyze content from various sources and provide insights based on data. What would you like to know?",
        reply_markup=reply_markup
    )


async def latest_insights(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with engine.connect() as conn:
        query = text("""
        SELECT * FROM content_insights 
        WHERE generated_at >= CURRENT_DATE - INTERVAL '1 day'
        ORDER BY score DESC LIMIT 5
        """)
        insights_df = pd.read_sql(query, conn)

    if insights_df.empty:
        await update.message.reply_text("No recent insights found. Check back later!")
        return

    await update.message.reply_text("🔍 *LATEST INSIGHTS* 🔍\n\nHere are my latest discoveries:", parse_mode='Markdown')

    for _, insight in insights_df.iterrows():
        message = f"""
*{insight['insight_type'].upper()}*: {insight['title']}

{insight['description']}

Relevance Score: {insight['score']:.2f}
        """

        # Add buttons for follow-up actions
        keyboard = [
            [InlineKeyboardButton("Similar Content", callback_data=f"similar_{insight['id']}")],
            [InlineKeyboardButton("Explore Topic", callback_data=f"explore_{insight['insight_type']}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(message, reply_markup=reply_markup, parse_mode='Markdown')


async def trending_topics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    with engine.connect() as conn:
        query = text("""
        SELECT * FROM content_insights 
        WHERE insight_type = 'trending_topic'
        ORDER BY generated_at DESC, score DESC LIMIT 5
        """)
        topics_df = pd.read_sql(query, conn)

    if topics_df.empty:
        await update.message.reply_text("No trending topics found currently.")
        return

    message = "📊 *TRENDING TOPICS* 📊\n\nHere's what's generating buzz across platforms:\n\n"

    for _, topic in topics_df.iterrows():
        message += f"• *{topic['title']}*: {topic['description']}\n\n"

    await update.message.reply_text(message, parse_mode='Markdown')


async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data

    if data.startswith("similar_"):
        insight_id = data.split("_")[1]
        # Look up related content
        await query.message.reply_text(f"Finding similar content to insight #{insight_id}...")
        # Implement lookup logic here

    elif data.startswith("explore_"):
        topic_type = data.split("_")[1]
        await query.message.reply_text(f"Exploring more about {topic_type}...")
        # Implement topic exploration here


async def daily_update(context: ContextTypes.DEFAULT_TYPE):
    # This would be scheduled to run daily
    with engine.connect() as conn:
        query = text("""
        SELECT * FROM content_insights 
        WHERE generated_at >= CURRENT_DATE
        ORDER BY score DESC LIMIT 7
        """)
        insights_df = pd.read_sql(query, conn)

    if insights_df.empty:
        return

    message = "🤖 *JARVIS DAILY BRIEFING* 🤖\n\nHere's your summary of today's most relevant content:\n\n"

    for _, insight in insights_df.iterrows():
        message += f"• *{insight['title']}*: {insight['description']}\n\n"

    message += "\nAsk me for more details on any topic!"

    # Send to subscribed users
    await context.bot.send_message(chat_id='YOUR_CHAT_ID', text=message, parse_mode='Markdown')


# Main function to start the bot
def main():
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.Regex(r"🔍 Latest Insights"), latest_insights))
    application.add_handler(MessageHandler(filters.Regex(r"📊 Trending Topics"), trending_topics))
    application.add_handler(CallbackQueryHandler(handle_button))

    # Start the Bot
    application.run_polling()


if __name__ == '__main__':
    main()
