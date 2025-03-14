# loader/telegram_bot.py
import logging
import psycopg2
import telegram
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from typing import List, Dict, Any
import os
from datetime import datetime, timedelta


class NewsBot:
    """Telegram bot for sending news notifications to users"""

    def __init__(self, token: str, db_config: Dict[str, Any]):
        self.logger = logging.getLogger("NewsBot")
        self.db_config = db_config

        # Initialize Telegram bot
        self.bot = telegram.Bot(token=token)
        self.updater = Updater(token=token, use_context=True)
        self.dispatcher = self.updater.dispatcher

        # Register handlers
        self._register_handlers()

    def _register_handlers(self):
        """Register command and message handlers"""
        self.dispatcher.add_handler(CommandHandler("start", self._start_command))
        self.dispatcher.add_handler(CommandHandler("help", self._help_command))
        self.dispatcher.add_handler(CommandHandler("latest", self._latest_command))
        self.dispatcher.add_handler(CommandHandler("science", self._science_command))
        self.dispatcher.add_handler(CommandHandler("crypto", self._crypto_command))
        self.dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, self._handle_message))

    def _start_command(self, update, context):
        """Handle /start command"""
        update.message.reply_text(
            "Welcome to your personalized news bot! 📰\n\n"
            "I'll send you curated updates on science news, cryptocurrency, and topics relevant to you.\n\n"
            "Commands:\n"
            "/latest - Get latest high-relevance news\n"
            "/science - Get latest science news\n"
            "/crypto - Get cryptocurrency updates\n"
            "/help - Show this help message"
        )

    def _help_command(self, update, context):
        """Handle /help command"""
        update.message.reply_text(
            "Available commands:\n"
            "/latest - Get latest high-relevance news\n"
            "/science - Get latest science news\n"
            "/crypto - Get cryptocurrency updates\n"
            "/help - Show this help message"
        )

    def _latest_command(self, update, context):
        """Handle /latest command"""
        news_items = self._get_latest_news(limit=5)
        if news_items:
            update.message.reply_text("📰 Here are your latest updates:")
            for item in news_items:
                message = f"*{item['title']}*\n\n{item['summary']}\n\n[Read more]({item['url']})"
                update.message.reply_text(message, parse_mode=telegram.ParseMode.MARKDOWN)
        else:
            update.message.reply_text("No recent news found.")

    def _science_command(self, update, context):
        """Handle /science command"""
        news_items = self._get_category_news("science", limit=3)
        if news_items:
            update.message.reply_text("🔬 Here are the latest science updates:")
            for item in news_items:
                message = f"*{item['title']}*\n\n{item['summary']}\n\n[Read more]({item['url']})"
                update.message.reply_text(message, parse_mode=telegram.ParseMode.MARKDOWN)
        else:
            update.message.reply_text("No recent science news found.")

    def _crypto_command(self, update, context):
        """Handle /crypto command"""
        news_items = self._get_category_news("cryptocurrency", limit=3)
        if news_items:
            update.message.reply_text("💰 Here are the latest cryptocurrency updates:")
            for item in news_items:
                message = f"*{item['title']}*\n\n{item['summary']}\n\n[Read more]({item['url']})"
                update.message.reply_text(message, parse_mode=telegram.ParseMode.MARKDOWN)
        else:
            update.message.reply_text("No recent cryptocurrency updates found.")

    def _handle_message(self, update, context):
        """Handle text messages"""
        # Simple implementation - could be enhanced for more interactivity
        update.message.reply_text(
            "I'm your news notification bot. Use commands like /latest, /science, or /crypto to get updates."
        )

    def _get_latest_news(self, limit: int = 5) -> List[Dict]:
        """Get latest high-relevance news items"""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            r.id, r.title, r.source, r.category, r.url, 
                            r.published_date, r.summary, p.relevance_score
                        FROM 
                            news_etl.raw_news r
                        JOIN 
                            news_etl.processed_news p ON r.id = p.id
                        WHERE 
                            p.relevance_score >= 0.7
                            AND r.published_date >= %s
                        ORDER BY 
                            p.relevance_score DESC, r.published_date DESC
                        LIMIT %s
                    """, (datetime.now() - timedelta(days=1), limit))

                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            self.logger.error(f"Database error when getting latest news: {str(e)}")
            return []

    def _get_category_news(self, category: str, limit: int = 3) -> List[Dict]:
        """Get news items for a specific category"""
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            r.id, r.title, r.source, r.category, r.url, 
                            r.published_date, r.summary, p.relevance_score
                        FROM 
                            news_etl.raw_news r
                        JOIN 
                            news_etl.processed_news p ON r.id = p.id
                        WHERE 
                            r.category = %s
                            AND r.published_date >= %s
                        ORDER BY 
                            p.relevance_score DESC, r.published_date DESC
                        LIMIT %s
                    """, (category, datetime.now() - timedelta(days=1), limit))

                    columns = [desc[0] for desc in cur.description]
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Exception as e:
            self.logger.error(f"Database error when getting {category} news: {str(e)}")
            return []

    def send_notification(self, chat_id: str, news_item: Dict) -> bool:
        """Send a notification for a specific news item"""
        try:
            message = f"*{news_item['title']}*\n\n{news_item['summary']}\n\n[Read more]({news_item['url']})"
            self.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=telegram.ParseMode.MARKDOWN
            )

            # Log the notification in database
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO news_etl.notifications (news_id, message_text, status)
                        VALUES (%s, %s, %s)
                    """, (news_item['id'], message, 'sent'))
                    conn.commit()

            return True
        except Exception as e:
            self.logger.error(f"Error sending notification: {str(e)}")
            return False

    def start(self):
        """Start the bot"""
        self.updater.start_polling()
        self.logger.info("Bot started")

    def stop(self):
        """Stop the bot"""
        self.updater.stop()
        self.logger.info("Bot stopped")
