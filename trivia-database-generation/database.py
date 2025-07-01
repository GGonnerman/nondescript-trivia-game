from functools import cache
from dotenv import load_dotenv
from datetime import date
from typing import Any
import psycopg2
from psycopg2 import extensions as ext
import os
from custom_schemas import Category, Question, Episode
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    # filename="log.out",
    level=logging.DEBUG,
    encoding="utf-8",
    format="%(asctime)s {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    datefmt="%H:%M:%S",
)


class Database:
    def __init__(
        self,
        is_development: bool = True,
        purge_database: bool = False,
        table_prefix: str = "triviagame_",
    ) -> None:
        self.table_prefix = table_prefix

        dotenv_file = ".env.development" if is_development else ".env.production"
        _ = load_dotenv(dotenv_file)

        PGUSER: str | None = os.environ.get("PGUSER")
        if PGUSER is None:
            raise Exception("Missing PGUSER in .env")
        PGPASSWORD: str | None = os.environ.get("PGPASSWORD")
        if PGPASSWORD is None:
            raise Exception("Missing PGPASSWORD in .env")
        PGHOST: str | None = os.environ.get("PGHOST")
        if PGHOST is None:
            raise Exception("Missing PGHOST in .env")
        PGDATABASE: str | None = os.environ.get("PGDATABASE")
        if PGDATABASE is None:
            raise Exception("Missing PGDATABASE in .env")

        logger.debug("Setting up sql connection...")

        try:
            self.connection: ext.connection = psycopg2.connect(
                host=PGHOST,
                user=PGUSER,
                password=PGPASSWORD,
                database=PGDATABASE,
            )
        except Exception as e:
            logger.error(f"Failed to connect to MySQL database on {PGUSER} @ {PGHOST} on {PGDATABASE}")
            logger.error(e)
            raise

        logger.debug("Successfully connected")

        logger.debug("Creating cursor...")

        self.cursor: ext.cursor = self.connection.cursor()

        logger.debug("Finished setup")

        if purge_database:
            self.purge()

    def purge(self):
        logger.debug("Purging database")
        tables = [
            "customgame_has_category",
            "customgame",
            "user",
            "category",
            "episode",
            "question",
        ]
        table_names = [f"{self.table_prefix}{table}" for table in tables]
        logger.debug(f"Purging with tables: {table_names}")
        for table_name in table_names:
            self.cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
        self.connection.commit()
        # exit(1)

    def insert_episodes(self, season_number: int, episodes: list[dict[Any, Any]]):
        episodes = [{**episode, "season_number": season_number, "episode_number": index + 1} for index, episode in enumerate(episodes)]

        episodes = Episode.validate(episodes)
        logger.debug("Inserting episodes...")
        insert_stm = f'INSERT INTO "{self.table_prefix}episode" (air_date, season_number, episode_number) VALUES (%(air_date)s, %(season_number)s, %(episode_number)s)'

        self.cursor.executemany(insert_stm, episodes)
        self.connection.commit()

    def insert_categories(self, air_date, categories: list[dict[Any, Any]]):
        episode_id = self.get_episode_from_air_date(air_date)
        categories = [{**category, "episode_idepisode": episode_id} for category in categories]

        categories = Category.validate(categories)

        logger.debug("Inserting categories...")

        insert_stm = f"INSERT INTO {self.table_prefix}category (name, episode_idepisode, round) VALUES (%(name)s, %(episode_idepisode)s, %(round)s)"

        self.cursor.executemany(insert_stm, categories)
        self.connection.commit()

    @cache
    def get_episode_from_air_date(self, air_date: date):
        query_stm = f"SELECT idepisode FROM {self.table_prefix}episode WHERE air_date = %(air_date)s"
        self.cursor.execute(query_stm, {"air_date": air_date})
        logger.debug(f"Getting the episode id for date {air_date}...")
        return self.cursor.fetchone()[0]

    def insert_questions(self, question_metalist: list[tuple[date, int, str, dict[str, Any]]]):
        for question_list in question_metalist:
            air_date = question_list[0]
            round = question_list[1]
            category_name = question_list[2]
            questions = question_list[3]

            category_id = self.get_category(air_date, round, category_name)

            insert_stm = f"INSERT INTO {self.table_prefix}question (clue_value, comment, question, answer, category_idcategory) VALUES (%(clue_value)s, %(comment)s, %(question)s, %(answer)s, %(category_idcategory)s)"

            questions = [{"comment": "", **question, "category_idcategory": category_id} for question in questions]
            questions = Question.validate(questions)

            logger.debug("Inserting questions...")

            self.cursor.executemany(insert_stm, questions)
        self.connection.commit()

    def get_category(self, air_date: date, round: int, name: str):
        episode_id = self.get_episode_from_air_date(air_date)
        query_stm = f"SELECT idcategory FROM {self.table_prefix}category WHERE episode_idepisode = %(episode_id)s AND round = %(round)s AND name = %(name)s"
        params = {
            "episode_id": episode_id,
            "round": round,
            "name": name,
        }
        self.cursor.execute(query_stm, params)
        return self.cursor.fetchone()[0]

    def close(self) -> None:
        self.connection.close()


if __name__ == "__main__":
    c = Database(True)
    c.close()
