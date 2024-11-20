import asyncio
import aiohttp
import time
import logging
import json
import sys
import sqlite3
from typing import Dict, List, Optional, Tuple, Set
from collections import deque
from datetime import datetime
import aiohttp.client_exceptions
from contextlib import contextmanager
from functools import partial

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler(sys.stdout)],
)


class ConnectionPool:
    def __init__(self, db_path: str, max_connections: int = 5):
        self.db_path = db_path
        self.max_connections = max_connections
        self._connections = []
        self._available = asyncio.Queue()
        self._lock = asyncio.Lock()

    async def _create_connection(self):
        conn = sqlite3.connect(self.db_path, isolation_level=None)  # Autocommit mode
        conn.execute(
            "PRAGMA journal_mode=WAL"
        )  # Write-Ahead Logging for better concurrency
        conn.execute(
            "PRAGMA synchronous=NORMAL"
        )  # Faster writes with reasonable safety
        conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        return conn

    async def initialize(self):
        async with self._lock:
            for _ in range(self.max_connections):
                conn = await self._create_connection()
                self._connections.append(conn)
                await self._available.put(conn)

    @contextmanager
    def get_connection(self):
        conn = self._available.get_nowait()
        try:
            yield conn
        finally:
            self._available.put_nowait(conn)

    async def close(self):
        while not self._available.empty():
            conn = await self._available.get()
            conn.close()


class AOPSScraper:
    def __init__(
        self,
        session_id: str,
        start_class_id: int = 4,
        end_class_id: int = 3900,
        max_concurrent_classes: int = 3,
        max_retries: int = 3,
        requests_per_minute: int = 20,
    ):
        self.base_url = "https://artofproblemsolving.com/m/class/ajax.php"
        self.session_id = session_id
        self.start_class_id = start_class_id
        self.end_class_id = end_class_id
        self.db_path = "aops_data.db"
        self.max_concurrent_classes = max_concurrent_classes
        self.max_retries = max_retries
        self.save_queue = asyncio.Queue(maxsize=1000)
        self.conn_pool = ConnectionPool(self.db_path)

        self.requests_per_minute = requests_per_minute
        self.request_interval = 60.0 / requests_per_minute
        self.last_request_time = time.time()
        self.request_times = deque(maxlen=requests_per_minute)
        self.request_lock = asyncio.Lock()

        self.class_semaphore = asyncio.Semaphore(max_concurrent_classes)
        self.request_semaphore = asyncio.Semaphore(5)

        self._init_db()

    def _init_db(self):
        """Initialize the SQLite database with necessary tables and indexes."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA cache_size=-64000")  # 64MB cache

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS classes (
                    class_id INTEGER PRIMARY KEY,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS lessons (
                    lesson_id INTEGER,
                    class_id INTEGER,
                    has_problems BOOLEAN,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (lesson_id, class_id),
                    FOREIGN KEY (class_id) REFERENCES classes(class_id)
                )
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS problems (
                    problem_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    class_id INTEGER,
                    lesson_id INTEGER,
                    problem_type TEXT,
                    problem_text TEXT,
                    answer TEXT,
                    answer_type TEXT,
                    alt_answers TEXT,
                    solution_text TEXT,
                    formatting_tips TEXT,
                    available_hints INTEGER,
                    can_hint BOOLEAN,
                    problem_has_solution BOOLEAN,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (class_id, lesson_id) REFERENCES lessons(class_id, lesson_id)
                )
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_problems_class_lesson 
                ON problems(class_id, lesson_id)
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_lessons_class 
                ON lessons(class_id)
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS scraping_progress (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    current_class_id INTEGER,
                    current_lesson_id INTEGER,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            cursor.execute(
                """
                INSERT OR IGNORE INTO scraping_progress (id, current_class_id, current_lesson_id)
                VALUES (1, NULL, NULL)
            """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS failed_requests (
                    class_id INTEGER,
                    lesson_id INTEGER,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    last_attempt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (class_id, lesson_id)
                )
            """
            )

            conn.commit()

    async def _wait_for_rate_limit(self):
        """Implement token bucket rate limiting."""
        async with self.request_lock:
            current_time = time.time()

            while self.request_times and current_time - self.request_times[0] > 60:
                self.request_times.popleft()

            if len(self.request_times) >= self.requests_per_minute:
                wait_time = 60 - (current_time - self.request_times[0])
                if wait_time > 0:
                    await asyncio.sleep(wait_time)

            self.request_times.append(current_time)

            time_since_last = current_time - self.last_request_time
            if time_since_last < self.request_interval:
                await asyncio.sleep(self.request_interval - time_since_last)

            self.last_request_time = time.time()

    def _get_progress(self) -> Tuple[Optional[int], Optional[int]]:
        """Get the current scraping progress."""
        with self.conn_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT current_class_id, current_lesson_id FROM scraping_progress WHERE id = 1"
            )
            result = cursor.fetchone()
            return result if result else (None, None)

    def _update_progress(self, class_id: int, lesson_id: int):
        """Update the current scraping progress."""
        with self.conn_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE scraping_progress 
                SET current_class_id = ?, current_lesson_id = ?, last_updated = CURRENT_TIMESTAMP
                WHERE id = 1
                """,
                (class_id, lesson_id),
            )
            conn.commit()

    async def _make_request(
        self, session: aiohttp.ClientSession, form_data: Dict, retry_count: int = 0
    ) -> Optional[Dict]:
        """Make an async POST request with retries and rate limiting."""
        async with self.request_semaphore:
            await self._wait_for_rate_limit()

            try:
                async with session.post(
                    self.base_url,
                    data=form_data,
                    cookies={"platsessionid": self.session_id},
                    timeout=15,
                ) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 60))
                        logging.warning(
                            f"Rate limited. Waiting {retry_after} seconds..."
                        )
                        await asyncio.sleep(retry_after)
                        return await self._make_request(session, form_data, retry_count)

                    response.raise_for_status()
                    return await response.json()

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if retry_count < self.max_retries:
                    wait_time = (2**retry_count) * 5
                    logging.warning(
                        f"Request failed, retrying in {wait_time} seconds..."
                    )
                    await asyncio.sleep(wait_time)
                    return await self._make_request(session, form_data, retry_count + 1)
                logging.error(f"Request failed after {self.max_retries} retries: {e}")
                return None
            except Exception as e:
                logging.error(f"Failed to parse response: {e}")
                return None

    async def get_problems(
        self, session: aiohttp.ClientSession, class_id: int, lesson_id: int
    ) -> Optional[Dict]:
        """Get problems with automatic retries."""
        form_data = {
            "class_id": class_id,
            "lesson[]": lesson_id,
            "display": 1,
            "a": "get_class_homework",
        }
        return await self._make_request(session, form_data)

    async def process_class(
        self, session: aiohttp.ClientSession, class_id: int, start_lesson: int = 1
    ):
        """Process a single class with all its lessons."""
        async with self.class_semaphore:
            max_lessons = 100
            consecutive_errors = 0

            for lesson_id in range(start_lesson, max_lessons + 1):
                if consecutive_errors >= 3:
                    logging.warning(
                        f"Too many consecutive errors for class {class_id}, skipping remaining lessons"
                    )
                    break

                problems_data = await self.get_problems(session, class_id, lesson_id)
                if (
                    problems_data
                    and "response" in problems_data
                    and problems_data["response"]
                    and problems_data["response"]["problems"]
                ):
                    consecutive_errors = 0
                    await self.save_queue.put((class_id, lesson_id, problems_data))
                    await asyncio.sleep(0.1)
                else:
                    if problems_data:
                        consecutive_errors += 1
                        logging.warning(
                            f"Class {class_id}, Lesson {lesson_id}: "
                            f"Error {problems_data.get('error_code')} - {problems_data.get('error_msg')}"
                        )
                    break

                self._update_progress(class_id, lesson_id)

    async def batch_saver(self):
        """Save data in batches for better performance."""
        batch = []
        batch_size = 50
        last_save = time.time()
        save_interval = 5  # seconds

        while True:
            try:
                # if self.save_queue.empty() and batch:
                #     await self._save_batch(batch)
                #     batch = []
                #     last_save = time.time()

                try:
                    class_id, lesson_id, data = await asyncio.wait_for(
                        self.save_queue.get(), timeout=1.0
                    )
                    batch.append((class_id, lesson_id, data))
                except asyncio.TimeoutError:
                    continue

                if (
                    len(batch) >= batch_size
                    or (time.time() - last_save) >= save_interval
                ):
                    await self._save_batch(batch)
                    batch = []
                    last_save = time.time()

            except asyncio.CancelledError:
                logging.info("Batch saver task cancelled")
                if batch:
                    await self._save_batch(batch)
                break
            except Exception as e:
                logging.error(f"Error in batch saver: {e}")
                if batch:
                    await self._save_batch(batch)
                batch = []

    async def _save_batch(self, batch: List[Tuple[int, int, Dict]]):
        """Save a batch of data to the database."""
        if not batch:
            return

        with self.conn_pool.get_connection() as conn:
            cursor = conn.cursor()

            for class_id, lesson_id, data in batch:
                cursor.execute(
                    "INSERT OR IGNORE INTO classes (class_id) VALUES (?)", (class_id,)
                )

                has_problems = (
                    "response" in data
                    and "problems" in data["response"]
                    and len(data["response"]["problems"]) > 0
                )

                cursor.execute(
                    "INSERT OR IGNORE INTO lessons (lesson_id, class_id, has_problems) VALUES (?, ?, ?)",
                    (lesson_id, class_id, has_problems),
                )

                if has_problems:
                    problems = data["response"]["problems"]
                    cursor.executemany(
                        """
                        INSERT INTO problems (
                            class_id, lesson_id, problem_type, problem_text,
                            answer, answer_type, alt_answers, solution_text, formatting_tips,
                            available_hints, can_hint, problem_has_solution
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                class_id,
                                lesson_id,
                                p.get("problem_type"),
                                p.get("problem_text"),
                                p.get("answer"),
                                p.get("answer_type"),
                                str(p.get("alt_answers")),
                                p.get("solution_text"),
                                json.dumps(p.get("formatting_tips")),
                                p.get("available_hints"),
                                p.get("can_hint"),
                                p.get("problem_has_solution"),
                            )
                            for p in problems
                        ],
                    )

            conn.commit()
            # Log information for last batch
            logging.info(f"C{class_id}L{lesson_id}: Saved batch of {len(batch)} items")

    async def scrape(self):
        """Main scraping function with concurrent class processing."""
        await self.conn_pool.initialize()

        try:
            saver_task = asyncio.create_task(self.batch_saver())

            last_class_id, last_lesson_id = self._get_progress()
            start_class = last_class_id if last_class_id else self.start_class_id
            async with aiohttp.ClientSession() as session:
                tasks = []
                for class_id in range(start_class, self.end_class_id + 1):
                    start_lesson = (
                        last_lesson_id
                        if class_id == start_class and last_lesson_id
                        else 1
                    )
                    task = asyncio.create_task(
                        self.process_class(session, class_id, start_lesson)
                    )
                    tasks.append(task)
                    last_lesson_id = None

                    if len(tasks) >= self.max_concurrent_classes:
                        await asyncio.gather(*tasks)
                        tasks = []

                if tasks:
                    await asyncio.gather(*tasks)

        except KeyboardInterrupt:
            logging.info("Scraping interrupted. Progress saved.")
            sys.exit(0)
        except Exception as e:
            logging.error(f"Error occurred: {e}")
            raise
        finally:
            if "saver_task" in locals():
                saver_task.cancel()
                await asyncio.gather(saver_task, return_exceptions=True)

            await self.conn_pool.close()


if __name__ == "__main__":
    SESSION_ID = "e545b442-9353-498d-8c83-1f3162fd1106.uYYIc5Zym6p%2Fu829y5IuCUXUWYsqjQ8M6ZTTuIOoA70"

    scraper = AOPSScraper(
        SESSION_ID, max_concurrent_classes=10, max_retries=3, requests_per_minute=360
    )

    asyncio.run(scraper.scrape())

# NOTE: I didn't start saving formatting tips until class 845.
