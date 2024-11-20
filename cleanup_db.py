import sqlite3
import logging
from typing import List, Tuple
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("cleanup.log"), logging.StreamHandler(sys.stdout)],
)


def connect_db(db_path: str = "aops_data.db") -> sqlite3.Connection:
    """Create a database connection with optimized settings."""
    conn = sqlite3.connect(db_path, isolation_level=None)  # Autocommit mode
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
    return conn


def cleanup_database(conn: sqlite3.Connection) -> Tuple[int, int, int]:
    """
    Clean up the database by removing:
    1. Problems with no problem_text
    2. Lessons with no problems
    3. Classes with no lessons

    Returns:
        Tuple of (problems_removed, lessons_removed, classes_removed)
    """
    cursor = conn.cursor()

    # 1. Remove problems with no problem_text
    cursor.execute(
        """
        DELETE FROM problems 
        WHERE problem_text IS NULL 
        OR trim(problem_text) = ''
    """
    )
    problems_removed = cursor.rowcount
    logging.info(f"Removed {problems_removed} problems with no problem text")

    # 2. Get lessons with no problems
    cursor.execute(
        """
        DELETE FROM lessons 
        WHERE (class_id, lesson_id) IN (
            SELECT DISTINCT l.class_id, l.lesson_id 
            FROM lessons l 
            LEFT JOIN problems p ON l.class_id = p.class_id AND l.lesson_id = p.lesson_id 
            WHERE p.problem_id IS NULL
        )
    """
    )
    lessons_removed = cursor.rowcount
    logging.info(f"Removed {lessons_removed} lessons with no problems")

    # 3. Remove classes with no lessons
    cursor.execute(
        """
        DELETE FROM classes 
        WHERE class_id IN (
            SELECT DISTINCT c.class_id 
            FROM classes c 
            LEFT JOIN lessons l ON c.class_id = l.class_id 
            WHERE l.lesson_id IS NULL
        )
    """
    )
    classes_removed = cursor.rowcount
    logging.info(f"Removed {classes_removed} classes with no lessons")

    return problems_removed, lessons_removed, classes_removed


def find_duplicate_problems(conn: sqlite3.Connection):
    """Find problems that have identical problem_text and solution_text pairs."""
    cursor = conn.cursor()
    
    # First get total number of unique problem-solution pairs
    cursor.execute("""
        SELECT COUNT(DISTINCT problem_text || '|' || solution_text) as unique_pairs
        FROM problems 
        WHERE problem_text IS NOT NULL 
        AND solution_text IS NOT NULL
    """)
    total_unique_pairs = cursor.fetchone()[0]
    logging.info(f"Total unique problem-solution pairs: {total_unique_pairs}")
    
    # Then get duplicate counts
    cursor.execute("""
        WITH problem_pairs AS (
            SELECT COUNT(*) as occurrence_count
            FROM problems 
            WHERE problem_text IS NOT NULL 
            AND solution_text IS NOT NULL
            GROUP BY problem_text, solution_text
            HAVING COUNT(*) > 1
        )
        SELECT 
            COUNT(*) as unique_duplicate_pairs,
            SUM(occurrence_count) as total_occurrences
        FROM problem_pairs
    """)
    
    unique_pairs, total_occurrences = cursor.fetchone()
    if unique_pairs:
        total_duplicates = total_occurrences - unique_pairs  # subtract original occurrences
        logging.info(f"Found {unique_pairs} problem-solution pairs that appear multiple times")
        logging.info(f"Total number of duplicate entries: {total_duplicates}")
    else:
        logging.info("No duplicate problem-solution pairs found")
    
    return total_unique_pairs, unique_pairs, total_duplicates if unique_pairs else 0


def main():
    try:
        conn = connect_db()
        
        # Find duplicates first
        logging.info("\n=== Checking for Duplicate Problems ===")
        duplicates = find_duplicate_problems(conn)
        
        # Then run cleanup
        logging.info("\n=== Running Database Cleanup ===")
        problems, lessons, classes = cleanup_database(conn)
        logging.info(
            f"""
Database cleanup completed:
- Removed {problems} problems with no text
- Removed {lessons} lessons with no problems
- Removed {classes} classes with no lessons
        """
        )
    except sqlite3.Error as e:
        logging.error(f"Database error occurred: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
