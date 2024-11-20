import sqlite3
import csv
import logging
import sys
from typing import List, Tuple

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("export.log"), logging.StreamHandler(sys.stdout)],
)

def connect_db(db_path: str = "aops_data.db") -> sqlite3.Connection:
    """Create a database connection with optimized settings."""
    conn = sqlite3.connect(db_path, isolation_level=None)  # Autocommit mode
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
    return conn

def export_to_csv(conn: sqlite3.Connection, output_file: str = "aops_problems.csv") -> Tuple[int, List[str]]:
    """
    Export problems from the database to a CSV file.
    Returns tuple of (number of problems exported, list of column names).
    """
    cursor = conn.cursor()
    
    # Get all problems with their class and lesson information
    cursor.execute("""
        SELECT 
            p.problem_id,
            p.class_id,
            p.lesson_id,
            p.problem_type,
            p.problem_text,
            p.answer,
            p.answer_type,
            p.alt_answers,
            p.solution_text,
            p.formatting_tips,
            p.available_hints,
            p.can_hint,
            p.problem_has_solution,
            p.scraped_at
        FROM problems p
        ORDER BY p.class_id, p.lesson_id, p.problem_id
    """)
    
    # Get column names from cursor description
    columns = [description[0] for description in cursor.description]
    
    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
        
        # Write header
        writer.writerow(columns)
        
        # Write data in batches
        batch_size = 1000
        rows_written = 0
        
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
                
            writer.writerows(rows)
            rows_written += len(rows)
            logging.info(f"Exported {rows_written} problems...")
    
    logging.info(f"Successfully exported {rows_written} problems to {output_file}")
    return rows_written, columns

def main():
    try:
        conn = connect_db()
        total_exported, columns = export_to_csv(conn)
        
        logging.info(f"""
Export completed:
- Total problems exported: {total_exported}
- Columns exported: {', '.join(columns)}
- Output file: aops_problems.csv
        """)
        
    except sqlite3.Error as e:
        logging.error(f"Database error occurred: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
