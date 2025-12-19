import pandas as pd
import logging

logger = logging.getLogger(__name__)

def check_duplicates(df: pd.DataFrame) -> bool:
    """
    Checks for duplicate (symbol, date) rows.
    Returns True if duplicates exist.
    """
    duplicate_count = df.duplicated(subset=["symbol", "date"]).sum()

    if duplicate_count > 0:
        logger.warning(f"Found {duplicate_count} duplicate rows")
        return True

    logger.info("No duplicate rows found")
    return False