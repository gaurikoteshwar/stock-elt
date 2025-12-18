import logging
from datetime import datetime
#This creates a central logger that writes to both console and etl.log.
# Create logger
logger = logging.getLogger("stock_etl")
logger.setLevel(logging.INFO)

# Create file handler
fh = logging.FileHandler("etl.log")
fh.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)

# Add handler to logger
logger.addHandler(fh)

# Optional: log to console as well
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)