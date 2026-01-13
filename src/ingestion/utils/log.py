import os
from colorama import init, Fore, Style
from .date import DATE

# Initialize colorama
init(autoreset=True)

# Base logs folder
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Exception folder
EXCEPTION_DIR = os.path.join(LOG_DIR, "exceptions")
os.makedirs(EXCEPTION_DIR, exist_ok=True)

# Regular log folders
STATUS_FOLDERS = {
    "INFO": os.path.join(LOG_DIR, "INFO"),
    "SUCCESS": os.path.join(LOG_DIR, "SUCCESS"),
    "CREATE": os.path.join(LOG_DIR, "CREATE"),
    "SKIP": os.path.join(LOG_DIR, "SKIP")
}

# Create directories if they don't exist
for folder in STATUS_FOLDERS.values():
    os.makedirs(folder, exist_ok=True)

# Exception statuses
EXCEPTION_STATUSES = ["WARN", "ERROR"]

# Color map for terminal output
COLORS = {
    "INFO": Fore.CYAN,
    "WARN": Fore.YELLOW,
    "SUCCESS": Fore.GREEN,
    "SKIP": Fore.YELLOW,
    "ERROR": Fore.RED,
    "CREATE": Fore.MAGENTA
}

def log(message: str, status: str = "INFO", color: bool = True):
    """
    Logs a message to terminal and to files.
    
    - Exceptions (WARN, ERROR) → saved in logs/exceptions/
    - Regular logs (INFO, SUCCESS, CREATE, SKIP) → saved in their folders
    - Terminal output can be colored
    """
    status_upper = status.upper()
    color_code = COLORS.get(status_upper, Fore.WHITE)

    # Prepare the log line
    line = f"[{status_upper}] {message}"

    # Terminal output
    if color:
        print(f"{color_code}{line}{Style.RESET_ALL}")
        line_to_write = f"{color_code}{line}{Style.RESET_ALL}"
    else:
        print(line)
        line_to_write = line

    # Handle exceptions
    if status_upper in EXCEPTION_STATUSES:
        exc_file = os.path.join(EXCEPTION_DIR, f"{status_upper.lower()}_{DATE}.log")
        with open(exc_file, "a", encoding="utf-8") as f:
            f.write(line_to_write + "\n")
        return  # Don't write to regular folders

    # Handle regular logs
    if status_upper in STATUS_FOLDERS:
        file_path = os.path.join(STATUS_FOLDERS[status_upper], f"{status_upper.lower()}_{DATE}.log")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(line_to_write + "\n")
