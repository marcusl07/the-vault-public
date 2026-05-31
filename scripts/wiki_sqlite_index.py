from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import re
import sqlite3
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


DB_SCHEMA_VERSION = 1
SYSTEM_SLUGS = {"index", "log", "review", "catalog"}
