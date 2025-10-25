from pathlib import Path
import sqlite3
from typing import Generator

# Carpeta y archivo de base de datos
DB_DIR = Path(__file__).resolve().parent / "data"
DB_PATH = DB_DIR / "penitenciario.db"

def get_db() -> Generator[sqlite3.Connection, None, None]:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    try:
        yield conn
    finally:
        conn.close()

# Esquema (sin comillas triples para evitar pegado con indentación)
SCHEMA_SQL = (
    "PRAGMA foreign_keys = ON;"
    "\n\nCREATE TABLE IF NOT EXISTS celdas ("
    "\n  id INTEGER PRIMARY KEY,"
    "\n  pabellon TEXT NOT NULL,"
    "\n  numero   TEXT NOT NULL,"
    "\n  capacidad INTEGER NOT NULL CHECK (capacidad BETWEEN 1 AND 12),"
    "\n  UNIQUE (pabellon, numero)"
    "\n);"
    "\n\nCREATE TABLE IF NOT EXISTS agentes ("
    "\n  id INTEGER PRIMARY KEY,"
    "\n  legajo   TEXT NOT NULL UNIQUE,"
    "\n  nombre   TEXT NOT NULL,"
    "\n  apellido TEXT NOT NULL,"
    "\n  rango    TEXT NOT NULL CHECK (rango IN ('Auxiliar','Oficial','Sargento','Suboficial','Inspector')),"
    "\n  activo   INTEGER NOT NULL DEFAULT 1"
    "\n);"
    "\n\nCREATE TABLE IF NOT EXISTS internos ("
    "\n  id INTEGER PRIMARY KEY,"
    "\n  dni TEXT UNIQUE,"
    "\n  nombre   TEXT NOT NULL,"
    "\n  apellido TEXT NOT NULL,"
    "\n  fecha_ingreso TEXT NOT NULL,"
    "\n  estado   TEXT NOT NULL DEFAULT 'Activo' CHECK (estado IN ('Activo','Trasladado','Liberado')),"
    "\n  celda_id INTEGER,"
    "\n  causa TEXT,"                                  
    "\n  condena_meses INTEGER CHECK (condena_meses >= 0),"  
    "\n  FOREIGN KEY (celda_id) REFERENCES celdas(id) ON UPDATE CASCADE ON DELETE SET NULL"
    "\n);"
)


def _ensure_internos_extra_columns(db: sqlite3.Connection) -> None:
    """Agrega columnas nuevas si faltan (idempotente)."""
    cols = {r["name"] for r in db.execute("PRAGMA table_info('internos')").fetchall()}
    if "causa" not in cols:
        db.execute("ALTER TABLE internos ADD COLUMN causa TEXT")
    if "condena_meses" not in cols:
        db.execute("ALTER TABLE internos ADD COLUMN condena_meses INTEGER CHECK (condena_meses >= 0)")
    db.commit()

def init_db(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA_SQL)          # crea tablas si faltan
    _ensure_internos_extra_columns(db)    # migra columnas nuevas si ya existía la tabla
    db.commit()


# --- Helpers de introspección de BD ---
SYSTEM_TABLE_PREFIXES = ("sqlite_",)  # Excluimos tablas internas de SQLite

def _is_system_table(name: str) -> bool:
    return any(name.startswith(p) for p in SYSTEM_TABLE_PREFIXES)

def list_tables(db: sqlite3.Connection, include_system: bool = False):
    """
    Devuelve metadatos de tablas: nombre, columnas (name,type,notnull,pk,default) y row_count.
    Por defecto excluye tablas internas sqlite_* (pero incluye nuestras como _healthcheck).
    """
    rows = db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = [r["name"] for r in rows]

    result = []
    for t in table_names:
        if not include_system and _is_system_table(t):
            continue

        cols = db.execute(f"PRAGMA table_info('{t}')").fetchall()
        columns = [{
            "name": c["name"],
            "type": c["type"],
            "notnull": int(c["notnull"]),
            "pk": int(c["pk"]),
            "default": c["dflt_value"],
        } for c in cols]

        try:
            count = db.execute(f"SELECT COUNT(*) AS n FROM '{t}'").fetchone()["n"]
        except sqlite3.OperationalError:
            count = None

        result.append({
            "table": t,
            "row_count": count,
            "columns": columns,
        })

    return result

def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}
