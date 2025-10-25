# app.py (limpio)

from fastapi import FastAPI, Depends, HTTPException, Body
from fastapi.responses import RedirectResponse
import sqlite3
from datetime import date
from typing import Any, Dict, Optional, List, Literal, Annotated
from pydantic import BaseModel, Field, StringConstraints
from db import get_db, DB_PATH, init_db, list_tables
from fastapi import FastAPI, Depends, HTTPException, Body
from fastapi.responses import StreamingResponse, JSONResponse
from datetime import timedelta
import io, csv


# =========================
# Schemas Celdas
# =========================
PabellonStr = Annotated[str, StringConstraints(min_length=1, max_length=50, strip_whitespace=True)]
NumeroStr   = Annotated[str, StringConstraints(min_length=1, max_length=50, strip_whitespace=True)]
Capacidad   = Annotated[int, Field(ge=1, le=12)]

class CeldaIn(BaseModel):
    pabellon: PabellonStr = Field(..., description="Ej: A, B, Norte, etc.")
    numero:   NumeroStr   = Field(..., description="Ej: 1, 12, 3B")
    capacidad: Capacidad  = Field(..., description="Capacidad entre 1 y 12")

class CeldaOut(CeldaIn):
    id: int

# =========================
# Schemas Agentes
# =========================
RangoLiteral = Literal['Auxiliar','Oficial','Sargento','Suboficial','Inspector']

LegajoStr   = Annotated[str, StringConstraints(min_length=1, max_length=30, strip_whitespace=True)]
NombreStr   = Annotated[str, StringConstraints(min_length=1, max_length=80, strip_whitespace=True)]
ApellidoStr = Annotated[str, StringConstraints(min_length=1, max_length=80, strip_whitespace=True)]

class AgenteIn(BaseModel):
    legajo:   LegajoStr   = Field(..., description="Único. Ej: SP-00123")
    nombre:   NombreStr   = Field(..., description="Nombre del agente")
    apellido: ApellidoStr = Field(..., description="Apellido del agente")
    rango:    RangoLiteral = Field(..., description="Uno de: Auxiliar, Oficial, Sargento, Suboficial, Inspector")
    activo:   bool = Field(True, description="Activo (True/False)")

class AgenteOut(AgenteIn):
    id: int

# =========================
# Schemas Internos
# =========================
EstadoLiteral = Literal['Activo','Trasladado','Liberado']

DniStr      = Annotated[str, StringConstraints(pattern=r'^\d{7,10}$')]
Nombre80    = Annotated[str, StringConstraints(min_length=1, max_length=80, strip_whitespace=True)]
Apellido80  = Annotated[str, StringConstraints(min_length=1, max_length=80, strip_whitespace=True)]

class InternoIn(BaseModel):
    dni: Optional[DniStr] = Field(None, description="Único. Solo números (7-10). Opcional.")
    nombre:   Nombre80
    apellido: Apellido80
    fecha_ingreso: date = Field(..., description="YYYY-MM-DD")
    estado:   EstadoLiteral = Field('Activo')
    celda_id: Optional[int] = Field(None, description="ID de celda; nulo si no tiene asignación")
    causa:    Optional[str] = Field(None, min_length=1, max_length=200, description="Causa judicial o expediente")
    condena_meses: Optional[int] = Field(None, ge=0, le=600, description="Duración de la condena en meses")

class InternoOut(InternoIn):
    id: int

# =========================
# Schemas Stats (/stats)
# =========================
from typing import Dict  # ya usás typing, pero por si falta Dict

class PabellonStat(BaseModel):
    pabellon: str
    capacidad: int
    ocupados: int
    ocupacion: float  # 0..1

class Totales(BaseModel):
    internos: int
    celdas: int
    agentes: int

class Capacidad(BaseModel):
    capacidad_total: int
    camas_ocupadas: int
    tasa_ocupacion: float  # 0..1

class UltimoIngreso(BaseModel):
    id: int
    nombre: str
    apellido: str
    fecha_ingreso: str
    pabellon: Optional[str] = None
    celda: Optional[str] = None

class StatsResponse(BaseModel):
    totales: Totales
    capacidad: Capacidad
    nuevos_periodo: int
    por_pabellon: List[PabellonStat]
    estados: Dict[str, int]
    ultimos_ingresos: List[UltimoIngreso]

# =========================
# FastAPI app
# =========================
app = FastAPI(title="Servicio Penitenciario API", version="0.1.0")

# --- Sistema / redirect
@app.get("/health", tags=["Sistema"])
def health():
    return {"status": "ok", "service": "servicio-penitenciario", "version": "0.1.0"}

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")

# --- DB health
@app.get("/db/health", tags=["Base de datos"])
def db_health(db: sqlite3.Connection = Depends(get_db)):
    version = db.execute("SELECT sqlite_version()").fetchone()[0]
    db.execute("CREATE TABLE IF NOT EXISTS _healthcheck (id INTEGER PRIMARY KEY, ts TEXT)")
    db.execute("DELETE FROM _healthcheck")
    db.execute("INSERT INTO _healthcheck (ts) VALUES (datetime('now'))")
    ts = db.execute("SELECT ts FROM _healthcheck ORDER BY id DESC LIMIT 1").fetchone()[0]
    return {"status": "ok", "sqlite_version": version, "db_path": str(DB_PATH), "last_write_test": ts}

# --- DB init & tables
@app.post("/db/init", tags=["Base de datos"])
def db_init_endpoint(db: sqlite3.Connection = Depends(get_db)) -> Dict[str, Any]:
    try:
        init_db(db)
        from db import list_tables
        tables = list_tables(db, include_system=False)
        return {
            "status": "ok",
            "message": "Esquema inicializado/verificado.",
            "tables": tables,
            "tables_count": len(tables),
            "db_path": str(DB_PATH),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al inicializar BD: {e!r}")

@app.get("/db/tables", tags=["Base de datos"])
def db_tables(db: sqlite3.Connection = Depends(get_db)) -> Dict[str, Any]:
    try:
        from db import list_tables
        tables = list_tables(db, include_system=False)
        return {"status": "ok", "db_path": str(DB_PATH), "tables_count": len(tables), "tables": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al listar tablas: {e}")


@app.get("/db/indexes", tags=["Base de datos"])
def listar_indices(db: sqlite3.Connection = Depends(get_db)):
    rows = db.execute("""
        SELECT name, tbl_name
        FROM sqlite_master
        WHERE type='index'
        ORDER BY tbl_name, name
    """).fetchall()
    return [dict(r) for r in rows]


@app.post("/db/indexes", tags=["Base de datos"])
def crear_indices(db: sqlite3.Connection = Depends(get_db)):
    stmts = [
        "CREATE INDEX IF NOT EXISTS idx_internos_dni    ON internos(dni)",
        "CREATE INDEX IF NOT EXISTS idx_internos_estado ON internos(estado)",
        "CREATE INDEX IF NOT EXISTS idx_internos_celda  ON internos(celda_id)",
        "CREATE INDEX IF NOT EXISTS idx_internos_fecha  ON internos(fecha_ingreso)",
        "CREATE INDEX IF NOT EXISTS idx_celdas_pabellon ON celdas(pabellon)"
    ]
    for s in stmts:
        db.execute(s)
    db.commit()
    return {"status": "ok", "created_or_exists": len(stmts)}

# =========================
# Celdas CRUD
# =========================
@app.post("/celdas", response_model=CeldaOut, tags=["Celdas"])
def crear_celda(
    payload: CeldaIn = Body(
        ...,
        examples={
            "valido": {
                "summary": "Válido",
                "value": {"pabellon": "A", "numero": "1", "capacidad": 6}
            },
            "duplicado": {
                "summary": "Conflicto UNIQUE (pabellon+numero)",
                "value": {"pabellon": "A", "numero": "1", "capacidad": 6}
            }
        }
    ),
    db: sqlite3.Connection = Depends(get_db),
):
    try:
        db.execute("""
            INSERT INTO celdas (pabellon, numero, capacidad)
            VALUES (?, ?, ?)
        """, (payload.pabellon, payload.numero, payload.capacidad))
        db.commit()
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=409, detail=f"Violación de integridad: {e}")

    row = db.execute("""
        SELECT id, pabellon, numero, capacidad
        FROM celdas
        WHERE rowid = last_insert_rowid()
    """).fetchone()
    return dict(row)


@app.get("/celdas", response_model=List[CeldaOut], tags=["Celdas"])
def listar_celdas(
    pabellon: Optional[str] = None,
    numero: Optional[str] = None,
    db: sqlite3.Connection = Depends(get_db),
):
    query = "SELECT id, pabellon, numero, capacidad FROM celdas WHERE 1=1"
    params: List[Any] = []
    if pabellon:
        query += " AND pabellon = ?"
        params.append(pabellon)
    if numero:
        query += " AND numero = ?"
        params.append(numero)
    query += " ORDER BY pabellon, numero"
    rows = db.execute(query, params).fetchall()
    return [dict(r) for r in rows]

@app.get("/celdas/{celda_id}", response_model=CeldaOut, tags=["Celdas"])
def obtener_celda(celda_id: int, db: sqlite3.Connection = Depends(get_db)):
    row = db.execute("SELECT id, pabellon, numero, capacidad FROM celdas WHERE id = ?", (celda_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Celda no encontrada")
    return dict(row)

@app.put("/celdas/{celda_id}", response_model=CeldaOut, tags=["Celdas"])
def actualizar_celda(celda_id: int, payload: CeldaIn, db: sqlite3.Connection = Depends(get_db)):
    exists = db.execute("SELECT 1 FROM celdas WHERE id = ?", (celda_id,)).fetchone()
    if not exists:
        raise HTTPException(status_code=404, detail="Celda no encontrada")
    try:
        db.execute("UPDATE celdas SET pabellon = ?, numero = ?, capacidad = ? WHERE id = ?",
                   (payload.pabellon, payload.numero, payload.capacidad, celda_id))
        db.commit()
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=409, detail=f"Violación de integridad: {e}")
    row = db.execute("SELECT id, pabellon, numero, capacidad FROM celdas WHERE id = ?", (celda_id,)).fetchone()
    return dict(row)

@app.delete("/celdas/{celda_id}", tags=["Celdas"])
def eliminar_celda(celda_id: int, db: sqlite3.Connection = Depends(get_db)):
    ref = db.execute("SELECT 1 FROM internos WHERE celda_id = ?", (celda_id,)).fetchone()
    if ref:
        raise HTTPException(status_code=409, detail="No se puede borrar: hay internos asignados a esta celda")
    cur = db.execute("DELETE FROM celdas WHERE id = ?", (celda_id,))
    db.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Celda no encontrada")
    return {"status": "ok", "deleted_id": celda_id}

# =========================
# Agentes CRUD
# =========================
@app.post("/agentes", response_model=AgenteOut, tags=["Agentes"])
def crear_agente(
    payload: AgenteIn = Body(
        ...,
        examples={
            "valido": {
                "summary": "Válido",
                "value": {"legajo": "SP-0001","nombre": "Debora","apellido": "Romano","rango": "Oficial","activo": True}
            },
            "duplicado": {
                "summary": "Conflicto UNIQUE (legajo)",
                "value": {"legajo": "SP-0001","nombre": "Otra","apellido": "Persona","rango": "Oficial","activo": True}
            },
            "rango_invalido": {
                "summary": "CHECK(rango) falla",
                "value": {"legajo": "SP-0002","nombre": "Carlos","apellido": "Luna","rango": "Capitan","activo": True}
            }
        }
    ),
    db: sqlite3.Connection = Depends(get_db),
):
    try:
        db.execute("""
            INSERT INTO agentes (legajo, nombre, apellido, rango, activo)
            VALUES (?, ?, ?, ?, ?)
        """, (payload.legajo, payload.nombre, payload.apellido, payload.rango, int(payload.activo)))
        db.commit()
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=409, detail=f"Violación de integridad: {e}")

    row = db.execute("""
        SELECT id, legajo, nombre, apellido, rango, (activo != 0) AS activo
        FROM agentes
        WHERE rowid = last_insert_rowid()
    """).fetchone()
    return dict(row)



@app.get("/agentes", response_model=List[AgenteOut], tags=["Agentes"])
def listar_agentes(
    legajo: Optional[str] = None,
    nombre: Optional[str] = None,
    apellido: Optional[str] = None,
    rango: Optional[RangoLiteral] = None,
    activo: Optional[bool] = None,
    db: sqlite3.Connection = Depends(get_db),
):
    query = """
        SELECT id, legajo, nombre, apellido, rango, (activo != 0) AS activo
        FROM agentes
        WHERE 1=1
    """
    params: List[Any] = []
    if legajo:
        query += " AND legajo = ?"
        params.append(legajo)
    if nombre:
        query += " AND nombre = ?"
        params.append(nombre)
    if apellido:
        query += " AND apellido = ?"
        params.append(apellido)
    if rango:
        query += " AND rango = ?"
        params.append(rango)
    if activo is not None:
        query += " AND activo = ?"
        params.append(int(activo))
    query += " ORDER BY apellido, nombre"
    rows = db.execute(query, params).fetchall()
    return [dict(r) for r in rows]




@app.get("/agentes/{agente_id}", response_model=AgenteOut, tags=["Agentes"])
def obtener_agente(agente_id: int, db: sqlite3.Connection = Depends(get_db)):
    row = db.execute("""
        SELECT id, legajo, nombre, apellido, rango, (activo != 0) AS activo
        FROM agentes
        WHERE id = ?
    """, (agente_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Agente no encontrado")
    return dict(row)

@app.put("/agentes/{agente_id}", response_model=AgenteOut, tags=["Agentes"])
def actualizar_agente(agente_id: int, payload: AgenteIn, db: sqlite3.Connection = Depends(get_db)):
    existe = db.execute("SELECT 1 FROM agentes WHERE id = ?", (agente_id,)).fetchone()
    if not existe:
        raise HTTPException(status_code=404, detail="Agente no encontrado")
    try:
        db.execute("""
            UPDATE agentes
            SET legajo = ?, nombre = ?, apellido = ?, rango = ?, activo = ?
            WHERE id = ?
        """, (payload.legajo, payload.nombre, payload.apellido, payload.rango, int(payload.activo), agente_id))
        db.commit()
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=409, detail=f"Violación de integridad: {e}")
    row = db.execute("""
        SELECT id, legajo, nombre, apellido, rango, (activo != 0) AS activo
        FROM agentes
        WHERE id = ?
    """, (agente_id,)).fetchone()
    return dict(row)

@app.delete("/agentes/{agente_id}", tags=["Agentes"])
def eliminar_agente(agente_id: int, db: sqlite3.Connection = Depends(get_db)):
    cur = db.execute("DELETE FROM agentes WHERE id = ?", (agente_id,))
    db.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Agente no encontrado")
    return {"status": "ok", "deleted_id": agente_id}

# =========================
# Internos CRUD
# =========================
def _celda_existe(db: sqlite3.Connection, celda_id: int) -> bool:
    return db.execute("SELECT 1 FROM celdas WHERE id = ?", (celda_id,)).fetchone() is not None



@app.post("/internos", response_model=InternoOut, tags=["Internos"])
def crear_interno(
    payload: InternoIn = Body(
        ...,
        example={
            "dni": "30123456",
            "nombre": "Juan",
            "apellido": "Pérez",
            "fecha_ingreso": "2025-08-20",
            "estado": "Activo",
            "celda_id": 1,
            "causa": "Robo simple",
            "condena_meses": 24
        }
    ),
    db: sqlite3.Connection = Depends(get_db),
):
    if payload.estado != 'Activo' and payload.celda_id is not None:
        raise HTTPException(status_code=400, detail="Un interno no Activo no puede tener celda asignada")
    if payload.celda_id is not None and not _celda_existe(db, payload.celda_id):
        raise HTTPException(status_code=404, detail="Celda indicada no existe")

    try:
        db.execute("""
            INSERT INTO internos (dni, nombre, apellido, fecha_ingreso, estado, celda_id, causa, condena_meses)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            payload.dni,
            payload.nombre,
            payload.apellido,
            payload.fecha_ingreso.isoformat(),
            payload.estado,
            payload.celda_id,
            payload.causa,
            payload.condena_meses
        ))
        db.commit()
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=409, detail=f"Violación de integridad: {e}")

    row = db.execute("""
        SELECT id, dni, nombre, apellido, fecha_ingreso, estado, celda_id, causa, condena_meses
        FROM internos
        WHERE rowid = last_insert_rowid()
    """).fetchone()
    return dict(row)

@app.get("/internos", response_model=List[InternoOut], tags=["Internos"])
def listar_internos(
    dni: Optional[str] = None,
    estado: Optional[EstadoLiteral] = None,
    celda_id: Optional[int] = None,
    apellido: Optional[str] = None,
    causa: Optional[str] = None,
    db: sqlite3.Connection = Depends(get_db),
):
    query = """
        SELECT id, dni, nombre, apellido, fecha_ingreso, estado, celda_id, causa, condena_meses
        FROM internos
        WHERE 1=1
    """
    params: List[Any] = []
    if dni:
        query += " AND dni = ?";        params.append(dni)
    if estado:
        query += " AND estado = ?";     params.append(estado)
    if celda_id is not None:
        query += " AND celda_id = ?";   params.append(celda_id)
    if apellido:
        query += " AND apellido = ?";   params.append(apellido)
    if causa:
        query += " AND causa = ?";      params.append(causa)

    query += " ORDER BY apellido, nombre"
    rows = db.execute(query, params).fetchall()
    return [dict(r) for r in rows]




@app.get("/internos/{interno_id}", response_model=InternoOut, tags=["Internos"])
def obtener_interno(interno_id: int, db: sqlite3.Connection = Depends(get_db)):
    row = db.execute("""
        SELECT id, dni, nombre, apellido, fecha_ingreso, estado, celda_id, causa, condena_meses
        FROM internos
        WHERE id = ?
    """, (interno_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Interno no encontrado")
    return dict(row)

@app.post("/celdas", response_model=CeldaOut, tags=["Celdas"])
def crear_celda(
    payload: CeldaIn = Body(
        ...,
        examples={
            "valido": {
                "summary": "Válido",
                "value": {"pabellon": "A", "numero": "1", "capacidad": 6}
            },
            "duplicado": {
                "summary": "Conflicto UNIQUE (pabellon+numero)",
                "value": {"pabellon": "A", "numero": "1", "capacidad": 6}
            }
        }
    ),
    db: sqlite3.Connection = Depends(get_db),
):
    try:
        db.execute("""
            INSERT INTO celdas (pabellon, numero, capacidad)
            VALUES (?, ?, ?)
        """, (payload.pabellon, payload.numero, payload.capacidad))
        db.commit()
    except sqlite3.IntegrityError as e:
        raise HTTPException(status_code=409, detail=f"Violación de integridad: {e}")

    row = db.execute("""
        SELECT id, pabellon, numero, capacidad
        FROM celdas
        WHERE rowid = last_insert_rowid()
    """).fetchone()
    return dict(row)

@app.delete("/internos/{interno_id}", tags=["Internos"])
def eliminar_interno(interno_id: int, db: sqlite3.Connection = Depends(get_db)):
    cur = db.execute("DELETE FROM internos WHERE id = ?", (interno_id,))
    db.commit()
    if cur.rowcount == 0:
        raise HTTPException(status_code=404, detail="Interno no encontrado")
    return {"status": "ok", "deleted_id": interno_id}
# =========================
# Stats (/stats)
# =========================
from fastapi import Query
from datetime import timedelta

@app.get("/stats", response_model=StatsResponse, tags=["Stats"])
def get_stats(
    desde: Optional[date] = Query(None, description="YYYY-MM-DD (incluido)"),
    hasta: Optional[date] = Query(None, description="YYYY-MM-DD (incluido)"),
    db: sqlite3.Connection = Depends(get_db),
):
    # Rango por defecto: últimos 30 días hasta hoy
    if not hasta:
        hasta = date.today()
    if not desde:
        desde = hasta - timedelta(days=30)
    if desde > hasta:
        raise HTTPException(status_code=400, detail="Parámetros inválidos: 'desde' no puede ser mayor que 'hasta'.")

    cur = db.cursor()

    # ---- Totales
    cur.execute("SELECT COUNT(*) FROM internos")
    total_internos = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM celdas")
    total_celdas = cur.fetchone()[0]

    try:
        cur.execute("SELECT COUNT(*) FROM agentes")
        total_agentes = cur.fetchone()[0]
    except Exception:
        total_agentes = 0  # por si aún no existe la tabla

    # ---- Capacidad
    cur.execute("SELECT COALESCE(SUM(capacidad),0) FROM celdas")
    capacidad_total = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*)
        FROM internos
        WHERE estado='Activo' AND celda_id IS NOT NULL
    """)
    camas_ocupadas = cur.fetchone()[0]

    tasa_ocupacion = round((camas_ocupadas / capacidad_total), 3) if capacidad_total else 0.0

    # ---- Nuevos en rango
    cur.execute("""
        SELECT COUNT(*)
        FROM internos
        WHERE date(fecha_ingreso) BETWEEN date(?) AND date(?)
    """, (desde.isoformat(), hasta.isoformat()))
    nuevos_periodo = cur.fetchone()[0]

    # ---- Por pabellón (a partir de celdas.pabellon)
    cur.execute("""
        SELECT c.pabellon,
               SUM(c.capacidad) AS capacidad,
               SUM(
                   CASE
                       WHEN i.estado='Activo' THEN 1
                       ELSE 0
                   END
               ) AS ocupados
        FROM celdas c
        LEFT JOIN internos i ON i.celda_id = c.id
        GROUP BY c.pabellon
        ORDER BY c.pabellon
    """)
    por_pabellon: List[PabellonStat] = []
    for pab, cap, occ in cur.fetchall():
        cap = cap or 0
        occ = occ or 0
        por = round((occ / cap), 3) if cap else 0.0
        por_pabellon.append(PabellonStat(pabellon=pab, capacidad=cap, ocupados=occ, ocupacion=por))

    # ---- Estados (Activo / Trasladado / Liberado)
    cur.execute("SELECT estado, COUNT(*) FROM internos GROUP BY estado")
    estados = { (k or "Desconocido"): v for k, v in cur.fetchall() }

    # ---- Últimos ingresos (10)
    cur.execute("""
        SELECT i.id, i.nombre, i.apellido, i.fecha_ingreso, c.pabellon, c.numero
        FROM internos i
        LEFT JOIN celdas c ON c.id = i.celda_id
        ORDER BY date(i.fecha_ingreso) DESC, i.id DESC
        LIMIT 10
    """)
    ultimos = [
        UltimoIngreso(
            id=r[0], nombre=r[1], apellido=r[2], fecha_ingreso=str(r[3]),
            pabellon=r[4], celda=str(r[5]) if r[5] is not None else None
        )
        for r in cur.fetchall()
    ]

    return StatsResponse(
        totales=Totales(internos=total_internos, celdas=total_celdas, agentes=total_agentes),
        capacidad=Capacidad(capacidad_total=capacidad_total, camas_ocupadas=camas_ocupadas, tasa_ocupacion=tasa_ocupacion),
        nuevos_periodo=nuevos_periodo,
        por_pabellon=por_pabellon,
        estados=estados,
        ultimos_ingresos=ultimos
    )
# =========================
# Reportes (/reportes)
# =========================
@app.get("/reportes/internos", tags=["Reportes"])
def reporte_internos(
    formato: Literal["csv", "json"] = "csv",
    desde: Optional[date] = None,
    hasta: Optional[date] = None,
    estado: Optional[EstadoLiteral] = None,     # 'Activo', 'Trasladado', 'Liberado'
    pabellon: Optional[str] = None,             # ej: 'A'
    db: sqlite3.Connection = Depends(get_db),
):
    # Rango por defecto: últimos 30 días
    if not hasta:
        hasta = date.today()
    if not desde:
        desde = hasta - timedelta(days=30)
    if desde > hasta:
        raise HTTPException(status_code=400, detail="'desde' no puede ser mayor que 'hasta'")

    # Query base
    sql = """
        SELECT i.id, i.dni, i.nombre, i.apellido, i.fecha_ingreso, i.estado,
               i.celda_id,
               c.pabellon, c.numero
        FROM internos i
        LEFT JOIN celdas c ON c.id = i.celda_id
        WHERE date(i.fecha_ingreso) BETWEEN date(?) AND date(?)
    """
    params: list[Any] = [desde.isoformat(), hasta.isoformat()]

    if estado:
        sql += " AND i.estado = ?"
        params.append(estado)
    if pabellon:
        sql += " AND c.pabellon = ?"
        params.append(pabellon)

    sql += " ORDER BY date(i.fecha_ingreso) DESC, i.apellido, i.nombre, i.id DESC"

    rows = db.execute(sql, params).fetchall()
    data = [dict(r) for r in rows]

    # Si pide JSON
    if formato == "json":
        return JSONResponse(content=data)

    # Si pide CSV
    headers = [
        "id","dni","nombre","apellido","fecha_ingreso","estado",
        "celda_id","pabellon","celda_numero"
    ]
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(headers)
    for r in data:
        writer.writerow([
            r["id"], r["dni"], r["nombre"], r["apellido"], r["fecha_ingreso"], r["estado"],
            r["celda_id"], r["pabellon"], r["numero"]
        ])
    buf.seek(0)
    filename = f"reporte_internos_{desde.isoformat()}_{hasta.isoformat()}.csv"

    return StreamingResponse(
        buf,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
