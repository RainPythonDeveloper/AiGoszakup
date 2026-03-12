"""
AI Agent Tools — инструменты, которые LLM может вызывать.

Ключевой принцип ТЗ: LLM НЕ считает — она делегирует вычисления SQL/Python.
Каждый tool выполняет конкретную аналитическую задачу.
"""
import logging
from datetime import date

import psycopg2
from psycopg2.extras import RealDictCursor

from src.config import db_config
from src.analytics.fair_price import calculate_fair_price
from src.analytics.anomaly_detector import (
    detect_iqr_anomalies,
    detect_iforest_anomalies,
    detect_consensus_anomalies,
    detect_volume_anomalies,
    detect_supplier_concentration,
)

logger = logging.getLogger(__name__)


def get_conn():
    return psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.database,
        user=db_config.user,
        password=db_config.password,
    )


# ============================================================
# Tool definitions (описания для LLM)
# ============================================================

TOOL_DEFINITIONS = [
    {
        "name": "execute_sql",
        "description": (
            "Выполняет SELECT SQL-запрос к базе данных госзакупок и возвращает результат. "
            "Доступные таблицы: subjects, announcements, lots, contracts, contract_subjects, "
            "plans, applications, payments, contract_acts, "
            "ref_methods, ref_statuses, ref_units, ref_kato, ref_enstru, ref_currencies, "
            "inflation_index, "
            "mv_price_statistics, mv_volume_trends, mv_supplier_stats, "
            "mv_regional_coefficients, mv_data_overview. "
            "ТОЛЬКО SELECT запросы. Максимум 100 строк."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "SQL SELECT запрос к базе данных"
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "get_fair_price",
        "description": (
            "Рассчитывает справедливую (рыночную) цену для товара/работы/услуги по коду ENSTRU. "
            "Использует формулу: FairPrice = MedianPrice × RegionalCoeff × InflationIndex. "
            "Возвращает: fair_price, median, Q1, Q3, sample_size, confidence."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "enstru_code": {
                    "type": "string",
                    "description": "Код ENSTRU (ТРУ), например '441130.000.000001'"
                },
                "region": {
                    "type": "string",
                    "description": "Регион доставки (название области), опционально"
                }
            },
            "required": ["enstru_code"]
        }
    },
    {
        "name": "detect_anomalies",
        "description": (
            "Выявляет ценовые аномалии для указанного ENSTRU кода или по всей базе. "
            "Методы: 'iqr' (IQR статистический), 'isolation_forest' (ML Isolation Forest), "
            "'consensus' (пересечение IQR и IF — высокая уверенность). "
            "Возвращает список аномалий с severity, deviation %, confidence, details."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "enstru_code": {
                    "type": "string",
                    "description": "Код ENSTRU для проверки (если пусто — проверяет все)"
                },
                "method": {
                    "type": "string",
                    "description": "Метод: 'consensus' (IQR+IF, по умолчанию), 'iqr', 'isolation_forest'"
                },
                "threshold": {
                    "type": "number",
                    "description": "IQR множитель (1.5=умеренные, 3.0=экстремальные). По умолчанию 1.5"
                }
            }
        }
    },
    {
        "name": "check_supplier_concentration",
        "description": (
            "Проверяет концентрацию поставщиков: выявляет заказчиков, "
            "у которых один поставщик получает более 80% от общей суммы закупок. "
            "Индикатор возможных коррупционных рисков."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "customer_bin": {
                    "type": "string",
                    "description": "БИН заказчика для проверки (если пусто — проверяет всех)"
                },
                "threshold_pct": {
                    "type": "number",
                    "description": "Порог концентрации в процентах (по умолчанию 80)"
                }
            }
        }
    },
    {
        "name": "get_data_overview",
        "description": (
            "Возвращает общую статистику по загруженным данным: "
            "количество записей в каждой таблице."
        ),
        "parameters": {
            "type": "object",
            "properties": {}
        }
    },
]


# ============================================================
# Tool implementations
# ============================================================

def execute_sql(query: str) -> dict:
    """Выполняет SQL SELECT запрос (только чтение)."""
    # Проверка безопасности: только SELECT
    normalized = query.strip().upper()
    if not normalized.startswith("SELECT"):
        return {"error": "Разрешены только SELECT запросы"}

    # Запрещаем опасные операции
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE",
                 "GRANT", "REVOKE", "COPY", "EXECUTE", "CALL"]
    for word in forbidden:
        if word in normalized:
            return {"error": f"Запрещённая операция: {word}"}

    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query + " LIMIT 100" if "LIMIT" not in normalized else query)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []

        # Конвертируем в сериализуемый формат
        result_rows = []
        for row in rows:
            clean_row = {}
            for key, val in dict(row).items():
                if isinstance(val, (date,)):
                    clean_row[key] = str(val)
                elif val is None:
                    clean_row[key] = None
                else:
                    clean_row[key] = val
            result_rows.append(clean_row)

        return {
            "columns": columns,
            "rows": result_rows,
            "row_count": len(result_rows),
        }
    except Exception as e:
        return {"error": str(e)}
    finally:
        conn.close()


def get_fair_price(enstru_code: str, region: str | None = None) -> dict:
    """Вычисляет Fair Price для ENSTRU кода."""
    result = calculate_fair_price(enstru_code, region=region)

    if result is None:
        return {"error": f"Нет данных для ENSTRU={enstru_code}, регион={region}"}

    return {
        "enstru_code": result.enstru_code,
        "region": result.region,
        "fair_price": result.fair_price,
        "median_price": result.median_price,
        "regional_coefficient": result.regional_coeff,
        "inflation_index": result.inflation_index,
        "confidence": result.confidence,
        "sample_size": result.sample_size,
        "price_range": {
            "q1": result.q1,
            "q3": result.q3,
            "min": result.min_price,
            "max": result.max_price,
        },
    }


def detect_anomalies(enstru_code: str | None = None, method: str = "consensus",
                     threshold: float = 1.5) -> dict:
    """Выявляет ценовые аномалии (IQR, Isolation Forest, или consensus)."""
    conn = get_conn()
    try:
        if method == "isolation_forest":
            anomalies = detect_iforest_anomalies(conn, enstru_code)
        elif method == "consensus":
            anomalies = detect_consensus_anomalies(conn, enstru_code, iqr_threshold=threshold)
        else:
            anomalies = detect_iqr_anomalies(conn, enstru_code, threshold)

        return {
            "method": method,
            "total": len(anomalies),
            "anomalies": [
                {
                    "record_id": a.record_id,
                    "enstru_code": a.enstru_code,
                    "price": a.price,
                    "median_price": a.median_price,
                    "deviation_pct": a.deviation_pct,
                    "type": a.anomaly_type,
                    "severity": a.severity,
                    "confidence": a.confidence,
                    "details": a.details,
                }
                for a in sorted(anomalies, key=lambda x: abs(x.deviation_pct), reverse=True)[:50]
            ],
        }
    finally:
        conn.close()


def check_supplier_concentration(customer_bin: str | None = None,
                                  threshold_pct: float = 80.0) -> dict:
    """Проверяет концентрацию поставщиков."""
    conn = get_conn()
    try:
        results = detect_supplier_concentration(conn, threshold_pct)

        if customer_bin:
            results = [r for r in results if r['customer_bin'] == customer_bin]

        return {
            "total": len(results),
            "alerts": results[:50],
        }
    finally:
        conn.close()


def get_data_overview() -> dict:
    """Возвращает обзор данных."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT entity, cnt FROM mv_data_overview ORDER BY entity")
            rows = cur.fetchall()
        return {entity: int(cnt) for entity, cnt in rows}
    except Exception:
        # mv может быть не обновлён
        tables = ['subjects', 'announcements', 'lots', 'contracts',
                  'contract_subjects', 'plans', 'applications']
        result = {}
        with conn.cursor() as cur:
            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                result[table] = cur.fetchone()[0]
        return result
    finally:
        conn.close()


# Маппинг имя → функция
TOOL_MAP = {
    "execute_sql": execute_sql,
    "get_fair_price": get_fair_price,
    "detect_anomalies": detect_anomalies,
    "check_supplier_concentration": check_supplier_concentration,
    "get_data_overview": get_data_overview,
}


def call_tool(name: str, arguments: dict) -> dict:
    """Вызывает tool по имени с аргументами."""
    if name not in TOOL_MAP:
        return {"error": f"Unknown tool: {name}"}

    try:
        return TOOL_MAP[name](**arguments)
    except Exception as e:
        logger.error(f"Tool {name} failed: {e}")
        return {"error": str(e)}
