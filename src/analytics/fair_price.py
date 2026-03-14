"""
Fair Price Engine — расчёт справедливой цены ТРУ.

Формула: FairPrice = MedianPrice × RegionalCoeff × InflationIndex

Источники данных:
- mv_price_statistics: медиана, Q1, Q3, weighted avg
- mv_regional_coefficients: региональные коэффициенты
- inflation_index: кумулятивный ИПЦ
"""
import logging
from dataclasses import dataclass
from datetime import date

import psycopg2

from src.config import db_config

logger = logging.getLogger(__name__)


@dataclass
class FairPriceResult:
    enstru_code: str
    region: str | None
    fair_price: float
    median_price: float
    regional_coeff: float
    inflation_index: float
    seasonal_coeff: float    # сезонный коэффициент (квартальный)
    confidence: str          # 'high', 'medium', 'low'
    sample_size: int
    q1: float | None
    q3: float | None
    min_price: float | None
    max_price: float | None


def get_conn():
    return psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.database,
        user=db_config.user,
        password=db_config.password,
    )


def calculate_fair_price(
    enstru_code: str,
    region: str | None = None,
    target_date: date | None = None,
    conn=None,
) -> FairPriceResult | None:
    """
    Рассчитывает справедливую цену для ТРУ.

    Args:
        enstru_code: код ENSTRU (ТРУ)
        region: регион доставки (из КАТО)
        target_date: дата для расчёта ИПЦ (по умолчанию — сегодня)
        conn: соединение с БД (опционально)

    Returns:
        FairPriceResult или None если нет данных
    """
    should_close = conn is None
    if conn is None:
        conn = get_conn()

    try:
        if target_date is None:
            target_date = date.today()

        # 1. Получить медиану из ценовой статистики
        median_price, sample_size, q1, q3, min_p, max_p = _get_price_stats(
            conn, enstru_code, region
        )

        if median_price is None or median_price <= 0:
            logger.warning(f"No price data for ENSTRU={enstru_code}, region={region}")
            return None

        # 2. Региональный коэффициент
        regional_coeff = _get_regional_coefficient(conn, enstru_code, region)

        # 3. Индекс инфляции (ИПЦ)
        inflation_idx = _get_inflation_index(conn, target_date)

        # 4. Сезонный коэффициент (квартальный)
        seasonal_coeff = _get_seasonal_coefficient(conn, enstru_code, target_date)

        # 5. Формула Fair Price = Median × RegCoeff × CPI × SeasonCoeff
        fair_price = median_price * regional_coeff * inflation_idx * seasonal_coeff

        # 6. Уровень доверия
        if sample_size >= 30:
            confidence = 'high'
        elif sample_size >= 10:
            confidence = 'medium'
        else:
            confidence = 'low'

        return FairPriceResult(
            enstru_code=enstru_code,
            region=region,
            fair_price=round(fair_price, 2),
            median_price=round(median_price, 2),
            regional_coeff=round(regional_coeff, 4),
            inflation_index=round(inflation_idx, 4),
            seasonal_coeff=round(seasonal_coeff, 4),
            confidence=confidence,
            sample_size=sample_size,
            q1=round(q1, 2) if q1 else None,
            q3=round(q3, 2) if q3 else None,
            min_price=round(min_p, 2) if min_p else None,
            max_price=round(max_p, 2) if max_p else None,
        )

    finally:
        if should_close:
            conn.close()


def _get_price_stats(conn, enstru_code: str, region: str | None):
    """Получает ценовую статистику из mv_price_statistics."""
    with conn.cursor() as cur:
        if region:
            cur.execute("""
                SELECT
                    SUM(median_price * sample_size) / NULLIF(SUM(sample_size), 0),
                    SUM(sample_size)::INT,
                    MIN(q1), MAX(q3), MIN(min_price), MAX(max_price)
                FROM mv_price_statistics
                WHERE enstru_code = %s AND delivery_region = %s
            """, (enstru_code, region))
        else:
            cur.execute("""
                SELECT
                    SUM(median_price * sample_size) / NULLIF(SUM(sample_size), 0),
                    SUM(sample_size)::INT,
                    MIN(q1), MAX(q3), MIN(min_price), MAX(max_price)
                FROM mv_price_statistics
                WHERE enstru_code = %s
            """, (enstru_code,))

        row = cur.fetchone()
        if row and row[0]:
            return float(row[0]), int(row[1] or 0), row[2], row[3], row[4], row[5]
        return None, 0, None, None, None, None


def _get_regional_coefficient(conn, enstru_code: str, region: str | None) -> float:
    """
    Вычисляет региональный коэффициент.
    Коэффициент = regional_median / national_median.
    Если данных нет — возвращает 1.0.
    """
    if not region:
        return 1.0

    with conn.cursor() as cur:
        # Медиана по конкретному региону
        cur.execute("""
            SELECT regional_median, sample_size
            FROM mv_regional_coefficients
            WHERE enstru_code = %s AND region = %s
        """, (enstru_code, region))
        regional = cur.fetchone()

        # Медиана по всем регионам (национальная)
        cur.execute("""
            SELECT SUM(regional_median * sample_size) / NULLIF(SUM(sample_size), 0)
            FROM mv_regional_coefficients
            WHERE enstru_code = %s
        """, (enstru_code,))
        national = cur.fetchone()

    if regional and national and national[0] and national[0] > 0 and regional[0]:
        return float(regional[0]) / float(national[0])

    return 1.0


def _get_inflation_index(conn, target_date: date) -> float:
    """
    Получает относительный ИПЦ (target_month / base_month).
    Базовый период — самый ранний в таблице (2024-01 = 100.0).
    """
    year = target_date.year
    month = target_date.month

    with conn.cursor() as cur:
        # ИПЦ на целевую дату
        cur.execute("""
            SELECT cpi FROM inflation_index
            WHERE year = %s AND month = %s
        """, (year, month))
        target = cur.fetchone()

        # Базовый ИПЦ (2024-01)
        cur.execute("SELECT cpi FROM inflation_index ORDER BY year, month LIMIT 1")
        base = cur.fetchone()

    if target and base and base[0] > 0:
        return float(target[0]) / float(base[0])

    # Если нет данных на нужную дату, ищем ближайший
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cpi FROM inflation_index
            ORDER BY ABS(year - %s) + ABS(month - %s)
            LIMIT 1
        """, (year, month))
        nearest = cur.fetchone()

    if nearest and base and base[0] > 0:
        return float(nearest[0]) / float(base[0])

    return 1.0


def _get_seasonal_coefficient(conn, enstru_code: str, target_date: date) -> float:
    """
    Вычисляет сезонный коэффициент на основе исторических данных.

    Сравнивает медиану цен за целевой квартал с общей годовой медианой
    для данного ENSTRU кода. Например, стройматериалы летом дороже.

    Returns:
        Коэффициент > 1.0 если квартал дороже среднего, < 1.0 если дешевле.
        1.0 если данных недостаточно.
    """
    quarter = (target_date.month - 1) // 3 + 1  # 1..4

    with conn.cursor() as cur:
        # Медиана за целевой квартал по всем годам
        cur.execute("""
            SELECT
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cs.price_per_unit) AS quarter_median,
                COUNT(*) AS quarter_count
            FROM contract_subjects cs
            JOIN contracts c ON cs.contract_id = c.id
            WHERE cs.enstru_code = %s
              AND cs.price_per_unit > 0
              AND EXTRACT(QUARTER FROM c.sign_date) = %s
        """, (enstru_code, quarter))
        q_row = cur.fetchone()

        # Общая медиана по всем кварталам
        cur.execute("""
            SELECT
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cs.price_per_unit) AS annual_median,
                COUNT(*) AS annual_count
            FROM contract_subjects cs
            JOIN contracts c ON cs.contract_id = c.id
            WHERE cs.enstru_code = %s
              AND cs.price_per_unit > 0
        """, (enstru_code,))
        a_row = cur.fetchone()

    if (q_row and a_row
            and q_row[0] and a_row[0]
            and float(a_row[0]) > 0
            and int(q_row[1] or 0) >= 5
            and int(a_row[1] or 0) >= 20):
        coeff = float(q_row[0]) / float(a_row[0])
        # Ограничиваем диапазон: 0.7 .. 1.5 (±50% от нормы)
        return max(0.7, min(1.5, coeff))

    return 1.0


def batch_fair_prices(enstru_codes: list[str], region: str | None = None) -> list[FairPriceResult]:
    """Пакетный расчёт Fair Price для списка ENSTRU кодов."""
    conn = get_conn()
    results = []
    try:
        for code in enstru_codes:
            result = calculate_fair_price(code, region=region, conn=conn)
            if result:
                results.append(result)
    finally:
        conn.close()
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    conn = get_conn()
    try:
        # Показать топ ENSTRU кодов с данными
        with conn.cursor() as cur:
            cur.execute("""
                SELECT enstru_code, SUM(sample_size) as total
                FROM mv_price_statistics
                WHERE enstru_code IS NOT NULL
                GROUP BY enstru_code
                ORDER BY total DESC
                LIMIT 10
            """)
            rows = cur.fetchall()

        if rows:
            print("Top ENSTRU codes with price data:")
            for code, total in rows:
                result = calculate_fair_price(code, conn=conn)
                if result:
                    print(f"  {code}: FairPrice={result.fair_price:,.2f} "
                          f"(median={result.median_price:,.2f}, "
                          f"samples={result.sample_size}, "
                          f"confidence={result.confidence})")
        else:
            print("No price data available yet. Run ETL pipeline first.")
    finally:
        conn.close()
