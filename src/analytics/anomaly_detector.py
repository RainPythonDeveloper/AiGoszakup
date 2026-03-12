"""
Anomaly Detector — выявление ценовых аномалий в госзакупках.

Методы:
1. IQR (Interquartile Range) — основной статистический
2. Isolation Forest — ML (unsupervised, ловит многомерные аномалии)
3. Consensus (IQR + IF) — высокая уверенность когда оба метода согласны
"""
import logging
from dataclasses import dataclass

import numpy as np
import psycopg2
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from src.config import db_config

logger = logging.getLogger(__name__)


@dataclass
class AnomalyResult:
    record_type: str         # 'lot', 'contract_subject'
    record_id: int
    enstru_code: str | None
    price: float
    median_price: float
    deviation_pct: float     # отклонение от медианы в %
    anomaly_type: str        # 'overpriced', 'underpriced', 'normal'
    method: str              # 'iqr', 'isolation_forest', 'consensus'
    severity: str            # 'critical', 'high', 'medium', 'low'
    details: str
    confidence: float = 0.0  # 0..1, уверенность детекции


def get_conn():
    return psycopg2.connect(
        host=db_config.host,
        port=db_config.port,
        dbname=db_config.database,
        user=db_config.user,
        password=db_config.password,
    )


def detect_iqr_anomalies(conn, enstru_code: str | None = None,
                          threshold: float = 1.5) -> list[AnomalyResult]:
    """
    IQR метод: выброс если price < Q1 - 1.5*IQR или price > Q3 + 1.5*IQR.
    threshold=1.5 — умеренные выбросы, 3.0 — экстремальные.
    """
    results = []

    with conn.cursor() as cur:
        # Получить статистику по ENSTRU кодам
        if enstru_code:
            cur.execute("""
                SELECT enstru_code, q1, q3, median_price, sample_size
                FROM mv_price_statistics
                WHERE enstru_code = %s AND q1 IS NOT NULL AND q3 IS NOT NULL
            """, (enstru_code,))
        else:
            cur.execute("""
                SELECT enstru_code, q1, q3, median_price, sample_size
                FROM mv_price_statistics
                WHERE q1 IS NOT NULL AND q3 IS NOT NULL AND sample_size >= 5
            """)
        stats = cur.fetchall()

    for code, q1, q3, median, sample_size in stats:
        iqr = float(q3) - float(q1)
        if iqr <= 0:
            continue

        lower = float(q1) - threshold * iqr
        upper = float(q3) + threshold * iqr

        # Проверить contract_subjects
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cs.id, cs.price_per_unit
                FROM contract_subjects cs
                WHERE cs.enstru_code = %s
                  AND cs.price_per_unit IS NOT NULL
                  AND (cs.price_per_unit < %s OR cs.price_per_unit > %s)
            """, (code, lower, upper))

            for rec_id, price in cur.fetchall():
                deviation = ((float(price) - float(median)) / float(median)) * 100

                if float(price) > upper:
                    anomaly_type = 'overpriced'
                    severity = 'critical' if deviation > 200 else 'high' if deviation > 100 else 'medium'
                else:
                    anomaly_type = 'underpriced'
                    severity = 'high' if deviation < -80 else 'medium' if deviation < -50 else 'low'

                results.append(AnomalyResult(
                    record_type='contract_subject',
                    record_id=rec_id,
                    enstru_code=code,
                    price=float(price),
                    median_price=float(median),
                    deviation_pct=round(deviation, 1),
                    anomaly_type=anomaly_type,
                    method='iqr',
                    severity=severity,
                    details=f"IQR: price={price:.2f}, range=[{lower:.2f}, {upper:.2f}], "
                            f"median={median:.2f}, samples={sample_size}",
                ))

    logger.info(f"IQR anomalies found: {len(results)}")
    return results


def detect_iforest_anomalies(conn, enstru_code: str | None = None,
                              contamination: float = 0.05) -> list[AnomalyResult]:
    """
    Isolation Forest — ML метод детекции аномалий.
    Строит ансамбль деревьев, изолирует выбросы (аномалии изолируются быстрее).

    Features:
    - log(price_per_unit)
    - отклонение от медианы группы
    - z-score внутри группы
    - log(quantity)
    - log(total_price)
    """
    results = []

    with conn.cursor() as cur:
        query = """
            SELECT cs.id, cs.enstru_code, cs.price_per_unit, cs.quantity, cs.total_price
            FROM contract_subjects cs
            WHERE cs.price_per_unit IS NOT NULL AND cs.price_per_unit > 0
              AND cs.enstru_code IS NOT NULL
        """
        params = []
        if enstru_code:
            query += " AND cs.enstru_code = %s"
            params.append(enstru_code)
        cur.execute(query, params)
        rows = cur.fetchall()

    if len(rows) < 20:
        logger.info(f"IF: too few records ({len(rows)}), skipping")
        return results

    ids = np.array([r[0] for r in rows])
    codes = np.array([r[1] for r in rows])
    prices = np.array([float(r[2]) for r in rows])
    quantities = np.array([float(r[3] or 1) for r in rows])
    totals = np.array([float(r[4] or prices[i] * quantities[i]) for i, r in enumerate(rows)])

    # Медианы по группам
    unique_codes = np.unique(codes)
    code_median = {}
    code_std = {}
    code_mean = {}
    for c in unique_codes:
        mask = codes == c
        code_median[c] = np.median(prices[mask])
        code_std[c] = np.std(prices[mask]) if mask.sum() > 1 else 1.0
        code_mean[c] = np.mean(prices[mask])

    medians = np.array([code_median[c] for c in codes])
    stds = np.array([code_std[c] for c in codes])
    means = np.array([code_mean[c] for c in codes])

    # Feature matrix
    log_price = np.log1p(prices)
    dev_from_median = (prices - medians) / np.clip(medians, 0.01, None)
    zscore_grp = (prices - means) / np.clip(stds, 0.01, None)
    log_qty = np.log1p(quantities)
    log_total = np.log1p(totals)

    X = np.column_stack([log_price, dev_from_median, zscore_grp, log_qty, log_total])
    # Очистка NaN/Inf
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=200,
        contamination=contamination,
        random_state=42,
        n_jobs=-1,
    )
    labels = model.fit_predict(X_scaled)
    scores = model.decision_function(X_scaled)

    # score_range для нормализации confidence
    score_min = scores.min()
    score_max = scores.max()
    score_range = score_max - score_min if score_max != score_min else 1.0

    anomaly_mask = labels == -1
    anomaly_indices = np.where(anomaly_mask)[0]

    for idx in anomaly_indices:
        rec_id = int(ids[idx])
        code = codes[idx]
        price = float(prices[idx])
        median = float(medians[idx])
        deviation = ((price - median) / median) * 100 if median > 0 else 0

        if price > median:
            anomaly_type = 'overpriced'
            severity = 'critical' if deviation > 200 else 'high' if deviation > 100 else 'medium'
        else:
            anomaly_type = 'underpriced'
            severity = 'high' if deviation < -80 else 'medium' if deviation < -50 else 'low'

        # Confidence: нормализованный IF score (0..1, чем ниже score тем выше confidence)
        confidence = 1.0 - (scores[idx] - score_min) / score_range

        results.append(AnomalyResult(
            record_type='contract_subject',
            record_id=rec_id,
            enstru_code=code,
            price=price,
            median_price=round(median, 2),
            deviation_pct=round(deviation, 1),
            anomaly_type=anomaly_type,
            method='isolation_forest',
            severity=severity,
            details=f"IF: score={scores[idx]:.4f}, confidence={confidence:.2f}, "
                    f"deviation={deviation:+.1f}%, samples={int((codes == code).sum())}",
            confidence=round(confidence, 3),
        ))

    logger.info(f"Isolation Forest anomalies: {len(results)} "
                f"(contamination={contamination}, features=5, records={len(rows)})")
    return results


def detect_consensus_anomalies(conn, enstru_code: str | None = None,
                                iqr_threshold: float = 1.5,
                                if_contamination: float = 0.05) -> list[AnomalyResult]:
    """
    Consensus метод: пересечение IQR и Isolation Forest.
    Если оба метода считают запись аномалией — высокая уверенность.
    """
    iqr_results = detect_iqr_anomalies(conn, enstru_code, iqr_threshold)
    if_results = detect_iforest_anomalies(conn, enstru_code, if_contamination)

    iqr_ids = {a.record_id for a in iqr_results}
    if_map = {a.record_id: a for a in if_results}

    consensus = []
    for a in iqr_results:
        if a.record_id in if_map:
            if_anomaly = if_map[a.record_id]
            consensus.append(AnomalyResult(
                record_type=a.record_type,
                record_id=a.record_id,
                enstru_code=a.enstru_code,
                price=a.price,
                median_price=a.median_price,
                deviation_pct=a.deviation_pct,
                anomaly_type=a.anomaly_type,
                method='consensus',
                severity=a.severity,
                details=f"Consensus (IQR + IF): deviation={a.deviation_pct:+.1f}%, "
                        f"IF confidence={if_anomaly.confidence:.2f}",
                confidence=if_anomaly.confidence,
            ))

    logger.info(f"Consensus anomalies (IQR ∩ IF): {len(consensus)} "
                f"(IQR={len(iqr_results)}, IF={len(if_results)})")
    return consensus


def detect_volume_anomalies(conn, year_threshold: float = 2.0) -> list[dict]:
    """
    Выявляет аномальные объёмы закупок по годам.
    Если объём за год превышает среднегодовой в year_threshold раз.
    """
    results = []

    with conn.cursor() as cur:
        cur.execute("""
            SELECT enstru_code, customer_bin, customer_name, year,
                   total_quantity, total_amount,
                   AVG(total_quantity) OVER (
                       PARTITION BY enstru_code, customer_bin
                   ) as avg_quantity,
                   AVG(total_amount) OVER (
                       PARTITION BY enstru_code, customer_bin
                   ) as avg_amount
            FROM mv_volume_trends
            WHERE total_quantity > 0
        """)
        rows = cur.fetchall()

    for code, cbin, cname, year, qty, amount, avg_qty, avg_amount in rows:
        if avg_qty and float(avg_qty) > 0 and float(qty) > float(avg_qty) * year_threshold:
            ratio = float(qty) / float(avg_qty)
            results.append({
                'type': 'volume_spike',
                'enstru_code': code,
                'customer_bin': cbin,
                'customer_name': cname,
                'year': year,
                'quantity': float(qty),
                'avg_quantity': round(float(avg_qty), 2),
                'ratio': round(ratio, 2),
                'severity': 'high' if ratio > 5 else 'medium',
            })

    logger.info(f"Volume anomalies found: {len(results)}")
    return results


def detect_supplier_concentration(conn, threshold_pct: float = 80.0) -> list[dict]:
    """
    Выявляет заказчиков, у которых один поставщик получает > threshold_pct% от суммы.
    """
    results = []

    with conn.cursor() as cur:
        cur.execute("""
            WITH customer_totals AS (
                SELECT customer_bin, SUM(contract_sum) as total
                FROM contracts
                WHERE contract_sum > 0
                GROUP BY customer_bin
            ),
            supplier_shares AS (
                SELECT c.customer_bin, c.supplier_bin,
                       SUM(c.contract_sum) as supplier_total,
                       ct.total as customer_total,
                       100.0 * SUM(c.contract_sum) / NULLIF(ct.total, 0) as share_pct
                FROM contracts c
                JOIN customer_totals ct ON c.customer_bin = ct.customer_bin
                WHERE c.contract_sum > 0
                GROUP BY c.customer_bin, c.supplier_bin, ct.total
            )
            SELECT ss.customer_bin, s1.name_ru as customer_name,
                   ss.supplier_bin, s2.name_ru as supplier_name,
                   ss.supplier_total, ss.customer_total, ss.share_pct
            FROM supplier_shares ss
            LEFT JOIN subjects s1 ON ss.customer_bin = s1.bin
            LEFT JOIN subjects s2 ON ss.supplier_bin = s2.bin
            WHERE ss.share_pct >= %s
            ORDER BY ss.share_pct DESC
        """, (threshold_pct,))
        rows = cur.fetchall()

    for cbin, cname, sbin, sname, sup_total, cust_total, share in rows:
        results.append({
            'type': 'supplier_concentration',
            'customer_bin': cbin,
            'customer_name': cname,
            'supplier_bin': sbin,
            'supplier_name': sname,
            'supplier_total': float(sup_total) if sup_total else 0,
            'customer_total': float(cust_total) if cust_total else 0,
            'share_pct': round(float(share), 1),
            'severity': 'critical' if float(share) > 95 else 'high',
        })

    logger.info(f"Supplier concentration alerts: {len(results)}")
    return results


def run_full_anomaly_detection(conn=None, use_ml: bool = True) -> dict:
    """
    Запускает все методы детекции аномалий.

    Args:
        conn: PostgreSQL connection
        use_ml: включить Isolation Forest (True) или только IQR (False)
    """
    should_close = conn is None
    if conn is None:
        conn = get_conn()

    try:
        logger.info("=== Running Anomaly Detection ===")

        iqr_anomalies = detect_iqr_anomalies(conn)
        volume_anomalies = detect_volume_anomalies(conn)
        concentration = detect_supplier_concentration(conn)

        result = {
            'iqr_anomalies': iqr_anomalies,
            'volume_anomalies': volume_anomalies,
            'supplier_concentration': concentration,
        }

        if use_ml:
            if_anomalies = detect_iforest_anomalies(conn)
            consensus_anomalies = detect_consensus_anomalies(conn)
            result['if_anomalies'] = if_anomalies
            result['consensus_anomalies'] = consensus_anomalies

        result['summary'] = {
            'iqr_anomalies': len(iqr_anomalies),
            'if_anomalies': len(result.get('if_anomalies', [])),
            'consensus_anomalies': len(result.get('consensus_anomalies', [])),
            'volume_anomalies': len(volume_anomalies),
            'supplier_concentration': len(concentration),
        }
        result['summary']['total'] = sum(result['summary'].values())

        logger.info(f"=== Anomaly Detection Complete: {result['summary']} ===")
        return result

    finally:
        if should_close:
            conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    results = run_full_anomaly_detection(use_ml=True)
    print(f"\nSummary: {results['summary']}")

    if results.get('consensus_anomalies'):
        print(f"\nTop 10 Consensus Anomalies (IQR + Isolation Forest):")
        for a in sorted(results['consensus_anomalies'],
                        key=lambda x: abs(x.deviation_pct), reverse=True)[:10]:
            print(f"  [{a.severity}] {a.enstru_code}: {a.price:,.2f} vs median {a.median_price:,.2f} "
                  f"({a.deviation_pct:+.1f}%) confidence={a.confidence:.2f}")
    elif results.get('iqr_anomalies'):
        print(f"\nTop 5 IQR Anomalies:")
        for a in sorted(results['iqr_anomalies'],
                        key=lambda x: abs(x.deviation_pct), reverse=True)[:5]:
            print(f"  [{a.severity}] {a.enstru_code}: {a.price:,.2f} vs median {a.median_price:,.2f} "
                  f"({a.deviation_pct:+.1f}%)")

    if results['supplier_concentration']:
        print(f"\nTop 5 Supplier Concentrations:")
        for c in results['supplier_concentration'][:5]:
            print(f"  [{c['severity']}] {c['customer_name']}: {c['supplier_name']} "
                  f"= {c['share_pct']:.1f}%")
