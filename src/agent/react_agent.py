"""
ReAct AI Agent — агент для анализа госзакупок.

Паттерн ReAct: Reason → Act → Observe → Repeat.
LLM выбирает инструменты, получает результаты, формирует ответ.
LLM НЕ считает сама — делегирует SQL/Python через tools.
"""
import json
import logging
import re
from collections import Counter

import httpx

from src.config import llm_config
from src.agent.tools import TOOL_DEFINITIONS, call_tool

logger = logging.getLogger(__name__)

# ============================================================
# Классификатор типа вопроса
# ============================================================
QUESTION_TYPES = {
    "поиск": [
        r"найди", r"покажи", r"выведи", r"список", r"какие", r"сколько",
        r"кто", r"где", r"search", r"find", r"табл", r"іздеу", r"көрсет",
    ],
    "сравнение": [
        r"сравни", r"сопоставь", r"vs", r"против", r"разница", r"отличи",
        r"больше|меньше.*чем", r"относительно", r"салыстыр",
    ],
    "аналитика": [
        r"топ", r"top", r"рейтинг", r"тренд", r"динамик", r"статистик",
        r"средн", r"итого", r"агрегат", r"обзор", r"талдау",
    ],
    "аномалии": [
        r"аномал", r"выброс", r"подозрит", r"отклонен", r"завышен",
        r"занижен", r"необычн", r"anomal", r"ауытқу",
    ],
    "справедливость_цены": [
        r"справедлив", r"адекватн", r"fair.?price", r"рыночн.*цен",
        r"обоснован.*цен", r"оценк.*цен", r"әділ баға",
    ],
}


def classify_question(text: str) -> str:
    """Определяет тип вопроса по ключевым словам."""
    text_lower = text.lower()
    scores: Counter = Counter()

    for qtype, patterns in QUESTION_TYPES.items():
        for pattern in patterns:
            if re.search(pattern, text_lower):
                scores[qtype] += 1

    if not scores:
        return "поиск"  # по умолчанию
    return scores.most_common(1)[0][0]

SYSTEM_PROMPT = """Ты — AI-агент аналитики государственных закупок Республики Казахстан.
Ты определяешь тип вопроса (поиск, сравнение, аналитика, аномалии, оценка справедливости цены) и выбираешь соответствующие инструменты.

Твои возможности:
1. Поиск данных о закупках (объявления, лоты, договоры, планы)
2. Сравнение цен, поставщиков, регионов
3. Аналитика (агрегаты, тренды, топ-K)
4. Выявление аномалий (ценовые, объёмные, концентрация поставщиков)
5. Оценка справедливости цены (Fair Price = Медиана × РегКоэфф × ИПЦ)

ВАЖНЫЕ ПРАВИЛА:
- НИКОГДА не считай в уме. Используй инструменты (execute_sql, get_fair_price и др.).
- Отвечай ТОЛЬКО на основе ФАКТИЧЕСКИХ данных из базы.
- Запрещено: общие фразы, домысливание, ответы без цифровых показателей.
- Формат чисел: пробел как разделитель тысяч (1 234 567,89 тг).
- Отвечай на русском или казахском, в зависимости от языка вопроса.

ОБЯЗАТЕЛЬНЫЙ ФОРМАТ ОТВЕТА:
Каждый ответ ДОЛЖЕН содержать следующие блоки:

📌 **Вердикт:** (1-3 предложения — главный вывод)

📊 **Использованные данные:**
- Период: (например, 2024-2026)
- Фильтры: (БИН, ENSTRU, регион, статус)
- Сущности: (договоры, лоты, объявления)

📈 **Аналитика:**
- Средневзвешенные значения, медианы
- Выявленные отклонения (% от медианы)
- Сравнение (если применимо)

📐 **Метод оценки:** (IQR, Isolation Forest, Fair Price формула и т.д.)

⚠️ **Уверенность и ограничения:**
- Размер выборки: N объектов
- Уровень доверия: высокий/средний/низкий
- Известные ограничения данных

📋 **Детализация (Top-K):**
Для каждой записи указывай прямую ссылку на портал:
- Объявление: https://goszakup.gov.kz/ru/announce/index/{number_anno}
- Договор: https://goszakup.gov.kz/ru/egContract/cpublic/show/{contract_id}

Доступные таблицы БД:
- subjects (организации: bin, name_ru, is_customer, is_supplier)
- announcements (объявления: id, number_anno, name_ru, total_sum, customer_bin, publish_date, method_id, status_id)
- lots (лоты: id, announcement_id, name_ru, amount, count, price_per_unit, enstru_code, customer_bin, delivery_kato)
- contracts (договоры: id, contract_number, contract_sum, sign_date, customer_bin, supplier_bin, status_id)
- contract_subjects (предметы договора: id, contract_id, enstru_code, name_ru, price_per_unit, quantity, total_price, delivery_kato)
- plans (планы: bin, enstru_code, plan_sum, plan_count, year, method_id, delivery_kato)
- applications (заявки: announcement_id, supplier_bin, lot_id, price_offer)
- ref_methods (способы закупки: id, name_ru)
- ref_statuses (статусы: id, name_ru, code)
- ref_kato (КАТО регионы: code, name_ru, parent_code)
- ref_enstru (коды ТРУ: code, name_ru, parent_code)
- ref_units (единицы измерения: id, name_ru)
- mv_price_statistics (ценовая статистика: enstru_code, delivery_region, median_price, q1, q3, sample_size, period, year)
- mv_volume_trends (тренды объёмов: enstru_code, customer_bin, year, total_quantity, total_amount)
- mv_supplier_stats (статистика поставщиков: supplier_bin, contract_count, total_sum, unique_customers)
- mv_regional_coefficients (региональные коэффициенты: enstru_code, region, regional_median)
- inflation_index (ИПЦ: year, month, cpi)

Формирование ссылок:
- Для объявлений: SELECT number_anno FROM announcements — ссылка https://goszakup.gov.kz/ru/announce/index/{number_anno}
- Для договоров: SELECT contract_number FROM contracts — ссылка https://goszakup.gov.kz/ru/egContract/cpublic/show/{contract_number}
"""


class ReActAgent:
    """ReAct агент, использующий OpenAI-совместимый API (nitec-ai.kz)."""

    def __init__(self):
        self.model = llm_config.model
        self.api_url = f"{llm_config.base_url.rstrip('/')}/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {llm_config.api_key}",
            "Content-Type": "application/json",
        }
        self.max_iterations = 5
        self.conversation_history: list[dict] = []

    def _build_tools_for_api(self) -> list[dict]:
        """Формирует tools в формате OpenAI API."""
        return [
            {
                "type": "function",
                "function": {
                    "name": tool["name"],
                    "description": tool["description"],
                    "parameters": tool["parameters"],
                },
            }
            for tool in TOOL_DEFINITIONS
        ]

    async def _call_llm(self, messages: list[dict], tools: list[dict] | None = None) -> dict:
        """Вызывает LLM API."""
        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": llm_config.max_tokens,
            "temperature": llm_config.temperature,
        }
        if tools:
            payload["tools"] = tools
            payload["tool_choice"] = "auto"

        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(self.api_url, json=payload, headers=self.headers)
            resp.raise_for_status()
            return resp.json()

    async def chat(self, user_message: str) -> str:
        """
        Обрабатывает сообщение пользователя через ReAct цикл.

        Returns:
            Текстовый ответ агента.
        """
        # Классифицируем тип вопроса
        question_type = classify_question(user_message)
        logger.info(f"Question type: {question_type} | Message: {user_message[:100]}")

        # Добавляем сообщение пользователя
        self.conversation_history.append({
            "role": "user",
            "content": user_message,
        })

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            *self.conversation_history,
        ]

        tools = self._build_tools_for_api()

        # ReAct цикл
        for iteration in range(self.max_iterations):
            logger.info(f"ReAct iteration {iteration + 1}")

            response = await self._call_llm(messages, tools)
            choice = response["choices"][0]
            message = choice["message"]

            # Если LLM не вызывает инструменты — это финальный ответ
            if not message.get("tool_calls"):
                final_answer = message.get("content", "")
                self.conversation_history.append({
                    "role": "assistant",
                    "content": final_answer,
                })
                return final_answer

            # LLM вызывает инструменты — обрабатываем
            messages.append(message)

            for tool_call in message["tool_calls"]:
                func_name = tool_call["function"]["name"]
                try:
                    func_args = json.loads(tool_call["function"]["arguments"])
                except json.JSONDecodeError:
                    func_args = {}

                logger.info(f"Calling tool: {func_name}({func_args})")
                result = call_tool(func_name, func_args)
                result_str = json.dumps(result, ensure_ascii=False, default=str)

                # Добавляем результат инструмента
                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "content": result_str[:4000],  # ограничиваем размер
                })

        # Fallback если превышен лимит итераций
        final_response = await self._call_llm(messages)
        answer = final_response["choices"][0]["message"].get("content", "Не удалось получить ответ.")
        self.conversation_history.append({
            "role": "assistant",
            "content": answer,
        })
        return answer

    def reset(self):
        """Сбрасывает историю диалога."""
        self.conversation_history = []


# Синглтон агента
_agent_instance: ReActAgent | None = None


def get_agent() -> ReActAgent:
    global _agent_instance
    if _agent_instance is None:
        _agent_instance = ReActAgent()
    return _agent_instance
