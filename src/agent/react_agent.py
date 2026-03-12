"""
ReAct AI Agent — агент для анализа госзакупок.

Паттерн ReAct: Reason → Act → Observe → Repeat.
LLM выбирает инструменты, получает результаты, формирует ответ.
LLM НЕ считает сама — делегирует SQL/Python через tools.
"""
import json
import logging

import httpx

from src.config import llm_config
from src.agent.tools import TOOL_DEFINITIONS, call_tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Ты — AI-агент аналитики государственных закупок Республики Казахстан.

Твои возможности:
1. Поиск и анализ данных о закупках (объявления, лоты, договоры, планы)
2. Расчёт справедливой цены (Fair Price) для ТРУ по коду ENSTRU
3. Выявление ценовых аномалий и подозрительных паттернов
4. Анализ концентрации поставщиков
5. Ответы на вопросы по данным из базы

ВАЖНЫЕ ПРАВИЛА:
- НИКОГДА не считай в уме. Для любых вычислений используй инструменты (execute_sql, get_fair_price).
- Отвечай на основе ФАКТИЧЕСКИХ данных из базы, а не своих знаний.
- При анализе цен ВСЕГДА указывай размер выборки и уровень доверия.
- Формат чисел: разделяй тысячи пробелом (1 234 567,89 тг).
- Отвечай на русском или казахском языке, в зависимости от вопроса.
- Будь кратким но информативным.

Доступные таблицы БД:
- subjects (организации: bin, name_ru, is_customer, is_supplier)
- announcements (объявления: number_anno, name_ru, total_sum, customer_bin, publish_date)
- lots (лоты: name_ru, amount, count, price_per_unit, enstru_code, customer_bin)
- contracts (договоры: contract_number, contract_sum, sign_date, customer_bin, supplier_bin)
- contract_subjects (предметы договора: enstru_code, price_per_unit, quantity, delivery_kato)
- plans (планы: bin, enstru_code, plan_sum, year)
- ref_methods (способы закупки), ref_statuses (статусы), ref_kato (КАТО)
- mv_price_statistics (ценовая статистика по ENSTRU+регион+период)
- mv_supplier_stats (статистика поставщиков)
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
