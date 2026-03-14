"""
FastAPI сервер для AI-агента госзакупок.
REST API + WebSocket чат.
"""
import json
import logging
import time
import secrets
from collections import defaultdict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

from src.config import app_config
from src.agent.react_agent import get_agent, ReActAgent
from src.agent.tools import get_data_overview, execute_sql, get_fair_price

logger = logging.getLogger(__name__)

app = FastAPI(
    title="AI Госзакупки РК",
    description="AI-агент анализа государственных закупок Республики Казахстан",
    version="1.0.0",
)

# ============================================================
# CORS — configurable via CORS_ORIGINS env var
# ============================================================
_cors_origins: list[str] = []
if app_config.cors_origins:
    _cors_origins = [o.strip() for o in app_config.cors_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=bool(_cors_origins),
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-API-Key"],
)


# ============================================================
# Optional API Key auth (enabled when APP_API_KEY is set)
# ============================================================
async def verify_api_key(request: Request):
    """If APP_API_KEY is configured, require X-API-Key header."""
    if not app_config.api_key:
        return  # auth disabled — open access
    key = request.headers.get("X-API-Key", "")
    if not secrets.compare_digest(key, app_config.api_key):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


# ============================================================
# WebSocket rate limiter
# ============================================================
_MAX_WS_CONNECTIONS = 50
_WS_MSG_PER_MINUTE = 20


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, WebSocket] = {}
        self.agents: dict[str, ReActAgent] = {}
        self._msg_timestamps: dict[str, list[float]] = defaultdict(list)

    async def connect(self, websocket: WebSocket, client_id: str) -> bool:
        if len(self.active_connections) >= _MAX_WS_CONNECTIONS:
            await websocket.close(code=1013, reason="Too many connections")
            return False
        await websocket.accept()
        self.active_connections[client_id] = websocket
        self.agents[client_id] = ReActAgent()
        return True

    def disconnect(self, client_id: str):
        self.active_connections.pop(client_id, None)
        self.agents.pop(client_id, None)
        self._msg_timestamps.pop(client_id, None)

    def is_rate_limited(self, client_id: str) -> bool:
        now = time.time()
        timestamps = self._msg_timestamps[client_id]
        # Keep only timestamps from last 60s
        self._msg_timestamps[client_id] = [t for t in timestamps if now - t < 60]
        if len(self._msg_timestamps[client_id]) >= _WS_MSG_PER_MINUTE:
            return True
        self._msg_timestamps[client_id].append(now)
        return False

    async def send_message(self, client_id: str, message: dict):
        ws = self.active_connections.get(client_id)
        if ws:
            await ws.send_json(message)

    def get_agent(self, client_id: str) -> ReActAgent:
        if client_id not in self.agents:
            self.agents[client_id] = ReActAgent()
        return self.agents[client_id]


manager = ConnectionManager()


# ============================================================
# REST API Endpoints
# ============================================================

@app.get("/api/health")
async def health():
    """Проверка работоспособности."""
    overview = get_data_overview()
    return {"status": "ok", "data": overview}


@app.get("/api/overview", dependencies=[Depends(verify_api_key)])
async def data_overview():
    """Обзор загруженных данных."""
    return get_data_overview()


@app.post("/api/query", dependencies=[Depends(verify_api_key)])
async def query_data(request: dict):
    """Выполняет SQL запрос (только SELECT)."""
    sql = request.get("query", "")
    return execute_sql(sql)


@app.post("/api/fair-price", dependencies=[Depends(verify_api_key)])
async def fair_price(request: dict):
    """Рассчитывает справедливую цену."""
    enstru = request.get("enstru_code", "")
    region = request.get("region")
    return get_fair_price(enstru, region)


@app.post("/api/chat", dependencies=[Depends(verify_api_key)])
async def chat_endpoint(request: dict):
    """REST endpoint для чата с AI агентом."""
    message = request.get("message", "")
    if not message:
        return {"error": "message is required"}

    agent = get_agent()
    try:
        response = await agent.chat(message)
        return {"response": response}
    except Exception as e:
        logger.error(f"Chat error: {e}")
        return {"error": "Внутренняя ошибка сервера. Попробуйте позже."}


# ============================================================
# WebSocket Chat
# ============================================================

@app.websocket("/ws/chat/{client_id}")
async def websocket_chat(websocket: WebSocket, client_id: str):
    """WebSocket endpoint для real-time чата."""
    # Validate client_id length to prevent abuse
    if not client_id or len(client_id) > 64:
        await websocket.close(code=1008, reason="Invalid client_id")
        return

    connected = await manager.connect(websocket, client_id)
    if not connected:
        return

    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            user_text = message.get("message", "")

            if not user_text:
                continue

            # Rate limiting
            if manager.is_rate_limited(client_id):
                await manager.send_message(client_id, {
                    "type": "error",
                    "content": "Слишком много запросов. Подождите минуту.",
                })
                continue

            # Отправляем индикатор "думает"
            await manager.send_message(client_id, {
                "type": "thinking",
                "content": "Анализирую запрос..."
            })

            agent = manager.get_agent(client_id)

            try:
                response = await agent.chat(user_text)
                await manager.send_message(client_id, {
                    "type": "response",
                    "content": response,
                })
            except Exception as e:
                logger.error(f"Agent error: {e}")
                await manager.send_message(client_id, {
                    "type": "error",
                    "content": "Произошла ошибка при обработке запроса. Попробуйте ещё раз.",
                })

    except WebSocketDisconnect:
        manager.disconnect(client_id)


# ============================================================
# Chat UI (простой HTML)
# ============================================================

@app.get("/", response_class=HTMLResponse)
async def chat_ui():
    return """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AI Госзакупки РК</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
               background: #f0f2f5; height: 100vh; display: flex; flex-direction: column; }
        .header { background: #1a73e8; color: white; padding: 16px 24px;
                  font-size: 18px; font-weight: 600; }
        .header small { font-weight: 400; opacity: 0.8; font-size: 13px; }
        .chat-container { flex: 1; overflow-y: auto; padding: 20px; }
        .message { max-width: 80%; margin-bottom: 16px; padding: 12px 16px;
                   border-radius: 12px; line-height: 1.5; white-space: pre-wrap; }
        .user { background: #1a73e8; color: white; margin-left: auto;
                border-bottom-right-radius: 4px; }
        .assistant { background: white; border: 1px solid #e0e0e0;
                     border-bottom-left-radius: 4px; }
        .thinking { background: #fff3cd; border: 1px solid #ffc107;
                    font-style: italic; }
        .error { background: #f8d7da; border: 1px solid #dc3545; }
        .input-area { background: white; border-top: 1px solid #e0e0e0;
                      padding: 16px; display: flex; gap: 12px; }
        .input-area input { flex: 1; padding: 12px 16px; border: 1px solid #ddd;
                            border-radius: 24px; font-size: 15px; outline: none; }
        .input-area input:focus { border-color: #1a73e8; }
        .input-area button { background: #1a73e8; color: white; border: none;
                             padding: 12px 24px; border-radius: 24px; cursor: pointer;
                             font-size: 15px; font-weight: 500; }
        .input-area button:hover { background: #1557b0; }
        .input-area button:disabled { background: #ccc; cursor: not-allowed; }
        .examples { padding: 20px; text-align: center; color: #666; }
        .examples button { background: #e8f0fe; color: #1a73e8; border: 1px solid #c2d7f5;
                           padding: 8px 16px; border-radius: 20px; margin: 4px;
                           cursor: pointer; font-size: 13px; }
        .examples button:hover { background: #d2e3fc; }
    </style>
</head>
<body>
    <div class="header">
        AI Госзакупки РК
        <small>Аналитика государственных закупок</small>
    </div>
    <div class="chat-container" id="chat">
        <div class="examples">
            <p>Примеры запросов:</p>
            <button onclick="sendExample(this)">Сколько данных загружено?</button>
            <button onclick="sendExample(this)">Топ-5 заказчиков по сумме договоров</button>
            <button onclick="sendExample(this)">Покажи ценовые аномалии</button>
            <button onclick="sendExample(this)">Анализ концентрации поставщиков</button>
        </div>
    </div>
    <div class="input-area">
        <input type="text" id="input" placeholder="Задайте вопрос о госзакупках..."
               onkeypress="if(event.key==='Enter')send()">
        <button id="btn" onclick="send()">Отправить</button>
    </div>

<script>
    const clientId = 'user_' + Math.random().toString(36).substr(2, 9);
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${proto}//${location.host}/ws/chat/${clientId}`);
    const chat = document.getElementById('chat');
    const input = document.getElementById('input');
    const btn = document.getElementById('btn');

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        // Remove thinking messages
        document.querySelectorAll('.thinking').forEach(el => el.remove());

        const div = document.createElement('div');
        div.className = 'message ' + (data.type === 'error' ? 'error' :
                                       data.type === 'thinking' ? 'thinking' : 'assistant');
        div.textContent = data.content;
        chat.appendChild(div);
        chat.scrollTop = chat.scrollHeight;

        if (data.type !== 'thinking') {
            btn.disabled = false;
            input.disabled = false;
        }
    };

    ws.onclose = () => {
        addMessage('Соединение потеряно. Обновите страницу.', 'error');
    };

    function send() {
        const text = input.value.trim();
        if (!text) return;

        // Remove examples on first message
        document.querySelector('.examples')?.remove();

        addMessage(text, 'user');
        ws.send(JSON.stringify({ message: text }));
        input.value = '';
        btn.disabled = true;
        input.disabled = true;
    }

    function sendExample(el) {
        input.value = el.textContent;
        send();
    }

    function addMessage(text, type) {
        const div = document.createElement('div');
        div.className = 'message ' + type;
        div.textContent = text;
        chat.appendChild(div);
        chat.scrollTop = chat.scrollHeight;
    }
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    uvicorn.run(app, host="0.0.0.0", port=app_config.port)
