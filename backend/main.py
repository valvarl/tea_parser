import os
import uvicorn
from app import create_app

# создаём экземпляр FastAPI один раз, чтобы IDE / тесты могли его импортировать
app = create_app()

if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", 8000))

    # передаём uvicorn строку "main:app", а не сам объект,
    # чтобы флаг reload отработал без предупреждений
    uvicorn.run(
        "main:app",           # <module>:<variable>
        host=host,
        port=port,
        reload=True,          # hot-reload
        factory=False,        # у нас уже есть app, фабрика не нужна
    )