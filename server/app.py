from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.logging_config import configure_logging, get_logger
from server.routes import router

configure_logging()
log = get_logger(__name__)

app = FastAPI(title="QA-AI-Hero", description="Maccabi Healthcare — ESB MVP")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.on_event("startup")
async def on_startup():
    log.info("server_started")


@app.on_event("shutdown")
async def on_shutdown():
    log.info("server_shutdown")
