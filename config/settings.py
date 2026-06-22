from typing import Literal, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # ★ שדות int אופציונליים: ערך ריק ב-.env (למשל "KAFKA_TARGET_PARTITIONS=") מגיע כ-""
    # ו-pydantic v2 לא יודע לפרסר "" ל-int → קריסה. ממירים ריק ל-None *לפני* הפרסור.
    @field_validator("KAFKA_TARGET_PARTITIONS", mode="before")
    @classmethod
    def _blank_int_to_none(cls, v):
        if isinstance(v, str) and v.strip() == "":
            return None
        return v

    # שדה int עם default: ערך ריק ב-.env → חוזרים ל-default (במקום קריסה).
    @field_validator("KAFKA_PARTITION_PROBE_MAX", mode="before")
    @classmethod
    def _blank_int_to_default(cls, v):
        if isinstance(v, str) and v.strip() == "":
            return 16
        return v

    # Copilot Studio Agent (Phase A) — שדות מובנים לפי microsoft-agents-copilotstudio-client SDK
    COPILOT_ENVIRONMENT_ID: Optional[str] = None       # e.g. Default-f4c80c7c-e1aa-4090-...
    COPILOT_AGENT_IDENTIFIER: Optional[str] = None     # Schema name (e.g. crbf3_integrationQaTestGenerator)
    COPILOT_TENANT_ID: Optional[str] = None
    COPILOT_APP_CLIENT_ID: Optional[str] = None        # Agent app ID
    COPILOT_CLOUD: str = "PROD"                        # PROD | GOV | HIGH | DOD | ...
    COPILOT_AGENT_TYPE: str = "PUBLISHED"              # PUBLISHED | PREBUILT
    # Direct connection URL מ-"Native app" channel ב-Copilot Studio.
    # אם מוגדר — מחליף את environment_id/cloud (השדות נשארים נדרשים רק ל-MSAL auth).
    COPILOT_DIRECT_CONNECT_URL: Optional[str] = None
    # Embed mode (fallback): URL ל-iframe של WebChat (מ-Channels -> Web app -> Embed code).
    # אם מסופק — ה-UI מציג iframe במקום chat שלנו, והיוזר מזין ידנית את suite_id בסיום.
    COPILOT_WEBCHAT_URL: Optional[str] = None
    # Canvas mode (★ מסלול ראשי): Token endpoint מ-Copilot Studio Custom website channel.
    # אם מסופק — ה-UI מטמיע Bot Framework WebChat עם file upload + auto JSON detection.
    # זה ה-token endpoint עבור מחלקת ESB (תת-מחלקה של אינטגרציה).
    COPILOT_TOKEN_ENDPOINT: Optional[str] = None
    # Token endpoint נפרד עבור מחלקת .NET (Kafka/Couchbase Worker tests).
    DOTNET_COPILOT_TOKEN_ENDPOINT: Optional[str] = None
    # ★ Token endpoint של סוכן Payload Builder (Copilot Studio) — מקבל את מסמך האפיון ומחזיר
    # templates + field_catalog ל-Compiler. אופציונלי: אם ריק, Compiler עובד במצב regex-only.
    DOTNET_PAYLOAD_COPILOT_TOKEN_ENDPOINT: Optional[str] = None
    # Timeout להמתנה לתשובה מסוכן Payload Builder (שניות). הסוכן בונה JSON גדול —
    # נותנים מרווח. אם עדיין נכשל, מקדים את הפלט של הסוכן (להוריד field_catalog).
    DOTNET_PAYLOAD_BUILDER_TIMEOUT_SECONDS: int = 300
    # WebSocket — אם True, WebChat מתחבר ל-DirectLine ב-WebSocket (סטרימינג אמיתי, מילה-מילה).
    # ברשתות ארגוניות שחוסמות WebSocket — הגדר False וה-WebChat יחזור ל-HTTP polling.
    COPILOT_USE_WEBSOCKET: bool = True
    # פערים בין polls (ms) — רלוונטי רק כש-COPILOT_USE_WEBSOCKET=False.
    # ערך נמוך = יותר תכוף = קרוב יותר לסטרימינג, אבל יותר עומס רשת. ברירת מחדל 1000ms.
    COPILOT_POLLING_INTERVAL_MS: int = 1000

    # .NET department — Kafka (direct via confluent-kafka SDK)
    KAFKA_BOOTSTRAP_SERVERS: Optional[str] = None         # "broker1:9092,broker2:9092"
    KAFKA_SECURITY_PROTOCOL: str = "PLAINTEXT"            # PLAINTEXT | SASL_SSL | SSL
    KAFKA_SASL_MECHANISM: str = "PLAIN"                   # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
    KAFKA_SASL_USERNAME: Optional[str] = None
    KAFKA_SASL_PASSWORD: Optional[str] = None
    KAFKA_CONSUMER_GROUP_PREFIX: str = "qa-ai-hero"
    # ★ שם consumer group מדויק (ללא suffix אקראי). הגדר אם ה-principal שלך מורשה
    # ל-group ספציפי (ACL literal). אם ריק → נשתמש ב-PREFIX + suffix אקראי.
    KAFKA_CONSUMER_GROUP: Optional[str] = None
    KAFKA_DEFAULT_TIMEOUT_SECONDS: int = 30
    # ★ רצפת timeout ל-kafka_wait — ה-Worker אסינכרוני (כותב ל-target תוך עד דקה-שתיים).
    # ה-wait ימתין לפחות כך הרבה (early-return ברגע שנמצא match).
    KAFKA_WAIT_MIN_SECONDS: int = 150
    # ★ סטיית clock מותרת בין השעון שלנו לזמן ה-broker, לצורך ה-timestamp filter:
    # מקבלים מסר target רק אם timestamp >= publish_ts - SKEW (מונע לתפוס מסר ישן מ-TC קודם).
    KAFKA_TIMESTAMP_SKEW_SECONDS: int = 10
    # ★ Confluent REST Proxy — מסלול מועדף. אם מאוכלס, ה-.NET runner מפרסם/צורך דרך HTTP
    # (httpx, מכבד VERIFY_SSL) במקום הקליינט הנייטיב. עוקף בעיות ACL/cert של librdkafka,
    # כי ה-proxy מפרסם ב-principal פריבילגי משלו ומשתמש ב-Basic-Auth רק בשכבת ה-HTTP.
    KAFKA_REST_PROXY_URL: Optional[str] = None            # "https://cnf-cnct01-test:8082"
    KAFKA_REST_USERNAME: Optional[str] = None             # ריק → fallback ל-KAFKA_SASL_USERNAME
    KAFKA_REST_PASSWORD: Optional[str] = None             # ריק → fallback ל-KAFKA_SASL_PASSWORD
    # ★ כיסוי partitions ב-consume: ה-group משותף עם ה-Worker, אז subscribe נותן רק חלק
    # מה-partitions (rebalance) ומפספס את המסר. הפתרון: manual assign של *כל* ה-partitions.
    # כש-GET /topics חסום (אין Describe ACL) אי-אפשר לגלות כמה partitions יש — לכן probe:
    # מאמתים 0..PROBE_MAX-1 ושומרים רק את התקפים (seek-to-end per-partition). מגלה את
    # המספר האמיתי לבד, בלי תלות ב-ACL או בספירה ידנית.
    KAFKA_PARTITION_PROBE_MAX: int = 16
    # ★ override אופציונלי: אם ידוע מספר ה-partitions של ה-target — מאמתים 0..N-1 ישירות
    # (מדלג על ה-probe). ריק → probe אוטומטי (ברירת מחדל מומלצת כשהמספר לא ודאי).
    KAFKA_TARGET_PARTITIONS: Optional[int] = None

    # .NET department — Couchbase (direct via couchbase SDK)
    COUCHBASE_CONNECTION_STRING: Optional[str] = None     # "couchbase://node1" / "couchbases://..."
    COUCHBASE_USERNAME: Optional[str] = None
    COUCHBASE_PASSWORD: Optional[str] = None
    COUCHBASE_DEFAULT_TIMEOUT_SECONDS: int = 30

    # Azure AI Foundry — מסלול חלופי לכתיבת test cases (עוקף את Copilot Studio)
    AZURE_FOUNDRY_ENDPOINT: Optional[str] = None
    FOUNDRY_WRITER_AGENT_ID: Optional[str] = None
    # אימות: browser (פותח דפדפן, ברירת מחדל) | device_code (CLI code) | default (chain — דורש az login/VS)
    FOUNDRY_AUTH_MODE: str = "browser"
    # אופציות אימות: interactive (פותח דפדפן), device_code (CLI), client_secret (server-to-server), token (ידני)
    COPILOT_AUTH_MODE: str = "interactive"
    COPILOT_CLIENT_SECRET: Optional[str] = None        # רק אם COPILOT_AUTH_MODE=client_secret
    COPILOT_TOKEN: Optional[str] = None                # רק אם COPILOT_AUTH_MODE=token (paste manually)

    # Azure OpenAI (Phase B)
    AZURE_OPENAI_ENDPOINT: Optional[str] = None
    AZURE_OPENAI_KEY: Optional[str] = None
    AZURE_OPENAI_DEPLOYMENT: str = "gpt51-qa"
    # ★ deployment ייעודי ל-compiler (".NET brain"). None → נופל ל-AZURE_OPENAI_DEPLOYMENT. מאפשר
    # לשדרג רק את המוח (mini→חזק) למשימת פירוש-התסריט, בלי לגעת בשאר. ראה property compiler_deployment.
    COMPILER_AZURE_OPENAI_DEPLOYMENT: Optional[str] = None
    AZURE_OPENAI_API_VERSION: str = "2024-08-01-preview"
    # ★ true → להשתמש ב-v1 API (/openai/v1/) במקום ה-route הקלאסי. נדרש למודלים חדשים שנפרסים
    # ב-Foundry וחשופים רק דרך v1 (gpt-5.x / o-series — ה-route הקלאסי מחזיר 404). gpt-4.x = false.
    AZURE_OPENAI_USE_V1: bool = False

    # Azure DevOps
    ADO_ORG_URL: Optional[str] = None
    ADO_PROJECT: Optional[str] = None
    ADO_PAT: Optional[str] = None

    # Runner mode
    RUNNER_MODE: Literal["mock", "esb"] = "mock"
    PLAYWRIGHT_HEADLESS: bool = True
    VERIFY_SSL: bool = True

    # Confluent Control Center
    CONFLUENT_URL: Optional[str] = None
    CONFLUENT_USERNAME: Optional[str] = None
    CONFLUENT_PASSWORD: Optional[str] = None

    # Kibana
    KIBANA_URL: Optional[str] = None
    KIBANA_USERNAME: Optional[str] = None
    KIBANA_PASSWORD: Optional[str] = None

    # HTTP
    HTTP_TIMEOUT_SECONDS: int = 30

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    LOG_LEVEL: str = "INFO"

    def token_endpoint_for(self, department: str) -> Optional[str]:
        """מחזיר את ה-token endpoint המתאים לפי department.
        backward-compat: department לא ידוע / לא מוגדר → ESB.
        """
        if (department or "").lower() == "dotnet":
            return self.DOTNET_COPILOT_TOKEN_ENDPOINT
        return self.COPILOT_TOKEN_ENDPOINT

    def canvas_mode_for(self, department: str) -> bool:
        return bool(self.token_endpoint_for(department))

    @property
    def kafka_enabled(self) -> bool:
        # ה-runner מוכן לרוץ אם יש או native bootstrap או REST proxy.
        return bool(self.KAFKA_BOOTSTRAP_SERVERS or self.KAFKA_REST_PROXY_URL)

    @property
    def kafka_rest_enabled(self) -> bool:
        return bool(self.KAFKA_REST_PROXY_URL)

    @property
    def kafka_rest_auth(self) -> tuple:
        """(username, password) ל-Basic Auth מול ה-REST Proxy. fallback ל-SASL creds."""
        user = self.KAFKA_REST_USERNAME or self.KAFKA_SASL_USERNAME or ""
        pwd = self.KAFKA_REST_PASSWORD or self.KAFKA_SASL_PASSWORD or ""
        return (user, pwd)

    @property
    def couchbase_enabled(self) -> bool:
        return bool(self.COUCHBASE_CONNECTION_STRING)

    @property
    def copilot_canvas_mode(self) -> bool:
        return bool(self.COPILOT_TOKEN_ENDPOINT)

    @property
    def copilot_embed_mode(self) -> bool:
        # Canvas mode מקבל עדיפות אם שניהם מוגדרים.
        if self.copilot_canvas_mode:
            return False
        return bool(self.COPILOT_WEBCHAT_URL)

    @property
    def foundry_enabled(self) -> bool:
        return bool(self.AZURE_FOUNDRY_ENDPOINT and self.FOUNDRY_WRITER_AGENT_ID)

    @property
    def copilot_real_enabled(self) -> bool:
        # ל-MSAL נדרשים תמיד tenant + client. ל-endpoint או direct_url, או env_id+agent_id.
        auth_ready = bool(self.COPILOT_TENANT_ID and self.COPILOT_APP_CLIENT_ID)
        endpoint_ready = bool(self.COPILOT_DIRECT_CONNECT_URL) or bool(
            self.COPILOT_ENVIRONMENT_ID and self.COPILOT_AGENT_IDENTIFIER
        )
        return auth_ready and endpoint_ready

    @property
    def ado_enabled(self) -> bool:
        return bool(self.ADO_ORG_URL and self.ADO_PROJECT and self.ADO_PAT)

    @property
    def azure_openai_enabled(self) -> bool:
        return bool(self.AZURE_OPENAI_ENDPOINT and self.AZURE_OPENAI_KEY)

    @property
    def compiler_deployment(self) -> str:
        """ה-deployment למוח (compiler) — COMPILER_AZURE_OPENAI_DEPLOYMENT אם הוגדר, אחרת ברירת-המחדל."""
        return self.COMPILER_AZURE_OPENAI_DEPLOYMENT or self.AZURE_OPENAI_DEPLOYMENT


settings = Settings()
