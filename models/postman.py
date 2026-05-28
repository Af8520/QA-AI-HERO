from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class PostmanHeader(BaseModel):
    key: str
    value: str
    disabled: bool = False


class PostmanBody(BaseModel):
    mode: Optional[str] = None  # "raw" | "formdata" | "urlencoded" | "graphql" | None
    raw: Optional[str] = None
    formdata: List[Dict[str, Any]] = Field(default_factory=list)
    urlencoded: List[Dict[str, Any]] = Field(default_factory=list)
    options: Dict[str, Any] = Field(default_factory=dict)


class PostmanAuth(BaseModel):
    type: Optional[str] = None  # "bearer" | "basic" | "apikey" | None
    params: Dict[str, Any] = Field(default_factory=dict)


class PostmanRequest(BaseModel):
    name: str
    method: str
    url_raw: str
    headers: List[PostmanHeader] = Field(default_factory=list)
    body: Optional[PostmanBody] = None
    auth: Optional[PostmanAuth] = None
    description: Optional[str] = None


class PostmanCollection(BaseModel):
    name: str
    requests: List[PostmanRequest] = Field(default_factory=list)
    info: Dict[str, Any] = Field(default_factory=dict)

    def find_by_name(self, name: str) -> Optional[PostmanRequest]:
        if not name:
            return None
        target = name.strip().lower()
        for r in self.requests:
            if r.name.strip().lower() == target:
                return r
        return None

    def request_names(self) -> List[str]:
        return [r.name for r in self.requests]


class PostmanEnvironment(BaseModel):
    name: str = "default"
    values: Dict[str, str] = Field(default_factory=dict)
