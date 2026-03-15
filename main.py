import os
import re
import time
from typing import Any, Dict, Optional

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

PRESENCA_BASE_URL = os.getenv("PRESENCA_BASE_URL", "").rstrip("/")
PRESENCA_LOGIN = os.getenv("PRESENCA_LOGIN", "")
PRESENCA_SENHA = os.getenv("PRESENCA_SENHA", "")
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "10"))

PRESENCA_DEVICE_USER_AGENT = os.getenv("PRESENCA_DEVICE_USER_AGENT", "")
PRESENCA_DEVICE_OS = os.getenv("PRESENCA_DEVICE_OS", "")
PRESENCA_DEVICE_MODEL = os.getenv("PRESENCA_DEVICE_MODEL", "")
PRESENCA_DEVICE_NAME = os.getenv("PRESENCA_DEVICE_NAME", "")
PRESENCA_DEVICE_TYPE = os.getenv("PRESENCA_DEVICE_TYPE", "")
PRESENCA_GEO_LAT = os.getenv("PRESENCA_GEO_LAT", "-1.0")
PRESENCA_GEO_LON = os.getenv("PRESENCA_GEO_LON", "-5.0")

_LAST_CALL_TS = 0.0


def throttle() -> None:
    global _LAST_CALL_TS
    now = time.time()
    wait = 2.0 - (now - _LAST_CALL_TS)
    if wait > 0:
        time.sleep(wait)
    _LAST_CALL_TS = time.time()


def normalize_digits(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def normalize_cpf(value: Any) -> str:
    cpf = normalize_digits(value)
    return cpf if len(cpf) == 11 else ""


def safe_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return {"raw_text": resp.text[:2000]}


def auth_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }


def find_first_url(obj: Any) -> Optional[str]:
    if isinstance(obj, dict):
        for _, v in obj.items():
            found = find_first_url(v)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_first_url(item)
            if found:
                return found
    elif isinstance(obj, str) and obj.startswith("http"):
        return obj
    return None


def find_first_id(obj: Any) -> Optional[str]:
    id_keys = {"id", "autorizacaoId", "authorizationId", "termoId"}
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in id_keys and v:
                return str(v)
        for _, v in obj.items():
            found = find_first_id(v)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_first_id(item)
            if found:
                return found
    return None


def presenca_login_token() -> str:
    if not PRESENCA_BASE_URL:
        raise RuntimeError("PRESENCA_BASE_URL_nao_configurada")
    if not PRESENCA_LOGIN or not PRESENCA_SENHA:
        raise RuntimeError("PRESENCA_LOGIN_ou_SENHA_nao_configurada")

    throttle()
    url = f"{PRESENCA_BASE_URL}/login"
    payload = {"login": PRESENCA_LOGIN, "senha": PRESENCA_SENHA}

    print(f"[LOGIN] URL: {url}", flush=True)
    resp = requests.post(url, json=payload, timeout=TIMEOUT_SECONDS)
    print(f"[LOGIN] STATUS: {resp.status_code}", flush=True)

    if not resp.ok:
        raise RuntimeError(f"login_falhou_http_{resp.status_code}: {resp.text[:300]}")

    data = safe_json(resp)
    token = data.get("token")
    if not token:
        raise RuntimeError(f"token_ausente_no_login: {data}")

    return token


def presenca_gerar_termo(token: str, cpf: str, nome: str, telefone: str):
    throttle()
    url = f"{PRESENCA_BASE_URL}/consultas/termo-inss"
    payload = {
        "cpf": cpf,
        "nome": nome,
        "telefone": normalize_digits(telefone),
        "produtoId": 28
    }

    print(f"[TERMO] URL: {url}", flush=True)
    print(f"[TERMO] PAYLOAD: {payload}", flush=True)

    resp = requests.post(url, json=payload, headers=auth_headers(token), timeout=TIMEOUT_SECONDS)
    body = safe_json(resp)

    termo_link = find_first_url(body)
    autorizacao_id = None
    if isinstance(body, dict):
        autorizacao_id = body.get("autorizacaoId") or body.get("id")
    if not autorizacao_id:
        autorizacao_id = find_first_id(body)

    print(f"[TERMO] STATUS: {resp.status_code}", flush=True)
    print(f"[TERMO] ID: {autorizacao_id}", flush=True)
    print(f"[TERMO] LINK: {termo_link}", flush=True)
    print(f"[TERMO] BODY: {body}", flush=True)

    return resp.status_code, termo_link, autorizacao_id, body


@app.route("/")
def home():
    return jsonify({
        "status": "ok",
        "mensagem": "API Kommo Presença online"
    })


@app.route("/consulta", methods=["GET", "POST"])
def consulta():
    try:
        cpf = ""
        nome = ""
        telefone = ""
        lead_id = None

        if request.method == "GET":
            cpf = normalize_cpf(request.args.get("cpf"))
            nome = (request.args.get("nome") or "CLIENTE").strip()
            telefone = request.args.get("telefone") or "11999999999"

        if request.method == "POST":
            data = request.get_json(silent=True) or {}
            cpf = normalize_cpf(data.get("cpf"))
            nome = (data.get("nome") or "CLIENTE").strip()
            telefone = data.get("telefone") or "11999999999"
            lead_id = data.get("lead_id")

        if not cpf:
            return jsonify({
                "status": "erro",
                "mensagem": "CPF não informado ou inválido"
            }), 400

        print("=== INICIO TESTE PRESENCA ===", flush=True)
        print(f"CPF: {cpf}", flush=True)
        print(f"NOME: {nome}", flush=True)
        print(f"TELEFONE: {telefone}", flush=True)

        token = presenca_login_token()
        st_termo, termo_link, autorizacao_id, termo_body = presenca_gerar_termo(token, cpf, nome, telefone)

        print("=== FIM TESTE PRESENCA ===", flush=True)

        return jsonify({
            "status": "ok",
            "lead_id": lead_id,
            "etapa_testada": "login_e_termo",
            "termo_http": st_termo,
            "autorizacao_id": autorizacao_id,
            "link_autorizacao": termo_link,
            "detalhe_termo": termo_body
        })

    except Exception as e:
        print("[ERRO GERAL]", str(e), flush=True)
        return jsonify({
            "status": "erro",
            "mensagem": str(e)
        }), 500


if __name__ == "__main__":
    porta = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=porta)
