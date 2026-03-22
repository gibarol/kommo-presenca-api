import os
import re
import time
from typing import Dict, Any

import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

# =========================
# CONFIG
# =========================
BASE_URL = os.getenv("PRESENCA_BASE_URL").rstrip("/")
LOGIN = os.getenv("PRESENCA_LOGIN")
SENHA = os.getenv("PRESENCA_SENHA")

KOMMO_SUBDOMAIN = os.getenv("KOMMO_SUBDOMAIN")
KOMMO_TOKEN = os.getenv("KOMMO_TOKEN")

KOMMO_TARGET_STATUS_ID = int(os.getenv("KOMMO_TARGET_STATUS_ID", "0"))
KOMMO_MSG_FIELD_ID = int(os.getenv("KOMMO_MSG_FIELD_ID", "994693"))

TIMEOUT = 30

# =========================
# LOG
# =========================
def log_step(etapa: str, msg: str, data: Any = None):
    print(f"[{etapa}] {msg}")
    if data:
        print(f"[{etapa}] DATA: {data}")

# =========================
# UTIL
# =========================
def limpar_numero(numero: str) -> str:
    numero = re.sub(r"\D", "", numero)

    if len(numero) == 10:
        # adiciona 9 após DDD
        numero = numero[:2] + "9" + numero[2:]

    return numero

def preparar_texto_para_campo_kommo(texto: str) -> str:
    if not texto:
        return ""

    texto = (
        texto.replace("\n", " ")
             .replace("\r", " ")
             .replace("🙂", "")
             .replace("💰", "")
             .replace("📉", "")
             .replace("⚠️", "")
    )

    texto = re.sub(r"\s+", " ", texto).strip()
    return texto

def kommo_headers():
    return {
        "Authorization": f"Bearer {KOMMO_TOKEN}",
        "Content-Type": "application/json"
    }

# =========================
# KOMMO
# =========================
def criar_nota(lead_id, texto):
    url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads/{lead_id}/notes"

    body = [{
        "note_type": "common",
        "params": {
            "text": texto
        }
    }]

    requests.post(url, headers=kommo_headers(), json=body)

def atualizar_campo_msg(lead_id, texto):
    texto_limpo = preparar_texto_para_campo_kommo(texto)

    url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads"

    body = [{
        "id": int(lead_id),
        "custom_fields_values": [{
            "field_id": KOMMO_MSG_FIELD_ID,
            "values": [{"value": texto_limpo}]
        }]
    }]

    requests.patch(url, headers=kommo_headers(), json=body)

def mover_lead(lead_id):
    url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads"

    body = [{
        "id": int(lead_id),
        "status_id": KOMMO_TARGET_STATUS_ID
    }]

    requests.patch(url, headers=kommo_headers(), json=body)

def aplicar_tag(lead_id, tag):
    url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads/tags"

    body = [{
        "name": tag
    }]

    requests.post(url, headers=kommo_headers(), json=body)

# =========================
# PRESENÇA
# =========================
def consultar_vinculo(cpf):
    url = f"{BASE_URL}/v3/operacoes/consignado-privado/consultar-vinculos"

    resp = requests.post(
        url,
        json={"cpf": cpf},
        auth=(LOGIN, SENHA),
        timeout=TIMEOUT
    )

    return resp.json()

# =========================
# MENSAGENS
# =========================
def montar_mensagem(data):
    nome = data.get("nome", "cliente")

    if data["status"] == "elegivel":
        return f"Olá, {nome}, tudo bem? 🙂 Identificamos uma oportunidade para você. Você possui aproximadamente R$ {data['valor']} disponível com parcela estimada de R$ {data['parcela']}. Se quiser, posso seguir com a simulação completa para você."

    if data["status"] == "aguardando_virada_folha":
        return f"Olá, {nome}, tudo bem? 🙂 Sua consulta está em período de virada de folha. Esse é um bloqueio temporário que acontece poucos dias no mês. Assim que normalizar, posso consultar novamente para você."

    return f"Olá, {nome}, tudo bem? 🙂 No momento não encontramos uma opção disponível para liberação. Se quiser, posso verificar outras alternativas para você."

# =========================
# MAIN
# =========================
@app.post("/kommo-webhook")
async def webhook(request: Request):
    body = await request.json()

    lead_id = body.get("leads", {}).get("add", [{}])[0].get("id")
    nome = body.get("leads", {}).get("add", [{}])[0].get("name", "")
    cpf = body.get("leads", {}).get("add", [{}])[0].get("custom_fields", {}).get("cpf", "")
    telefone = body.get("leads", {}).get("add", [{}])[0].get("phone", "")

    telefone = limpar_numero(telefone)

    log_step("INICIO", "Recebido webhook", {"cpf": cpf, "telefone": telefone})

    data = {
        "nome": nome
    }

    try:
        resp = consultar_vinculo(cpf)

        if "Virada" in str(resp):
            data["status"] = "aguardando_virada_folha"
            tag = "Aguardando Virada de Folha"

        elif resp.get("elegivel"):
            data["status"] = "elegivel"
            data["valor"] = resp.get("valor")
            data["parcela"] = resp.get("parcela")
            tag = "Elegível CLT"

        else:
            data["status"] = "nao_elegivel"
            tag = "Não Elegível CLT"

    except Exception as e:
        log_step("ERRO", str(e))
        data["status"] = "nao_elegivel"
        tag = "Erro API"

    mensagem = montar_mensagem(data)

    # NOTA COMPLETA
    nota = f"""
RETORNO API PRESENÇA

Lead: {lead_id}
Nome: {nome}
CPF: {cpf}
Telefone: {telefone}

Status: {data['status']}
Valor: {data.get('valor')}
Parcela: {data.get('parcela')}

Mensagem enviada:
{mensagem}
"""

    criar_nota(lead_id, nota)

    atualizar_campo_msg(lead_id, mensagem)

    mover_lead(lead_id)

    aplicar_tag(lead_id, tag)

    log_step("FIM", "Fluxo concluído")

    return JSONResponse({"ok": True})
