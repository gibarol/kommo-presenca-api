import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs

import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI()

# =========================
# CONFIG
# =========================
BASE_URL = os.getenv("PRESENCA_BASE_URL", "https://presenca-bank-api.azurewebsites.net").rstrip("/")
PRESENCA_LOGIN = os.getenv("PRESENCA_LOGIN", "")
PRESENCA_SENHA = os.getenv("PRESENCA_SENHA", "")

KOMMO_TOKEN = os.getenv("KOMMO_TOKEN", "")
KOMMO_SUBDOMAIN = os.getenv("KOMMO_SUBDOMAIN", "")
KOMMO_TARGET_STATUS_ID = int(os.getenv("KOMMO_TARGET_STATUS_ID", "103281440"))

TIMEOUT = int(os.getenv("TIMEOUT_SECONDS", "45"))
THROTTLE_SECONDS = float(os.getenv("THROTTLE_SECONDS", "2"))
WAIT_AFTER_AUTO_SIGN = int(os.getenv("WAIT_AFTER_AUTO_SIGN", "20"))
VINCULOS_RETRY_TENTATIVAS = int(os.getenv("VINCULOS_RETRY_TENTATIVAS", "6"))
VINCULOS_RETRY_ESPERA = int(os.getenv("VINCULOS_RETRY_ESPERA", "8"))
SIMULACAO_PRAZO_PADRAO = int(os.getenv("SIMULACAO_PRAZO_PADRAO", "12"))
SIMULACAO_MULTIPLICADOR = float(os.getenv("SIMULACAO_MULTIPLICADOR", "12"))

CPF_FIELD_ID = 974096
KOMMO_MSG_FIELD_ID = int(os.getenv("KOMMO_MSG_FIELD_ID", "994693"))

TAG_ELEGIVEL = os.getenv("KOMMO_TAG_ELEGIVEL", "Elegível CLT")
TAG_NAO_ELEGIVEL = os.getenv("KOMMO_TAG_NAO_ELEGIVEL", "Não Elegível CLT")
TAG_AGUARDANDO_AUTORIZACAO = os.getenv("KOMMO_TAG_AGUARDANDO_AUTORIZACAO", "Aguardando Autorização CLT")
TAG_AGUARDANDO_VIRADA = os.getenv("KOMMO_TAG_AGUARDANDO_VIRADA", "Aguardando Virada de Folha")

STATUS_SUCESSO = "sucesso"
STATUS_AGUARDANDO_AUTORIZACAO = "aguardando_autorizacao"
STATUS_AGUARDANDO_VIRADA_FOLHA = "aguardando_virada_folha"

_LAST_CALL_TS = 0.0


# =========================
# LOG
# =========================
def log_step(step: str, message: str, data: Any = None) -> None:
    print(f"[{step}] {message}", flush=True)
    if data is not None:
        print(f"[{step}] DATA: {data}", flush=True)


# =========================
# HELPERS
# =========================
def throttle() -> None:
    global _LAST_CALL_TS
    now = time.time()
    wait = THROTTLE_SECONDS - (now - _LAST_CALL_TS)
    if wait > 0:
        time.sleep(wait)
    _LAST_CALL_TS = time.time()


def only_digits(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def normalize_cpf(value: Any) -> str:
    cpf = only_digits(value)
    return cpf if len(cpf) == 11 else ""


def normalize_phone(value: Any) -> str:
    """
    Regras:
    - remove tudo que não for número
    - remove prefixo 55 se houver
    - se tiver 10 dígitos: insere 9 após o DDD
    - se tiver 11 dígitos: ok
    - se tiver mais de 11: tenta aproveitar os últimos 11
    - se tiver estrutura parcial com DDD + 8 dígitos: insere 9 no meio
    """
    original = str(value or "")
    digits = only_digits(original)

    if not digits:
        log_step("PHONE", "Telefone vazio", {"original": original})
        return ""

    if digits.startswith("55") and len(digits) > 11:
        digits = digits[2:]

    if len(digits) == 10:
        treated = digits[:2] + "9" + digits[2:]
        log_step("PHONE", "Telefone com 10 dígitos, inserido 9", {
            "original": original,
            "digits": digits,
            "treated": treated
        })
        return treated

    if len(digits) == 11:
        return digits

    if len(digits) > 11:
        tail = digits[-11:]
        if len(tail) == 11:
            log_step("PHONE", "Telefone >11, usando últimos 11", {
                "original": original,
                "digits": digits,
                "treated": tail
            })
            return tail

    if len(digits) >= 10:
        ddd = digits[:2]
        ultimos_8 = digits[-8:]
        treated = ddd + "9" + ultimos_8
        if len(treated) == 11:
            log_step("PHONE", "Telefone reconstruído com DDD + 9 + últimos 8", {
                "original": original,
                "digits": digits,
                "treated": treated
            })
            return treated

    log_step("PHONE", "Telefone inválido após normalização", {
        "original": original,
        "digits": digits
    })
    return ""


def split_phone(phone: str) -> Tuple[str, str]:
    digits = only_digits(phone)
    if len(digits) == 11:
        return digits[:2], digits[2:]
    if len(digits) == 10:
        fixed = digits[:2] + "9" + digits[2:]
        return fixed[:2], fixed[2:]
    return "11", "999999999"


def normalize_cnpj_like(value: Any) -> str:
    digits = only_digits(value)
    if not digits:
        return ""
    if len(digits) >= 14:
        return digits[-14:]
    return digits.zfill(14)


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


def first_name(nome: Optional[str]) -> str:
    texto = str(nome or "").strip()
    return texto.split(" ")[0] if texto else ""


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


def body_text(body: Any) -> str:
    return str(body).lower()


def body_has_missing_authorization(body: Any) -> bool:
    text = body_text(body)
    return (
        "autorização válida" in text
        or "autorizacao válida" in text
        or "autorizacao valida" in text
        or "necessario uma autorização válida" in text
        or "necessário uma autorização válida" in text
    )


def body_has_invalid_cpf_trabalhador(body: Any) -> bool:
    text = body_text(body)
    return "cpftrabalhador" in text and ("inválido" in text or "invalido" in text)


def body_has_phone_already_used(body: Any) -> bool:
    text = body_text(body)
    return "telefone já utilizado" in text or "telefone ja utilizado" in text


def body_has_cpf_not_found(body: Any) -> bool:
    text = body_text(body)
    return "cpf não encontrado na base" in text or "cpf nao encontrado na base" in text


def body_has_virada_folha(body: Any) -> bool:
    text = body_text(body)
    return (
        "virada de competencia credito do trabalhador" in text
        or "virada de competência crédito do trabalhador" in text
        or "virada de competencia crédito do trabalhador" in text
        or "virada de competência credito do trabalhador" in text
        or "virada de folha" in text
    )


def body_has_credito_trabalhador_competencia(body: Any) -> bool:
    text = body_text(body)
    return (
        "linha de competência crédito do trabalhador" in text
        or "linha de competencia credito do trabalhador" in text
        or "crédito do trabalhador" in text
        or "credito do trabalhador" in text
    )


def body_is_definitive_inelegible(body: Any) -> bool:
    if body_has_virada_folha(body):
        return False
    return any([
        body_has_cpf_not_found(body),
        body_has_invalid_cpf_trabalhador(body),
        body_has_credito_trabalhador_competencia(body),
    ])


def extract_candidates_vinculos(body: Any) -> List[dict]:
    if isinstance(body, list):
        return [x for x in body if isinstance(x, dict)]

    if isinstance(body, dict):
        for key in ["data", "result", "vinculos", "items", "content", "id"]:
            val = body.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
        return [body]

    return []


def pick_vinculo(vinculos: List[dict]) -> Optional[dict]:
    if not vinculos:
        return None

    elegiveis = []
    for v in vinculos:
        elegivel = v.get("elegivel")
        if elegivel is True or str(elegivel).lower() in {"true", "sim", "1"}:
            elegiveis.append(v)

    return elegiveis[0] if elegiveis else vinculos[0]


def extract_valor_parcela(margem_resp: dict) -> float:
    for k in ["valorMargemDisponivel", "margemDisponivel", "valorParcela", "parcelaMaxima"]:
        val = margem_resp.get(k)
        if val is not None:
            try:
                return float(str(val).replace(",", "."))
            except Exception:
                pass
    return 0.0


def extract_oferta(simul_resp: Any, fallback_parcela: float) -> Tuple[float, float]:
    if isinstance(simul_resp, list) and simul_resp:
        first = simul_resp[0]
        if isinstance(first, dict):
            valor = first.get("valorLiberado") or first.get("valor") or first.get("valorDisponivel") or 0
            parcela = first.get("valorParcela") or first.get("parcela") or fallback_parcela or 0
            try:
                return float(str(valor).replace(",", ".")), float(str(parcela).replace(",", "."))
            except Exception:
                return 0.0, fallback_parcela

    if isinstance(simul_resp, dict):
        for key in ["data", "result", "items", "content"]:
            val = simul_resp.get(key)
            if isinstance(val, list) and val:
                return extract_oferta(val, fallback_parcela)

        valor = simul_resp.get("valorLiberado") or simul_resp.get("valor") or simul_resp.get("valorDisponivel") or 0
        parcela = simul_resp.get("valorParcela") or simul_resp.get("parcela") or fallback_parcela or 0
        try:
            return float(str(valor).replace(",", ".")), float(str(parcela).replace(",", "."))
        except Exception:
            return 0.0, fallback_parcela

    return 0.0, fallback_parcela


def format_brl(value: Any) -> str:
    try:
        n = float(value)
        s = f"{n:,.2f}"
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"
    except Exception:
        return "R$ 0,00"


def limpar_mensagem_tecnica(msg: Any) -> str:
    texto = str(msg or "").strip()
    texto_lower = texto.lower()

    if body_has_virada_folha(texto):
        return "Virada de folha"

    if "cpf não encontrado na base" in texto_lower or "cpf nao encontrado na base" in texto_lower:
        return "CPF não encontrado na base"

    if "telefone já utilizado" in texto_lower or "telefone ja utilizado" in texto_lower:
        return "Telefone já utilizado em outro termo"

    if "margem zerada" in texto_lower:
        return "Margem zerada"

    if "rate limit" in texto_lower:
        return "Limite temporário de consultas atingido"

    if "autorização ainda não refletiu" in texto_lower or "autorizacao ainda não refletiu" in texto_lower:
        return "Autorização ainda em processamento"

    return texto or "-"


def preparar_texto_para_campo_kommo(texto: str) -> str:
    """
    Prepara a mensagem para ser salva em campo do Kommo.
    Remove emojis simples, quebras de linha e espaços duplicados.
    Deixa tudo em uma linha só para o bot enviar inteiro.
    """
    if not texto:
        return ""

    texto = str(texto)

    texto = (
        texto.replace("🙂", "")
             .replace("💰", "")
             .replace("📉", "")
             .replace("📌", "")
             .replace("✅", "")
             .replace("⚠️", "")
             .replace("📆", "")
    )

    texto = texto.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    texto = re.sub(r"\s+", " ", texto).strip()

    return texto


def build_response(
    lead_id: Optional[str],
    status: str,
    elegibilidade: Optional[str] = None,
    valor_disponivel: Optional[float] = None,
    parcela: Optional[float] = None,
    autorizacao_id: Optional[str] = None,
    link_autorizacao: Optional[str] = None,
    mensagem_tecnica: Optional[str] = None,
    nome: Optional[str] = None
) -> Dict[str, Any]:
    primeiro_nome = first_name(nome)
    saudacao_nome = f"{primeiro_nome}, " if primeiro_nome else ""

    mensagem_cliente = ""
    tipo_mensagem = ""

    if status == STATUS_AGUARDANDO_AUTORIZACAO:
        tipo_mensagem = "aguardando_autorizacao"
        mensagem_cliente = (
            f"Olá, {saudacao_nome}tudo bem? 🙂\n\n"
            "Para continuar sua consulta, preciso que você conclua esta autorização rápida:\n\n"
            f"{link_autorizacao or ''}\n\n"
            "Assim que finalizar, me avise aqui para eu seguir com a análise."
        )

    elif status == STATUS_AGUARDANDO_VIRADA_FOLHA:
        tipo_mensagem = "aguardando_virada_folha"
        mensagem_cliente = (
            f"Olá, {saudacao_nome}tudo bem? 🙂\n\n"
            "Sua consulta está em período de virada de folha.\n\n"
            "Esse é um bloqueio temporário que costuma acontecer em poucos dias do mês.\n\n"
            "📌 Assim que a base normalizar, posso consultar novamente para você."
        )

    elif elegibilidade == "sim":
        tipo_mensagem = "elegivel"
        mensagem_cliente = (
            f"Olá, {saudacao_nome}tudo bem? 🙂\n\n"
            "Temos uma boa notícia por aqui.\n\n"
            f"💰 Valor disponível: {format_brl(valor_disponivel)}\n"
            f"📉 Parcela estimada: {format_brl(parcela)}\n\n"
            "Se quiser, sigo com os próximos passos para você."
        )

    elif elegibilidade == "nao":
        tipo_mensagem = "nao_elegivel"
        mensagem_cliente = (
            f"Olá, {saudacao_nome}tudo bem? 🙂\n\n"
            "No momento não encontramos uma condição disponível para essa consulta.\n\n"
            "Se quiser, posso verificar novamente mais tarde ou analisar outra possibilidade."
        )

    return {
        "lead_id": lead_id,
        "status": status,
        "elegibilidade": elegibilidade,
        "valor_disponivel": valor_disponivel,
        "parcela": parcela,
        "autorizacao_id": autorizacao_id,
        "link_autorizacao": link_autorizacao,
        "mensagem_cliente": mensagem_cliente,
        "tipo_mensagem": tipo_mensagem,
        "mensagem_tecnica": mensagem_tecnica
    }


def do_post(url: str, payload: dict, headers: Optional[Dict[str, str]] = None, timeout: Tuple[int, int] = (10, 45)) -> requests.Response:
    throttle()
    return requests.post(url, json=payload, headers=headers, timeout=timeout)


def do_put(url: str, payload: dict, headers: Optional[Dict[str, str]] = None, timeout: Tuple[int, int] = (10, 20)) -> requests.Response:
    throttle()
    return requests.put(url, json=payload, headers=headers, timeout=timeout)


# =========================
# KOMMO HELPERS
# =========================
def kommo_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {KOMMO_TOKEN}",
        "Content-Type": "application/json"
    }


def extrair_lead_id_do_webhook(payload: Any, raw_body_text: str = "") -> Optional[str]:
    try:
        if isinstance(payload, dict):
            if "leads" in payload and isinstance(payload["leads"], dict):
                status_arr = payload["leads"].get("status", [])
                if status_arr and isinstance(status_arr[0], dict):
                    if status_arr[0].get("id"):
                        return str(status_arr[0]["id"])

            embedded = payload.get("_embedded", {})
            leads = embedded.get("leads", [])
            if leads and isinstance(leads[0], dict) and leads[0].get("id"):
                return str(leads[0]["id"])
    except Exception:
        pass

    try:
        if raw_body_text:
            parsed = parse_qs(raw_body_text, keep_blank_values=True)
            lead_id = parsed.get("leads[status][0][id]", [None])[0]
            if lead_id:
                return str(lead_id)
    except Exception:
        pass

    return None


def buscar_lead_kommo(lead_id: str) -> Optional[Dict[str, str]]:
    if not KOMMO_TOKEN or not KOMMO_SUBDOMAIN:
        log_step("KOMMO", "token ou subdomínio ausente")
        return None

    lead_url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads/{lead_id}?with=contacts"
    log_step("KOMMO", f"Buscando lead: {lead_url}")

    lead_resp = requests.get(lead_url, headers=kommo_headers(), timeout=30)
    log_step("KOMMO", f"Lead status: {lead_resp.status_code}", lead_resp.text[:2000])

    if not lead_resp.ok:
        return None

    lead_data = safe_json(lead_resp)
    nome = lead_data.get("name", "")

    cpf = ""
    for field in lead_data.get("custom_fields_values", []) or []:
        if field.get("field_id") == CPF_FIELD_ID:
            values = field.get("values", [])
            if values:
                cpf = str(values[0].get("value", "") or "")
                break

    telefone = ""
    contatos = lead_data.get("_embedded", {}).get("contacts", []) or []
    if contatos:
        contato_id = contatos[0].get("id")
        if contato_id:
            contato_url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/contacts/{contato_id}"
            log_step("KOMMO", f"Buscando contato: {contato_url}")

            contato_resp = requests.get(contato_url, headers=kommo_headers(), timeout=30)
            log_step("KOMMO", f"Contato status: {contato_resp.status_code}", contato_resp.text[:2000])

            if contato_resp.ok:
                contato_data = safe_json(contato_resp)
                for field in contato_data.get("custom_fields_values", []) or []:
                    if field.get("field_code") == "PHONE":
                        values = field.get("values", [])
                        if values:
                            telefone = str(values[0].get("value", "") or "")
                            break

    return {
        "cpf": cpf,
        "nome": nome,
        "telefone": telefone
    }


def criar_nota_kommo(lead_id: str, texto: str) -> None:
    if not lead_id or not texto:
        return

    url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads/{lead_id}/notes"
    body = [{
        "note_type": "common",
        "params": {
            "text": texto
        }
    }]

    try:
        resp = requests.post(url, headers=kommo_headers(), json=body, timeout=30)
        log_step("KOMMO_NOTA", f"Status: {resp.status_code}", resp.text[:1000])
    except Exception as e:
        log_step("KOMMO_NOTA", f"Erro ao criar nota: {str(e)}")


def atualizar_mensagem_api_kommo(lead_id: str, texto: str) -> None:
    """
    Salva a mensagem pronta da API no campo personalizado do lead.
    Aqui usaremos o campo Interesse (field_id 994693), mas em formato limpo
    e sem quebra de linha para o bot disparar inteira.
    """
    if not lead_id or not texto:
        return

    if not KOMMO_MSG_FIELD_ID:
        log_step("KOMMO_CAMPO_MSG", "KOMMO_MSG_FIELD_ID não configurado")
        return

    texto_limpo = preparar_texto_para_campo_kommo(texto)

    url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads"

    body = [
        {
            "id": int(lead_id),
            "custom_fields_values": [
                {
                    "field_id": KOMMO_MSG_FIELD_ID,
                    "values": [{"value": texto_limpo}]
                }
            ]
        }
    ]

    try:
        resp = requests.patch(url, headers=kommo_headers(), json=body, timeout=30)
        log_step("KOMMO_CAMPO_MSG", f"Status: {resp.status_code}", resp.text[:1000])
        log_step("KOMMO_CAMPO_MSG", "Texto salvo no campo", texto_limpo)
    except Exception as e:
        log_step("KOMMO_CAMPO_MSG", f"Erro ao atualizar mensagem da API: {str(e)}")


def aplicar_tags_kommo(lead_id: str, nomes_tags: List[str]) -> None:
    if not lead_id or not nomes_tags:
        return

    tags_payload = []
    for nome_tag in nomes_tags:
        nome_tag = str(nome_tag or "").strip()
        if not nome_tag:
            continue
        tags_payload.append({"name": nome_tag})

    if not tags_payload:
        return

    url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads"
    body = [{
        "id": int(lead_id),
        "_embedded": {
            "tags": tags_payload
        }
    }]

    try:
        resp = requests.patch(url, headers=kommo_headers(), json=body, timeout=30)
        log_step("KOMMO_TAG_APLICAR", f"Status: {resp.status_code}", resp.text[:1000])
    except Exception as e:
        log_step("KOMMO_TAG_APLICAR", f"Erro ao aplicar tags: {str(e)}")


def mover_lead_kommo(lead_id: str, status_id: int) -> None:
    if not lead_id or not status_id:
        return

    url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads"
    body = [
        {
            "id": int(lead_id),
            "status_id": int(status_id)
        }
    ]

    try:
        resp = requests.patch(url, headers=kommo_headers(), json=body, timeout=30)
        log_step("KOMMO_MOVE", f"Status: {resp.status_code}", resp.text[:1000])
    except Exception as e:
        log_step("KOMMO_MOVE", f"Erro ao mover lead: {str(e)}")


def definir_tags_por_resultado(data: Dict[str, Any]) -> List[str]:
    status = data.get("status")
    elegibilidade = data.get("elegibilidade")

    if status == STATUS_AGUARDANDO_VIRADA_FOLHA:
        return [TAG_AGUARDANDO_VIRADA]

    if status == STATUS_AGUARDANDO_AUTORIZACAO:
        return [TAG_AGUARDANDO_AUTORIZACAO]

    if elegibilidade == "sim":
        return [TAG_ELEGIVEL]

    return [TAG_NAO_ELEGIVEL]


# =========================
# PRESENÇA API
# =========================
def presenca_login_token() -> str:
    if not PRESENCA_LOGIN or not PRESENCA_SENHA:
        raise RuntimeError("PRESENCA_LOGIN_ou_SENHA_nao_CONFIGURADA")

    url = f"{BASE_URL}/login"
    payload = {"login": PRESENCA_LOGIN, "senha": PRESENCA_SENHA}

    log_step("LOGIN", f"URL: {url}")
    resp = do_post(url, payload, timeout=(10, 30))
    log_step("LOGIN", f"STATUS: {resp.status_code}")

    if not resp.ok:
        raise RuntimeError(f"login_falhou_http_{resp.status_code}: {resp.text[:500]}")

    data = safe_json(resp)
    token = data.get("token")
    if not token:
        raise RuntimeError(f"token_ausente_no_login: {data}")

    return token


def gerar_termo(headers: Dict[str, str], cpf: str, nome: str, telefone: str) -> Dict[str, Any]:
    if not telefone or len(telefone) != 11:
        log_step("TERMO", "Telefone inválido para geração de termo", {
            "cpf": cpf,
            "nome": nome,
            "telefone": telefone
        })
        return {
            "http_status": 400,
            "autorizacao_id": None,
            "link_autorizacao": None,
            "detalhe_termo": {"erro": "telefone_invalido_para_termo"},
            "erro_telefone_reutilizado": False
        }

    url = f"{BASE_URL}/consultas/termo-inss"
    payload = {
        "cpf": cpf,
        "nome": nome,
        "telefone": normalize_phone(telefone),
        "produtoId": 28
    }

    log_step("TERMO", f"URL: {url}", payload)
    resp = do_post(url, payload, headers=headers, timeout=(10, 30))
    body = safe_json(resp)

    log_step("TERMO", f"STATUS: {resp.status_code}", body)

    if resp.status_code >= 400 and body_has_phone_already_used(body):
        return {
            "http_status": resp.status_code,
            "autorizacao_id": None,
            "link_autorizacao": None,
            "detalhe_termo": body,
            "erro_telefone_reutilizado": True
        }

    termo_link = find_first_url(body)
    autorizacao_id = None

    if isinstance(body, dict):
        autorizacao_id = body.get("autorizacaoId") or body.get("id")

    if not autorizacao_id:
        autorizacao_id = find_first_id(body)

    log_step("TERMO_GERADO", f"ID: {autorizacao_id} | LINK: {termo_link}", body)

    return {
        "http_status": resp.status_code,
        "autorizacao_id": autorizacao_id,
        "link_autorizacao": termo_link,
        "detalhe_termo": body,
        "erro_telefone_reutilizado": False
    }


def assinar_termo(headers: Dict[str, str], autorizacao_id: str) -> Dict[str, Any]:
    url = f"{BASE_URL}/consultas/termo-inss/{autorizacao_id}"
    headers_put = dict(headers)
    headers_put["tenant-id"] = "superuser"

    payload_user = {
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "operationalSystem": "macOS 10.15.7",
        "deviceModel": "MacBook Pro",
        "deviceName": "MacBook Pro 15\"",
        "deviceType": "desktop",
        "geoLocation": {
            "latitude": "-27.6450",
            "longitude": "-48.6678"
        }
    }

    payload_doc = {
        "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "OperationalSystem": "macOS 10.15.7",
        "DeviceModel": "MacBook Pro",
        "DeviceName": "MacBook Pro 15\"",
        "DeviceType": "desktop",
        "GeoLocation": {
            "Latitude": "-27.6450",
            "Longitude": "-48.6678"
        }
    }

    log_step("ASSINAR_TERMO", f"URL: {url}")
    log_step("ASSINATURA", f"tentando auto assinatura ID={autorizacao_id}")

    try:
        log_step("ASSINAR_TERMO", "Tentando payload_user", payload_user)
        resp = do_put(url, payload_user, headers=headers_put, timeout=(10, 20))
        body = safe_json(resp)
        log_step("ASSINAR_TERMO", f"STATUS USER: {resp.status_code}", body)

        if resp.status_code in (200, 201, 202, 204):
            resultado = {"ok": True, "detalhe": body, "modo": "user_payload"}
            log_step("ASSINATURA_RESULTADO", "Auto assinatura concluída", resultado)
            return resultado
    except requests.exceptions.ReadTimeout as e:
        resultado = {"ok": True, "detalhe": {"warning": "timeout_no_put_user_payload"}, "modo": "user_payload_timeout"}
        log_step("ASSINAR_TERMO", f"TIMEOUT USER: {str(e)}")
        log_step("ASSINATURA_RESULTADO", "Timeout tratado como sucesso operacional", resultado)
        return resultado
    except Exception as e:
        log_step("ASSINAR_TERMO", f"ERRO USER: {str(e)}")

    try:
        log_step("ASSINAR_TERMO", "Tentando payload_doc", payload_doc)
        resp2 = do_put(url, payload_doc, headers=headers_put, timeout=(10, 20))
        body2 = safe_json(resp2)
        log_step("ASSINAR_TERMO", f"STATUS DOC: {resp2.status_code}", body2)

        if resp2.status_code in (200, 201, 202, 204):
            resultado = {"ok": True, "detalhe": body2, "modo": "doc_payload"}
            log_step("ASSINATURA_RESULTADO", "Auto assinatura concluída em doc_payload", resultado)
            return resultado

        resultado = {"ok": False, "detalhe": body2, "modo": "doc_payload"}
        log_step("ASSINATURA_RESULTADO", "Auto assinatura não confirmada", resultado)
        return resultado
    except requests.exceptions.ReadTimeout as e:
        resultado = {"ok": True, "detalhe": {"warning": "timeout_no_put_doc_payload"}, "modo": "doc_payload_timeout"}
        log_step("ASSINAR_TERMO", f"TIMEOUT DOC: {str(e)}")
        log_step("ASSINATURA_RESULTADO", "Timeout doc tratado como sucesso operacional", resultado)
        return resultado
    except Exception as e:
        resultado = {"ok": False, "detalhe": {"erro": str(e)}, "modo": "doc_payload_exception"}
        log_step("ASSINAR_TERMO", f"ERRO DOC: {str(e)}")
        log_step("ASSINATURA_RESULTADO", "Erro na auto assinatura", resultado)
        return resultado


def consultar_vinculos(headers: Dict[str, str], cpf: str) -> requests.Response:
    url = f"{BASE_URL}/v3/operacoes/consignado-privado/consultar-vinculos"
    payload = {"cpf": cpf}

    log_step("VINCULOS", f"URL: {url}", payload)
    resp = do_post(url, payload, headers=headers, timeout=(10, 45))
    log_step("VINCULOS", f"STATUS: {resp.status_code}", safe_json(resp))

    return resp


def tentar_vinculos_com_retry(headers: Dict[str, str], cpf: str, tentativas: int, espera: int) -> requests.Response:
    ultimo_response = None
    ultimo_erro = None

    for i in range(tentativas):
        try:
            log_step("VINCULOS_RETRY", f"Tentativa {i+1}/{tentativas}")
            resp = consultar_vinculos(headers, cpf)
            body = safe_json(resp)

            if resp.status_code == 200:
                return resp

            ultimo_response = resp

            if resp.status_code == 400 and body_has_virada_folha(body):
                log_step("VINCULOS_RETRY", "Virada de folha detectada", body)
                return resp

            if resp.status_code == 400 and body_is_definitive_inelegible(body):
                log_step("VINCULOS_RETRY", "Inelegibilidade definitiva detectada", body)
                return resp

            if resp.status_code == 400 and body_has_missing_authorization(body):
                log_step("VINCULOS_RETRY", "Autorização ainda ausente", body)
            elif resp.status_code == 429:
                log_step("VINCULOS_RETRY", "Rate limit em vínculos", body)
            else:
                return resp

        except requests.exceptions.ReadTimeout as e:
            log_step("VINCULOS_RETRY", f"timeout tentativa {i+1}: {str(e)}")
            ultimo_erro = e
        except Exception as e:
            log_step("VINCULOS_RETRY", f"erro tentativa {i+1}: {str(e)}")
            ultimo_erro = e

        if i < tentativas - 1:
            log_step("VINCULOS_RETRY", f"aguardando {espera}s...")
            time.sleep(espera)

    if ultimo_response is not None:
        return ultimo_response

    if ultimo_erro is not None:
        raise ultimo_erro

    raise RuntimeError("Falha desconhecida ao consultar vínculos")


def consultar_margem(headers: Dict[str, str], cpf: str, matricula: str, cnpj: str) -> Any:
    time.sleep(2)

    url = f"{BASE_URL}/v3/operacoes/consignado-privado/consultar-margem"
    payload = {"cpf": cpf, "matricula": matricula, "cnpj": cnpj}

    log_step("MARGEM", f"URL: {url}", payload)

    try:
        resp = do_post(url, payload, headers=headers, timeout=(10, 60))
        body = safe_json(resp)

        log_step("MARGEM", f"STATUS: {resp.status_code}", body)

        if resp.status_code == 429:
            return {"erro_rate_limit": True, "mensagem": "Limite de requisições atingido no endpoint de margem"}

        resp.raise_for_status()
        return body

    except requests.exceptions.ReadTimeout:
        return {"erro_timeout": True, "mensagem": "Timeout ao consultar margem"}
    except Exception as e:
        return {"erro_generico": True, "mensagem": str(e)}


def simular(headers: Dict[str, str], cpf: str, telefone: str, matricula: str, cnpj: str, margem: dict) -> Any:
    url = f"{BASE_URL}/v5/operacoes/simulacao/disponiveis"

    ddd, numero = split_phone(telefone)
    valor_parcela = extract_valor_parcela(margem)

    if valor_parcela <= 0:
        return {
            "erro_simulacao": True,
            "mensagem": "Margem zerada ou sem parcela válida"
        }

    valor_solicitado = round(max(valor_parcela, 1) * SIMULACAO_MULTIPLICADOR, 2)

    payload = {
        "tomador": {
            "telefone": {
                "ddd": ddd,
                "numero": numero
            },
            "cpf": cpf,
            "nome": margem.get("nome") or "CLIENTE",
            "dataNascimento": margem.get("dataNascimento") or "1982-10-05",
            "nomeMae": margem.get("nomeMae") or "NAO INFORMADO",
            "email": "email@teste.com",
            "sexo": margem.get("sexo") or "M",
            "vinculoEmpregaticio": {
                "cnpjEmpregador": cnpj,
                "registroEmpregaticio": matricula
            },
            "dadosBancarios": {
                "codigoBanco": None,
                "agencia": None,
                "conta": None,
                "digitoConta": None,
                "formaCredito": None
            },
            "endereco": {
                "cep": "",
                "rua": "",
                "numero": "",
                "complemento": "",
                "cidade": "",
                "estado": "",
                "bairro": ""
            }
        },
        "proposta": {
            "valorSolicitado": valor_solicitado,
            "quantidadeParcelas": SIMULACAO_PRAZO_PADRAO,
            "produtoId": 28,
            "valorParcela": valor_parcela
        },
        "documentos": []
    }

    log_step("SIMULACAO", f"URL: {url}", payload)

    resp = do_post(url, payload, headers=headers, timeout=(10, 60))
    body = safe_json(resp)

    log_step("SIMULACAO", f"STATUS: {resp.status_code}", body)

    if resp.status_code == 400:
        return {
            "erro_simulacao": True,
            "mensagem": "Simulação recusada pelo banco",
            "detalhe": body
        }

    resp.raise_for_status()
    return body


# =========================
# PROCESSAMENTO
# =========================
def processar_fluxo_com_vinculos_body(
    headers: Dict[str, str],
    cpf: str,
    telefone: str,
    vinculos_body: Any,
    lead_id: Optional[str],
    nome: Optional[str]
) -> Dict[str, Any]:
    vinculos = extract_candidates_vinculos(vinculos_body)
    vinculo = pick_vinculo(vinculos)

    if not vinculo:
        return build_response(
            lead_id=lead_id,
            status=STATUS_SUCESSO,
            elegibilidade="nao",
            mensagem_tecnica="Nenhum vínculo encontrado",
            nome=nome
        )

    matricula = str(
        vinculo.get("matricula")
        or vinculo.get("registroEmpregaticio")
        or vinculo.get("registro")
        or vinculo.get("matriculaRegistro")
        or ""
    )

    cnpj = normalize_cnpj_like(
        vinculo.get("numeroInscricaoEmpregador")
        or vinculo.get("cnpjEmpregador")
        or vinculo.get("cnpj")
        or ""
    )

    if not matricula or not cnpj:
        return build_response(
            lead_id=lead_id,
            status=STATUS_SUCESSO,
            elegibilidade="nao",
            mensagem_tecnica="Não foi possível extrair matrícula/cnpj",
            nome=nome
        )

    margem = consultar_margem(headers, cpf, matricula, cnpj)

    if isinstance(margem, dict) and (
        margem.get("erro_rate_limit")
        or margem.get("erro_timeout")
        or margem.get("erro_generico")
    ):
        return build_response(
            lead_id=lead_id,
            status=STATUS_SUCESSO,
            elegibilidade="nao",
            mensagem_tecnica=f"Erro na margem: {margem}",
            nome=nome
        )

    valor_parcela = extract_valor_parcela(margem)
    if valor_parcela <= 0:
        return build_response(
            lead_id=lead_id,
            status=STATUS_SUCESSO,
            elegibilidade="nao",
            mensagem_tecnica="Margem zerada",
            nome=nome
        )

    simulacao = simular(headers, cpf, telefone, matricula, cnpj, margem)

    if isinstance(simulacao, dict) and simulacao.get("erro_simulacao"):
        return build_response(
            lead_id=lead_id,
            status=STATUS_SUCESSO,
            elegibilidade="nao",
            mensagem_tecnica=f"Erro na simulação: {simulacao}",
            nome=nome
        )

    valor_disponivel, parcela = extract_oferta(simulacao, valor_parcela)

    if valor_disponivel <= 0 or parcela <= 0:
        return build_response(
            lead_id=lead_id,
            status=STATUS_SUCESSO,
            elegibilidade="nao",
            mensagem_tecnica="Oferta inválida",
            nome=nome
        )

    return build_response(
        lead_id=lead_id,
        status=STATUS_SUCESSO,
        elegibilidade="sim",
        valor_disponivel=valor_disponivel,
        parcela=parcela,
        mensagem_tecnica="Consulta concluída com sucesso",
        nome=nome
    )


# =========================
# FLUXO COMPLETO
# =========================
def tentar_fluxo_completo(
    cpf: str,
    nome: str,
    telefone: str,
    lead_id: Optional[str],
    autorizacao_id: Optional[str] = None
) -> Dict[str, Any]:
    token = presenca_login_token()
    headers = auth_headers(token)

    telefone = normalize_phone(telefone)

    log_step("FLUXO", f"Início do fluxo | lead_id={lead_id} | cpf={cpf} | telefone={telefone}")

    if autorizacao_id:
        assinatura = assinar_termo(headers, autorizacao_id)
        log_step("FLUXO", "Resultado assinatura com autorizacao_id", assinatura)

        if not assinatura.get("ok"):
            return build_response(
                lead_id=lead_id,
                status=STATUS_AGUARDANDO_AUTORIZACAO,
                autorizacao_id=autorizacao_id,
                link_autorizacao=None,
                mensagem_tecnica="Falha ao assinar termo com autorizacao_id recebido",
                nome=nome
            )

        time.sleep(WAIT_AFTER_AUTO_SIGN)

        try:
            resp_vinc = tentar_vinculos_com_retry(headers, cpf, VINCULOS_RETRY_TENTATIVAS, VINCULOS_RETRY_ESPERA)
            body = safe_json(resp_vinc)

            if resp_vinc.status_code == 200:
                return processar_fluxo_com_vinculos_body(headers, cpf, telefone, body, lead_id, nome)

            if resp_vinc.status_code == 429:
                return build_response(
                    lead_id=lead_id,
                    status=STATUS_SUCESSO,
                    elegibilidade="nao",
                    mensagem_tecnica="Rate limit em vínculos",
                    nome=nome
                )

            if resp_vinc.status_code == 400 and body_has_virada_folha(body):
                return build_response(
                    lead_id=lead_id,
                    status=STATUS_AGUARDANDO_VIRADA_FOLHA,
                    elegibilidade=None,
                    autorizacao_id=autorizacao_id,
                    mensagem_tecnica="Virada de folha",
                    nome=nome
                )

            if resp_vinc.status_code == 400 and body_is_definitive_inelegible(body):
                return build_response(
                    lead_id=lead_id,
                    status=STATUS_SUCESSO,
                    elegibilidade="nao",
                    mensagem_tecnica=f"Inelegibilidade definitiva: {body}",
                    nome=nome
                )

            if resp_vinc.status_code == 400 and body_has_missing_authorization(body):
                return build_response(
                    lead_id=lead_id,
                    status=STATUS_AGUARDANDO_AUTORIZACAO,
                    autorizacao_id=autorizacao_id,
                    link_autorizacao=None,
                    mensagem_tecnica="Autorização ainda não refletiu após assinatura",
                    nome=nome
                )

        except Exception as e:
            log_step("POS_ASSINATURA", f"erro: {str(e)}")

        return build_response(
            lead_id=lead_id,
            status=STATUS_SUCESSO,
            elegibilidade="nao",
            mensagem_tecnica="Falha após assinatura com autorizacao_id",
            nome=nome
        )

    try:
        resp_vinc = tentar_vinculos_com_retry(headers, cpf, VINCULOS_RETRY_TENTATIVAS, VINCULOS_RETRY_ESPERA)
        body_vinc = safe_json(resp_vinc)

        if resp_vinc.status_code == 200:
            return processar_fluxo_com_vinculos_body(headers, cpf, telefone, body_vinc, lead_id, nome)

        if resp_vinc.status_code == 429:
            return build_response(
                lead_id=lead_id,
                status=STATUS_SUCESSO,
                elegibilidade="nao",
                mensagem_tecnica="Rate limit em vínculos",
                nome=nome
            )

        if resp_vinc.status_code == 400 and body_has_virada_folha(body_vinc):
            return build_response(
                lead_id=lead_id,
                status=STATUS_AGUARDANDO_VIRADA_FOLHA,
                mensagem_tecnica="Virada de folha",
                nome=nome
            )

        if resp_vinc.status_code == 400 and body_is_definitive_inelegible(body_vinc):
            return build_response(
                lead_id=lead_id,
                status=STATUS_SUCESSO,
                elegibilidade="nao",
                mensagem_tecnica=f"Inelegibilidade definitiva: {body_vinc}",
                nome=nome
            )

        if not (resp_vinc.status_code == 400 and body_has_missing_authorization(body_vinc)):
            return build_response(
                lead_id=lead_id,
                status=STATUS_SUCESSO,
                elegibilidade="nao",
                mensagem_tecnica=f"Resposta inicial fora do fluxo esperado: {body_vinc}",
                nome=nome
            )

        log_step("FLUXO", "Entrando em geração de termo por falta de autorização", body_vinc)

    except Exception as e:
        log_step("VINCULOS_INICIAL", f"erro antes de gerar termo: {str(e)}")
        return build_response(
            lead_id=lead_id,
            status=STATUS_SUCESSO,
            elegibilidade="nao",
            mensagem_tecnica=f"Erro inicial vínculos: {str(e)}",
            nome=nome
        )

    termo = gerar_termo(headers, cpf, nome, telefone)
    novo_id = termo.get("autorizacao_id")
    link = termo.get("link_autorizacao")

    if termo.get("erro_telefone_reutilizado"):
        return build_response(
            lead_id=lead_id,
            status=STATUS_SUCESSO,
            elegibilidade="nao",
            mensagem_tecnica="Telefone já utilizado em outro termo",
            nome=nome
        )

    if not novo_id:
        return build_response(
            lead_id=lead_id,
            status=STATUS_SUCESSO,
            elegibilidade="nao",
            link_autorizacao=link,
            mensagem_tecnica=f"Não foi possível gerar autorizacao_id: {termo}",
            nome=nome
        )

    assinatura_auto = assinar_termo(headers, novo_id)
    log_step("ASSINATURA_AUTO", "Resultado", assinatura_auto)

    if assinatura_auto.get("ok"):
        try:
            log_step("AUTOAUTORIZACAO", f"Aguardando {WAIT_AFTER_AUTO_SIGN}s após autoassinatura")
            time.sleep(WAIT_AFTER_AUTO_SIGN)

            resp_vinc_2 = tentar_vinculos_com_retry(headers, cpf, VINCULOS_RETRY_TENTATIVAS, VINCULOS_RETRY_ESPERA)
            body_vinc_2 = safe_json(resp_vinc_2)

            if resp_vinc_2.status_code == 200:
                return processar_fluxo_com_vinculos_body(headers, cpf, telefone, body_vinc_2, lead_id, nome)

            if resp_vinc_2.status_code == 429:
                return build_response(
                    lead_id=lead_id,
                    status=STATUS_SUCESSO,
                    elegibilidade="nao",
                    autorizacao_id=novo_id,
                    link_autorizacao=link,
                    mensagem_tecnica="Rate limit em vínculos após autoassinatura",
                    nome=nome
                )

            if resp_vinc_2.status_code == 400 and body_has_virada_folha(body_vinc_2):
                return build_response(
                    lead_id=lead_id,
                    status=STATUS_AGUARDANDO_VIRADA_FOLHA,
                    autorizacao_id=novo_id,
                    link_autorizacao=link,
                    mensagem_tecnica="Virada de folha",
                    nome=nome
                )

            if resp_vinc_2.status_code == 400 and body_is_definitive_inelegible(body_vinc_2):
                return build_response(
                    lead_id=lead_id,
                    status=STATUS_SUCESSO,
                    elegibilidade="nao",
                    autorizacao_id=novo_id,
                    link_autorizacao=link,
                    mensagem_tecnica=f"Inelegibilidade definitiva após autoassinatura: {body_vinc_2}",
                    nome=nome
                )

            if resp_vinc_2.status_code == 400 and body_has_missing_authorization(body_vinc_2):
                log_step("AUTOAUTORIZACAO", "autorização ainda não refletiu, aguardando retry final...", body_vinc_2)
                time.sleep(10)

                resp_vinc_3 = tentar_vinculos_com_retry(headers, cpf, 2, 5)
                body_vinc_3 = safe_json(resp_vinc_3)

                if resp_vinc_3.status_code == 200:
                    return processar_fluxo_com_vinculos_body(headers, cpf, telefone, body_vinc_3, lead_id, nome)

                if resp_vinc_3.status_code == 429:
                    return build_response(
                        lead_id=lead_id,
                        status=STATUS_SUCESSO,
                        elegibilidade="nao",
                        autorizacao_id=novo_id,
                        link_autorizacao=link,
                        mensagem_tecnica="Rate limit no retry final",
                        nome=nome
                    )

                if resp_vinc_3.status_code == 400 and body_has_virada_folha(body_vinc_3):
                    return build_response(
                        lead_id=lead_id,
                        status=STATUS_AGUARDANDO_VIRADA_FOLHA,
                        autorizacao_id=novo_id,
                        link_autorizacao=link,
                        mensagem_tecnica="Virada de folha",
                        nome=nome
                    )

                if resp_vinc_3.status_code == 400 and body_is_definitive_inelegible(body_vinc_3):
                    return build_response(
                        lead_id=lead_id,
                        status=STATUS_SUCESSO,
                        elegibilidade="nao",
                        autorizacao_id=novo_id,
                        link_autorizacao=link,
                        mensagem_tecnica=f"Inelegibilidade definitiva no retry final: {body_vinc_3}",
                        nome=nome
                    )

                if resp_vinc_3.status_code == 400 and body_has_missing_authorization(body_vinc_3):
                    return build_response(
                        lead_id=lead_id,
                        status=STATUS_AGUARDANDO_AUTORIZACAO,
                        autorizacao_id=novo_id,
                        link_autorizacao=link,
                        mensagem_tecnica=f"Mesmo após retry final, autorização ainda não refletiu: {body_vinc_3}",
                        nome=nome
                    )

        except Exception as e:
            log_step("AUTOAUTORIZACAO", f"erro após assinatura: {str(e)}")

    return build_response(
        lead_id=lead_id,
        status=STATUS_AGUARDANDO_AUTORIZACAO,
        autorizacao_id=novo_id,
        link_autorizacao=link,
        mensagem_tecnica="Fluxo terminou aguardando autorização",
        nome=nome
    )


# =========================
# FORMATAR NOTA KOMMO
# =========================
def montar_texto_nota_kommo(lead_id: str, nome: str, cpf: str, telefone: str, data: Dict[str, Any]) -> str:
    elegibilidade = data.get("elegibilidade")
    valor_disponivel = data.get("valor_disponivel")
    parcela = data.get("parcela")
    autorizacao_id = data.get("autorizacao_id")
    link_autorizacao = data.get("link_autorizacao")
    mensagem_tecnica = limpar_mensagem_tecnica(data.get("mensagem_tecnica"))
    status = data.get("status")

    primeiro_nome = first_name(nome) or "cliente"

    if status == STATUS_AGUARDANDO_AUTORIZACAO:
        return (
            "📌 RETORNO API PRESENÇA\n\n"
            f"Olá, {primeiro_nome} 🙂\n\n"
            f"Lead ID: {lead_id}\n"
            f"Status: AGUARDANDO AUTORIZAÇÃO\n"
            f"Nome: {nome}\n"
            f"CPF: {cpf}\n"
            f"Telefone: {telefone}\n\n"
            f"Autorização ID: {autorizacao_id or '-'}\n"
            f"Link autorização: {link_autorizacao or '-'}\n"
            f"Motivo técnico: {mensagem_tecnica}"
        )

    if status == STATUS_AGUARDANDO_VIRADA_FOLHA:
        return (
            "📆 RETORNO API PRESENÇA\n\n"
            f"Olá, {primeiro_nome} 🙂\n\n"
            f"Lead ID: {lead_id}\n"
            f"Status: AGUARDANDO VIRADA DE FOLHA\n"
            f"Nome: {nome}\n"
            f"CPF: {cpf}\n"
            f"Telefone: {telefone}\n\n"
            "Observação: esse bloqueio costuma acontecer em poucos dias do mês.\n"
            f"Motivo técnico: {mensagem_tecnica}"
        )

    if elegibilidade == "sim":
        return (
            "✅ RETORNO API PRESENÇA\n\n"
            f"Olá, {primeiro_nome} 🙂\n\n"
            f"Lead ID: {lead_id}\n"
            f"Status: ELEGÍVEL\n"
            f"Nome: {nome}\n"
            f"CPF: {cpf}\n"
            f"Telefone: {telefone}\n\n"
            f"💰 Valor disponível: {format_brl(valor_disponivel)}\n"
            f"📉 Parcela estimada: {format_brl(parcela)}\n\n"
            f"Motivo técnico: {mensagem_tecnica}"
        )

    return (
        "⚠️ RETORNO API PRESENÇA\n\n"
        f"Olá, {primeiro_nome} 🙂\n\n"
        f"Lead ID: {lead_id}\n"
        f"Status: NÃO ELEGÍVEL\n"
        f"Nome: {nome}\n"
        f"CPF: {cpf}\n"
        f"Telefone: {telefone}\n\n"
        f"Motivo técnico: {mensagem_tecnica}"
    )


# =========================
# ROTAS
# =========================
@app.get("/")
def home():
    return {"status": "api rodando"}


@app.get("/consulta")
def consulta(
    cpf: str,
    nome: str,
    telefone: str,
    autorizacao_id: Optional[str] = None,
    lead_id: Optional[str] = None
):
    try:
        cpf = normalize_cpf(cpf)
        telefone = normalize_phone(telefone)

        if not cpf:
            return JSONResponse(
                status_code=200,
                content=build_response(
                    lead_id=lead_id,
                    status=STATUS_SUCESSO,
                    elegibilidade="nao",
                    mensagem_tecnica="CPF inválido",
                    nome=nome
                )
            )

        resultado = tentar_fluxo_completo(
            cpf=cpf,
            nome=nome,
            telefone=telefone,
            lead_id=lead_id,
            autorizacao_id=autorizacao_id
        )

        return JSONResponse(status_code=200, content=resultado)

    except Exception as e:
        log_step("ERRO_GERAL", str(e))
        return JSONResponse(
            status_code=200,
            content=build_response(
                lead_id=lead_id,
                status=STATUS_SUCESSO,
                elegibilidade="nao",
                mensagem_tecnica=str(e),
                nome=nome
            )
        )


@app.post("/kommo-webhook")
async def kommo_webhook(request: Request):
    try:
        raw_body = await request.body()
        raw_body_text = raw_body.decode("utf-8", errors="ignore")
        log_step("KOMMO_WEBHOOK", "RAW BODY", raw_body_text)

        payload = {}
        try:
            payload = await request.json()
        except Exception:
            payload = {}

        log_step("KOMMO_WEBHOOK", "PAYLOAD JSON", payload)

        lead_id = extrair_lead_id_do_webhook(payload, raw_body_text)
        log_step("KOMMO_WEBHOOK", f"LEAD ID EXTRAIDO: {lead_id}")

        if not lead_id:
            return {"status": "ok", "mensagem": "lead_id_nao_encontrado"}

        dados_lead = buscar_lead_kommo(lead_id)
        log_step("KOMMO_WEBHOOK", "DADOS LEAD", dados_lead)

        if not dados_lead:
            criar_nota_kommo(lead_id, "Erro ao buscar os dados do lead no Kommo.")
            return {"status": "ok", "mensagem": "erro_busca_lead"}

        cpf = normalize_cpf(dados_lead.get("cpf"))
        nome = str(dados_lead.get("nome") or "")
        telefone = normalize_phone(dados_lead.get("telefone"))

        log_step("KOMMO_WEBHOOK", f"DADOS NORMALIZADOS | CPF={cpf} | NOME={nome} | TEL={telefone}")

        if not cpf or not nome or not telefone:
            criar_nota_kommo(
                lead_id,
                "Não foi possível consultar: faltam dados obrigatórios no lead (CPF, nome ou telefone)."
            )
            atualizar_mensagem_api_kommo(
                lead_id=lead_id,
                texto="Olá 🙂 No momento não consegui concluir sua consulta porque faltam alguns dados. Me envie por favor seu CPF e telefone atualizados."
            )
            mover_lead_kommo(lead_id, KOMMO_TARGET_STATUS_ID)
            aplicar_tags_kommo(lead_id, [TAG_NAO_ELEGIVEL])
            return {"status": "ok", "mensagem": "dados_incompletos"}

        data = tentar_fluxo_completo(
            cpf=cpf,
            nome=nome,
            telefone=telefone,
            lead_id=str(lead_id),
            autorizacao_id=None
        )

        log_step("KOMMO_WEBHOOK", "RESULTADO FINAL DO FLUXO", data)

        texto_nota = montar_texto_nota_kommo(
            lead_id=str(lead_id),
            nome=nome,
            cpf=cpf,
            telefone=telefone,
            data=data
        )

        criar_nota_kommo(lead_id, texto_nota)

        # grava a mensagem pronta da API no campo Interesse (field_id 994693), sem quebra de linha
        atualizar_mensagem_api_kommo(
            lead_id=lead_id,
            texto=data.get("mensagem_cliente", "")
        )

        mover_lead_kommo(lead_id, KOMMO_TARGET_STATUS_ID)

        tags = definir_tags_por_resultado(data)
        aplicar_tags_kommo(lead_id, tags)

        return {"status": "ok", "resultado": data}

    except Exception as e:
        log_step("ERRO_KOMMO_WEBHOOK", str(e))
        return {"status": "erro", "mensagem": str(e)}
