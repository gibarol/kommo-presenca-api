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

# ETAPAS
KOMMO_STATUS_COM_OFERTA = int(os.getenv("KOMMO_STATUS_COM_OFERTA", "104339388"))
KOMMO_STATUS_SEM_OFERTA = int(os.getenv("KOMMO_STATUS_SEM_OFERTA", "104339392"))

CPF_FIELD_ID = int(os.getenv("CPF_FIELD_ID", "974096"))
KOMMO_MSG_FIELD_ID = int(os.getenv("KOMMO_MSG_FIELD_ID", "994693"))

TIMEOUT = int(os.getenv("TIMEOUT_SECONDS", "45"))
THROTTLE_SECONDS = float(os.getenv("THROTTLE_SECONDS", "2"))
WAIT_AFTER_AUTO_SIGN = int(os.getenv("WAIT_AFTER_AUTO_SIGN", "20"))
VINCULOS_RETRY_TENTATIVAS = int(os.getenv("VINCULOS_RETRY_TENTATIVAS", "6"))
VINCULOS_RETRY_ESPERA = int(os.getenv("VINCULOS_RETRY_ESPERA", "8"))

MARGEM_MINIMA_APROVACAO = float(os.getenv("MARGEM_MINIMA_APROVACAO", "70"))
FALLBACK_MULTIPLICADOR_DISPONIVEL = float(os.getenv("FALLBACK_MULTIPLICADOR_DISPONIVEL", "8"))

PRAZOS_SIMULACAO = [12, 18, 24, 36]

TAG_ELEGIVEL = os.getenv("KOMMO_TAG_ELEGIVEL", "Elegível CLT")
TAG_NAO_ELEGIVEL = os.getenv("KOMMO_TAG_NAO_ELEGIVEL", "Não Elegível CLT")
TAG_AGUARDANDO_AUTORIZACAO = os.getenv("KOMMO_TAG_AGUARDANDO_AUTORIZACAO", "Aguardando Autorização CLT")
TAG_AGUARDANDO_VIRADA = os.getenv("KOMMO_TAG_AGUARDANDO_VIRADA", "Aguardando Virada de Folha")

STATUS_SUCESSO = "sucesso"
STATUS_AGUARDANDO_AUTORIZACAO = "aguardando_autorizacao"
STATUS_AGUARDANDO_VIRADA_FOLHA = "aguardando_virada_folha"

# DADOS GENÉRICOS PARA SIMULAÇÃO
SIM_BANK_CODE = os.getenv("SIM_BANK_CODE", "237")
SIM_AGENCY = os.getenv("SIM_AGENCY", "0001")
SIM_ACCOUNT = os.getenv("SIM_ACCOUNT", "123456")
SIM_ACCOUNT_DIGIT = os.getenv("SIM_ACCOUNT_DIGIT", "0")
SIM_FORMA_CREDITO = os.getenv("SIM_FORMA_CREDITO", "CC")
SIM_CEP = os.getenv("SIM_CEP", "00000000")
SIM_RUA = os.getenv("SIM_RUA", "NAO INFORMADO")
SIM_NUMERO = os.getenv("SIM_NUMERO", "0")
SIM_COMPLEMENTO = os.getenv("SIM_COMPLEMENTO", "")
SIM_CIDADE = os.getenv("SIM_CIDADE", "SAO PAULO")
SIM_ESTADO = os.getenv("SIM_ESTADO", "SP")
SIM_BAIRRO = os.getenv("SIM_BAIRRO", "CENTRO")
SIM_EMAIL = os.getenv("SIM_EMAIL", "cliente@teste.com")

# TRAVA ANTI-LOOP
RECENT_LEAD_LOCKS: Dict[str, float] = {}
LOCK_SECONDS = int(os.getenv("LOCK_SECONDS", "180"))

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
    return only_digits(value)


def safe_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return {"raw_text": resp.text[:2000]}


def parse_float_br(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        s = str(value).strip()
        s = s.replace("R$", "").replace(" ", "")
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", ".")
        return float(s)
    except Exception:
        return 0.0


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


def is_truthy(value: Any) -> bool:
    return value is True or str(value).strip().lower() in {"true", "sim", "1", "yes"}


def vinculo_elegivel_no_banco(v: dict) -> bool:
    return is_truthy(v.get("elegivel"))


def vinculo_tem_dados_minimos(v: dict) -> bool:
    matricula = str(
        v.get("matricula")
        or v.get("registroEmpregaticio")
        or v.get("registro")
        or v.get("matriculaRegistro")
        or ""
    ).strip()

    cnpj = normalize_cnpj_like(
        v.get("numeroInscricaoEmpregador")
        or v.get("cnpjEmpregador")
        or v.get("cnpj")
        or ""
    )

    return bool(matricula and cnpj)


def ordenar_vinculos_para_teste(vinculos: List[dict]) -> List[dict]:
    candidatos = []

    for v in vinculos:
        score = 0
        if is_truthy(v.get("elegivel")):
            score += 100
        if is_truthy(v.get("empresaElegivel")):
            score += 50
        if is_truthy(v.get("possuiMargem")):
            score += 30
        if is_truthy(v.get("ativo")):
            score += 20
        if vinculo_tem_dados_minimos(v):
            score += 10

        candidatos.append((score, v))

    candidatos.sort(key=lambda x: x[0], reverse=True)
    return [item[1] for item in candidatos]


def extract_valor_parcela(margem_resp: dict) -> float:
    for k in ["valorMargemDisponivel", "margemDisponivel", "valorParcela", "parcelaMaxima"]:
        val = margem_resp.get(k)
        if val is not None:
            f = parse_float_br(val)
            if f > 0:
                return f
    return 0.0


def simulacao_sem_retorno_util(simulacao: Any) -> bool:
    if simulacao is None:
        return True

    if isinstance(simulacao, dict):
        if simulacao.get("raw_text") == "":
            return True
        if simulacao.get("erro_simulacao"):
            return True
        if simulacao.get("sem_conteudo"):
            return True
        if not simulacao:
            return True

    if isinstance(simulacao, list) and len(simulacao) == 0:
        return True

    return False


def simulacao_tem_erro_de_negocio(simulacao: Any) -> bool:
    texto = body_text(simulacao)

    termos = [
        "empresa não elegível",
        "empresa nao elegivel",
        "tempo de cargo inferior",
        "email no formato inválido",
        "email no formato invalido",
        "idade inferior",
        "idade superior",
        "produto não permitido",
        "produto nao permitido",
        "política de crédito",
        "politica de credito",
        "regra de produto",
        "nao elegivel",
        "não elegível",
    ]

    return any(t in texto for t in termos)


def calcular_valor_disponivel_fallback(valor_parcela: float) -> float:
    try:
        return round(float(valor_parcela) * FALLBACK_MULTIPLICADOR_DISPONIVEL, 2)
    except Exception:
        return 0.0


def extrair_opcoes_simulacao(simul_resp: Any) -> List[dict]:
    if isinstance(simul_resp, list):
        return [x for x in simul_resp if isinstance(x, dict)]

    if isinstance(simul_resp, dict):
        for key in ["data", "result", "items", "content", "simulacoes", "opcoes", "ofertas"]:
            val = simul_resp.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
        return [simul_resp]

    return []


def obter_prazo_opcao(opcao: dict) -> int:
    candidatos = [
        opcao.get("quantidadeParcelas"),
        opcao.get("prazo"),
        opcao.get("parcelas"),
        opcao.get("numeroParcelas"),
    ]
    for c in candidatos:
        try:
            return int(str(c).strip())
        except Exception:
            pass

    texto = str(opcao)
    achou = re.search(r"(\d+)\s*x", texto.lower())
    if achou:
        try:
            return int(achou.group(1))
        except Exception:
            pass

    return 0


def obter_valor_disponivel_opcao(opcao: dict) -> float:
    candidatos = [
        opcao.get("valorLiberado"),
        opcao.get("valorDisponivel"),
        opcao.get("valor"),
        opcao.get("totalLiberado"),
        opcao.get("valorTotalLiberado"),
    ]
    for c in candidatos:
        v = parse_float_br(c)
        if v > 0:
            return v

    texto = str(opcao)
    achou = re.search(r"total liberado\s*r?\$?\s*([\d\.,]+)", texto.lower())
    if achou:
        return parse_float_br(achou.group(1))

    return 0.0


def obter_parcela_opcao(opcao: dict, fallback_parcela: float) -> float:
    candidatos = [
        opcao.get("valorParcela"),
        opcao.get("parcela"),
        opcao.get("parcelaMaxima"),
        opcao.get("valorParcelaMaxima"),
    ]
    for c in candidatos:
        v = parse_float_br(c)
        if v > 0:
            return v

    texto = str(opcao)
    achou = re.search(r"parcela máxima\s*([\d\.,]+)", texto.lower())
    if achou:
        v = parse_float_br(achou.group(1))
        if v > 0:
            return v

    return fallback_parcela


def extrair_resultados_validos_da_simulacao(simul_resp: Any, fallback_parcela: float, prazo_forcado: int) -> List[Dict[str, Any]]:
    opcoes = extrair_opcoes_simulacao(simul_resp)
    resultados = []

    for opcao in opcoes:
        valor = obter_valor_disponivel_opcao(opcao)
        parcela = obter_parcela_opcao(opcao, fallback_parcela)
        prazo = obter_prazo_opcao(opcao) or prazo_forcado

        if valor > 0 and parcela > 0:
            resultados.append({
                "prazo": prazo,
                "valor_disponivel": valor,
                "parcela": parcela,
                "origem": "simulacao_real",
                "raw": opcao
            })

    return resultados


def escolher_melhor_simulacao(resultados: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not resultados:
        return None

    prioridade_map = {36: 1, 24: 2, 18: 3, 12: 4}
    resultados_ordenados = sorted(
        resultados,
        key=lambda x: (prioridade_map.get(x["prazo"], 99), -x["valor_disponivel"])
    )
    melhor = resultados_ordenados[0]
    log_step("SIMULACAO_ESCOLHA", "Melhor simulação escolhida", melhor)
    return melhor


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
    if "empresa não elegível" in texto_lower or "empresa nao elegivel" in texto_lower:
        return "Empresa não elegível"
    if "tempo de cargo inferior" in texto_lower:
        return "Tempo de cargo inferior ao permitido"
    if "email no formato inválido" in texto_lower or "email no formato invalido" in texto_lower:
        return "Email no formato inválido"

    return texto or "-"


def preparar_texto_para_campo_kommo(texto: str) -> str:
    if not texto:
        return ""
    texto = str(texto)
    texto = (
        texto.replace("\r\n", " ")
        .replace("\n", " ")
        .replace("\r", " ")
        .replace("\t", " ")
        .replace("\u00a0", " ")
        .replace("\u200b", "")
        .replace("\ufeff", "")
    )
    texto = texto.encode("ascii", "ignore").decode("ascii")
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto


def lead_esta_travado(lead_id: str) -> bool:
    agora = time.time()
    expira_em = RECENT_LEAD_LOCKS.get(str(lead_id), 0)
    return expira_em > agora


def travar_lead(lead_id: str) -> None:
    RECENT_LEAD_LOCKS[str(lead_id)] = time.time() + LOCK_SECONDS


def limpar_travas_expiradas() -> None:
    agora = time.time()
    expirados = [k for k, v in RECENT_LEAD_LOCKS.items() if v <= agora]
    for k in expirados:
        RECENT_LEAD_LOCKS.pop(k, None)


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
    mensagem_cliente_campo = ""

    if elegibilidade == "sim":
        mensagem_cliente_campo = preparar_texto_para_campo_kommo(
            "Temos um valor aqui para você. Só um instante que já vamos lhe atender."
        )

    return {
        "lead_id": lead_id,
        "status": status,
        "elegibilidade": elegibilidade,
        "valor_disponivel": valor_disponivel,
        "parcela": parcela,
        "autorizacao_id": autorizacao_id,
        "link_autorizacao": link_autorizacao,
        "mensagem_cliente": mensagem_cliente_campo,
        "tipo_mensagem": "elegivel" if elegibilidade == "sim" else "sem_envio",
        "mensagem_tecnica": mensagem_tecnica,
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
                if status_arr and isinstance(status_arr[0], dict) and status_arr[0].get("id"):
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
    status_id = str(lead_data.get("status_id", "") or "")

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
        "telefone": telefone,
        "status_id": status_id
    }


def criar_nota_kommo(lead_id: str, texto: str) -> None:
    if not lead_id or not texto:
        return

    url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads/{lead_id}/notes"
    body = [{
        "note_type": "common",
        "params": {"text": texto}
    }]

    try:
        resp = requests.post(url, headers=kommo_headers(), json=body, timeout=30)
        log_step("KOMMO_NOTA", f"Status: {resp.status_code}", resp.text[:1000])
    except Exception as e:
        log_step("KOMMO_NOTA", f"Erro ao criar nota: {str(e)}")


def atualizar_mensagem_api_kommo(lead_id: str, texto: str) -> None:
    if not lead_id:
        return

    texto_limpo = preparar_texto_para_campo_kommo(texto)

    url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads"
    body = [{
        "id": int(lead_id),
        "custom_fields_values": [
            {
                "field_id": KOMMO_MSG_FIELD_ID,
                "values": [{"value": texto_limpo}]
            }
        ]
    }]

    try:
        resp = requests.patch(url, headers=kommo_headers(), json=body, timeout=30)
        log_step("KOMMO_CAMPO_MSG", f"Status: {resp.status_code}", resp.text[:1000])
        log_step("KOMMO_CAMPO_MSG", "Texto salvo no campo", texto_limpo)
    except Exception as e:
        log_step("KOMMO_CAMPO_MSG", f"Erro ao atualizar mensagem da API: {str(e)}")


def limpar_campo_mensagem_api_kommo(lead_id: str) -> None:
    if not lead_id:
        return

    url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads"
    body = [{
        "id": int(lead_id),
        "custom_fields_values": [
            {
                "field_id": KOMMO_MSG_FIELD_ID,
                "values": [{"value": ""}]
            }
        ]
    }]

    try:
        resp = requests.patch(url, headers=kommo_headers(), json=body, timeout=30)
        log_step("KOMMO_CAMPO_MSG", f"Campo limpado status: {resp.status_code}", resp.text[:1000])
    except Exception as e:
        log_step("KOMMO_CAMPO_MSG", f"Erro ao limpar campo mensagem: {str(e)}")


def aplicar_tags_kommo(lead_id: str, nomes_tags: List[str]) -> None:
    if not lead_id or not nomes_tags:
        return

    managed_tags = {
        TAG_ELEGIVEL,
        TAG_NAO_ELEGIVEL,
        TAG_AGUARDANDO_AUTORIZACAO,
        TAG_AGUARDANDO_VIRADA,
    }

    try:
        url_get = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads/{lead_id}"
        resp_get = requests.get(url_get, headers=kommo_headers(), timeout=30)
        log_step("KOMMO_TAG_GET", f"Status: {resp_get.status_code}", resp_get.text[:1000])

        tags_existentes = []
        if resp_get.ok:
            data = safe_json(resp_get)
            tags_atuais = data.get("_embedded", {}).get("tags", []) if isinstance(data, dict) else []
            for tag in tags_atuais:
                nome_tag = str(tag.get("name", "")).strip()
                if nome_tag:
                    tags_existentes.append(nome_tag)

        tags_filtradas = [tag for tag in tags_existentes if tag not in managed_tags]

        for nome_tag in nomes_tags:
            nome_tag = str(nome_tag or "").strip()
            if nome_tag and nome_tag not in tags_filtradas:
                tags_filtradas.append(nome_tag)

        tags_payload = [{"name": tag} for tag in tags_filtradas]

        url_patch = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads"
        body = [{
            "id": int(lead_id),
            "_embedded": {"tags": tags_payload}
        }]

        resp_patch = requests.patch(url_patch, headers=kommo_headers(), json=body, timeout=30)
        log_step("KOMMO_TAG_APLICAR", f"Status: {resp_patch.status_code}", resp_patch.text[:1000])

    except Exception as e:
        log_step("KOMMO_TAG_APLICAR", f"Erro ao aplicar tags: {str(e)}")


def mover_lead_kommo(lead_id: str, status_id: int) -> None:
    if not lead_id or not status_id:
        return

    url = f"https://{KOMMO_SUBDOMAIN}.kommo.com/api/v4/leads"
    body = [{
        "id": int(lead_id),
        "status_id": int(status_id)
    }]

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
        "geoLocation": {"latitude": "-27.6450", "longitude": "-48.6678"}
    }

    try:
        resp = do_put(url, payload_user, headers=headers_put, timeout=(10, 20))
        body = safe_json(resp)
        log_step("ASSINAR_TERMO", f"STATUS USER: {resp.status_code}", body)
        if resp.status_code in (200, 201, 202, 204):
            return {"ok": True, "detalhe": body, "modo": "user_payload"}
    except requests.exceptions.ReadTimeout:
        return {"ok": True, "detalhe": {"warning": "timeout_no_put_user_payload"}, "modo": "user_payload_timeout"}
    except Exception as e:
        log_step("ASSINAR_TERMO", f"ERRO USER: {str(e)}")

    return {"ok": False, "detalhe": {"erro": "falha_assinatura"}, "modo": "user_payload"}


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
                return resp

            if resp.status_code == 400 and body_is_definitive_inelegible(body):
                return resp

            if i < tentativas - 1:
                time.sleep(espera)

        except Exception as e:
            ultimo_erro = e
            if i < tentativas - 1:
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


def montar_payload_simulacao(
    cpf: str,
    telefone: str,
    nome_real: str,
    matricula: str,
    cnpj: str,
    margem: dict,
    prazo: int,
    enrich: bool = False
) -> dict:
    ddd, numero = split_phone(telefone)
    valor_parcela = extract_valor_parcela(margem)
    valor_solicitado = round(max(valor_parcela, 1) * prazo, 2)

    payload = {
        "tomador": {
            "telefone": {"ddd": ddd, "numero": numero},
            "cpf": cpf,
            "nome": nome_real or "CLIENTE",
            "dataNascimento": margem.get("dataNascimento") or "1982-10-05",
            "nomeMae": margem.get("nomeMae") or "NAO INFORMADO",
            "email": SIM_EMAIL,
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
            "quantidadeParcelas": prazo,
            "produtoId": 28,
            "valorParcela": valor_parcela
        },
        "documentos": []
    }

    if enrich:
        payload["tomador"]["dadosBancarios"] = {
            "codigoBanco": SIM_BANK_CODE,
            "agencia": SIM_AGENCY,
            "conta": SIM_ACCOUNT,
            "digitoConta": SIM_ACCOUNT_DIGIT,
            "formaCredito": SIM_FORMA_CREDITO
        }
        payload["tomador"]["endereco"] = {
            "cep": SIM_CEP,
            "rua": SIM_RUA,
            "numero": SIM_NUMERO,
            "complemento": SIM_COMPLEMENTO,
            "cidade": SIM_CIDADE,
            "estado": SIM_ESTADO,
            "bairro": SIM_BAIRRO
        }

    return payload


def simular(
    headers: Dict[str, str],
    cpf: str,
    telefone: str,
    nome_real: str,
    matricula: str,
    cnpj: str,
    margem: dict,
    prazo: int
) -> Any:
    url = f"{BASE_URL}/v5/operacoes/simulacao/disponiveis"
    valor_parcela = extract_valor_parcela(margem)

    if valor_parcela <= 0:
        return {"erro_simulacao": True, "mensagem": "Margem zerada ou sem parcela válida", "prazo": prazo}

    payload = montar_payload_simulacao(cpf, telefone, nome_real, matricula, cnpj, margem, prazo, enrich=False)
    log_step("SIMULACAO_PAYLOAD_FINAL", f"Payload enviado para simulação {prazo}x", payload)

    try:
        resp = do_post(url, payload, headers=headers, timeout=(10, 60))
        body = safe_json(resp)
        log_step("SIMULACAO", f"STATUS {prazo}x: {resp.status_code}", body)

        if resp.status_code not in (204, 400):
            resp.raise_for_status()
            return body

        payload2 = montar_payload_simulacao(cpf, telefone, nome_real, matricula, cnpj, margem, prazo, enrich=True)
        log_step("SIMULACAO_PAYLOAD_FINAL", f"Retry enriquecido {prazo}x", payload2)

        resp2 = do_post(url, payload2, headers=headers, timeout=(10, 60))
        body2 = safe_json(resp2)
        log_step("SIMULACAO", f"STATUS RETRY {prazo}x: {resp2.status_code}", body2)

        if resp2.status_code == 204:
            return {"sem_conteudo": True, "raw_text": "", "prazo": prazo}

        if resp2.status_code == 400:
            return {
                "erro_simulacao": True,
                "mensagem": "Simulação recusada pelo banco",
                "detalhe": body2,
                "prazo": prazo
            }

        resp2.raise_for_status()
        return body2

    except requests.exceptions.ReadTimeout:
        return {"erro_simulacao": True, "mensagem": "Timeout na simulação", "prazo": prazo}
    except Exception as e:
        return {"erro_simulacao": True, "mensagem": f"Erro na simulação: {str(e)}", "prazo": prazo}


def tentar_simulacoes_multiplos_prazos(
    headers: Dict[str, str],
    cpf: str,
    telefone: str,
    nome_real: str,
    matricula: str,
    cnpj: str,
    margem: dict
) -> Dict[str, Any]:
    resultados = []
    erros_negocio = []
    valor_parcela = extract_valor_parcela(margem)

    for prazo in PRAZOS_SIMULACAO:
        resultado = simular(
            headers=headers,
            cpf=cpf,
            telefone=telefone,
            nome_real=nome_real,
            matricula=matricula,
            cnpj=cnpj,
            margem=margem,
            prazo=prazo
        )

        if simulacao_tem_erro_de_negocio(resultado):
            erros_negocio.append({
                "prazo": prazo,
                "detalhe": resultado
            })
            log_step("SIMULACAO_MULTIPLA", f"Prazo {prazo}x com erro de negócio", resultado)
            continue

        if simulacao_sem_retorno_util(resultado):
            log_step("SIMULACAO_MULTIPLA", f"Prazo {prazo}x sem retorno útil", resultado)
            continue

        extraidos = extrair_resultados_validos_da_simulacao(resultado, valor_parcela, prazo)
        if extraidos:
            resultados.extend(extraidos)
        else:
            log_step("SIMULACAO_MULTIPLA", f"Prazo {prazo}x sem oferta válida extraída", resultado)

    log_step("SIMULACAO_MULTIPLA", "Resultados válidos encontrados", resultados)
    log_step("SIMULACAO_MULTIPLA", "Erros de negócio encontrados", erros_negocio)

    return {
        "resultados": resultados,
        "erros_negocio": erros_negocio
    }


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
    log_step("ETAPA_1_VINCULOS", "Lista de vínculos recebida", vinculos)

    if not vinculos:
        return build_response(
            lead_id=lead_id,
            status=STATUS_SUCESSO,
            elegibilidade="nao",
            mensagem_tecnica="Nenhum vínculo encontrado",
            nome=nome
        )

    vinculos_elegiveis = [v for v in vinculos if vinculo_elegivel_no_banco(v)]
    log_step("ETAPA_1_VINCULOS", "Vínculos elegíveis filtrados", vinculos_elegiveis)

    if not vinculos_elegiveis:
        return build_response(
            lead_id=lead_id,
            status=STATUS_SUCESSO,
            elegibilidade="nao",
            mensagem_tecnica="Todos os vínculos retornaram inelegíveis no banco",
            nome=nome
        )

    vinculos_ordenados = ordenar_vinculos_para_teste(vinculos_elegiveis)
    log_step("ETAPA_1_VINCULOS", "Vínculos elegíveis ordenados para teste", vinculos_ordenados)

    erros_encontrados = []

    for idx, vinculo in enumerate(vinculos_ordenados, start=1):
        log_step("ETAPA_2_TESTE_VINCULO", f"Testando vínculo {idx}", vinculo)

        matricula = str(
            vinculo.get("matricula")
            or vinculo.get("registroEmpregaticio")
            or vinculo.get("registro")
            or vinculo.get("matriculaRegistro")
            or ""
        ).strip()

        cnpj = normalize_cnpj_like(
            vinculo.get("numeroInscricaoEmpregador")
            or vinculo.get("cnpjEmpregador")
            or vinculo.get("cnpj")
            or ""
        )

        if not matricula or not cnpj:
            erros_encontrados.append({"tipo": "vinculo_incompleto", "vinculo": vinculo})
            continue

        log_step("ETAPA_2_TESTE_VINCULO", "Matrícula e empregador válidos", {
            "matricula": matricula,
            "cnpj": cnpj
        })

        margem = consultar_margem(headers, cpf, matricula, cnpj)
        log_step("ETAPA_3_MARGEM", "Retorno da margem", margem)

        if isinstance(margem, dict) and margem.get("erro_rate_limit"):
            return build_response(
                lead_id=lead_id,
                status=STATUS_SUCESSO,
                elegibilidade="nao",
                mensagem_tecnica="Rate limit na margem",
                nome=nome
            )

        if isinstance(margem, dict) and (margem.get("erro_timeout") or margem.get("erro_generico")):
            erros_encontrados.append({"tipo": "erro_margem", "detalhe": margem, "vinculo": vinculo})
            continue

        valor_parcela = extract_valor_parcela(margem)

        if valor_parcela < MARGEM_MINIMA_APROVACAO:
            erros_encontrados.append({
                "tipo": "margem_abaixo_minima",
                "valor_parcela": valor_parcela,
                "vinculo": vinculo
            })
            continue

        log_step("ETAPA_3_MARGEM", "Margem suficiente para retorno comercial", {
            "valor_parcela": valor_parcela,
            "minimo_configurado": MARGEM_MINIMA_APROVACAO
        })

        resultados_simulacao = tentar_simulacoes_multiplos_prazos(
            headers=headers,
            cpf=cpf,
            telefone=telefone,
            nome_real=nome or "CLIENTE",
            matricula=matricula,
            cnpj=cnpj,
            margem=margem
        )

        resultados_validos = resultados_simulacao.get("resultados", [])
        erros_negocio = resultados_simulacao.get("erros_negocio", [])

        melhor = escolher_melhor_simulacao(resultados_validos)

        if melhor:
            return build_response(
                lead_id=lead_id,
                status=STATUS_SUCESSO,
                elegibilidade="sim",
                valor_disponivel=melhor["valor_disponivel"],
                parcela=melhor["parcela"],
                mensagem_tecnica=f"Consulta concluída com opção {melhor['prazo']}x",
                nome=nome
            )

        if erros_negocio:
            return build_response(
                lead_id=lead_id,
                status=STATUS_SUCESSO,
                elegibilidade="nao",
                mensagem_tecnica=f"Simulação recusada por regra do banco: {erros_negocio[0]['detalhe']}",
                nome=nome
            )

        valor_disponivel_fallback = calcular_valor_disponivel_fallback(valor_parcela)
        log_step("ETAPA_5_RESULTADO", "Sem oferta retornada, usando fallback por margem válida", {
            "valor_disponivel_fallback": valor_disponivel_fallback,
            "parcela": valor_parcela
        })

        return build_response(
            lead_id=lead_id,
            status=STATUS_SUCESSO,
            elegibilidade="sim",
            valor_disponivel=valor_disponivel_fallback,
            parcela=valor_parcela,
            mensagem_tecnica="Retorno por fallback: vínculo elegível e margem válida, sem oferta explícita",
            nome=nome
        )

    log_step("ETAPA_FINAL", "Nenhum vínculo passou nas regras mínimas", erros_encontrados)

    return build_response(
        lead_id=lead_id,
        status=STATUS_SUCESSO,
        elegibilidade="nao",
        mensagem_tecnica="Nenhum vínculo elegível com margem mínima de R$ 70,00",
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

            if resp_vinc.status_code == 400 and body_has_virada_folha(body):
                return build_response(
                    lead_id=lead_id,
                    status=STATUS_AGUARDANDO_VIRADA_FOLHA,
                    autorizacao_id=autorizacao_id,
                    mensagem_tecnica="Virada de folha",
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

    if assinatura_auto.get("ok"):
        try:
            time.sleep(WAIT_AFTER_AUTO_SIGN)
            resp_vinc_2 = tentar_vinculos_com_retry(headers, cpf, VINCULOS_RETRY_TENTATIVAS, VINCULOS_RETRY_ESPERA)
            body_vinc_2 = safe_json(resp_vinc_2)

            if resp_vinc_2.status_code == 200:
                return processar_fluxo_com_vinculos_body(headers, cpf, telefone, body_vinc_2, lead_id, nome)

            if resp_vinc_2.status_code == 400 and body_has_virada_folha(body_vinc_2):
                return build_response(
                    lead_id=lead_id,
                    status=STATUS_AGUARDANDO_VIRADA_FOLHA,
                    autorizacao_id=novo_id,
                    link_autorizacao=link,
                    mensagem_tecnica="Virada de folha",
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
# NOTA INTERNA
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
            f"Cliente: {primeiro_nome}\n"
            f"Status: AGUARDANDO AUTORIZAÇÃO\n"
            f"CPF: {cpf}\n"
            f"Telefone: {telefone}\n"
            f"Autorização ID: {autorizacao_id or '-'}\n"
            f"Link: {link_autorizacao or '-'}\n"
            f"Motivo técnico: {mensagem_tecnica}"
        )

    if status == STATUS_AGUARDANDO_VIRADA_FOLHA:
        return (
            "📆 RETORNO API PRESENÇA\n\n"
            f"Cliente: {primeiro_nome}\n"
            f"Status: AGUARDANDO VIRADA DE FOLHA\n"
            f"CPF: {cpf}\n"
            f"Telefone: {telefone}\n"
            f"Motivo técnico: {mensagem_tecnica}"
        )

    if elegibilidade == "sim":
        return (
            "✅ RETORNO API PRESENÇA\n\n"
            f"Cliente: {primeiro_nome}\n"
            f"Status: ELEGÍVEL\n"
            f"CPF: {cpf}\n"
            f"Telefone: {telefone}\n"
            f"Valor disponível: {format_brl(valor_disponivel)}\n"
            f"Parcela estimada: {format_brl(parcela)}\n"
            f"Motivo técnico: {mensagem_tecnica}"
        )

    return (
        "⚠️ RETORNO API PRESENÇA\n\n"
        f"Cliente: {primeiro_nome}\n"
        f"Status: SEM OFERTA\n"
        f"CPF: {cpf}\n"
        f"Telefone: {telefone}\n"
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

        limpar_travas_expiradas()

        if lead_esta_travado(lead_id):
            log_step("KOMMO_WEBHOOK", f"Lead {lead_id} ignorado por trava anti-loop")
            return {"status": "ok", "mensagem": "lead_ignorado_por_trava"}

        travar_lead(lead_id)

        dados_lead = buscar_lead_kommo(lead_id)
        log_step("KOMMO_WEBHOOK", "DADOS LEAD", dados_lead)

        if not dados_lead:
            criar_nota_kommo(lead_id, "Erro ao buscar os dados do lead no Kommo.")
            return {"status": "ok", "mensagem": "erro_busca_lead"}

        status_id_atual = str(dados_lead.get("status_id", "") or "")

        if status_id_atual in {str(KOMMO_STATUS_COM_OFERTA), str(KOMMO_STATUS_SEM_OFERTA)}:
            log_step("KOMMO_WEBHOOK", f"Lead {lead_id} já está em etapa final, ignorando")
            return {"status": "ok", "mensagem": "lead_ja_em_etapa_final"}

        cpf = normalize_cpf(dados_lead.get("cpf"))
        nome = str(dados_lead.get("nome") or "")
        telefone = normalize_phone(dados_lead.get("telefone"))

        log_step("KOMMO_WEBHOOK", f"DADOS NORMALIZADOS | CPF={cpf} | NOME={nome} | TEL={telefone}")

        if not cpf or not nome or not telefone:
            criar_nota_kommo(
                lead_id,
                "Não foi possível consultar: faltam dados obrigatórios no lead (CPF, nome ou telefone)."
            )
            limpar_campo_mensagem_api_kommo(lead_id)
            if status_id_atual != str(KOMMO_STATUS_SEM_OFERTA):
                mover_lead_kommo(lead_id, KOMMO_STATUS_SEM_OFERTA)
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

        if data.get("elegibilidade") == "sim":
            atualizar_mensagem_api_kommo(
                lead_id=lead_id,
                texto="Temos um valor aqui para você. Só um instante que já vamos lhe atender."
            )
            if status_id_atual != str(KOMMO_STATUS_COM_OFERTA):
                mover_lead_kommo(lead_id, KOMMO_STATUS_COM_OFERTA)
            aplicar_tags_kommo(lead_id, [TAG_ELEGIVEL])

        else:
            limpar_campo_mensagem_api_kommo(lead_id)
            if status_id_atual != str(KOMMO_STATUS_SEM_OFERTA):
                mover_lead_kommo(lead_id, KOMMO_STATUS_SEM_OFERTA)
            aplicar_tags_kommo(lead_id, definir_tags_por_resultado(data))

        return {"status": "ok", "resultado": data}

    except Exception as e:
        log_step("ERRO_KOMMO_WEBHOOK", str(e))
        return {"status": "erro", "mensagem": str(e)}
