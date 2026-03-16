import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

# =========================
# CONFIG
# =========================
BASE_URL = os.getenv("PRESENCA_BASE_URL", "https://presenca-bank-api.azurewebsites.net").rstrip("/")
PRESENCA_LOGIN = os.getenv("PRESENCA_LOGIN", "")
PRESENCA_SENHA = os.getenv("PRESENCA_SENHA", "")
TIMEOUT = int(os.getenv("TIMEOUT_SECONDS", "45"))

_LAST_CALL_TS = 0.0


# =========================
# HELPERS
# =========================
def throttle() -> None:
    global _LAST_CALL_TS
    now = time.time()
    wait = 2.0 - (now - _LAST_CALL_TS)
    if wait > 0:
        time.sleep(wait)
    _LAST_CALL_TS = time.time()


def only_digits(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def normalize_cpf(value: Any) -> str:
    cpf = only_digits(value)
    return cpf if len(cpf) == 11 else ""


def normalize_phone(value: Any) -> str:
    return only_digits(value)


def split_phone(phone: str) -> Tuple[str, str]:
    digits = only_digits(phone)
    if len(digits) >= 11:
        return digits[:2], digits[2:]
    if len(digits) == 10:
        return digits[:2], digits[2:]
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


# =========================
# PRESENÇA API
# =========================
def presenca_login_token() -> str:
    if not PRESENCA_LOGIN or not PRESENCA_SENHA:
        raise RuntimeError("PRESENCA_LOGIN_ou_SENHA_nao_configurada")

    throttle()
    url = f"{BASE_URL}/login"
    payload = {
        "login": PRESENCA_LOGIN,
        "senha": PRESENCA_SENHA
    }

    print(f"[LOGIN] URL: {url}", flush=True)
    resp = requests.post(url, json=payload, timeout=TIMEOUT)
    print(f"[LOGIN] STATUS: {resp.status_code}", flush=True)

    if not resp.ok:
        raise RuntimeError(f"login_falhou_http_{resp.status_code}: {resp.text[:500]}")

    data = safe_json(resp)
    token = data.get("token")
    if not token:
        raise RuntimeError(f"token_ausente_no_login: {data}")

    return token


def gerar_termo(headers: Dict[str, str], cpf: str, nome: str, telefone: str) -> Dict[str, Any]:
    throttle()
    url = f"{BASE_URL}/consultas/termo-inss"
    payload = {
        "cpf": cpf,
        "nome": nome,
        "telefone": normalize_phone(telefone),
        "produtoId": 28
    }

    print(f"[TERMO] URL: {url}", flush=True)
    print(f"[TERMO] PAYLOAD: {payload}", flush=True)

    resp = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)
    body = safe_json(resp)

    print(f"[TERMO] STATUS: {resp.status_code}", flush=True)
    print(f"[TERMO] BODY: {body}", flush=True)

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
        "detalhe_termo": body
    }


def assinar_termo(headers: Dict[str, str], autorizacao_id: str) -> Dict[str, Any]:
    throttle()
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

    print(f"[ASSINAR TERMO] URL: {url}", flush=True)

    try:
        print(f"[ASSINAR TERMO] PAYLOAD USER: {payload_user}", flush=True)
        resp = requests.put(url, json=payload_user, headers=headers_put, timeout=(10, 20))
        body = safe_json(resp)

        print(f"[ASSINAR TERMO] STATUS USER: {resp.status_code}", flush=True)
        print(f"[ASSINAR TERMO] BODY USER: {body}", flush=True)

        if resp.status_code in (200, 201, 202, 204):
            return {"ok": True, "detalhe": body, "modo": "user_payload"}
    except requests.exceptions.ReadTimeout as e:
        print(f"[ASSINAR TERMO] TIMEOUT USER: {str(e)}", flush=True)
        return {
            "ok": True,
            "detalhe": {"warning": "timeout_no_put_user_payload"},
            "modo": "user_payload_timeout"
        }
    except Exception as e:
        print(f"[ASSINAR TERMO] ERRO USER: {str(e)}", flush=True)

    try:
        print(f"[ASSINAR TERMO] PAYLOAD DOC: {payload_doc}", flush=True)
        resp2 = requests.put(url, json=payload_doc, headers=headers_put, timeout=(10, 20))
        body2 = safe_json(resp2)

        print(f"[ASSINAR TERMO] STATUS DOC: {resp2.status_code}", flush=True)
        print(f"[ASSINAR TERMO] BODY DOC: {body2}", flush=True)

        if resp2.status_code in (200, 201, 202, 204):
            return {"ok": True, "detalhe": body2, "modo": "doc_payload"}

        return {"ok": False, "detalhe": body2, "modo": "doc_payload"}
    except requests.exceptions.ReadTimeout as e:
        print(f"[ASSINAR TERMO] TIMEOUT DOC: {str(e)}", flush=True)
        return {
            "ok": True,
            "detalhe": {"warning": "timeout_no_put_doc_payload"},
            "modo": "doc_payload_timeout"
        }
    except Exception as e:
        print(f"[ASSINAR TERMO] ERRO DOC: {str(e)}", flush=True)
        return {"ok": False, "detalhe": {"erro": str(e)}, "modo": "doc_payload_exception"}


def consultar_vinculos(headers: Dict[str, str], cpf: str) -> requests.Response:
    throttle()
    url = f"{BASE_URL}/v3/operacoes/consignado-privado/consultar-vinculos"
    payload = {"cpf": cpf}

    print(f"[VINCULOS] URL: {url}", flush=True)
    print(f"[VINCULOS] PAYLOAD: {payload}", flush=True)

    resp = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)
    print(f"[VINCULOS] STATUS: {resp.status_code}", flush=True)
    print(f"[VINCULOS] BODY: {safe_json(resp)}", flush=True)

    return resp


def tentar_vinculos_com_retry(headers: Dict[str, str], cpf: str, tentativas: int = 3, espera: int = 5) -> requests.Response:
    ultimo_response = None
    ultimo_erro = None

    for i in range(tentativas):
        try:
            resp = consultar_vinculos(headers, cpf)

            if resp.status_code == 200:
                return resp

            ultimo_response = resp
        except requests.exceptions.ReadTimeout as e:
            print(f"[VINCULOS RETRY] timeout tentativa {i+1}: {str(e)}", flush=True)
            ultimo_erro = e
        except Exception as e:
            print(f"[VINCULOS RETRY] erro tentativa {i+1}: {str(e)}", flush=True)
            ultimo_erro = e

        if i < tentativas - 1:
            time.sleep(espera)

    if ultimo_response is not None:
        return ultimo_response

    if ultimo_erro is not None:
        raise ultimo_erro

    raise RuntimeError("Falha desconhecida ao consultar vínculos")


def consultar_margem(headers: Dict[str, str], cpf: str, matricula: str, cnpj: str) -> Any:
    throttle()
    time.sleep(2)

    url = f"{BASE_URL}/v3/operacoes/consignado-privado/consultar-margem"
    payload = {
        "cpf": cpf,
        "matricula": matricula,
        "cnpj": cnpj
    }

    print(f"[MARGEM] URL: {url}", flush=True)
    print(f"[MARGEM] PAYLOAD: {payload}", flush=True)

    resp = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)
    print(f"[MARGEM] STATUS: {resp.status_code}", flush=True)
    print(f"[MARGEM] BODY: {safe_json(resp)}", flush=True)

    if resp.status_code == 429:
        return {
            "erro_rate_limit": True,
            "mensagem": "Limite de requisições atingido no endpoint de margem"
        }

    resp.raise_for_status()
    return resp.json()


def simular(headers: Dict[str, str], cpf: str, telefone: str, matricula: str, cnpj: str, margem: dict) -> Any:
    throttle()
    url = f"{BASE_URL}/v5/operacoes/simulacao/disponiveis"

    ddd, numero = split_phone(telefone)

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
            "valorSolicitado": 0,
            "quantidadeParcelas": 0,
            "produtoId": 28,
            "valorParcela": margem.get("valorMargemDisponivel", 0)
        },
        "documentos": []
    }

    print(f"[SIMULACAO] URL: {url}", flush=True)
    print(f"[SIMULACAO] PAYLOAD: {payload}", flush=True)

    resp = requests.post(url, json=payload, headers=headers, timeout=TIMEOUT)
    print(f"[SIMULACAO] STATUS: {resp.status_code}", flush=True)
    print(f"[SIMULACAO] BODY: {safe_json(resp)}", flush=True)

    resp.raise_for_status()
    return resp.json()


# =========================
# FLUXO AUTORIZADO
# =========================
def executar_fluxo_autorizado(headers: Dict[str, str], cpf: str, telefone: str) -> Dict[str, Any]:
    resp_vinc = consultar_vinculos(headers, cpf)
    if resp_vinc.status_code != 200:
        return {
            "status": "erro",
            "mensagem": f"Falha ao consultar vínculos: HTTP {resp_vinc.status_code}",
            "detalhe_vinculos": safe_json(resp_vinc)
        }

    vinculos_body = safe_json(resp_vinc)
    vinculos = extract_candidates_vinculos(vinculos_body)
    vinculo = pick_vinculo(vinculos)

    if not vinculo:
        return {
            "status": "erro",
            "mensagem": "Nenhum vínculo encontrado",
            "detalhe_vinculos": vinculos_body
        }

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
        return {
            "status": "erro",
            "mensagem": "Não foi possível extrair matrícula/cnpj do vínculo",
            "detalhe_vinculo": vinculo
        }

    margem = consultar_margem(headers, cpf, matricula, cnpj)
    if isinstance(margem, dict) and margem.get("erro_rate_limit"):
        return {
            "status": "erro",
            "mensagem": "Rate limit do Presença atingido. Aguarde e tente novamente.",
            "detalhe": margem
        }

    simulacao = simular(headers, cpf, telefone, matricula, cnpj, margem)
    valor_disponivel, parcela = extract_oferta(simulacao, extract_valor_parcela(margem))

    return {
        "status": "sucesso",
        "elegibilidade": "sim",
        "valor_disponivel": valor_disponivel,
        "parcela": parcela
    }


# =========================
# FLUXO COMPLETO
# =========================
def tentar_fluxo_completo(cpf: str, nome: str, telefone: str, autorizacao_id: Optional[str] = None) -> Dict[str, Any]:
    token = presenca_login_token()
    headers = auth_headers(token)

    def processar_fluxo_com_vinculos_body(vinculos_body: Any) -> Dict[str, Any]:
        vinculos = extract_candidates_vinculos(vinculos_body)
        vinculo = pick_vinculo(vinculos)

        if not vinculo:
            return {
                "status": "erro",
                "mensagem": "Nenhum vínculo encontrado",
                "detalhe_vinculos": vinculos_body
            }

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
            return {
                "status": "erro",
                "mensagem": "Não foi possível extrair matrícula/cnpj do vínculo",
                "detalhe_vinculo": vinculo
            }

        margem = consultar_margem(headers, cpf, matricula, cnpj)
        if isinstance(margem, dict) and margem.get("erro_rate_limit"):
            return {
                "status": "erro",
                "mensagem": "Rate limit do Presença atingido. Aguarde e tente novamente.",
                "detalhe": margem
            }

        simulacao = simular(headers, cpf, telefone, matricula, cnpj, margem)
        valor_disponivel, parcela = extract_oferta(simulacao, extract_valor_parcela(margem))

        return {
            "status": "sucesso",
            "elegibilidade": "sim",
            "valor_disponivel": valor_disponivel,
            "parcela": parcela
        }

    # 1) se já veio autorizacao_id
    if autorizacao_id:
        assinatura = assinar_termo(headers, autorizacao_id)
        print(f"[ASSINATURA RESULTADO] {assinatura}", flush=True)

        if not assinatura.get("ok"):
            return {
                "status": "erro",
                "mensagem": "Falha ao assinar termo com autorizacao_id",
                "autorizacao_id": autorizacao_id,
                "detalhe": assinatura.get("detalhe"),
                "modo_assinatura": assinatura.get("modo")
            }

        time.sleep(5)

        try:
            resp_vinc = tentar_vinculos_com_retry(headers, cpf, tentativas=3, espera=5)
            if resp_vinc.status_code == 200:
                return processar_fluxo_com_vinculos_body(safe_json(resp_vinc))
        except Exception as e:
            print(f"[POS ASSINATURA] erro: {str(e)}", flush=True)

        return {
            "status": "aguardando_autorizacao",
            "autorizacao_id": autorizacao_id,
            "link_autorizacao": None
        }

    # 2) primeira tentativa direta
    resp_vinc = consultar_vinculos(headers, cpf)

    # se já está autorizado, usa ESSA resposta e não consulta de novo
    if resp_vinc.status_code == 200:
        return processar_fluxo_com_vinculos_body(safe_json(resp_vinc))

    # se bateu rate limit já na primeira consulta
    if resp_vinc.status_code == 429:
        return {
            "status": "erro",
            "mensagem": "Rate limit do Presença atingido no endpoint de vínculos. Aguarde e tente novamente.",
            "detalhe_vinculos": safe_json(resp_vinc)
        }

    # 3) não autorizado -> gera termo
    termo = gerar_termo(headers, cpf, nome, telefone)
    novo_id = termo.get("autorizacao_id")
    link = termo.get("link_autorizacao")

    if not novo_id:
        return {
            "status": "erro",
            "mensagem": "Não foi possível gerar autorizacao_id",
            "detalhe_termo": termo.get("detalhe_termo")
        }

    # 4) autoassina imediatamente
    assinatura_auto = assinar_termo(headers, novo_id)
    print(f"[ASSINATURA AUTO RESULTADO] {assinatura_auto}", flush=True)

    if assinatura_auto.get("ok"):
        try:
            time.sleep(5)
            resp_vinc_2 = tentar_vinculos_com_retry(headers, cpf, tentativas=3, espera=5)

            if resp_vinc_2.status_code == 200:
                return processar_fluxo_com_vinculos_body(safe_json(resp_vinc_2))

            if resp_vinc_2.status_code == 429:
                return {
                    "status": "erro",
                    "mensagem": "Rate limit do Presença atingido no endpoint de vínculos. Aguarde e tente novamente.",
                    "detalhe_vinculos": safe_json(resp_vinc_2)
                }

        except Exception as e:
            print(f"[AUTOAUTORIZACAO] erro após assinatura: {str(e)}", flush=True)

    # 5) só devolve pendência se não conseguiu mesmo
    return {
        "status": "aguardando_autorizacao",
        "autorizacao_id": novo_id,
        "link_autorizacao": link
    }
# =========================
# ROTAS
# =========================
@app.get("/")
def home():
    return {"status": "api rodando"}


@app.get("/consulta")
def consulta(cpf: str, nome: str, telefone: str, autorizacao_id: Optional[str] = None):
    try:
        cpf = normalize_cpf(cpf)
        telefone = normalize_phone(telefone)

        if not cpf:
            return JSONResponse(
                status_code=400,
                content={"status": "erro", "mensagem": "CPF inválido"}
            )

        resultado = tentar_fluxo_completo(
            cpf=cpf,
            nome=nome,
            telefone=telefone,
            autorizacao_id=autorizacao_id
        )

        return JSONResponse(status_code=200, content=resultado)

    except Exception as e:
        print("[ERRO GERAL]", str(e), flush=True)
        return JSONResponse(
            status_code=500,
            content={"status": "erro", "mensagem": str(e)}
        )
