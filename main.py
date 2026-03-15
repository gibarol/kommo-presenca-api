import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# =========================
# CONFIG / ENV
# =========================
BASE_URL = os.getenv("PRESENCA_BASE_URL", "https://presenca-bank-api.azurewebsites.net").rstrip("/")
PRESENCA_LOGIN = os.getenv("PRESENCA_LOGIN", "")
PRESENCA_SENHA = os.getenv("PRESENCA_SENHA", "")
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "15"))

# throttle recomendado pela doc
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


def normalize_digits(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def normalize_cpf(value: Any) -> str:
    cpf = normalize_digits(value)
    return cpf if len(cpf) == 11 else ""


def normalize_cnpj_like(value: Any) -> str:
    digits = normalize_digits(value)
    if not digits:
        return ""
    if len(digits) >= 14:
        return digits[-14:]
    return digits.zfill(14)


def split_phone(phone: str) -> Tuple[str, str]:
    digits = normalize_digits(phone)
    if len(digits) >= 11:
        return digits[:2], digits[2:]
    if len(digits) == 10:
        return digits[:2], digits[2:]
    return "11", "999999999"


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
        for key in ["data", "result", "vinculos", "items", "content"]:
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
    resp = requests.post(url, json=payload, timeout=TIMEOUT_SECONDS)
    print(f"[LOGIN] STATUS: {resp.status_code}", flush=True)

    if not resp.ok:
        raise RuntimeError(f"login_falhou_http_{resp.status_code}: {resp.text[:500]}")

    data = safe_json(resp)
    token = data.get("token")
    if not token:
        raise RuntimeError(f"token_ausente_no_login: {data}")

    return token


def presenca_vinculos(token: str, cpf: str):
    throttle()
    url = f"{BASE_URL}/v3/operacoes/consignado-privado/consultar-vinculos"
    payload = {"cpf": cpf}

    print(f"[VINCULOS] URL: {url}", flush=True)
    print(f"[VINCULOS] PAYLOAD: {payload}", flush=True)

    resp = requests.post(url, json=payload, headers=auth_headers(token), timeout=TIMEOUT_SECONDS)
    body = safe_json(resp)

    print(f"[VINCULOS] STATUS: {resp.status_code}", flush=True)
    print(f"[VINCULOS] BODY: {body}", flush=True)

    return resp.status_code, body


def presenca_margem(token: str, cpf: str, matricula: str, cnpj: str):
    throttle()
    url = f"{BASE_URL}/v3/operacoes/consignado-privado/consultar-margem"
    payload = {
        "cpf": cpf,
        "matricula": matricula,
        "cnpj": cnpj
    }

    print(f"[MARGEM] URL: {url}", flush=True)
    print(f"[MARGEM] PAYLOAD: {payload}", flush=True)

    resp = requests.post(url, json=payload, headers=auth_headers(token), timeout=TIMEOUT_SECONDS)
    body = safe_json(resp)

    print(f"[MARGEM] STATUS: {resp.status_code}", flush=True)
    print(f"[MARGEM] BODY: {body}", flush=True)

    return resp.status_code, body


def presenca_simulacao_disponiveis(token: str, margem_resp: dict, telefone: str, cpf: str, cnpj: str, matricula: str):
    throttle()
    url = f"{BASE_URL}/v5/operacoes/simulacao/disponiveis"

    nome = margem_resp.get("nome") or margem_resp.get("tomador", {}).get("nome") or "CLIENTE"
    data_nasc = margem_resp.get("dataNascimento") or margem_resp.get("tomador", {}).get("dataNascimento") or "1982-10-05"
    nome_mae = margem_resp.get("nomeMae") or margem_resp.get("tomador", {}).get("nomeMae") or "NAO INFORMADO"
    sexo = margem_resp.get("sexo") or margem_resp.get("tomador", {}).get("sexo") or "M"

    valor_parcela = extract_valor_parcela(margem_resp)
    ddd, numero = split_phone(telefone)

    payload = {
        "tomador": {
            "telefone": {
                "ddd": ddd,
                "numero": numero
            },
            "cpf": cpf,
            "nome": nome,
            "dataNascimento": data_nasc,
            "nomeMae": nome_mae,
            "email": "email@teste.com",
            "sexo": sexo,
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
            "valorParcela": valor_parcela
        },
        "documentos": []
    }

    print(f"[SIMULACAO] URL: {url}", flush=True)
    print(f"[SIMULACAO] PAYLOAD: {payload}", flush=True)

    resp = requests.post(url, json=payload, headers=auth_headers(token), timeout=TIMEOUT_SECONDS)
    body = safe_json(resp)

    print(f"[SIMULACAO] STATUS: {resp.status_code}", flush=True)
    print(f"[SIMULACAO] BODY: {body}", flush=True)

    return resp.status_code, body, payload


# =========================
# FLUXO TESTE AUTORIZADO
# =========================
def rodar_fluxo_autorizado_direto(cpf: str, telefone: str) -> Dict[str, Any]:
    print("=== INICIO TESTE AUTORIZADO DIRETO ===", flush=True)
    print("cpf:", cpf, flush=True)
    print("telefone:", telefone, flush=True)

    token = presenca_login_token()
    print("[PRESENCA] login ok", flush=True)

    st_v, vinc_body = presenca_vinculos(token, cpf)
    vinculos = extract_candidates_vinculos(vinc_body)

    if st_v != 200 or not vinculos:
        print("=== FIM TESTE AUTORIZADO DIRETO ===", flush=True)
        return {
            "status": "erro",
            "mensagem": "Falha ao consultar vínculos ou CPF não autorizado",
            "vinculos_http": st_v,
            "detalhe_vinculos": vinc_body
        }

    vinculo = pick_vinculo(vinculos)
    print("[PRESENCA] vinculo escolhido:", vinculo, flush=True)

    if not vinculo:
        print("=== FIM TESTE AUTORIZADO DIRETO ===", flush=True)
        return {
            "status": "sem_oferta",
            "mensagem": "Nenhum vínculo encontrado"
        }

    elegivel = vinculo.get("elegivel")
    elegivel_bool = elegivel is True or str(elegivel).lower() in {"true", "sim", "1"}

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

    print("[PRESENCA] matricula:", matricula, flush=True)
    print("[PRESENCA] cnpj:", cnpj, flush=True)

    if not matricula or not cnpj:
        print("=== FIM TESTE AUTORIZADO DIRETO ===", flush=True)
        return {
            "status": "erro",
            "mensagem": "Não foi possível extrair matrícula/cnpj do vínculo",
            "detalhe_vinculo": vinculo
        }

    st_m, margem_body = presenca_margem(token, cpf, matricula, cnpj)

    if st_m != 200:
        print("=== FIM TESTE AUTORIZADO DIRETO ===", flush=True)
        return {
            "status": "erro",
            "mensagem": "Falha ao consultar margem",
            "detalhe_margem": margem_body
        }

    valor_parcela = extract_valor_parcela(margem_body)
    print("[PRESENCA] valor_parcela:", valor_parcela, flush=True)

    st_s, simul_body, payload_sim = presenca_simulacao_disponiveis(
        token=token,
        margem_resp=margem_body if isinstance(margem_body, dict) else {},
        telefone=telefone,
        cpf=cpf,
        cnpj=cnpj,
        matricula=matricula
    )

    if st_s != 200:
        print("=== FIM TESTE AUTORIZADO DIRETO ===", flush=True)
        return {
            "status": "erro",
            "mensagem": "Falha na simulação",
            "elegibilidade": "sim" if elegivel_bool else "nao",
            "parcela": valor_parcela,
            "valor_disponivel": 0,
            "detalhe_simulacao": simul_body,
            "payload_simulacao": payload_sim
        }

    valor_disponivel, parcela = extract_oferta(simul_body, valor_parcela)

    print("[PRESENCA] valor_disponivel:", valor_disponivel, flush=True)
    print("[PRESENCA] parcela:", parcela, flush=True)
    print("=== FIM TESTE AUTORIZADO DIRETO ===", flush=True)

    return {
        "status": "sucesso" if elegivel_bool else "sem_oferta",
        "cpf": cpf,
        "elegibilidade": "sim" if elegivel_bool else "nao",
        "valor_disponivel": valor_disponivel,
        "parcela": parcela,
        "matricula": matricula,
        "cnpj": cnpj,
        "detalhe_vinculo": vinculo,
        "detalhe_margem": margem_body,
        "detalhe_simulacao": simul_body
    }


# =========================
# ROUTES
# =========================
@app.route("/")
def home():
    return jsonify({"status": "api rodando"})


@app.route("/teste-autorizado", methods=["GET"])
def teste_autorizado():
    try:
        cpf = normalize_cpf(request.args.get("cpf"))
        telefone = request.args.get("telefone") or "11999999999"

        if not cpf:
            return jsonify({
                "status": "erro",
                "mensagem": "CPF não informado ou inválido"
            }), 400

        resultado = rodar_fluxo_autorizado_direto(
            cpf=cpf,
            telefone=telefone
        )

        return jsonify(resultado)

    except Exception as e:
        print("[ERRO TESTE AUTORIZADO]", str(e), flush=True)
        return jsonify({
            "status": "erro",
            "mensagem": str(e)
        }), 500


if __name__ == "__main__":
    porta = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=porta)
