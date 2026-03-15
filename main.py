from flask import Flask, request, jsonify
import requests
import re

app = Flask(__name__)

# =========================
# CONFIG
# =========================

LOGIN_URL = "https://api-presenca-hml.facilinformatica.com.br/api/auth/login"
VINCULOS_URL = "https://api-presenca-hml.facilinformatica.com.br/api/v3/operacoes/consignado-privado/consultar-vinculos"
MARGEM_URL = "https://api-presenca-hml.facilinformatica.com.br/api/v3/operacoes/consignado-privado/consultar-margem"
SIMULACAO_URL = "https://api-presenca-hml.facilinformatica.com.br/api/v3/operacoes/consignado-privado/simulacao-disponiveis"

LOGIN = "adriano.ribeiro"
SENHA = "Adriano@2025"

# =========================
# UTILS
# =========================

def limpar_cpf(cpf):
    return re.sub(r"\D", "", cpf)

def limpar_cnpj(cnpj):
    return re.sub(r"\D", "", cnpj)

# =========================
# LOGIN
# =========================

def login():

    payload = {
        "login": LOGIN,
        "senha": SENHA
    }

    r = requests.post(LOGIN_URL, json=payload)

    if r.status_code != 200:
        raise Exception("Erro no login")

    token = r.json().get("accessToken")

    return token

# =========================
# CONSULTA VINCULOS
# =========================

def consultar_vinculos(token, cpf):

    headers = {
        "Authorization": f"Bearer {token}"
    }

    payload = {
        "cpf": cpf
    }

    r = requests.post(VINCULOS_URL, json=payload, headers=headers)

    return r.status_code, r.json()

# =========================
# CONSULTA MARGEM
# =========================

def consultar_margem(token, cpf, matricula, cnpj):

    headers = {
        "Authorization": f"Bearer {token}"
    }

    payload = {
        "cpf": cpf,
        "matricula": matricula,
        "cnpj": cnpj
    }

    r = requests.post(MARGEM_URL, json=payload, headers=headers)

    return r.status_code, r.json()

# =========================
# SIMULAÇÃO
# =========================

def simular(token, cpf, telefone, matricula, cnpj):

    headers = {
        "Authorization": f"Bearer {token}"
    }

    payload = {
        "cpf": cpf,
        "telefone": telefone,
        "matricula": matricula,
        "cnpj": cnpj
    }

    r = requests.post(SIMULACAO_URL, json=payload, headers=headers)

    return r.status_code, r.json()

# =========================
# ROTA TESTE CPF AUTORIZADO
# =========================

@app.route("/teste-autorizado", methods=["GET"])
def teste():

    try:

        cpf = limpar_cpf(request.args.get("cpf"))
        telefone = request.args.get("telefone")

        if not cpf:
            return jsonify({"erro": "cpf obrigatório"})

        token = login()

        # =========================
        # VINCULOS
        # =========================

        status_v, vinculos = consultar_vinculos(token, cpf)

        if status_v != 200:
            return jsonify({
                "erro": "erro consulta vinculos",
                "detalhe": vinculos
            })

        lista = vinculos.get("data")

        if not lista:
            return jsonify({
                "erro": "cpf sem vinculo"
            })

        vinculo = lista[0]

        matricula = vinculo.get("matricula")
        cnpj = limpar_cnpj(vinculo.get("numeroInscricaoEmpregador"))

        # =========================
        # MARGEM
        # =========================

        status_m, margem = consultar_margem(token, cpf, matricula, cnpj)

        if status_m != 200:
            return jsonify({
                "erro": "erro margem",
                "detalhe": margem
            })

        valor_parcela = margem.get("valorParcela")

        # =========================
        # SIMULAÇÃO
        # =========================

        status_s, simulacao = simular(token, cpf, telefone, matricula, cnpj)

        if status_s != 200:
            return jsonify({
                "erro": "erro simulacao",
                "detalhe": simulacao
            })

        oferta = simulacao.get("data")[0]

        valor = oferta.get("valorLiberado")

        return jsonify({

            "cpf": cpf,
            "elegibilidade": "sim",
            "valor_disponivel": valor,
            "parcela": valor_parcela,

            "debug": {
                "vinculo": vinculo,
                "margem": margem,
                "simulacao": simulacao
            }

        })

    except Exception as e:

        return jsonify({
            "erro": str(e)
        })


# =========================
# HEALTHCHECK
# =========================

@app.route("/")
def home():
    return {"status": "api rodando"}

# =========================

if __name__ == "__main__":
    app.run()
