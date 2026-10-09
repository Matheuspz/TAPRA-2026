import datetime
import logging
import requests
import azure.functions as func

app = func.FunctionApp()

# TIMER TRIGGER
@app.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def timer_trigger_tapra1(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('Tempo do Timer expirado!')

    logging.info(f'Timer executado em: {datetime.datetime.now()}')

# HTTP TRIGGER
@app.route(route="http_trigger_tapra2", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger_tapra2(req: func.HttpRequest) -> func.HttpResponse:

    name = req.params.get("name", "não informado")
    logging.info(f"Parametro recebido: {name}")

    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(
            f"MENSAGEM ENVIADA: OLÁ {name}.", 
            status_code=200
        )
    
    else:
        return func.HttpResponse(
             "Trigger HTTP executado com sucesso, porém sem um parametro.",
             status_code=200
        )

# TIMER HTTP SOLICITAÇÃO
@app.timer_trigger(schedule="30 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def timer_trigger_http_request(myTimer: func.TimerRequest) -> None:

    valor = "Abacate"
    url = f"https://funcapp-tapra-matheus-f6bhcydxeqh7c4eu.eastus-01.azurewebsites.net/api/http_trigger_http_response?valor={valor}"
    
    try:
        resposta = requests.get(url, timeout=15)
        logging.info(f"RESPOSTA: {resposta.text}")
    except Exception as e:
        logging.error(f"Erro ao chamar função http_trigger_http_response: {str(e)}")

# HTTP RESPOSTA
@app.route(route="http_trigger_http_response", auth_level=func.AuthLevel.ANONYMOUS)
def http_trigger_http_response(req: func.HttpRequest) -> func.HttpResponse:

    valor = req.params.get('valor', 'vazio')
    logging.info(f'Parametro recebido: {valor}')

    if not valor:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            valor = req_body.get('valor')

    if valor:
        return func.HttpResponse(
            f"SOLICITAÇÃO RECEBIDA: {valor}",
            status_code=200
        )
    else:
        return func.HttpResponse(
             "Trigger HTTP executado com sucesso, porém sem um parametro.",
             status_code=200
        )