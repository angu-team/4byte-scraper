from elasticsearch import Elasticsearch, helpers
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore, Lock

# Configuração inicial
rate_limit = Semaphore(15)
last_pages = []
lock = Lock()

MAX_RETRIES = 5
PAGE_LIMIT = 1000000 # Limite de páginas para o teste


def bulk_upsert(es_client, index, signatures):
    """
    Realiza o upsert em bulk no Elasticsearch.
    """
    try:
        actions = [
            {
                "_op_type": "index",  # Ou "update" para atualizar
                "_index": index,
                "_id": hex_signature,  # Definir o ID como o hex_signature
                "_source": {
                    "hex_signature": hex_signature,
                    "text_signature": text_signature,
                    "timestamp": time.time()  # Adiciona um timestamp para controle
                }
            }
            for hex_signature, text_signature in signatures
        ]

        # Realiza a operação em lote usando o helpers.bulk
        helpers.bulk(es_client, actions)
        print(f"[Elasticsearch] Bulk upsert realizado com sucesso para {len(signatures)} documentos.")
    except Exception as e:
        print(f"[Erro Elasticsearch] Falha no bulk upsert: {e}")


def fetch_page(api_url, page):
    """
    Obtém uma página da API e retorna os resultados.
    """
    try:
        if page in last_pages:
            print(f"Página {page} já foi obtida.")
            return []

        if len(last_pages) > 10000:
            last_pages.pop(0)

        headers = {
            "User-Agent": "CustomAgent"
        }

        retries = 0
        while retries < MAX_RETRIES:
            with rate_limit:
                response = requests.get(f"{api_url}&page={page}", headers=headers)

            if response.status_code == 200:
                data = response.json()
                last_pages.append(page)
                print(f"Página {page} sucesso")
                return data
            elif response.status_code == 429:
                retries += 1
                wait_time = 2 ** retries
                print(f"[Rate Limit] Página {page}: Tentando novamente em {wait_time} segundos (tentativa {retries}/{MAX_RETRIES})")
                time.sleep(wait_time)
            else:
                print(f"Erro ao obter página {page}: {response.status_code}")
                return []

        print(f"[Max Retries] Página {page}: Falha após {MAX_RETRIES} tentativas.")
        return

    except requests.RequestException as e:
        print(f"Erro ao buscar página {page}: {e}")
        return []


def main():
    api_url = "https://www.4byte.directory/api/v1/signatures/?format=json"
    index_name = "signatures"

    # Criar um cliente Elasticsearch compartilhado
    es_client = Elasticsearch(
        hosts=["https://suco-mirror.atqfhd.easypanel.host/"],  # Substitua pelo endereço do Elasticsearch
        timeout=30,  # Timeout para operações
        max_retries=5,  # Número de tentativas automáticas em caso de falha
        retry_on_timeout=True  # Habilitar tentativas em timeouts
    )

    next_page = 1
    processed_pages = 0  # Contador de páginas processadas
    has_more_pages = True

    with ThreadPoolExecutor(max_workers=15) as executor:
        while has_more_pages and processed_pages < PAGE_LIMIT:  # Parar ao atingir o limite
            fetch_futures = []

            # Thread-safe handling of next_page
            for _ in range(15):  # Lança 3 threads para scraping
                with lock:
                    page_to_fetch = next_page
                    next_page += 1

                # Submit the task to fetch a specific page
                fetch_futures.append(executor.submit(fetch_page, api_url, page_to_fetch))

            has_more_pages = False  # Assume não há mais páginas inicialmente
            for future in as_completed(fetch_futures):
                page_data = future.result()
                if page_data and "results" in page_data:
                    has_more_pages = True  # Há mais páginas se dados foram retornados
                    processed_pages += 1  # Incrementar contador de páginas processadas
                    signatures = [(entry['hex_signature'], entry['text_signature']) for entry in page_data['results']]

                    # Realizar o bulk upsert imediatamente para a página
                    bulk_upsert(es_client, index_name, signatures)

    print(f"[Info] Teste concluído após processar {processed_pages} páginas.")


if __name__ == "__main__":
    main()
