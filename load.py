import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore, Lock

#piece of shit code

rate_limit = Semaphore(15)
last_pages = []
lock = Lock() 

MAX_RETRIES = 5  

def fetch_page(api_url, page):
    try:
        # Check if page has already been processed
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
                print(f"Página {page} obtida com sucesso.")
                last_pages.append(page)
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
        return []

    except requests.RequestException as e:
        print(f"Erro ao buscar página {page}: {e}")
        return []


def get_total_pages(api_url):
    """
    Faz uma requisição inicial para obter o número total de registros.
    """
    try:
        headers = {"User-Agent": "CustomAgent"}
        response = requests.get(f"{api_url}&page=1", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print(data.get("count", 0))
            print(data.get("page_size", 1) + 1)
            total_pages = data.get("count", 0) // data.get("page_size", 1) + 1
            print(f"[Info] Número total de registros estimado: {total_pages}")
            return total_pages
        else:
            print(f"[Erro] Falha ao obter o número total de registros: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"[Erro] Erro ao buscar o total de registros: {e}")
        return None


def main():
    api_url = "https://www.4byte.directory/api/v1/signatures/?format=json"
    
    next_page = 1
    has_more_pages = True  

    # Get total number of pages
    #total_pages = get_total_pages(api_url)
    #print(f"Total de páginas: {total_pages}")


    with ThreadPoolExecutor(max_workers=15) as fetch_executor, ThreadPoolExecutor(max_workers=15) as post_executor:
        post_futures = []

        while has_more_pages:
            fetch_futures = []

            # Thread-safe handling of next_page
            for _ in range(15):  # Launch 
                with lock:
                    page_to_fetch = next_page
                    next_page += 1

                # Submit the task to fetch a specific page
                fetch_futures.append(fetch_executor.submit(fetch_page, api_url, page_to_fetch))
            
            has_more_pages = False  # Assume no more pages initially
            for future in as_completed(fetch_futures):
                page_data = future.result()
                if page_data:
                    has_more_pages = True  # More pages exist if data was returned
                    # Process page data (e.g., post to a database)
                    # for item in page_data:
                    #     post_futures.append(post_executor.submit(post_data, item))

        # for post_future in as_completed(post_futures):
        #     post_future.result()

if __name__ == "__main__":
    main()
