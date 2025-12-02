import os
import re
import time
import requests
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pdfplumber

# ---------------------------------------------------
# CONFIGURACIÓN ELASTICSEARCH
# ---------------------------------------------------

CLOUD_URL = "https://dbc32416c0ef45949b8ccbffa25c5a29.us-central1.gcp.cloud.es.io:443"
API_KEY = "RWZfODI1b0JtTUo5M0RpakplUDI6S2pEY19DajFJSHZaOS1nc1hSM3JvZw=="
INDEX_NAME = "index_proyecto"

es = Elasticsearch(
    CLOUD_URL,
    api_key=API_KEY,
    verify_certs=True
)

# ---------------------------------------------------
# SELENIUM CONFIG
# ---------------------------------------------------

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )


def scroll(driver):
    last = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1.5)
        new = driver.execute_script("return document.body.scrollHeight")
        if new == last:
            break
        last = new


# ---------------------------------------------------
# SCRAPING
# ---------------------------------------------------

def extraer_links(urls, driver):
    pdf_links = set()

    for url in urls:
        print(f"Visitando → {url}")
        driver.get(url)
        time.sleep(3)
        scroll(driver)

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")

        # A tags
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" in href.lower() or "drive.google.com" in href.lower():
                pdf_links.add(href)

        # onclick con rutas a PDF
        for l in re.findall(r'"([^"\\]+\\.pdf)"', html):
            pdf_links.add(l)

        # iframes
        for iframe in soup.find_all("iframe"):
            src = iframe.get("src", "")
            if ".pdf" in src:
                pdf_links.add(src)

    print(f"Total PDFs encontrados: {len(pdf_links)}")
    return list(pdf_links)


def descargar_pdf(url, carpeta="pdfs"):
    os.makedirs(carpeta, exist_ok=True)
    filename = url.split("/")[-1].replace("?", "_")
    ruta = os.path.join(carpeta, filename)

    try:
        resp = requests.get(url, timeout=20)
        if resp.status_code == 200:
            with open(ruta, "wb") as f:
                f.write(resp.content)
            return ruta
        else:
            print(f"Error {resp.status_code} descargando {url}")
            return None
    except Exception as e:
        print(f"Error descargando {url}: {e}")
        return None


def extraer_texto_pdf(ruta):
    try:
        with pdfplumber.open(ruta) as pdf:
            texto = "\n".join([
                p.extract_text() or "" 
                for p in pdf.pages
            ])
            return texto, len(pdf.pages)
    except:
        return "", 0


# ---------------------------------------------------
# SUBIR A ELASTIC
# ---------------------------------------------------

def subir_a_elastic(documentos):
    helpers.bulk(es, documentos, index=INDEX_NAME)
    print(f"✔ {len(documentos)} docs enviados a Elastic")


# ---------------------------------------------------
# EJECUCIÓN PRINCIPAL
# ---------------------------------------------------

def run_icfes_scraping():
    urls = [
        "https://altopuntaje.com/prueba-icfes-preguntas-saber-11-examenes/"
    ]

    driver = get_driver()
    links = extraer_links(urls, driver)
    driver.quit()

    documentos = []

    for link in links:
        ruta = descargar_pdf(link)
        if not ruta:
            continue

        texto, paginas = extraer_texto_pdf(ruta)

        documentos.append({
            "titulo": os.path.basename(ruta),
            "materia": "ICFES",
            "tipo_documento": "pdf",
            "url_origen": link,
            "fecha_extraccion": datetime.utcnow(),
            "paginas": paginas,
            "texto_completo": texto
        })

    if documentos:
        subir_a_elastic(documentos)
    else:
        print("No hay documentos para subir.")


if __name__ == "__main__":
    run_icfes_scraping()
