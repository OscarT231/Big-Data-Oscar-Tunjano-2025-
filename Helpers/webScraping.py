import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import os
from typing import List, Dict
from Helpers import Funciones


class WebScraping:
    """Clase para realizar web scraping y extracción de enlaces"""

    def __init__(self, dominio_base: str = "https://www.minsalud.gov.co"):
        self.dominio_base = dominio_base
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0 Safari/537.36'
            )
        })

    # ============================================================
    #               EXTRAER LINKS (LIMPIO Y FUNCIONAL)
    # ============================================================
    def extract_links(self, url: str, listado_extensiones: List[str] = None) -> List[Dict]:
        """Extrae links validos del dominio base"""
        if listado_extensiones is None:
            listado_extensiones = ['pdf', 'aspx']

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')
            container_div = soup.find('div', class_='containerblanco')

            links = []
            if not container_div:
                return links  # Si no hay contenido, no hay pelea

            dominio_objetivo = urlparse(self.dominio_base).netloc

            for a in container_div.find_all('a'):
                href = a.get('href')
                if not href:
                    continue

                full_url = urljoin(url, href)
                parsed = urlparse(full_url)

                # Solo permitir mismo dominio
                if parsed.netloc != dominio_objetivo:
                    continue

                url_sin_params = full_url.split('?')[0].lower()

                for ext in listado_extensiones:
                    ext = ext.lower()

                    # Detectar PDFs robustamente
                    if ext == "pdf":
                        if url_sin_params.endswith(".pdf"):
                            links.append({"url": full_url, "type": "pdf"})
                            break
                    else:
                        if url_sin_params.endswith(f".{ext}"):
                            links.append({"url": full_url, "type": ext})
                            break

            return links

        except Exception as e:
            print(f"Error procesando {url}: {e}")
            return []

    # ============================================================
    #               EXTRACCIÓN RECURSIVA
    # ============================================================
    def extraer_todos_los_links(self, url_inicial: str, json_file_path: str,
                                listado_extensiones: List[str] = None,
                                max_iteraciones: int = 100) -> Dict:

        if listado_extensiones is None:
            listado_extensiones = ['pdf', 'aspx']

        # Cargar links previos
        all_links = self._cargar_links_desde_json(json_file_path)

        # Primera extracción si no existían
        if not all_links:
            all_links = self.extract_links(url_inicial, listado_extensiones)

        # Obtener ASPX para visitar
        aspx_links = [
            l['url'] for l in all_links if l['type'] == 'aspx'
        ]

        visitados = set()
        iteraciones = 0

        while aspx_links and iteraciones < max_iteraciones:
            iteraciones += 1
            actual = aspx_links.pop(0)

            if actual in visitados:
                continue

            visitados.add(actual)

            nuevos = self.extract_links(actual, listado_extensiones)

            for link in nuevos:
                if not any(l['url'] == link['url'] for l in all_links):
                    all_links.append(link)
                    if link['type'] == 'aspx':
                        aspx_links.append(link['url'])

        self._guardar_links_en_json(json_file_path, {"links": all_links})

        return {
            "success": True,
            "total_links": len(all_links),
            "iteraciones": iteraciones,
            "links": all_links
        }

    # ============================================================
    #               JSON HANDLERS
    # ============================================================
    def _cargar_links_desde_json(self, json_file_path: str) -> List[Dict]:
        if not os.path.exists(json_file_path):
            return []
        try:
            with open(json_file_path, "r", encoding="utf-8") as f:
                return json.load(f).get("links", [])
        except:
            return []

    def _guardar_links_en_json(self, json_file_path: str, data: Dict):
        try:
            carpeta = os.path.dirname(json_file_path)
            if carpeta:
                os.makedirs(carpeta, exist_ok=True)

            with open(json_file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"Error guardando JSON: {e}")

    # ============================================================
    #               DESCARGA DE PDFs (FUNCIONAL)
    # ============================================================
    def descargar_pdfs(self, json_file_path: str, carpeta_destino: str = "static/uploads") -> Dict:

        links = self._cargar_links_desde_json(json_file_path)
        pdfs = [l for l in links if l.get("type") == "pdf"]

        if not pdfs:
            return {"success": True, "descargados": 0, "errores": 0}

        Funciones.crear_carpeta(carpeta_destino)
        Funciones.borrar_contenido_carpeta(carpeta_destino)

        descargados = 0
        errores = 0
        errores_lista = []

        from werkzeug.utils import secure_filename

        for i, link in enumerate(pdfs, 1):
            url_pdf = link['url']

            try:
                filename = os.path.basename(url_pdf.split("?")[0])
                if not filename.endswith(".pdf"):
                    filename += ".pdf"
                filename = secure_filename(filename)

                ruta = os.path.join(carpeta_destino, filename)

                r = self.session.get(url_pdf, stream=True, timeout=40)
                r.raise_for_status()

                with open(ruta, "wb") as f:
                    for chunk in r.iter_content(8192):
                        if chunk:
                            f.write(chunk)

                descargados += 1

            except Exception as e:
                errores += 1
                errores_lista.append({"url": url_pdf, "error": str(e)})

        return {
            "success": True,
            "total": len(pdfs),
            "descargados": descargados,
            "errores": errores,
            "errores_detalle": errores_lista
        }

    def close(self):
        self.session.close()
