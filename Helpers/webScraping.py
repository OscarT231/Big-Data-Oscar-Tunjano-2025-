import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin
import os
from typing import List, Dict
from Helpers import Funciones


class WebScraping:
    """Clase para realizar web scraping y extracción de enlaces"""
    
    def __init__(self, dominio_base: str = "https://www.minsalud.gov.co/Normativa/"):
        """
        Inicializa la clase WebScraping
        
        Args:
            dominio_base: Dominio base para validar enlaces
        """
        self.dominio_base = dominio_base
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/91.0.4472.124 Safari/537.36'
            )
        })
    
    # ---------------------- EXTRACCIÓN DE LINKS ----------------------

    def extract_links(self, url: str, listado_extensiones: List[str] = None) -> List[Dict]:
        """
        Extrae links internos según listado de extensiones (pdf, aspx, php, etc.)
        
        Args:
            url: URL de la página a analizar
            listado_extensiones: Lista de extensiones a filtrar
            
        Returns:
            Lista de diccionarios con 'url' y 'type'
        """
        print(f"Extrayendo links de: {url}")

        if listado_extensiones is None:
            listado_extensiones = ['pdf', 'aspx']
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'lxml')
            container_div = soup.find('div', class_='containerblanco')

            links = []
            if container_div:
                for link in container_div.find_all('a'):
                    href = link.get('href')
                    if not href:
                        continue
                    
                    full_url = urljoin(url, href)
                    
                    # Filtrar solo links dentro del dominio
                    if not full_url.startswith(self.dominio_base):
                        continue
                    
                    # Verificar extensión
                    for ext in listado_extensiones:
                        ext_lower = ext.lower().strip()
                        if full_url.lower().endswith(f".{ext_lower}"):
                            links.append({'url': full_url, 'type': ext_lower})
                            break
            
            return links
        
        except Exception as e:
            print(f"Error extrayendo links de {url}: {e}")
            return []
    
    # ---------------------- EXTRACCIÓN RECURSIVA ----------------------

    def extraer_todos_los_links(self, url_inicial: str, json_file_path: str,
                                listado_extensiones: List[str] = None,
                                max_iteraciones: int = 100) -> Dict:
        """
        Extrae todos los links de forma recursiva desde una URL inicial
        """

        if listado_extensiones is None:
            listado_extensiones = ['pdf', 'aspx']
        
        # Cargar links previos
        all_links = self._cargar_links_desde_json(json_file_path)

        # Si no hay links previos, iniciar con la URL base
        if not all_links:
            print(f"Extrayendo links iniciales desde: {url_inicial}")
            all_links = self.extract_links(url_inicial, listado_extensiones)

        # Mantener solo los del dominio base
        all_links = [
            link for link in all_links if link['url'].startswith(self.dominio_base)
        ]

        # Links .aspx que faltan por visitar
        aspx_links_to_visit = [
            link['url'] for link in all_links if link['type'] == 'aspx'
        ]

        visited_aspx_links = set()
        iteraciones = 0

        while aspx_links_to_visit and iteraciones < max_iteraciones:
            iteraciones += 1
            current_aspx_url = aspx_links_to_visit.pop(0)

            if current_aspx_url in visited_aspx_links:
                continue

            visited_aspx_links.add(current_aspx_url)
            print(f"Iteración {iteraciones}: Visitando {current_aspx_url}")

            new_links = self.extract_links(current_aspx_url, listado_extensiones)

            for link in new_links:
                if not any(l['url'] == link['url'] for l in all_links):
                    all_links.append(link)
                    if link['type'] == 'aspx':
                        aspx_links_to_visit.append(link['url'])

        if iteraciones >= max_iteraciones:
            print(f"Advertencia: Se alcanzó el máximo de iteraciones ({max_iteraciones})")

        # Guardar resultado
        output = {"links": all_links}
        self._guardar_links_en_json(json_file_path, output)

        print(f"Finalizado. Total links encontrados: {len(all_links)}")

        return {
            'success': True,
            'total_links': len(all_links),
            'links': all_links,
            'iteraciones': iteraciones
        }

    # ---------------------- MANEJO DE JSON ----------------------

    def _cargar_links_desde_json(self, json_file_path: str) -> List[Dict]:
        if not os.path.exists(json_file_path):
            print(f"No existe {json_file_path}. Se creará nuevo archivo.")
            return []

        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data.get("links", [])
        except Exception:
            print(f"Archivo JSON inválido: {json_file_path}. Inicializando vacío.")
            return []

    def _guardar_links_en_json(self, json_file_path: str, data: Dict):
        try:
            folder = os.path.dirname(json_file_path)
            if folder:
                os.makedirs(folder, exist_ok=True)
            
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            print(f"Links guardados en {json_file_path}")
        except Exception as e:
            print(f"Error guardando JSON: {e}")

    # ---------------------- DESCARGA DE PDF ----------------------

    def descargar_pdfs(self, json_file_path: str, carpeta_destino: str = "static/uploads") -> Dict:
        try:
            all_links = self._cargar_links_desde_json(json_file_path)
            pdf_links = [l for l in all_links if l['type'] == 'pdf']

            if not pdf_links:
                return {
                    'success': True,
                    'mensaje': 'No hay PDFs para descargar.',
                    'descargados': 0
                }

            Funciones.crear_carpeta(carpeta_destino)
            Funciones.borrar_contenido_carpeta(carpeta_destino)

            descargados = 0
            errores = []

            for i, link in enumerate(pdf_links, 1):
                pdf_url = link['url']
                print(f"Descargando [{i}/{len(pdf_links)}]: {pdf_url}")

                try:
                    nombre_archivo = os.path.basename(pdf_url.split("?")[0])
                    if not nombre_archivo.lower().endswith('.pdf'):
                        nombre_archivo += '.pdf'

                    from werkzeug.utils import secure_filename
                    nombre_archivo = secure_filename(nombre_archivo) or f"archivo_{i}.pdf"

                    ruta = os.path.join(carpeta_destino, nombre_archivo)
                    response = self.session.get(pdf_url, stream=True, timeout=60)
                    response.raise_for_status()

                    with open(ruta, 'wb') as f:
                        for chunk in response.iter_content(8192):
                            if chunk:
                                f.write(chunk)

                    descargados += 1

                except Exception as e:
                    print(f"Error descargando {pdf_url}: {e}")
                    errores.append({'url': pdf_url, 'error': str(e)})

            return {
                'success': True,
                'descargados': descargados,
                'errores': len(errores),
                'detalle_errores': errores
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def close(self):
        self.session.close()