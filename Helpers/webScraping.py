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
        self.dominio_base = dominio_base
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                          '(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def extract_links(self, url: str, listado_extensiones: List[str] = None) -> List[Dict]:
        """Extrae links con extensiones filtradas dentro del div containerblanco"""

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

                    # Solo permitir URLs dentro del dominio
                    if not full_url.startswith(self.dominio_base):
                        continue

                    for ext in listado_extensiones:
                        ext_lower = ext.lower().strip()
                        if full_url.lower().endswith(f'.{ext_lower}'):
                            links.append({
                                'url': full_url,
                                'type': ext_lower
                            })
                            break
            
            return links

        except Exception as e:
            print(f"Error procesando {url}: {e}")
            return []
    
    def extraer_todos_los_links(self, url_inicial: str, json_file_path: str,
                                listado_extensiones: List[str] = None,
                                max_iteraciones: int = 100) -> Dict:

        if listado_extensiones is None:
            listado_extensiones = ['pdf', 'aspx']
        
        # Cargar links previos
        all_links = self._cargar_links_desde_json(json_file_path)

        if not all_links:
            print(f"Extrayendo links de la URL inicial: {url_inicial}")
            all_links = self.extract_links(url_inicial, listado_extensiones)

        # Filtrar solo links del dominio
        all_links = [l for l in all_links if l['url'].startswith(self.dominio_base)]

        # Cola inicial de ASPX para visitar
        aspx_links_to_visit = [l['url'] for l in all_links if l['type'] == 'aspx']
        visited_aspx_links = set()

        iteraciones = 0

        while aspx_links_to_visit and iteraciones < max_iteraciones:
            iteraciones += 1
            current = aspx_links_to_visit.pop(0)

            if current in visited_aspx_links:
                continue

            visited_aspx_links.add(current)
            print(f"Iteración {iteraciones}: Visitando {current}")

            nuevos = self.extract_links(current, listado_extensiones)

            for link in nuevos:
                if not any(l['url'] == link['url'] for l in all_links):
                    all_links.append(link)
                    if link['type'] == 'aspx' and link['url'] not in visited_aspx_links:
                        aspx_links_to_visit.append(link['url'])

        if iteraciones >= max_iteraciones:
            print(f"Advertencia: se alcanzó el límite de {max_iteraciones} iteraciones.")

        # Guardar archivo final
        output = {"links": all_links}
        self._guardar_links_en_json(json_file_path, output)

        print(f"Finalizado: {len(all_links)} links encontrados.")

        return {
            'success': True,
            'total_links': len(all_links),
            'links': all_links,
            'iteraciones': iteraciones
        }
    
    def _cargar_links_desde_json(self, json_file_path: str) -> List[Dict]:
        if os.path.exists(json_file_path):
            try:
                with open(json_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return data.get("links", [])
            except:
                print(f"Advertencia: {json_file_path} contiene JSON inválido.")
                return []
        else:
            print(f"{json_file_path} no encontrado. Se creará nuevo archivo.")
            return []
    
    def _guardar_links_en_json(self, json_file_path: str, data: Dict):
        try:
            dir_path = os.path.dirname(json_file_path)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)

            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)

            print(f"Links guardados en {json_file_path}")

        except Exception as e:
            print(f"Error al guardar JSON: {e}")
    
    def descargar_pdfs(self, json_file_path: str, carpeta_destino: str = "static/uploads") -> Dict:
        
        try:
            all_links = self._cargar_links_desde_json(json_file_path)
            pdf_links = [l for l in all_links if l.get('type') == 'pdf']

            if not pdf_links:
                return {'success': True, 'mensaje': 'No hay PDFs para descargar'}

            Funciones.crear_carpeta(carpeta_destino)
            Funciones.borrar_contenido_carpeta(carpeta_destino)

            descargados, errores = 0, 0
            errores_detalle = []

            for i, link in enumerate(pdf_links, 1):
                pdf_url = link['url']
                try:
                    nombre = os.path.basename(pdf_url.split('?')[0])
                    if not nombre.endswith('.pdf'):
                        nombre += '.pdf'

                    from werkzeug.utils import secure_filename
                    nombre = secure_filename(nombre)
                    if not nombre:
                        nombre = f"archivo_{i}.pdf"

                    ruta = os.path.join(carpeta_destino, nombre)

                    print(f"Descargando {nombre}...")

                    resp = self.session.get(pdf_url, stream=True, timeout=60)
                    resp.raise_for_status()

                    with open(ruta, 'wb') as f:
                        for chunk in resp.iter_content(8192):
                            if chunk:
                                f.write(chunk)

                    descargados += 1

                except Exception as e:
                    errores += 1
                    errores_detalle.append({'url': pdf_url, 'error': str(e)})

            return {
                'success': True,
                'total': len(pdf_links),
                'descargados': descargados,
                'errores': errores,
                'detalle_errores': errores_detalle
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'descargados': 0,
                'errores': 0
            }
    
    def close(self):
        self.session.close()
