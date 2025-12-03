import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin
import os
from typing import List, Dict
from Helpers import Funciones


class WebScraping:
    """Clase para realizar web scraping y extracción de enlaces."""
    
    def __init__(self, dominio_base: str = "https://www.minsalud.gov.co/Normativa/"):
        self.dominio_base = dominio_base
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/91.0.4472.124 Safari/537.36'
            )
        })
    
    def extract_links(self, url: str, listado_extensiones: List[str] = None) -> List[Dict]:
        """Extrae links internos según las extensiones indicadas."""
        
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
                    
                    # Validar dominio
                    if not full_url.startswith(self.dominio_base):
                        continue
                    
                    # Validar extensión
                    for ext in listado_extensiones:
                        ext_lower = ext.lower()
                        if full_url.lower().endswith(f'.{ext_lower}'):
                            links.append({
                                'url': full_url,
                                'type': ext_lower
                            })
                            break
            
            return links
            
        except requests.exceptions.RequestException as e:
            print(f"Error obteniendo {url}: {e}")
            return []
        except Exception as e:
            print(f"Error procesando {url}: {e}")
            return []
    
    def extraer_todos_los_links(self, url_inicial: str, json_file_path: str, 
                                listado_extensiones: List[str] = None,
                                max_iteraciones: int = 100) -> Dict:
        """Extrae todos los links de forma recursiva desde una URL inicial."""
        
        if listado_extensiones is None:
            listado_extensiones = ['pdf', 'aspx']
        
        # Cargar links previos
        all_links = self._cargar_links_desde_json(json_file_path)
        
        if not all_links:
            all_links = self.extract_links(url_inicial, listado_extensiones)
        
        # Filtrar dominio
        all_links = [
            link for link in all_links
            if link['url'].startswith(self.dominio_base)
        ]

        # Links ASPX a visitar
        aspx_links_to_visit = [
            link['url'] for link in all_links
            if link['type'] == 'aspx'
        ]

        visited_aspx_links = set()
        iteraciones = 0
        
        while aspx_links_to_visit and iteraciones < max_iteraciones:
            iteraciones += 1
            current = aspx_links_to_visit.pop(0)
            
            if current in visited_aspx_links:
                continue
            
            visited_aspx_links.add(current)
            new_links = self.extract_links(current, listado_extensiones)
            
            for link in new_links:
                if not any(existing['url'] == link['url'] for existing in all_links):
                    all_links.append(link)
                    
                    if link['type'] == 'aspx' and link['url'] not in visited_aspx_links:
                        aspx_links_to_visit.append(link['url'])
        
        # Filtrar por dominio nuevamente
        all_links = [
            link for link in all_links 
            if link['url'].startswith(self.dominio_base)
        ]
        
        # Guardar
        json_output = {"links": all_links}
        self._guardar_links_en_json(json_file_path, json_output)
        
        return {
            'success': True,
            'total_links': len(all_links),
            'links': all_links,
            'iteraciones': iteraciones
        }
    
    def _cargar_links_desde_json(self, json_file_path: str) -> List[Dict]:
        """Carga links desde un archivo JSON."""
        
        if not os.path.exists(json_file_path):
            return []
        
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            return json_data.get("links", [])
        
        except json.JSONDecodeError:
            print(f"JSON inválido en {json_file_path}.")
            return []
    
    def _guardar_links_en_json(self, json_file_path: str, data: Dict):
        """Guarda links en un archivo JSON."""
        
        try:
            folder = os.path.dirname(json_file_path)
            if folder:
                os.makedirs(folder, exist_ok=True)
            
            with open(json_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
        
        except Exception as e:
            print(f"Error guardando JSON: {e}")
    
    def descargar_pdfs(self, json_file_path: str, carpeta_destino: str = "static/uploads") -> Dict:
        """Descarga los archivos PDF listados en el JSON."""
        
        try:
            all_links = self._cargar_links_desde_json(json_file_path)
            pdf_links = [l for l in all_links if l.get('type') == 'pdf']
            
            if not pdf_links:
                return {
                    'success': True,
                    'mensaje': 'No hay PDFs para descargar',
                    'descargados': 0,
                    'errores': 0
                }
            
            Funciones.crear_carpeta(carpeta_destino)
            Funciones.borrar_contenido_carpeta(carpeta_destino)
            
            descargados = 0
            errores = 0
            errores_detalle = []
            
            for i, link in enumerate(pdf_links, 1):
                pdf_url = link['url']
                
                try:
                    nombre_archivo = os.path.basename(pdf_url.split('?')[0])
                    
                    if not nombre_archivo.lower().endswith('.pdf'):
                        nombre_archivo += '.pdf'
                    
                    from werkzeug.utils import secure_filename
                    nombre_archivo = secure_filename(nombre_archivo)
                    
                    if not nombre_archivo:
                        nombre_archivo = f"archivo_{i}.pdf"
                    
                    ruta_archivo = os.path.join(carpeta_destino, nombre_archivo)
                    
                    response = self.session.get(pdf_url, stream=True, timeout=60)
                    response.raise_for_status()
                    
                    with open(ruta_archivo, 'wb') as f:
                        for chunk in response.iter_content(8192):
                            if chunk:
                                f.write(chunk)
                    
                    descargados += 1
                
                except Exception as e:
                    errores += 1
                    errores_detalle.append({'url': pdf_url, 'error': str(e)})
            
            resultado = {
                'success': True,
                'total': len(pdf_links),
                'descargados': descargados,
                'errores': errores,
                'carpeta_destino': carpeta_destino
            }
            
            if errores_detalle:
                resultado['archivos_con_error'] = errores_detalle
            
            return resultado
        
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'descargados': 0,
                'errores': 0
            }
    
    def close(self):
        """Cierra la sesión."""
        self.session.close()
