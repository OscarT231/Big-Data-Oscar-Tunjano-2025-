from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from typing import Dict, List, Optional, Any
import json


class ElasticSearch:
    def __init__(self, cloud_id: str, api_key: str):
        """Inicializa conexión a Elasticsearch Cloud"""
        self.client = Elasticsearch(
            cloud_id=cloud_id,
            api_key=api_key,
            verify_certs=True
        )

    def test_connection(self) -> bool:
        """Prueba la conexión a Elasticsearch"""
        try:
            info = self.client.info()
            print(f"✅ Conectado a Elastic: {info['version']['number']}")
            return True
        except Exception as e:
            print(f"❌ Error al conectar con Elastic: {e}")
            return False

    # ---------------------------------------------------------------------
    # COMANDOS ADMIN
    # ---------------------------------------------------------------------

    def ejecutar_comando(self, comando_json: str) -> Dict:
        """Ejecuta comandos JSON administrativos (crear índice, borrar, mappings...)"""
        try:
            comando = json.loads(comando_json)
            operacion = comando.get('operacion')
            index = comando.get('index')

            if operacion == 'crear_index':
                mappings = comando.get('mappings', {})
                settings = comando.get('settings', {})

                body = {}
                if mappings:
                    body["mappings"] = mappings
                if settings:
                    body["settings"] = settings

                resp = self.client.indices.create(index=index, body=body)
                return {"success": True, "data": resp}

            elif operacion == 'eliminar_index':
                resp = self.client.indices.delete(index=index)
                return {"success": True, "data": resp}

            elif operacion == 'actualizar_mappings':
                mappings = comando.get('mappings', {})
                resp = self.client.indices.put_mapping(index=index, body=mappings)
                return {"success": True, "data": resp}

            elif operacion == 'info_index':
                resp = self.client.indices.get(index=index)
                return {"success": True, "data": resp}

            elif operacion == 'listar_indices':
                resp = self.client.cat.indices(format='json')
                return {"success": True, "data": resp}

            else:
                return {"success": False, "error": f"Operación no soportada: {operacion}"}

        except json.JSONDecodeError as e:
            return {"success": False, "error": f"JSON inválido: {e}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def crear_index(self, nombre_index: str, mappings: Dict = None, settings: Dict = None) -> bool:
        """Crea un índice"""
        try:
            body = {}
            if mappings:
                body["mappings"] = mappings
            if settings:
                body["settings"] = settings

            self.client.indices.create(index=nombre_index, body=body)
            return True
        except Exception as e:
            print(f"Error al crear índice: {e}")
            return False

    def eliminar_index(self, nombre_index: str) -> bool:
        """Elimina un índice"""
        try:
            self.client.indices.delete(index=nombre_index)
            return True
        except Exception as e:
            print(f"Error al eliminar índice: {e}")
            return False

    def listar_indices(self) -> List[Dict]:
        """Lista todos los índices con datos legibles"""
        try:
            indices = self.client.cat.indices(format='json',
                                              h='index,docs.count,store.size,health,status')

            formateados = []
            for idx in indices:
                formateados.append({
                    "nombre": idx.get("index", ""),
                    "total_documentos": int(idx.get("docs.count", "0")) if idx.get("docs.count", "0").isdigit() else 0,
                    "tamaño": idx.get("store.size", "0b"),
                    "salud": idx.get("health", "unknown"),
                    "estado": idx.get("status", "unknown")
                })

            return formateados

        except Exception as e:
            print(f"Error al listar índices: {e}")
            return []

    # ---------------------------------------------------------------------
    # DML (Indexar, actualizar, eliminar documentos)
    # ---------------------------------------------------------------------

    def indexar_documento(self, index: str, documento: Dict, doc_id: str = None) -> bool:
        """Indexa un documento individual"""
        try:
            if doc_id:
                self.client.index(index=index, id=doc_id, document=documento)
            else:
                self.client.index(index=index, document=documento)
            return True
        except Exception as e:
            print(f"Error al indexar documento: {e}")
            return False

    def indexar_bulk(self, index: str, documentos: List[Dict]) -> Dict:
        """Indexación masiva"""
        try:
            acciones = [{"_index": index, "_source": doc} for doc in documentos]

            success, errors = bulk(self.client, acciones, raise_on_error=False)

            return {
                "success": True,
                "indexados": success,
                "errores": errors
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def actualizar_documento(self, index: str, doc_id: str, datos: Dict) -> bool:
        """Actualiza parcialmente un documento"""
        try:
            self.client.update(
                index=index,
                id=doc_id,
                body={"doc": datos}
            )
            return True
        except Exception as e:
            print(f"Error al actualizar documento: {e}")
            return False

    def eliminar_documento(self, index: str, doc_id: str) -> bool:
        """Elimina documento por ID"""
        try:
            self.client.delete(index=index, id=doc_id)
            return True
        except Exception as e:
            print(f"Error al eliminar documento: {e}")
            return False

    def ejecutar_dml(self, comando_json: str) -> Dict:
        """Ejecuta un comando DML en JSON (index, update, delete)"""
        try:
            comando = json.loads(comando_json)
            operacion = comando.get("operacion")
            index = comando.get("index")

            if operacion in ["index", "create"]:
                documento = comando.get("documento") or comando.get("body", {})
                doc_id = comando.get("id")

                if doc_id:
                    resp = self.client.index(index=index, id=doc_id, document=documento)
                else:
                    resp = self.client.index(index=index, document=documento)

                return {"success": True, "data": resp}

            elif operacion == "update":
                doc_id = comando.get("id")
                doc = comando.get("doc") or comando.get("documento", {})

                resp = self.client.update(
                    index=index,
                    id=doc_id,
                    body={"doc": doc}
                )
                return {"success": True, "data": resp}

            elif operacion == "delete":
                doc_id = comando.get("id")
                resp = self.client.delete(index=index, id=doc_id)
                return {"success": True, "data": resp}

            elif operacion == "delete_by_query":
                query = comando.get("query", {})
                resp = self.client.delete_by_query(index=index, body={"query": query})
                return {"success": True, "data": resp}

            return {"success": False, "error": f"Operación DML no soportada: {operacion}"}

        except Exception as e:
            return {"success": False, "error": str(e)}

    # ---------------------------------------------------------------------
    # BÚSQUEDAS
    # ---------------------------------------------------------------------

    def buscar(self, index: str, query: Dict, aggs=None, size: int = 10) -> Dict:
        """Ejecuta una búsqueda estándar"""
        try:
            q = query.get("query") if "query" in query else query

            resp = self.client.search(
                index=index,
                query=q,
                aggs=aggs,
                size=size
            )

            return {
                "success": True,
                "total": resp["hits"]["total"]["value"],
                "resultados": resp["hits"]["hits"],
                "aggs": resp.get("aggregations", {})
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    def buscar_texto(self, index: str, texto: str, campos: List[str] = None, size: int = 10) -> Dict:
        """Búsqueda simple por texto"""
        try:
            if campos:
                query = {
                    "query": {
                        "multi_match": {
                            "query": texto,
                            "fields": campos,
                            "type": "best_fields"
                        }
                    }
                }
            else:
                query = {
                    "query": {
                        "query_string": {"query": texto}
                    }
                }

            return self.buscar(index, query, size=size)

        except Exception as e:
            return {"success": False, "error": str(e)}

    def ejecutar_query(self, query_json: str) -> Dict:
        """Ejecuta una query JSON completa"""
        try:
            query = json.loads(query_json)
            index = query.pop("index", "_all")

            resp = self.client.search(
                index=index,
                query=query.get("query"),
                aggs=query.get("aggs"),
                size=query.get("size", 10)
            )

            return {
                "success": True,
                "total": resp["hits"]["total"]["value"],
                "hits": resp["hits"]["hits"],
                "aggs": resp.get("aggregations", {})
            }

        except Exception as e:
            return {"success": False, "error": str(e)}

    # ---------------------------------------------------------------------
    # OTROS
    # ---------------------------------------------------------------------

    def obtener_documento(self, index: str, doc_id: str) -> Optional[Dict]:
        """Obtiene un documento por ID"""
        try:
            resp = self.client.get(index=index, id=doc_id)
            return resp["_source"] if resp.get("found") else None
        except:
            return None

    def close(self):
        """Cierra la conexión"""
        self.client.close()
