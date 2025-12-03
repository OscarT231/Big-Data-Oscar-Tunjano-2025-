from flask import Flask, render_template, request, redirect, url_for, jsonify, session, flash
from dotenv import load_dotenv
from datetime import datetime
from werkzeug.utils import secure_filename
import os
from Helpers import MongoDB, ElasticSearch, Funciones, WebScraping

# Cargar variables de entorno
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'clave_super_secreta_12345')
# Configuración MongoDB
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DB')
MONGO_COLECCION = os.getenv('MONGO_COLECCION', 'usuario_roles')


# Configuración ElasticSearch Cloud
ELASTIC_CLOUD_ID       = os.getenv('ELASTIC_CLOUD_ID')
ELASTIC_API_KEY         = os.getenv('ELASTIC_API_KEY')
ELASTIC_INDEX_DEFAULT   = os.getenv('ELASTIC_INDEX_DEFAULT', 'index_proyecto')

# Versión de la aplicación
VERSION_APP = "1.2.0"
CREATOR_APP = "OscarDanTR"

# Inicializar conexiones
mongo = MongoDB(MONGO_URI, MONGO_DB)
elastic = ElasticSearch(ELASTIC_CLOUD_ID, ELASTIC_API_KEY)

# ==================== RUTAS ====================
####RUTA DE LANDINGN####
@app.route('/')
def landing():
    """Landing page pública"""
    return render_template('landing.html', version=VERSION_APP, creador=CREATOR_APP)

#### RUTA DE ABOUT####
@app.route('/about')
def about():
    """Página About"""
    return render_template('about.html', version=VERSION_APP, creador=CREATOR_APP)


############### RUTAS DE BUSCADOR EN ELASTIC INICIO #################
#### RUTA DE BUSCADOR####
@app.route('/buscador')
def buscador():
    """Página de búsqueda pública"""
    return render_template('buscador.html', version=VERSION_APP, creador=CREATOR_APP)

### RUTA DE BUSCARDOR ELASTIC ###
@app.route('/buscar-elastic', methods=['POST'])
def buscar_elastic():
    try:
        data = request.get_json()
        texto_buscar = data.get("texto", "").strip()

        if not texto_buscar:
            return jsonify({"success": False, "error": "No se envió texto a buscar"})

        cliente = elastic.client  # usar la instancia ya creada

        # Campos a buscar
        campos_busqueda = ["titulo^2", "texto_completo", "filename"]

        # Query multi_match con analyzer español y fuzziness
        query = {
            "query": {
                "multi_match": {
                    "query": texto_buscar,
                    "fields": campos_busqueda,
                    "type": "best_fields",
                    "operator": "or",
                    "fuzziness": "AUTO",       # permite errores de tipeo
                    "prefix_length": 2          # no fuzziness para las primeras 2 letras
                }
            }
        }

        respuesta = cliente.search(index="index_proyecto", body=query, size=100)

        hits = respuesta.get("hits", {}).get("hits", [])
        total = respuesta.get("hits", {}).get("total", {}).get("value", 0)

        return jsonify({
            "success": True,
            "total": total,
            "hits": hits,
            "aggs": {}
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}),500

############## RUTAS DE BUSCADOR EN ELASTIC FIN #################


# MONGO ############## RUTAS DE MONGO INICIO #################
####RUTA DE LOGIN CON VALIDACIÓN####
@app.route('/login', methods=['GET', 'POST'])
def login():
    """Página de login con validación"""
    if request.method == 'POST':
        usuario = request.form.get('usuario')
        password = request.form.get('password')
        
        # Validar usuario en MongoDB
        user_data = mongo.validar_usuario(usuario, password, MONGO_COLECCION)
        
        if user_data:
            # Guardar sesión
            session['usuario'] = usuario
            session['permisos'] = user_data.get('permisos', {})
            session['logged_in'] = True
            
            flash('¡Bienvenido! Inicio de sesión exitoso', 'success')
            return redirect(url_for('admin'))
        else:
            flash('Usuario o contraseña incorrectos', 'danger')
    
    return render_template('login.html')

### RUTA DE LISTAR USUARIOS ###
@app.route('/listar-usuarios')
def listar_usuarios():
    try:

        usuarios = mongo.listar_usuarios(MONGO_COLECCION)
        
        # Convertir ObjectId a string para serialización JSON
        for usuario in usuarios:
            usuario['_id'] = str(usuario['_id'])
        
        return jsonify(usuarios)
    except Exception as e:
        return jsonify({'error': str(e)}), 500 
    
### RUTA GESTIONAR USUARIOS ###
@app.route('/gestor_usuarios')
def gestor_usuarios():
    """Página de gestión de usuarios (protegida requiere login y permiso admin_usuarios) """
    if not session.get('logged_in'):
        flash('Por favor, inicia sesión para acceder a esta página', 'warning')
        return redirect(url_for('login'))
    
    permisos = session.get('permisos', {})
    if not permisos.get('admin_usuarios'):
        flash('No tiene permisos para gestionar usuarios', 'danger')
        return redirect(url_for('admin'))
    
    return render_template('gestor_usuarios.html', usuario=session.get('usuario'), permisos=permisos, version=VERSION_APP, creador=CREATOR_APP)

### RUTA CREAR USUARIO ###
@app.route('/crear-usuario', methods=['POST'])
def crear_usuario():
    """API para crear un nuevo usuario"""
    try:
        if not session.get('logged_in'):
            return jsonify({'success': False, 'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_usuarios'):
            return jsonify({'success': False, 'error': 'No tiene permisos para crear usuarios'}), 403
        
        data = request.get_json()
        usuario = data.get('usuario')
        password = data.get('password')
        permisos_usuario = data.get('permisos', {})
        
        if not usuario or not password:
            return jsonify({'success': False, 'error': 'Usuario y password son requeridos'}), 400
        
        # Verificar si el usuario ya existe
        usuario_existente = mongo.obtener_usuario(usuario, MONGO_COLECCION)
        if usuario_existente:
            return jsonify({'success': False, 'error': 'El usuario ya existe'}), 400
        
        # Crear usuario
        resultado = mongo.crear_usuario(usuario, password, permisos_usuario, MONGO_COLECCION)
        
        if resultado:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Error al crear usuario'}), 500
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

### RUTA ACTUALIZAR USUARIO ###
@app.route('/actualizar-usuario', methods=['POST'])
def actualizar_usuario():
    """API para actualizar un usuario existente"""
    try:
        if not session.get('logged_in'):
            return jsonify({'success': False, 'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_usuarios'):
            return jsonify({'success': False, 'error': 'No tiene permisos para actualizar usuarios'}), 403
        
        data = request.get_json()
        usuario_original = data.get('usuario_original')
        datos_usuario = data.get('datos', {})
        
        if not usuario_original:
            return jsonify({'success': False, 'error': 'Usuario original es requerido'}), 400
        
        # Verificar si el usuario existe
        usuario_existente = mongo.obtener_usuario(usuario_original, MONGO_COLECCION)
        if not usuario_existente:
            return jsonify({'success': False, 'error': 'Usuario no encontrado'}), 404
        
        # Si el nombre de usuario cambió, verificar que no exista otro con ese nombre
        nuevo_usuario = datos_usuario.get('usuario')
        if nuevo_usuario and nuevo_usuario != usuario_original:
            usuario_duplicado = mongo.obtener_usuario(nuevo_usuario, MONGO_COLECCION)
            if usuario_duplicado:
                return jsonify({'success': False, 'error': 'Ya existe otro usuario con ese nombre'}), 400
        
        # Actualizar usuario
        resultado = mongo.actualizar_usuario(usuario_original, datos_usuario, MONGO_COLECCION)
        
        if resultado:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Error al actualizar usuario'}), 500
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

### RUTA ELIMINAR USUARIO ###
@app.route('/eliminar-usuario', methods=['POST'])
def eliminar_usuario():
    """API para eliminar un usuario"""
    try:
        if not session.get('logged_in'):
            return jsonify({'success': False, 'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_usuarios'):
            return jsonify({'success': False, 'error': 'No tiene permisos para eliminar usuarios'}), 403
        
        data = request.get_json()
        usuario = data.get('usuario')
        
        if not usuario:
            return jsonify({'success': False, 'error': 'Usuario es requerido'}), 400
        
        # Verificar si el usuario existe
        usuario_existente = mongo.obtener_usuario(usuario, MONGO_COLECCION)
        if not usuario_existente:
            return jsonify({'success': False, 'error': 'Usuario no encontrado'}), 404
        
        # No permitir eliminar al usuario actual
        if usuario == session.get('usuario'):
            return jsonify({'success': False, 'error': 'No puede eliminarse a sí mismo'}), 400
        
        # Eliminar usuario
        resultado = mongo.eliminar_usuario(usuario, MONGO_COLECCION)
        
        if resultado:
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Error al eliminar usuario'}), 500
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# MONGO ############## RUTAS DE MONGO FIN #################

############### RUTAS DE ELASTIC INICIO #################
#### RUTA GESTOR ELASTIC ###
@app.route('/gestor_elastic')
def gestor_elastic():
    """Página de gestión de ElasticSearch (protegida requiere login y permiso admin_elastic)"""
    if not session.get('logged_in'):
        flash('Por favor, inicia sesión para acceder a esta página', 'warning')
        return redirect(url_for('login'))
    
    permisos = session.get('permisos', {})
    if not permisos.get('admin_elastic'):
        flash('No tiene permisos para gestionar ElasticSearch', 'danger')
        return redirect(url_for('admin'))
    
    return render_template('gestor_elastic.html', usuario=session.get('usuario'), permisos=permisos, version=VERSION_APP, creador=CREATOR_APP)

#### RUTA LISTAR ÍNDICES ELASTIC ###
@app.route('/listar-indices-elastic')
def listar_indices_elastic():
    """API para listar índices de ElasticSearch"""
    try:
        if not session.get('logged_in'):
            return jsonify({'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_elastic'):
            return jsonify({'error': 'No tiene permisos para gestionar ElasticSearch'}), 403
        
        indices = elastic.listar_indices()
        return jsonify(indices)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

#### RUTA EJECUTAR QUERY ELASTIC ###
@app.route('/ejecutar-query-elastic', methods=['POST'])
def ejecutar_query_elastic():
    """API para ejecutar una query en ElasticSearch"""
    try:
        if not session.get('logged_in'):
            return jsonify({'success': False, 'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_elastic'):
            return jsonify({'success': False, 'error': 'No tiene permisos para gestionar ElasticSearch'}), 403
        
        data = request.get_json()
        query_json = data.get('query')
        
        if not query_json:
            return jsonify({'success': False, 'error': 'Query es requerida'}), 400
        
        resultado = elastic.ejecutar_query(query_json)
        return jsonify(resultado)
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


#### RUTA CARGAR DOCUMENTOS A ELASTIC ###
@app.route('/cargar_doc_elastic')
def cargar_doc_elastic():
    """Página de carga de documentos a ElasticSearch (protegida requiere login y permiso admin_data_elastic)"""
    if not session.get('logged_in'):
        flash('Por favor, inicia sesión para acceder a esta página', 'warning')
        return redirect(url_for('login'))
    
    permisos = session.get('permisos', {})
    if not permisos.get('admin_data_elastic'):
        flash('No tiene permisos para cargar datos a ElasticSearch', 'danger')
        return redirect(url_for('admin'))
    
    return render_template('documentos_elastic.html', usuario=session.get('usuario'), permisos=permisos, version=VERSION_APP, creador=CREATOR_APP)

### RUTA PROCESAR WEBSCRAPING A ELASTIC ###
@app.route('/procesar-webscraping-elastic', methods=['POST'])
def procesar_webscraping_elastic():
    """API para procesar Web Scraping"""
    try:
        if not session.get('logged_in'):
            return jsonify({'success': False, 'error': 'No autorizado'}), 401
        
        permisos = session.get('permisos', {})
        if not permisos.get('admin_data_elastic'):
            return jsonify({'success': False, 'error': 'No tiene permisos para cargar datos'}), 403
        
        data = request.get_json()
        url = data.get('url')
        extensiones_navegar = data.get('extensiones_navegar', 'aspx')
        tipos_archivos = data.get('tipos_archivos', 'pdf')
        index = data.get('index')
        
        if not url or not index:
            return jsonify({'success': False, 'error': 'URL e índice son requeridos'}), 400
        
        # Procesar listas de extensiones
        lista_ext_navegar = [ext.strip() for ext in extensiones_navegar.split(',')]
        lista_tipos_archivos = [ext.strip() for ext in tipos_archivos.split(',')]
        
        # Combinar ambas listas para extraer todos los enlaces
        todas_extensiones = lista_ext_navegar + lista_tipos_archivos
        
        # Inicializar WebScraping
        scraper = WebScraping(dominio_base=url.rsplit('/', 1)[0] + '/')
        
        # Limpiar carpeta de uploads
        carpeta_upload = 'static/uploads'
        Funciones.crear_carpeta(carpeta_upload)
        Funciones.borrar_contenido_carpeta(carpeta_upload)
        
        # Extraer todos los enlaces
        json_path = os.path.join(carpeta_upload, 'links.json')
        resultado = scraper.extraer_todos_los_links(
            url_inicial=url,
            json_file_path=json_path,
            listado_extensiones=todas_extensiones,
            max_iteraciones=50
        )
        
        if not resultado['success']:
            return jsonify({'success': False, 'error': 'Error al extraer enlaces'}), 500
        
        # Descargar archivos PDF (o los tipos especificados)
        resultado_descarga = scraper.descargar_pdfs(json_path, carpeta_upload)
        
        scraper.close()
        
        # Listar archivos descargados
        archivos = Funciones.listar_archivos_carpeta(carpeta_upload, lista_tipos_archivos)
        
        return jsonify({
            'success': True,
            'archivos': archivos,
            'mensaje': f'Se descargaron {len(archivos)} archivos',
            'stats': {
                'total_enlaces': resultado['total_links'],
                'descargados': resultado_descarga.get('descargados', 0),
                'errores': resultado_descarga.get('errores', 0)
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500





########################## RUTAS DE ELASTIC FIN ##########################

#### RUTA DE ADMIN ####
@app.route('/admin')
def admin():
    """Página de administración (protegida requiere login)"""
    if not session.get('logged_in'):
        flash('Por favor, inicia sesión para acceder al área de administración', 'warning')
        return redirect(url_for('login'))
    
    return render_template('admin.html', usuario=session.get('usuario'), permisos=session.get('permisos'))

# ==================== MAIN ====================
if __name__ == '__main__':
    # Crear carpetas necesarias
    Funciones.crear_carpeta('static/uploads')

    # Verificar conexiones
    print("\n" + "="*50)
    print("VERIFICANDO CONEXIONES")
    if mongo.test_connection():
        print("✅ MongoDB Atlas: Conectado")
    else:
        print("❌ MongoDB Atlas: Error de conexión")
    
    if elastic.test_connection():
        print("✅ ElasticSearch Cloud: Conectado")
    else:
        print("❌ ElasticSearch Cloud: Error de conexión") 
    # Ejecutar la aplicación (localmente para pruebas)
    app.run(debug=True, host='0.0.0.0', port=5000)
