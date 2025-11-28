from flask import Flask, render_template, request, redirect, url_for, jsonify, session, flash
from dotenv import load_dotenv
from datetime import datetime
from werkzeug.utils import secure_filename
import os
from Helpers import MongoDB, ElasticSearch, Funciones

# Cargar variables de entorno
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'clave_super_secreta_12345')
# Configuración MongoDB
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB = os.getenv('MONGO_DB')
MONGO_COLECCION = os.getenv('MONGO_COLECCION', 'usuario_roles')


# Configuración ElasticSearch Cloud
ELASTIC_CLOUD_URL       = os.getenv('ELASTIC_CLOUD_URL')
ELASTIC_API_KEY         = os.getenv('ELASTIC_API_KEY')
ELASTIC_INDEX_DEFAULT   = os.getenv('ELASTIC_INDEX_DEFAULT', 'index_cuentos')

# Versión de la aplicación
VERSION_APP = "1.0.0"
CREATOR_APP = "OscarDanTR"

# Inicializar conexiones
mongo = MongoDB(MONGO_URI, MONGO_DB)
elastic = ElasticSearch(ELASTIC_CLOUD_URL, ELASTIC_API_KEY)

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
