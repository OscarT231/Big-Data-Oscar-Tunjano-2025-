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
@app.route('/')
def landing():
    """Landing page pública"""
    return render_template('landing.html', version=VERSION_APP, creador=CREATOR_APP)

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
