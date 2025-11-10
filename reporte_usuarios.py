import os
import pandas as pd
from firebase_admin import firestore, credentials, initialize_app
from datetime import datetime
import firebase_admin

# --- CÓDIGO DE INICIALIZACIÓN (Como lo tenías, sin el bloque if __name__ redundante) ---

# Usa tu inicialización nombrada y protegida aquí...
APP_NAME = 'splashmix-ai-prod' 
try:
    cred = credentials.Certificate('config_prod.json') 
    try:
        app_instance = firebase_admin.get_app(APP_NAME)
    except ValueError:
        app_instance = firebase_admin.initialize_app(cred, name=APP_NAME)
    db = firestore.client(app=app_instance)
    print(f"✔️ Firebase '{APP_NAME}' inicializada y cliente DB obtenido.")
except Exception as e:
    print(f"❌ ERROR CRÍTICO: Falló la inicialización de Firebase Admin SDK: {e}")
    # exit() 
    
# --- 2. Función Principal de Reporte ---

def generar_reporte_excel_usuarios(coleccion_nombre='usuarios', nombre_archivo='reporte_usuarios.xlsx'):
    """
    Lee todos los documentos de la colección de usuarios, extrae los campos
    clave y exporta los resultados a un archivo Excel.
    """
    if db is None:
        print("🔴 ERROR: La conexión a Firebase no está establecida.")
        return

    print(f"\n--- Iniciando generación de reporte para la colección '{coleccion_nombre}' ---")
    
    # Lista para almacenar todos los datos que formarán el reporte
    datos_reporte = []
    
    try:
        # 1. Leer todos los documentos usando .stream()
        docs = db.collection(coleccion_nombre).stream()
        
        count = 0
        for doc in docs:
            data = doc.to_dict()
            
            # 2. Extraer los datos necesarios de forma segura
            # Usamos .get() con valores por defecto para evitar errores si algún campo falta.
            fecha_registro_ts = data.get('fecha_registro')
            
            # Formatear el Timestamp a un string legible
            fecha_str = None
            if fecha_registro_ts and hasattr(fecha_registro_ts, 'strftime'):
                fecha_str = fecha_registro_ts.strftime('%Y-%m-%d %H:%M:%S')
            
            # 3. Construir el registro para la fila del reporte
            registro = {
                'fecha_registro': fecha_str or 'N/A',
                'correo_usuario': data.get('email', 'N/A'),
                'displayName': data.get('displayName', 'N/A'),
                'tokens': data.get('tokens', 0), # Usar 0 si no hay tokens
                'UID_DOCUMENTO': doc.id # Opcional: útil para auditoría
            }
            datos_reporte.append(registro)
            count += 1
        
        print(f"✔️ {count} documentos leídos y procesados.")
        
        if not datos_reporte:
            print("No hay datos para exportar.")
            return

        # 4. Crear DataFrame de Pandas y Exportar
        df = pd.DataFrame(datos_reporte)
        
        # 5. Guardar el DataFrame en Excel
        df.to_excel(nombre_archivo, index=False, engine='openpyxl')
        
        print(f"✅ ÉXITO: Reporte '{nombre_archivo}' generado con {count} registros.")

    except Exception as e:
        print(f"❌ Error durante la generación del reporte: {e}")

# --- 3. Ejecución Principal ---

if __name__ == '__main__':
    generar_reporte_excel_usuarios(coleccion_nombre='usuarios', nombre_archivo='reporte_usuarios.xlsx')