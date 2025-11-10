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
    # 
    # # --- CONSTANTES DE METADATOS ---
METADATA_COLLECTION = '_report_metadata'
CHECKPOINT_DOC_ID = 'usuarios_reporte'
CHECKPOINT_FIELD = 'last_processed_timestamp'


def generar_reporte_incremental(coleccion_usuarios='usuarios', nombre_archivo='reporte_usuarios.xlsx'):
    """
    Lee solo los documentos de usuarios nuevos desde la última ejecución y 
    los añade al archivo Excel existente.
    """
    if db is None:
        print("🔴 ERROR: La conexión a Firebase no está establecida.")
        return

    print(f"\n--- Iniciando reporte incremental para '{coleccion_usuarios}' ---")
    
    # --- PASO 1: LEER EL CHECKPOINT ---
    checkpoint_ref = db.collection(METADATA_COLLECTION).document(CHECKPOINT_DOC_ID)
    checkpoint_doc = checkpoint_ref.get()
    
    # Si el checkpoint existe, usamos su timestamp. Si no, usamos una fecha muy antigua.
    if checkpoint_doc.exists:
        last_ts = checkpoint_doc.to_dict().get(CHECKPOINT_FIELD)
        print(f"Punto de control encontrado: {last_ts.strftime('%Y-%m-%d %H:%M:%S') if last_ts else 'Inicio'}")
    else:
        # Usa una fecha muy anterior si es la primera ejecución
        last_ts = datetime(1970, 1, 1, tzinfo=None) 
        print("No se encontró punto de control. Se procesarán TODOS los usuarios (primera ejecución).")


    # --- PASO 2: CONSULTAR SOLO DOCUMENTOS NUEVOS ---
    datos_nuevos_reporte = []
    
    try:
        # Consulta: Trae todos los documentos donde 'fecha_registro' sea mayor que el último timestamp procesado
        query = db.collection(coleccion_usuarios).where(
            filter=firestore.FieldFilter('fecha_registro', '>', last_ts)
        ).order_by('fecha_registro').stream()
        
        count = 0
        new_last_ts = last_ts # Inicializamos el nuevo timestamp que guardaremos
        
        for doc in query:
            data = doc.to_dict()
            
            # Formatear el Timestamp a un string legible
            fecha_registro_ts = data.get('fecha_registro')
            fecha_str = fecha_registro_ts.strftime('%Y-%m-%d %H:%M:%S') if fecha_registro_ts else 'N/A'

            registro = {
                'fecha_registro': fecha_str,
                'correo_usuario': data.get('email', 'N/A'),
                'displayName': data.get('displayName', 'N/A'),
                'tokens': data.get('tokens', 0),
                'UID_DOCUMENTO': doc.id
            }
            datos_nuevos_reporte.append(registro)
            count += 1
            
            # Actualizamos el timestamp para el checkpoint
            if fecha_registro_ts > new_last_ts:
                new_last_ts = fecha_registro_ts
        
        print(f"✔️ {count} documentos NUEVOS encontrados y procesados.")

        if not datos_nuevos_reporte:
            print("No hay nuevos usuarios desde la última ejecución.")
            return

        # --- PASO 3: EXPORTAR Y ACTUALIZAR CHECKPOINT ---

        # 3a. Exportar a Excel (Lógica de apendizar, similar a tu código anterior)
        df_nuevos = pd.DataFrame(datos_nuevos_reporte)

        if os.path.exists(nombre_archivo):
            df_existente = pd.read_excel(nombre_archivo, engine='openpyxl')
            df_final = pd.concat([df_existente, df_nuevos], ignore_index=True)
            print(f"✏️ Añadidos {count} registros al Excel existente.")
        else:
            df_final = df_nuevos
            print(f"➕ Creando nuevo archivo Excel con {count} registros.")

        df_final.to_excel(nombre_archivo, index=False, engine='openpyxl')
        
        # 3b. Actualizar Checkpoint (¡Solo si la escritura fue exitosa!)
        checkpoint_ref.set({CHECKPOINT_FIELD: new_last_ts})
        print(f"✅ Checkpoint actualizado a: {new_last_ts.strftime('%Y-%m-%d %H:%M:%S')}")

    except Exception as e:
        print(f"❌ Error durante la generación del reporte. El checkpoint NO se actualizó: {e}")
        # Si hay un error, el checkpoint no se actualiza y la próxima vez intentará de nuevo.

# --- Ejecución Principal ---

if __name__ == '__main__':
    # Asegúrate de inicializar db antes de esto
    generar_reporte_incremental()