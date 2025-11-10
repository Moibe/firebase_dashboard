from firebase_admin import firestore, credentials, initialize_app
import firebase_admin
import os
import time

# --- 1. CONFIGURACIÓN E INICIALIZACIÓN ---
# Asumimos que la credencial y el nombre de la app son consistentes con tus otros scripts.
APP_NAME = 'splashmix-ai-prod' 
COLECCION_A_CORREGIR = 'usuarios'

try:
    if not firebase_admin._apps:
        cred = credentials.Certificate(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "config_prod.json"))
        app_instance = initialize_app(cred, name=APP_NAME)
    else:
        app_instance = firebase_admin.get_app(APP_NAME)
        
    db = firestore.client(app=app_instance)
    print(f"✔️ Conexión a Firebase '{APP_NAME}' establecida.")

except Exception as e:
    print(f"❌ ERROR CRÍTICO DE INICIALIZACIÓN: {e}")
    exit()

# --- 2. FUNCIÓN DE MIGRACIÓN ---

def corregir_nombre_campo_display_name(coleccion):
    """
    Renombra el campo 'diplayName' a 'displayName' en todos los documentos 
    de la colección especificada.
    """
    print(f"\n--- Iniciando corrección de esquema en la colección '{coleccion}' ---")
    
    # Referencia a la colección
    collection_ref = db.collection(coleccion)
    
    # Usamos .stream() para iterar sobre todos los documentos (lectura de todos)
    try:
        documentos = collection_ref.stream()
        documentos_corregidos = 0
        
        for doc in documentos:
            doc_data = doc.to_dict()
            
            # Verificamos si el campo incorrecto existe
            if 'diplayName' in doc_data:
                
                # 1. Extraer el valor del campo incorrecto
                nombre_antiguo = doc_data['diplayName']
                
                # 2. Definir la operación de actualización:
                #    a) Añadir el nuevo campo (displayName) con el valor.
                #    b) Eliminar el campo incorrecto (diplayName).
                updates = {
                    'displayName': nombre_antiguo,
                    'diplayName': firestore.DELETE_FIELD
                }
                
                # 3. Aplicar la actualización al documento
                doc.reference.update(updates)
                documentos_corregidos += 1
                print(f"  [ACTUALIZADO] Documento ID: {doc.id}. Nombre: '{nombre_antiguo}'")
            
            # Si el campo 'diplayName' no existe, lo ignoramos y pasamos al siguiente
            
        print(f"\n✅ Proceso completado. Total de documentos corregidos: {documentos_corregidos}")
    
    except Exception as e:
        print(f"\n❌ ERROR DURANTE LA MIGRACIÓN: {e}")

# --- 3. EJECUCIÓN PRINCIPAL ---

if __name__ == '__main__':
    # Usamos una pausa para evitar problemas de latencia si el script se ejecuta justo después de otro
    time.sleep(1) 
    
    corregir_nombre_campo_display_name(COLECCION_A_CORREGIR)