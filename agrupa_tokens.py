import os
from firebase_admin import firestore
import firebase_admin
from collections import defaultdict
from firebase_admin import credentials
from datetime import datetime # ¡Necesitas esto para la fecha!
import pandas as pd

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
    
# --- TU FUNCIÓN DE AGRUPACIÓN (Se mantiene sin cambios) ---
def agrupar_usuarios_por_tokens(nombre_coleccion='usuarios'):
    # ... (El código de la función agrupar_usuarios_por_tokens) ...
    print(f"--- Iniciando agrupación de usuarios por tokens en '{nombre_coleccion}' ---")
    usuarios_agrupados = defaultdict(list)
    try:
        # Usar la variable global 'db'
        docs = db.collection(nombre_coleccion).stream()
        count = 0
        for doc in docs:
            data = doc.to_dict()
            tokens = data.get('tokens', 0)
            if not isinstance(tokens, int):
                tokens = 0 
            usuario_info = {
                'id': doc.id,
                'email': data.get('email', 'N/A'),
                'displayName': data.get('displayName', 'N/A')
            }
            usuarios_agrupados[tokens].append(usuario_info)
            count += 1
        print(f"✔️ {count} documentos procesados y agrupados.")
        return usuarios_agrupados
    except Exception as e:
        print(f"❌ Error al procesar la agrupación: {e}")
        return defaultdict(list)


# --- NUEVA FUNCIÓN: EXPORTAR A EXCEL ---

def exportar_a_excel(grupos, nombre_archivo='reporte_tokens.xlsx'):
    """
    Guarda los datos de conteo en una nueva fila de un archivo de Excel.
    Si el archivo no existe, lo crea.
    
    Args:
        grupos (defaultdict): Diccionario de agrupaciones (tokens: [usuarios]).
        nombre_archivo (str): Nombre del archivo de Excel a guardar/actualizar.
    """
    
    # 1. Preparar la fila de datos
    fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Encontrar todos los valores de tokens únicos
    valores_tokens_existentes = sorted(grupos.keys())
    
    # Cabeceras y Valores
    columnas = ['Fecha y Hora']
    valores = [fecha_actual]
    
    for tokens in valores_tokens_existentes:
        columnas.append(f"{tokens} Tokens")
        valores.append(len(grupos[tokens]))
        
    # Crear la nueva fila de datos (como una Serie de Pandas)
    nueva_fila = pd.Series(valores, index=columnas)

    # 2. Lógica para manejar el archivo existente/nuevo
    
    if os.path.exists(nombre_archivo):
        # El archivo existe: Cargar el contenido, añadir la fila y guardar
        try:
            df_existente = pd.read_excel(nombre_archivo)
            
            # Verificar si las columnas son consistentes
            # Si un nuevo token aparece (ej. 100 Tokens), la cabecera cambiará.
            
            # La forma más segura de agregar es usando concat
            df_nuevo = pd.DataFrame([nueva_fila])
            df_final = pd.concat([df_existente, df_nuevo], ignore_index=True)
            
            print(f"✏️ Agregando nueva fila al archivo existente...")
            
        except Exception as e:
            print(f"❌ Error al leer o procesar el archivo Excel existente: {e}. Creando uno nuevo.")
            df_final = pd.DataFrame([nueva_fila])
            
    else:
        # El archivo NO existe: Crear un nuevo DataFrame solo con la fila actual
        df_final = pd.DataFrame([nueva_fila])
        print(f"➕ Creando nuevo archivo Excel: {nombre_archivo}")
        
    # 3. Guardar el DataFrame final al archivo Excel
    try:
        # Usamos engine='openpyxl' para compatibilidad moderna
        df_final.to_excel(nombre_archivo, index=False, engine='openpyxl')
        print(f"✔️ Archivo '{nombre_archivo}' actualizado correctamente.")
    except Exception as e:
        print(f"❌ Error al escribir en el archivo Excel: {e}")


# --- Ejemplo de Uso (Bloque principal) ---

if __name__ == '__main__':
    # ... (Asegúrate de que la inicialización de Firebase esté aquí si ejecutas este archivo) ...
    
    grupos = agrupar_usuarios_por_tokens('usuarios')
    
    if grupos:
        print("\n================ Generando Reporte Excel ================")
        # Llama a la nueva función de exportación
        exportar_a_excel(grupos, nombre_archivo='reporte_conteo_tokens.xlsx')
    else:
        print("No se encontraron usuarios para agrupar. No se generó el archivo Excel.")