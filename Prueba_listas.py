import pandas as pd
import random
from collections import Counter
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
import json
from flask import Flask, jsonify

# Cargar credenciales desde la variable de entorno
credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON")

if not credentials_json:
    raise ValueError("No se encontró el JSON de credenciales. Asegúrate de configurarlo en las variables de entorno.")

# Convertir la cadena JSON en un diccionario
credentials_dict = json.loads(credentials_json)

# Usar las credenciales para autenticar Google Sheets API
creds = service_account.Credentials.from_service_account_info(credentials_dict)

# Carga las credenciales desde el archivo JSON
#SERVICE_ACCOUNT_FILE = "pruebaom-8d17dd95cc2a.json"  # ← Cambia esto por tu archivo JSON
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


# ID de tu Google Sheets (lo sacas de la URL del documento)
SPREADSHEET_ID = '1Skn7QQDmyrbPr6YcG1zBI0x0AQyGWTUg_FDGAza_wQc' # ← Cambia esto por el ID real

# Conectar con la API de Google Sheets
service = build("sheets", "v4", credentials=creds)
sheet = service.spreadsheets()

# Obtener la metadata del documento para detectar el rango dinámico
sheet_metadata = sheet.get(spreadsheetId=SPREADSHEET_ID).execute()
sheets = sheet_metadata.get("sheets", "")
sheet_name = sheets[0]["properties"]["title"]  # Obtiene el nombre de la primera hoja
last_row = sheets[0]["properties"]["gridProperties"]["rowCount"]  # Última fila con datos
last_col = sheets[0]["properties"]["gridProperties"]["columnCount"]  # Última columna con datos

# Crear el rango dinámico basado en la cantidad de filas y columnas detectadas
RANGE_NAME = f"{sheet_name}!A1:{chr(64+last_col)}{last_row}"  # Convierte el índice a letra (solo hasta "Z")

# Obtener los datos reales del rango detectado
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
values = result.get("values", [])

# Convertir a DataFrame de Pandas
if values:
    df_go = pd.DataFrame(values[1:], columns=values[0])  # Usa la primera fila como encabezado
    #print(df.head())  # Mostrar las primeras filas
else:
    print("No hay datos en la hoja.")




def seleccionar_registros(Argegados, n):
    # Contar ocurrencias de cada elemento
    conteo = Counter(Argegados)
    
    # Ordenar los elementos por frecuencia de aparición (de menor a mayor)
    conteo_ordenado = sorted(conteo.items(), key=lambda x: x[1])
    #print( conteo_ordenado )
    seleccionados = []
    i = 0
    
    # Ir agregando registros según el criterio dado
    while len(seleccionados) < n and i < len(conteo_ordenado):
        valor_actual = conteo_ordenado[i][1]  # Cantidad de veces que aparece
        grupo_actual = [k for k, v in conteo_ordenado if v == valor_actual and k not in seleccionados]
        
        random.shuffle(grupo_actual)  # Mezclar para aleatoriedad
        
        faltantes = n - len(seleccionados)
        seleccionados.extend(grupo_actual[:faltantes])  # Agregar sin repetir
        
        i += 1  # Pasar al siguiente grupo de valores
    
    return seleccionados

def expand_column(df, column_name):
    """
    Expande los valores de una columna específica separada por comas en múltiples filas,
    manteniendo las demás columnas sin cambios.
    
    :param df: DataFrame de pandas
    :param column_name: Nombre de la columna a expandir
    :return: DataFrame con filas expandidas
    """
    df_expanded = df.copy()
    df_expanded[column_name] = df_expanded[column_name].str.split(',')
    df_expanded = df_expanded.explode(column_name)
    return df_expanded.reset_index(drop=True)


# Ejemplo de uso
df = expand_column(df_go, 'Fecha en que puede asistir')
df = df.rename(columns={"Espacio Open Mic": "Open_Space", 
                        "Comediante": "Name_Comedian",
                        "CC":"Id",
                        "Fecha en que puede asistir":"Fecha_Disp"
                       })




Fechas = df['Fecha_Disp'].unique().tolist()
Comedians = df['Name_Comedian'].unique().tolist()
df["Fecha_Disp"] = pd.to_datetime(df["Fecha_Disp"], errors='coerce')


group_by_fecha = dict()
for fecha in Fechas:
    group_by_fecha[fecha] = df[ df['Fecha_Disp'] == fecha ]['Name_Comedian'].unique().tolist()



list_by_fecha = dict()
Comedians = df['Name_Comedian'].unique().tolist()
Final_comedians = []
Argegados = []
length = 8
for fecha in Fechas:
#    print('FECHA: ',fecha)
    comunes = [item for item in group_by_fecha[fecha] if item in Comedians]
    new_posibles = random.sample( comunes , min(length, len(comunes)))
#    print('COMUNES: ', comunes )
#    print('POSIBLES: ', new_posibles )
    if len(new_posibles) != length:
#        print('No tiene el tamaño: ',length)
        nuevos = seleccionar_registros(Argegados, length - len(new_posibles) )
        list_by_fecha[fecha] =  new_posibles + nuevos 
#        print( 'Se le agregan: ', nuevos )
    else:
#        print('Sí tiene el tamaño')
        list_by_fecha[fecha] = new_posibles
#    print(list_by_fecha[fecha])
    Comedians = [item for item in Comedians if item not in list_by_fecha[fecha]]
    Argegados = Argegados + list_by_fecha[fecha]
    # print('--------------kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk--')
    # print('--------------kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk--')
    # print('--------------kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk--')
Final_comedians = Comedians 
print('-----------------')
Final_comedians


# Convertir las claves a string para que sea serializable en JSON
resultado = {str(k): v for k, v in list_by_fecha.items()}
json_data = json.dumps(resultado, indent=4)

# Inicializar Flask
app = Flask(__name__)

@app.route("/get_comedians", methods=["GET"])
def get_comedians():
    """Endpoint que devuelve los datos en formato JSON."""
    data = json_data
    return jsonify(data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
