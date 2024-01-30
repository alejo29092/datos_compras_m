import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
import select
import time
from datetime import datetime
from io import StringIO

with open('contraseña') as file_object:
    password = file_object.read()
datos_direccion = pd.read_csv('address_supermeiker_data.csv')
datos_direccion_2 = datos_direccion['address'].tolist()


def datos_aleatorios(num_filas):
    """
    Para este problema hice una lista en esta página: https://mockaroo.com/
    puede insertar datos directamente en sql, pero no era exactamente lo que necesito,
    asi que lo adapte a mis necesidades
    :return:
    """

    datos_nombres = pd.read_csv('data_name.csv')
    datos_nombres_2 = datos_nombres['name'].tolist()

    data = []
    for _ in range(num_filas):
        fila = {
            'nombre': np.random.choice(datos_nombres_2),
            'identificacion': np.random.randint(100, 50000000),
            'id_supermercado': np.random.randint(1, 1000),
            'id_producto': np.random.randint(1, 500),
            'cantidad': np.random.randint(1, 20),
            'fecha': datetime.now()

        }
        data.append(fila)

    df = pd.DataFrame(data)
    return df


#inicio_tiempo = time.time()

#data_random = datos_aleatorios(5)
#print(data_random)
#fin_tiempo = time.time()
#tiempo_transcurrido = fin_tiempo - inicio_tiempo
#print('este fue el tiempo transcurrido', tiempo_transcurrido)


def insert_table(data):
    """
    Está función recibe datos para confirmar en la tabla persona, si existe la persona, la función no la agrega,
    en caso contrario, agrega los nuevos datos
    :param data: data generada, o que se va a añadir
    """
    # Carga la contraseña desde un archivo
    with open('contraseña') as file_object:
        password = file_object.read().strip()

    conn = psycopg2.connect(
        dbname="datos_ms",
        user="user_data",
        password=password,
        host="localhost",
        port="5432"
    )
    try:
        # Seleccionar las columnas deseadas
        data_tabla_persona = data.loc[:, ['nombre', 'identificacion']]

        # Eliminar filas duplicadas del DataFrame original
        data_tabla_persona = data_tabla_persona.drop_duplicates(subset='identificacion', keep='first')

        # Verificar existencia de identificaciones duplicadas en la base de datos
        existing_identifications = check_existing_identifications(conn, data_tabla_persona['identificacion'])

        if existing_identifications:
            # Manejar la situación de identificaciones duplicadas (puedes imprimir un mensaje, por ejemplo)
            print(f'Identificaciones duplicadas encontradas y eliminadas: {existing_identifications}')

        with conn.cursor() as cursor:
            for index, row in data_tabla_persona.iterrows():
                cursor.execute("""
                      INSERT INTO persona (nombre, identificacion) VALUES (%s, %s)
                  """, (row['nombre'], row['identificacion']))

        conn.commit()

    finally:
        conn.close()


def check_existing_identifications(conn, identificaciones):
    """
    Está función es una union de ayuda de la anterior
    :param conn: ruta de la base de datos
    :param identificaciones: confirma los usuarios existentes en la base de datos
    :return:
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT identificacion
            FROM persona
            WHERE identificacion IN %s
        """, (tuple(identificaciones),))

        existing_identifications = set(row[0] for row in cursor.fetchall())

    return existing_identifications


# insert_table(data_random)


def datos_entrada():
    """
    Está función sea crea para confirmar cuantos datos fueron puestos en cada tabla(esta desarollada ya qe era
    pensada para ir uniendo datos en forma de llegada, pero es mejor hacerlo con tiempos, pero no se descarta esta
    posibilidad)
    :return: mensaje datos
    """
    with open('contraseña') as file_object:
        password = file_object.read()

    conn = psycopg2.connect(
        dbname="datos_ms",
        user="user_data",
        password=f"{password}",
        host="localhost",
        port="5432"
    )

    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    cur = conn.cursor()

    cur.execute("LISTEN datos_actualizados;")

    notificacion_recibida = False

    while True:
        try:
            if select.select([conn], [], [], 10) == ([], [], []):
                if notificacion_recibida:
                    print("Notificación recibida. Cerrando el ciclo.")
                    break
                else:
                    print("No hay notificaciones en 10 segundos.")
            else:
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    print(f"Notificación recibida: {notify.payload}")

                    contenido_notificacion = notify.payload

                    # Procesar la notificación y devolver resultados
                    # Puedes realizar aquí la lógica necesaria para generar y devolver nuevos datos

                    print(f"Contenido de la notificación: {contenido_notificacion}")

                    notificacion_recibida = True
                    break

        except Exception as e:
            print(f"Error: {e}")
    return notificacion_recibida

    cur.close()
    conn.close()


def datos_tabla(tabla, columna):
    """
    uso posterior

    :param tabla: tabla a buscar dato
    :param columna: columna a traer dato
    :return:
    """
    with open('contraseña') as file_object:
        password = file_object.read()

    conn = psycopg2.connect(
        dbname="datos_ms",
        user="user_data",
        password=f"{password}",
        host="localhost",
        port="5432"
    )

    cur = conn.cursor()

    try:

        consulta_sql = f"SELECT * FROM {tabla} WHERE {columna};"

        cur.execute(consulta_sql)

        resultados = cur.fetchall()

        for fila in resultados:
            print(f"Fila: {fila}")

    except psycopg2.Error as e:
        print(f"Error en la consulta SQL: {e}")

    finally:

        cur.close()
        conn.close()


def actualizar_ids(data_nueva_tabla_persona):
    """
    Actualiza la tabla primaria uniendo los id a la identificacion


    :param data_nueva_tabla_persona:
    :return:
    """
    conn = psycopg2.connect(
        dbname="datos_ms",
        user="user_data",
        password=f"{password}",
        host="localhost",
        port="5432")
    try:

        with conn.cursor() as cursor:

            cursor.execute("SELECT id, identificacion FROM persona")
            data_persona = pd.DataFrame(cursor.fetchall(), columns=['id', 'identificacion'])

            data_completa = pd.merge(data_nueva_tabla_persona, data_persona, on='identificacion', how='left')


    except Exception as e:

        print(f"Error: {e}")

    finally:

        conn.close()

    data_completa = data_completa.loc[:, ['id', 'id_producto', 'cantidad', 'id_supermercado', 'fecha']]
    data_completa = data_completa.rename(columns={'id': 'id_persona'})
    return data_completa


def convertir_a_tipos_nativos(data_compra):
    # Convertir todas las columnas a tipos nativos de Python
    for columna in data_compra.columns:
        data_compra[columna] = data_compra[columna].astype(str).apply(
            lambda x: x.item() if isinstance(x, np.int64) else x)
    return data_compra


def insert_compra(data_compra):
    """
    Inserta los datos faltantes en la tabla compra
    :param data_compra:
    :return:
    """
    with open('contraseña') as file_object:
        password = file_object.read()

    conn = psycopg2.connect(
        dbname="datos_ms",
        user="user_data",
        password=f"{password}",
        host="localhost",
        port="5432"
    )

    try:

        data_compra = convertir_a_tipos_nativos(data_compra)

        with conn.cursor() as cursor:

            for index, row in data_compra.iterrows():
                cursor.execute("""
                    INSERT INTO compra (id_persona, id_producto, cantidad, id_supermercado, fecha)
                    VALUES (%s, %s, %s, %s, %s)
                """, (row['id_persona'], row['id_producto'], row['cantidad'], row['id_supermercado'], row['fecha']))

        conn.commit()


    except Exception as e:

        print(f"Error: {e}")

    finally:

        conn.close()

# insert_compra(data_final)
