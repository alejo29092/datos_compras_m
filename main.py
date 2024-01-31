"""
autor: Alejandro
toda la data tratada aqui es hipotetica, y no es de ninguna empresa u persona real

"""
import datos_input as di
import time


def function():
    inicio_tiempo = time.time()
    print("Pon la cantidad de datos que quieres manejar, se crearan aleatoriamente:  ")
    data_new = di.datos_aleatorios(int(input('Ingrese la cantidad de datos: ')))
    di.insert_table(data_new)
    data_pre = di.actualizar_ids(data_new)
    di.insert_compra(data_pre)

    fin_tiempo = time.time()
    tiempo_transcurrido = fin_tiempo - inicio_tiempo
    print('este fue el tiempo transcurrido', tiempo_transcurrido)


if __name__ == "__main__":
    function()
