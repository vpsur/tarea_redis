from conectar import baseDatos
import json

def decode(value):
    if isinstance(value, bytes):
        return value.decode()
    return value

def decode_hash(h):
    # Convierte solo si son bytes
    return { (k.decode() if isinstance(k, bytes) else k):
             (v.decode() if isinstance(v, bytes) else v)
            for k, v in h.items() }

#2- Obtener y mostrar el número de claves registradas
def ver_numero_claves():
    numero_claves = baseDatos.dbsize()
    print(f"Número total de claves en la base de datos Redis: {numero_claves}")

#3- Obtener y mostrar un registro en base a una clave
def obtener_registro(clave):
    valor = baseDatos.hgetall(clave)
    print(f"Registro para la clave '{clave}': {valor}")

#4- Actualizar el valor de una clave y mostrar el nuevo valor
def actualizar_registro(clave, campo, nuevo_valor):
    baseDatos.hset(clave, campo, nuevo_valor)
    valor = baseDatos.hgetall(clave)
    print(f"Registro para la clave modificado es '{clave}': {valor}")

#5- Eliminar una clave-valor y mostrar la clave y el valor eliminado
def eliminar_clave(clave):
    valor = baseDatos.hgetall(clave)
    
    # Comprobar si hay valores en la clave
    if not valor:
        print(f"No existe la clave: {clave}")
        return

    # Hash de clicks asociado
    hash_clicks = f"{clave}:clicks"
    clicks = baseDatos.hgetall(hash_clicks)

    # Eliminar clave y hash de clicks
    baseDatos.delete(clave)
    baseDatos.delete(hash_clicks)  # <-- aquí va el nombre de la clave, NO el diccionario

    print(f"Clave '{clave}' eliminada → Valor eliminado: {valor}")
    if clicks:
        print(f"Clicks eliminados: {clicks}")
#6- Obtener y mostrar todas las claves guardadas
def obtener_todo():
    # Usuarios (excluyendo los clicks)
    usuarios_claves = sorted([c for c in baseDatos.keys("usuario:*") if ":clicks" not in c])

    # Clicks
    clicks_claves = sorted(baseDatos.keys("usuario:*:clicks"))

    # Ofertas
    ofertas_claves = sorted(baseDatos.keys("oferta:*"))

    # Mostrar resultados
    print("Claves de usuarios:")
    print(usuarios_claves)

    print("\nClaves de clicks:")
    print(clicks_claves)

    print("\nClaves de ofertas:")
    print(ofertas_claves)

#7- Obtener y mostrar todos los valores guardados
def obtener_valores():
     # Usuarios (excluyendo clicks)
    usuarios_claves = [c for c in baseDatos.keys("usuario:*") if ":clicks" not in c]
    print("\nValores de usuarios:")
    for clave in usuarios_claves:
        print(baseDatos.hgetall(clave))

    # Clicks
    clicks_claves = baseDatos.keys("usuario:*:clicks")
    print("\nValores de clicks:")
    for clave in clicks_claves:
        print(baseDatos.hgetall(clave))

    # Ofertas
    ofertas_claves = baseDatos.keys("oferta:*")
    print("\nValores de ofertas:")
    for clave in ofertas_claves:
        print(baseDatos.hgetall(clave))

#8 - Obtener y mostrar varios registros con una clave con un patrón en común usando
def obtener_por_patron(patron):
    print(f"Coincidencias para patrón '{patron}':")

    claves = [decode(c) for c in baseDatos.scan_iter(patron)]

    if not claves:
        print("No se encontraron claves.")
        return

    for clave in claves:
        valor = decode_hash(baseDatos.hgetall(clave))
        print(f"{clave}: {valor}")

#9 - Obtener y mostrar varios registros con una clave con un patrón en común usando
def obtener_varios_por_patron_lista(patron):
    print(f"Coincidencias para patrón '{patron}':")
    # Buscar claves que coincidan con el patrón
    claves = [c for c in baseDatos.scan_iter(patron)]
    
    for clave in claves:
        valor = baseDatos.hgetall(clave)
        print({"clave": clave, "valor": valor})


#10 - Obtener y mostrar varios registros con una clave con un patrón en común usando ? 
def obtener_varios_por_patron_interrogacion(patron):
    print(f"Coincidencias para patrón '{patron}':")
    
    # scan_iter permite usar * y ? en el patrón
    claves = [c for c in baseDatos.scan_iter(patron)]
    
    for clave in claves:
        valor = baseDatos.hgetall(clave)
        print({"clave": clave, "valor": valor})

#11 - Obtener y mostrar varios registros y filtrarlos por un valor en concreto.
def filtrar_registros_por_valor(patron_clave, campo, valor_buscado): 
    print(f"Coincidencias para patrón '{patron_clave}, campo,{campo} y valor{valor_buscado}':")
    # Obtener claves que coincidan con el patrón
    claves = [c for c in baseDatos.scan_iter(patron_clave)]
    
    for clave in claves:
        valor = baseDatos.hgetall(clave)
        # Filtrar por el valor buscado
        if valor.get(campo) == valor_buscado:
            print({"clave": clave, "valor": valor})

#12 - Actualizar una serie de registros en base a un filtro (por ejemplo aumentar su valor en 1 )
def actualizar_registros_filtro():
    # Buscar todas las claves de usuarios
    claves_usuarios = [c for c in baseDatos.scan_iter("usuario:*") if ":clicks" not in c]
    
    for clave in claves_usuarios:
        datos = baseDatos.hgetall(clave)
        if "edad" in datos:
            # Convertir a int, sumar 1, y guardar como string
            nueva_edad = str(int(datos["edad"]) + 1)
            baseDatos.hset(clave, "edad", nueva_edad)
            print(f"{clave} → edad actualizada: {nueva_edad}")

#13 - Eliminar una serie de registros en base a un filtro
def eliminar_registros_por_filtro(patron_clave):
    claves = [c for c in baseDatos.scan_iter(patron_clave)]
    
    if not claves:
        print(f"No se encontraron claves para eliminar con patrón '{patron_clave}'.")
        return
    
    for clave in claves:
        baseDatos.delete(clave)
        print(f"Eliminada la clave: {clave}")

#14 - Crear una estructura en JSON de array de los datos que vayais a almacenar
def exportar_json():
    resultado = {
        "usuarios": [],
        "ofertas": [],
        "clicks": []
    }

    #Usuarios
    for clave in baseDatos.keys("usuario:*"):
        if ":clicks" in clave:
            continue

        datos = baseDatos.hgetall(clave)
        datos["id"] = clave
        resultado["usuarios"].append(datos)

    #Ofertas
    for clave in baseDatos.keys("oferta:*"):
        datos = baseDatos.hgetall(clave)
        datos["id"] = clave
        resultado["ofertas"].append(datos)

    #Clicks
    for clave in baseDatos.keys("usuario:*:clicks"):
        usuario_id = clave.split(":")[1]
        datos = baseDatos.hgetall(clave)

        for oferta, n_clicks in datos.items():
            resultado["clicks"].append({
                "usuario": int(usuario_id),
                "oferta": int(oferta.split(":")[1]),
                "clicks": int(n_clicks)
            })

    with open("datos_exportados.json", "w", encoding="utf8") as fichero:
        json.dump(resultado, fichero, indent=4, ensure_ascii=False)

    print("✔ Archivo JSON generado correctamente.")

#15 - Realizar un filtro por cada atributo de la estructura JSON anterior
def filtrar_usuarios_por_nombre(nombre):
    with open("datos_exportados.json", "r", encoding="utf8") as f:
        data= json.load(f)
    
    print( [u for u in data["usuarios"] if u["nombre"].lower() == nombre.lower()])

#16 - Crear una lista en Redis
def crear_lista_usuarios_activos():
    baseDatos.delete("usuarios_activos")  

    usuarios = baseDatos.keys("usuario:*")

    for u in usuarios:
        if ":clicks" not in u:
            baseDatos.rpush("usuarios_activos", u)

    print("Lista 'usuarios_activos' creada:")
    print(baseDatos.lrange("usuarios_activos", 0, -1))

#17 - Obtener elementos de una lista con un filtro en concreto
def filtrar_lista_usuarios_por_edad(edad_minima):
    lista = baseDatos.lrange("usuarios_activos", 0, -1)
    

    for uid in lista:
        datos = baseDatos.hgetall(uid)
        if datos and int(datos["edad"]) >= edad_minima:
            print(({uid: datos}))

#18 - Crea datos con índices, definiendo un esquema de al menos tres campos 
def crear_formaciones_con_indices():
    # Formaciones
    formaciones = {
        "formacion:1": {"nombre": "Grado Superior en Informática", "nivel": "3", "lugar": "IES Martinez", "año": "2023"},
        "formacion:2": {"nombre": "Grado Medio en Administración", "nivel": "2", "lugar": "IES Garcia", "año": "2022"},
        "formacion:3": {"nombre": "Curso Python Avanzado", "nivel": "3", "lugar": "Academia Code", "año": "2023"},
        "formacion:4": {"nombre": "Curso Diseño UX", "nivel": "2", "lugar": "IES Martinez", "año": "2021"},
    }

    for clave, datos in formaciones.items():
        baseDatos.hset(clave, mapping=datos)

        baseDatos.sadd(f"idx_formacion_nombre:{datos['nombre']}", clave)
        baseDatos.sadd(f"idx_formacion_nivel:{datos['nivel']}", clave)
        baseDatos.sadd(f"idx_formacion_lugar:{datos['lugar']}", clave)
        baseDatos.sadd(f"idx_formacion_año:{datos['año']}", clave)

    print("Formaciones creadas con índices correctamente.")

#19 - Realiza una búsqueda con índices en base a un campo
def buscar_formacion_por_campo(campo, valor):
    indice = f"idx_formacion_{campo}:{valor}"
    claves = baseDatos.smembers(indice)  # Obtener todas las claves que coinciden con el valor
    resultados = [baseDatos.hgetall(c) for c in claves]
    print(resultados)

#20 - Realiza un group  by usando los índices
def group_by_formaciones(campo):
    resultados = {}

    # Obtener todos los índices disponibles para el campo
    patrones = baseDatos.keys(f"idx_formacion_{campo}:*")

    for indice in patrones:
        indice_str = indice if isinstance(indice, str) else indice.decode()
        valor = indice_str.split(f"idx_formacion_{campo}:")[1]  # extraer valor
        claves = baseDatos.smembers(indice_str)
        registros = [baseDatos.hgetall(c) for c in claves]
        resultados[valor] = registros

    return resultados
#Función principal
def main():
    print("Script ejecutado correctamente.")
    ver_numero_claves()
    obtener_registro("usuario:1")
    actualizar_registro("usuario:1", "nombre", "David")
    eliminar_clave("usuario:7")
    obtener_todo()
    obtener_valores()
    obtener_por_patron("usuario:*")
    obtener_varios_por_patron_lista("usuario:*:clicks")
    obtener_varios_por_patron_interrogacion("usuario:?")
    filtrar_registros_por_valor("usuario:*", "nombre", "Pedro")
    actualizar_registros_filtro()
    #eliminar_registros_por_filtro("usuario:*")
    exportar_json()
    filtrar_usuarios_por_nombre("Pedro")
    crear_lista_usuarios_activos()
    filtrar_lista_usuarios_por_edad(37)
    crear_formaciones_con_indices()
    buscar_formacion_por_campo("nombre", "Curso Python Avanzado")
    grupos_nivel = group_by_formaciones("nivel")
    print("Agrupadas por nivel:")
    for nivel, formaciones in grupos_nivel.items():
        print(f"\nNivel {nivel}:")
        for f in formaciones:
            print(f)

#Llamada a la función principal
if __name__ == "__main__":
    main()