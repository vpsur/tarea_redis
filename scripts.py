from conectar import baseDatos

#Obtener y mostrar el número de claves registradas
def ver_numero_claves():
    numero_claves = baseDatos.dbsize()
    print(f"Número total de claves en la base de datos Redis: {numero_claves}")

#Obtener y mostrar un registro en base a una clave
def obtener_registro(clave):
    valor = baseDatos.hgetall(clave)
    print(f"Registro para la clave '{clave}': {valor}")

#Actualizar el valor de una clave y mostrar el nuevo valor
def actualizar_registro(clave, campo, nuevo_valor):
    baseDatos.hset(clave, campo, nuevo_valor)
    valor = baseDatos.hgetall(clave)
    print(f"Registro para la clave modificado es '{clave}': {valor}")

#Eliminar una clave-valor y mostrar la clave y el valor eliminado
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
#Obtener y mostrar todas las claves guardadas
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

#Obtener y mostrar todos los valores guardados
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

#Función principal
def main():
    print("Script ejecutado correctamente.")
    ver_numero_claves()
    obtener_registro("usuario:1")
    actualizar_registro("usuario:1", "nombre", "David")
    eliminar_clave("usuario:7")
    obtener_todo()
    obtener_valores()

#Llamada a la función principal
if __name__ == "__main__":
    main()