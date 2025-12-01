#21 - Obtén datos de alguna de las bases de datos de la tarea anterior  mediante una sql e incluyelos en Redis(1 punto)
import mysql.connector
from conectar import baseDatos

conexion = mysql.connector.connect(
        host="localhost",
        user="root",
        password="admin",
        database="empleosDBmariaDB"
    )

cursor = conexion.cursor(dictionary=True)
cursor.execute("SELECT ID, DNI, Nombre, Apellido, Edad, Sexo, Nacionalidad, Email FROM Usuarios")
usuarios = cursor.fetchall()

for u in usuarios:
        clave = f"usuarioSQL:{u['ID']}"
        
        # Guardar datos del usuario como hash
        baseDatos.hset(clave, mapping={
            "dni": u["DNI"],
            "nombre": u["Nombre"],
            "apellido": u["Apellido"],
            "edad": u["Edad"],
            "sexo": u["Sexo"],
            "nacionalidad": u["Nacionalidad"],
            "email": u["Email"]
        })

        # ---------- Creación de índices ----------
        
        # Índice por sexo
        baseDatos.sadd(f"idx_usuario_sexo:{u['Sexo']}", clave)

        # Índice por nacionalidad
        baseDatos.sadd(f"idx_usuario_nacionalidad:{u['Nacionalidad']}", clave)

cursor.close()
conexion.close()

print("✔ Usuarios importados correctamente desde MariaDB a Redis.")