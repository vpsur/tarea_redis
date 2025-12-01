import mysql.connector
from conectar import baseDatos

conexion = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password="admin",
        database="empleosDBmariaDB"
    )
cursor = conexion.cursor()


claves_ofertas = baseDatos.keys("oferta:*")

for clave in claves_ofertas:
        oferta = baseDatos.hgetall(clave)

        
        oferta_str = {k.decode() if isinstance(k, bytes) else k:
                      v.decode() if isinstance(v, bytes) else v
                      for k, v in oferta.items()}

        # Preparar valores con fallback
        id_portal = 1  #Es un id de ejemplo
        titulo = oferta_str.get("titulo", "")
        empresa = oferta_str.get("empresa", "")
        ubicacion = oferta_str.get("ubicacion", "")
        descripcion = oferta_str.get("descripcion", "")
        try:
            salario = float(oferta_str.get("salario", 0))
        except ValueError:
            salario = 0.0
        url_oferta = oferta_str.get("url_oferta", "")
        sector = oferta_str.get("sector", "")
        tipo_contrato = oferta_str.get("tipo_contrato", "")
        duracion = oferta_str.get("duracion", "")
        jornada = oferta_str.get("jornada", "")

        valores = (
            id_portal, titulo, empresa, ubicacion, descripcion, salario,
            url_oferta, sector, tipo_contrato, duracion, jornada
        )

        try:
            cursor.execute("""
                INSERT INTO Oferta_Empleo
                (ID_Portal, Titulo, Empresa, Ubicacion, Descripcion, Salario, URL_Oferta,
                 Sector, Tipo_Contrato, Duracion, Jornada)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, valores)
        except Exception as e:
            print(f"⚠ No se pudo insertar la oferta {clave}: {e}")
            continue

conexion.commit()
cursor.close()
conexion.close()

print(f"✔ Se han importado {len(claves_ofertas)} ofertas desde Redis a MariaDB.")