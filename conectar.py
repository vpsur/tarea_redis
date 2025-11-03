import redis

# Conexión a Redis
conexionRedis = redis.ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True)
baseDatos = redis.Redis(connection_pool=conexionRedis)