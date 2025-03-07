import sys
import json
import openpyxl
import redis
import psycopg2
from redis.cluster import RedisCluster, ClusterNodae

REDIS_TIMEOUT = 1

def get_db_connection():
    """Создает подключение к базе данных."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

def read_excel(filename):
    """Читает unique_key и external_id из Excel."""
    try:
        workbook = openpyxl.load_workbook(filename)
        sheet = workbook.active
        data = []
        for row in sheet.iter_rows(min_row=2, values_only=True):
            unique_key = row[0]
            external_ids = row[1]
            if unique_key and external_ids:
                external_ids_list = [eid.strip() for eid in str(external_ids).split(',')]
                data.append((int(unique_key), external_ids_list))
        return data
    except FileNotFoundError:
        print(f"Файл {filename} не найден.")
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка при чтении Excel: {e}")
        sys.exit(1)

def get_route_keys(unique_keys):
    """Получает route_key и carrier_organisation_id по unique_key."""
    query = """
    SELECT p.unique_key, r.route_key, p.carrier_organisation_id
    FROM tpp_lko.passport p
    JOIN tpp_lko.route r ON r.id = p.route_id
    WHERE p.unique_key = ANY(%s::BIGINT[]);
    """
    
    conn = get_db_connection()
    if not conn:
        return {}
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (unique_keys,))
            result = cursor.fetchall()
            # Создаем словарь с unique_key как ключом и кортежем (route_key, carrier_id) как значением
            return {row[0]: (row[1], row[2]) for row in result}
    except psycopg2.Error as e:
        print(f"Ошибка выполнения запроса: {e}")
        return {}
    finally:
        conn.close()

def get_user_input():
    """Запрашивает у пользователя только OKTMO."""
    oktmo = input("Введите ОКТМО: ").strip()
    return oktmo

def check_redis_connection():
    """Проверяет подключение к Redis."""
    try:
        rc = RedisCluster(
            startup_nodes=NODES,
            decode_responses=True,
            ssl=True,
            ssl_ca_certs=PATH_TO_CA,
            socket_timeout=REDIS_TIMEOUT
        )
        rc.ping()
        return rc
    except redis.RedisError as e:
        print(f"Ошибка подключения к Redis: {e}")
        return None
    
def store_routes_in_redis(routes_by_carrier, oktmo):
    """Сохраняет маршруты в Redis с учетом разных перевозчиков."""
    rc = check_redis_connection()
    if not rc:
        print("Подключение к Redis не удалось. Выход из программы.")
        return
    
    # Выводим данные по группам перевозчиков
    print("\nДанные для загрузки в Redis:")
    print("-" * 50)
    for carrier_id, routes in routes_by_carrier.items():
        redis_key = f"mProcessor:{oktmo}:{carrier_id}:Routes"
        print(f"\nМапа для перевозчика {carrier_id} ({redis_key}):")
        print(f"Количество маршрутов: {len(routes)}")
        for route_id, external_id in routes:
            print(f"  external_id: {external_id:20} → route_id: {route_id}")
        print("-" * 50)
    
    confirm = input("\nВы уверены, что хотите загрузить эти данные в Redis? (Y/N): ").strip().lower()
    if confirm != 'y':
        print("Операция отменена пользователем.")
        return
    
    try:
        print("\nДобавление маршрутов в Redis...")
        for carrier_id, routes in routes_by_carrier.items():
            redis_key = f"mProcessor:{oktmo}:{carrier_id}:Routes"
            print(f"Загрузка в {redis_key}...")
            for route_id, external_id in routes:
                rc.hset(redis_key, f'"{external_id}"', json.dumps({"routeId": str(route_id)}))
            print(f"Успешно загружено {len(routes)} записей для перевозчика {carrier_id}")
        print("\nВсе маршруты успешно добавлены в Redis!")
    except redis.RedisError as e:
        print(f"Ошибка Redis: {e}")
    except Exception as e:
        print(f"Непредвиденная ошибка: {e}")

def main():
    if len(sys.argv) != 2:
        print("Использование: python3 main.py файл.xlsx")
        sys.exit(1)
    
    filename = sys.argv[1]
    data = read_excel(filename)

    if not data:
        print("Файл пуст или содержит некорректные данные.")
        sys.exit(1)

    unique_keys = [item[0] for item in data]
    route_info = get_route_keys(unique_keys)

    oktmo = get_user_input()

    # Группируем маршруты по carrier_id
    routes_by_carrier = {}
    for unique_key, external_ids in data:
        if unique_key in route_info:
            route_key, carrier_id = route_info[unique_key]
            if carrier_id not in routes_by_carrier:
                routes_by_carrier[carrier_id] = []
            for external_id in external_ids:
                routes_by_carrier[carrier_id].append((route_key, external_id))

    if routes_by_carrier:
        store_routes_in_redis(routes_by_carrier, oktmo)
    else:
        print("Нет данных для загрузки в Redis.")

if __name__ == "main":
    main()