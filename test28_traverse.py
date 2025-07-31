import json
import base64

def try_decode_base64(s):
    try:
        # Попытка декодировать base64 и преобразовать в строку UTF-8
        decoded_bytes = base64.b64decode(s)
        decoded_str = decoded_bytes.decode('utf-8')
        return decoded_str
    except Exception:
        return None

def traverse_json(data, path=None):
    if path is None:
        path = []
    # Исключаем ненужные ветви
    if len(path) > 0 and path[0] in ["segments", "experimentsConfig", "location", "seo", "browser"]:
        return
    if isinstance(data, dict):
        if not data:
            print("Путь:", path, "-> пустой словарь")
        for k, v in data.items():
            # Если ключ похож на asyncData, пробуем декодировать
            if k == "asyncData" and isinstance(v, str):
                decoded = try_decode_base64(v)
                if decoded is not None:
                    print("Путь:", path + [k], "-> Декодированное asyncData:")
                    print(decoded)
                else:
                    print("Путь:", path + [k], "-> Значение:", repr(v))
            else:
                traverse_json(v, path + [k])
    elif isinstance(data, list):
        if not data:
            print("Путь:", path, "-> пустой список")
        for i, v in enumerate(data):
            traverse_json(v, path + [i])
    else:
        # Листовой элемент — вывести путь и значение
        if path[-1] not in ['skuShelfGoods-1083405-pdpPage2column-2', 'skuShelfGoods-3509957-pdpPage2column-2']:
            print("Путь:", path, "-> Значение:", repr(data))


# Пример использования:
with open('response.json', encoding='utf-8') as f:
    json_data = json.load(f)

traverse_json(json_data)