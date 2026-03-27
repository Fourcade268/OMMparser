#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import sqlite3
import requests
import datetime

# Настройки
API_KEY = os.environ.get('STEAM_API_KEY')
FORCE_FULL = os.environ.get('FORCE_FULL_UPDATE') == 'true'
DB_PATH = "mods_cache.db"
MARKER_FILE = "last_full_update.txt"
TIME_FILE = "update_time.txt"
APP_ID = 221100  # DayZ
MAX_RETRIES = 3

def init_database():
    print("Инициализация базы данных...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mods (
            publishedfileid TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            subscriptions INTEGER DEFAULT 0,
            file_size INTEGER DEFAULT 0,
            time_created INTEGER DEFAULT 0,
            time_updated INTEGER DEFAULT 0,
            banned INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()
    print("База данных готова.")

def check_if_full_update_needed():
    """Проверяет, нужен ли полный парсинг на этой неделе"""
    now = datetime.datetime.utcnow()
    
    # Если сегодня не воскресенье (6) - полный парсинг точно не нужен
    if now.weekday() != 6:
        return False
        
    # Получаем текущий год и номер недели (например: 2026, 13)
    current_year, current_week, _ = now.isocalendar()
    current_marker = f"{current_year}-W{current_week}"
    
    # Читаем метку из файла
    if os.path.exists(MARKER_FILE):
        with open(MARKER_FILE, "r", encoding="utf-8") as f:
            last_marker = f.read().strip()
            # Если метка совпадает, значит на этой неделе в воскресенье мы уже чистили базу
            if last_marker == current_marker:
                return False
                
    return True

def save_full_update_marker():
    """Сохраняет метку о том, что полная чистка на этой неделе завершена"""
    now = datetime.datetime.utcnow()
    current_year, current_week, _ = now.isocalendar()
    current_marker = f"{current_year}-W{current_week}"
    
    try:
        with open(MARKER_FILE, "w", encoding="utf-8") as f:
            f.write(current_marker)
        print(f"Метка полной чистки ({current_marker}) успешно сохранена.")
    except Exception as e:
        print(f"Ошибка сохранения метки: {e}")

def get_last_update_time():
    try:
        if not os.path.exists(DB_PATH): return 0
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(time_updated) FROM mods")
        result = cursor.fetchone()
        conn.close()
        return result[0] if result and result[0] else 0
    except Exception:
        return 0

def save_mods_to_db(mods_data, full_clear=False):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if full_clear:
        cursor.execute('DELETE FROM mods')
        
    if mods_data:
        for mod in mods_data:
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO mods (publishedfileid, title, subscriptions, file_size, time_created, time_updated, banned)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(mod.get('publishedfileid', '')),
                    str(mod.get('title', '')),
                    int(mod.get('subscriptions', 0)),
                    int(mod.get('file_size', 0)),
                    int(mod.get('time_created', 0)),
                    int(mod.get('time_updated', 0)),
                    1 if mod.get('banned', False) else 0
                ))
            except Exception:
                continue

    # Зачистка "Unknown" и пустых дат
    cursor.execute("DELETE FROM mods WHERE time_created = 0 OR time_updated = 0 OR title = 'Unknown' OR title = ''")
    conn.commit()
    conn.close()

def make_request_with_retry(url, params, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code != 200:
                time.sleep(2)
                continue
            if not response.text or response.text.strip() == '':
                time.sleep(2)
                continue
            return response.json()
        except Exception:
            time.sleep(2)
            continue
    return None

def fetch_all_mods():
    if not API_KEY:
        print("ОШИБКА: API ключ не найден!")
        return

    # Умная проверка через файл (для воскресенья)
    is_scheduled_full = check_if_full_update_needed()
    
    # Полное обновление = воскресенье ИЛИ нажата галочка в GitHub
    is_full_update = is_scheduled_full or FORCE_FULL

    local_max_time = 0 if is_full_update else get_last_update_time()
    target_time = local_max_time - 259200 if local_max_time > 0 else 0
    
    # 21 - Идеальная сортировка Steam по дате последнего обновления
    query_type = 21 if not is_full_update else 0

    if is_full_update:
        print("Воскресенье! Выполняем первую за неделю ПОЛНУЮ чистку базы...")
    elif local_max_time > 0:
        print(f"Инкрементальное обновление. Качаем измененные после {target_time}...")
    else:
        print("База пуста. Выполняем полную загрузку всех модов...")

    all_fetched_mods = []
    cursor = '*'
    total = 0
    loaded = 0
    url = "https://api.steampowered.com/IPublishedFileService/QueryFiles/v1/"
    
    while cursor:
        params = {
            'key': API_KEY,
            'query_type': query_type,
            'cursor': cursor,
            'numperpage': 100,
            'appid': APP_ID,
            'return_short_description': 'true'
        }
        
        data = make_request_with_retry(url, params)
        if not data or 'response' not in data:
            print("Ошибка получения данных, прерываем загрузку...")
            break
            
        response_data = data['response']
        
        if cursor == '*':
            total = response_data.get('total', 0)
            print(f"Всего модов в Steam: {total}")
            
        publishedfiledetails = response_data.get('publishedfiledetails', [])
        all_older_than_target = True 
        
        for mod in publishedfiledetails:
            try:
                time_updated = int(mod.get('time_updated', 0))
                time_created = int(mod.get('time_created', 0))
                title = str(mod.get('title', '')).strip()
                
                # Пропускаем удаленные/скрытые моды
                if time_created == 0 or time_updated == 0 or not title or title == 'Unknown':
                    continue

                if time_updated >= target_time:
                    all_older_than_target = False
                
                processed_mod = {
                    'publishedfileid': str(mod.get('publishedfileid', '')),
                    'title': title,
                    'subscriptions': int(mod.get('subscriptions', 0)) if mod.get('subscriptions') else 0,
                    'file_size': int(mod.get('file_size', 0)) if mod.get('file_size') else 0,
                    'time_created': time_created,
                    'time_updated': time_updated,
                    'banned': bool(mod.get('banned', False))
                }
                
                if is_full_update or local_max_time == 0 or time_updated >= target_time:
                    all_fetched_mods.append(processed_mod)
                    
            except Exception:
                pass
                
        loaded += len(publishedfiledetails)
        
        if not is_full_update and local_max_time > 0 and all_older_than_target:
            break
        
        next_cursor = response_data.get('next_cursor')
        if not next_cursor or next_cursor == cursor:
            break
            
        cursor = next_cursor
        time.sleep(0.1)

    save_mods_to_db(all_fetched_mods, full_clear=is_full_update)

    # Если полная загрузка прошла успешно, ставим метку на эту неделю
    if is_full_update and all_fetched_mods:
        save_full_update_marker()

    # Создаем текстовый файл со временем обновления для интерфейса
    try:
        with open(TIME_FILE, "w", encoding="utf-8") as f:
            f.write(str(int(time.time())))
        print(f"Файл {TIME_FILE} успешно обновлен.")
    except Exception as e:
        print(f"Ошибка создания {TIME_FILE}: {e}")

    if all_fetched_mods:
        if is_full_update:
            print(f"✅ Полная чистка и парсинг завершены! В базу загружено {len(all_fetched_mods)} модов.")
        elif local_max_time == 0:
            print(f"✅ Первичный парсинг завершен! В базу загружено {len(all_fetched_mods)} модов.")
        else:
            print(f"✅ Инкрементальное обновление завершено! Найдено и обновлено {len(all_fetched_mods)} свежих модов.")
    else:
        print("✅ Нет новых обновлений. База данных актуальна.")

if __name__ == "__main__":
    init_database()
    fetch_all_mods()