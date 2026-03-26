#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import sqlite3
import requests
import datetime

# Настройки
API_KEY = os.environ.get('STEAM_API_KEY')
DB_PATH = "mods_cache.db"
APP_ID = 221100  # DayZ
MAX_RETRIES = 3

def init_database():
    """Инициализация базы данных для кэширования модов"""
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

def get_last_update_time():
    """Получает Unix-время самого свежего обновления из нашей БД"""
    try:
        if not os.path.exists(DB_PATH):
            return 0
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(time_updated) FROM mods")
        result = cursor.fetchone()
        conn.close()
        return result[0] if result and result[0] else 0
    except Exception as e:
        print(f"Ошибка чтения локальной БД: {e}")
        return 0

def save_mods_to_db(mods_data):
    """Сохранение новых и обновленных модов (БЕЗ удаления старых)"""
    if not mods_data:
        return
        
    print(f"Сохранение/обновление {len(mods_data)} модов в БД...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
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
    
    conn.commit()
    conn.close()
    print("Сохранение успешно завершено.")

def make_request_with_retry(url, params, retries=MAX_RETRIES):
    """Выполнение запроса с повторными попытками при ошибках"""
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                print(f"Ошибка HTTP: {response.status_code}. Попытка {attempt + 1}/{retries}")
                time.sleep(2)
                continue
            
            if not response.text or response.text.strip() == '':
                time.sleep(2)
                continue
            
            return response.json()
            
        except Exception as e:
            print(f"Ошибка запроса: {e}")
            time.sleep(2)
            continue
    return None

def fetch_all_mods():
    """Получение модов из Steam API (Инкрементально + Полная чистка раз в неделю)"""
    if not API_KEY:
        print("ОШИБКА: API ключ не найден!")
        return

    # Берем текущее время по UTC (как на серверах GitHub)
    now = datetime.datetime.utcnow()
    
    # Полное обновление ТОЛЬКО в воскресенье (6) И ТОЛЬКО в первый час ночи (0)
    is_full_update = (now.weekday() == 6 and now.hour == 0)

    if is_full_update:
        print("Воскресенье, 00:00 UTC! Выполняем ПОЛНУЮ чистку базы для удаления 'мертвых' модов...")
        local_max_time = 0
        target_time = 0
        # Жестко очищаем старую базу, чтобы удалить удаленные из Steam моды
        conn = sqlite3.connect(DB_PATH)
        conn.cursor().execute('DELETE FROM mods')
        conn.commit()
        conn.close()
    else:
        local_max_time = get_last_update_time()
        # Буфер 3 дня (259200 секунд) на случай кэширования на серверах Steam
        target_time = local_max_time - 259200 if local_max_time > 0 else 0
        
        if local_max_time > 0:
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
            'query_type': 19, # 19 = Сортировка по дате обновления
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
                
                if time_updated >= target_time:
                    all_older_than_target = False
                
                processed_mod = {
                    'publishedfileid': str(mod.get('publishedfileid', '')),
                    'title': str(mod.get('title', 'Unknown')),
                    'subscriptions': int(mod.get('subscriptions', 0)) if mod.get('subscriptions') else 0,
                    'file_size': int(mod.get('file_size', 0)) if mod.get('file_size') else 0,
                    'time_created': int(mod.get('time_created', 0)) if mod.get('time_created') else 0,
                    'time_updated': time_updated,
                    'banned': bool(mod.get('banned', False))
                }
                all_fetched_mods.append(processed_mod)
            except Exception:
                pass
                
        loaded += len(publishedfiledetails)
        print(f"Скачано за сессию: {loaded}")
        
        # Если это не полная чистка и мы дошли до старых модов — обрываем загрузку
        if not is_full_update and local_max_time > 0 and all_older_than_target:
            print("Дошли до старых модов! Инкрементальное обновление завершено, прерываем цикл.")
            break
        
        next_cursor = response_data.get('next_cursor')
        if not next_cursor or next_cursor == cursor:
            break
            
        cursor = next_cursor
        time.sleep(0.1) # Короткая пауза, так как при инкрементальном обновлении мы качаем быстро

    if all_fetched_mods:
        save_mods_to_db(all_fetched_mods)
        print(f"Парсинг успешно завершен. База данных {DB_PATH} обновлена.")
    else:
        print("Не удалось загрузить ни одного нового мода (или произошла ошибка).")

if __name__ == "__main__":
    init_database()
    fetch_all_mods()