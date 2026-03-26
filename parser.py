#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import sqlite3
import requests

# Настройки
API_KEY = os.environ.get('STEAM_API_KEY')  # Ключ будет браться из секретов GitHub
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

def save_mods_to_db(mods_data):
    """Сохранение модов в базу данных"""
    print(f"Сохранение {len(mods_data)} модов в БД...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Очищаем старые данные
    cursor.execute('DELETE FROM mods')
    
    # Вставляем новые данные
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
        except Exception as e:
            print(f"Ошибка сохранения мода {mod.get('title', 'unknown')}: {e}")
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
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return None
            
            if not response.text or response.text.strip() == '':
                print(f"Пустой ответ от API. Попытка {attempt + 1}/{retries}")
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return None
            
            return response.json()
            
        except Exception as e:
            print(f"Ошибка запроса на попытке {attempt + 1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(2)
                continue
            return None
    return None

def fetch_all_mods():
    """Получение всех модов из Steam API"""
    if not API_KEY:
        print("ОШИБКА: API ключ не найден! Убедитесь, что переменная STEAM_API_KEY задана.")
        return

    all_fetched_mods = []
    cursor = '*'
    total = 0
    loaded = 0
    url = "https://api.steampowered.com/IPublishedFileService/QueryFiles/v1/"
    
    params = {
        'key': API_KEY,
        'query_type': 0,
        'cursor': cursor,
        'numperpage': 100,
        'appid': APP_ID,
        'return_short_description': 'true'
    }
    
    print("Отправка первого запроса к Steam API...")
    data = make_request_with_retry(url, params)
    
    if not data or 'response' not in data:
        print("Критическая ошибка: Не удалось получить ответ от Steam API.")
        return
        
    response_data = data['response']
    total = response_data.get('total', 0)
    print(f"Всего модов найдено в Steam: {total}")
    
    while cursor and cursor != '*':
        # Задержка, чтобы не спамить API (чуть больше, чем в UI, так как спешить некуда)
        time.sleep(0.5)
        
        params['cursor'] = cursor
        data = make_request_with_retry(url, params)
        
        if not data or 'response' not in data:
            print("Ошибка получения страницы, прерываем цикл...")
            break
            
        response_data = data['response']
        publishedfiledetails = response_data.get('publishedfiledetails', [])
        
        for mod in publishedfiledetails:
            try:
                processed_mod = {
                    'publishedfileid': str(mod.get('publishedfileid', '')),
                    'title': str(mod.get('title', 'Unknown')),
                    'subscriptions': int(mod.get('subscriptions', 0)) if mod.get('subscriptions') else 0,
                    'file_size': int(mod.get('file_size', 0)) if mod.get('file_size') else 0,
                    'time_created': int(mod.get('time_created', 0)) if mod.get('time_created') else 0,
                    'time_updated': int(mod.get('time_updated', 0)) if mod.get('time_updated') else 0,
                    'banned': bool(mod.get('banned', False))
                }
                all_fetched_mods.append(processed_mod)
            except Exception as e:
                pass
                
        loaded += len(publishedfiledetails)
        print(f"Загружено: {loaded}/{total} ({(loaded/total)*100:.1f}%)")
        
        next_cursor = response_data.get('next_cursor')
        if not next_cursor or next_cursor == cursor:
            break
            
        cursor = next_cursor

    if all_fetched_mods:
        save_mods_to_db(all_fetched_mods)
        print(f"Парсинг успешно завершен. База данных {DB_PATH} обновлена.")
    else:
        print("Не удалось загрузить ни одного мода.")

if __name__ == "__main__":
    init_database()
    fetch_all_mods()