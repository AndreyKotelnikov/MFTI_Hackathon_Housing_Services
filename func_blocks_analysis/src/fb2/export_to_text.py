#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Обновленный скрипт для экспорта категоризированных комбинаций в текстовый формат.

Поддерживает типы действий:
- regular: обычные действия (без отступа)
- cancel_actions: действия отмены (с отступом и меткой)
- exit_actions: выход из процесса (с отступом и меткой)
- success_review: просмотр/ознакомление (с отступом и меткой)
- success_actions: целевые действия (с отступом и меткой)
"""

import json
from pathlib import Path
from typing import Dict, List


def format_block_to_text(block: Dict) -> str:
    """
    Форматирование блока в текстовый формат с типизацией действий.
    
    Args:
        block: Словарь с данными блока
        
    Returns:
        Отформатированная строка блока
    """
    lines = []
    
    # Заголовок блока
    header = f"{block['name']} ({block['total_count']:,}):"
    lines.append(header)
    
    # Собираем все типизированные действия из всех групп
    all_cancel_actions = []
    all_exit_actions = []
    all_success_review = []
    all_success_actions = []
    
    # Обрабатываем каждую группу
    for group in block['groups']:
        screen = group['screen']
        functional = group['functional']
        
        # 1. Сначала выводим regular_actions
        for action_data in group['regular_actions']:
            action = action_data['action']
            count = action_data['count']
            
            # Проверяем, нужно ли сворачивать (>5 действий в одной категории и это выбор темы)
            if len(group['regular_actions']) > 5 and 'Выбор актуальной темы' in functional:
                # Сворачиваем
                total = sum(a['count'] for a in group['regular_actions'])
                line = f"    {screen} => {functional} => [{len(group['regular_actions'])} действий] ({total:,})"
                lines.append(line)
                break
            elif len(group['regular_actions']) > 5 and 'Выбор темы через теги' in functional:
                # Сворачиваем
                total = sum(a['count'] for a in group['regular_actions'])
                line = f"    {screen} => {functional} => [{len(group['regular_actions'])} действий] ({total:,})"
                lines.append(line)
                break
            else:
                line = f"    {screen} => {functional} => {action} ({count:,})"
                lines.append(line)
        
        # 2. Собираем типизированные действия
        for action_data in group.get('cancel_actions', []):
            all_cancel_actions.append((screen, functional, action_data['action'], action_data['count']))
        
        for action_data in group.get('exit_actions', []):
            all_exit_actions.append((screen, functional, action_data['action'], action_data['count']))
        
        for action_data in group.get('success_review', []):
            all_success_review.append((screen, functional, action_data['action'], action_data['count']))
        
        for action_data in group.get('success_actions', []):
            all_success_actions.append((screen, functional, action_data['action'], action_data['count']))
    
    # 3. Выводим типизированные действия группами
    if all_cancel_actions:
        lines.append("    cancel_actions:")
        for screen, functional, action, count in all_cancel_actions:
            lines.append(f"        {screen} => {functional} => {action} ({count:,})")
    
    if all_exit_actions:
        lines.append("    exit_actions:")
        for screen, functional, action, count in all_exit_actions:
            lines.append(f"        {screen} => {functional} => {action} ({count:,})")
    
    if all_success_review:
        lines.append("    success_review:")
        for screen, functional, action, count in all_success_review:
            lines.append(f"        {screen} => {functional} => {action} ({count:,})")
    
    if all_success_actions:
        lines.append("    success_actions:")
        for screen, functional, action, count in all_success_actions:
            lines.append(f"        {screen} => {functional} => {action} ({count:,})")
    
    return '\n'.join(lines)


def export_to_text(input_file: str, output_file: str):
    """
    Экспорт категоризированных комбинаций в текстовый файл.
    
    Args:
        input_file: Путь к JSON файлу с категоризацией
        output_file: Путь к выходному текстовому файлу
    """
    print("="*80)
    print("ЭКСПОРТ КАТЕГОРИЗИРОВАННЫХ КОМБИНАЦИЙ В ТЕКСТОВЫЙ ФОРМАТ")
    print("="*80)
    
    # Загрузка данных
    print(f"\nЗагрузка данных из {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"✓ Загружено {len(data['blocks'])} блоков")
    
    # Подготовка выходного файла
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Формирование текста
    print("\nФорматирование данных...")
    text_lines = []
    
    # Добавляем заголовок
    text_lines.append("="*80)
    text_lines.append("КАТЕГОРИЗАЦИЯ КОМБИНАЦИЙ ПРИЛОЖЕНИЯ МКД")
    text_lines.append("="*80)
    text_lines.append(f"Всего блоков: {data['metadata']['blocks_count']}")
    text_lines.append(f"Всего комбинаций: {data['metadata']['final_combinations_count']}")
    text_lines.append(f"Всего событий: {data['metadata']['total_records']:,}")
    text_lines.append(f"Дата генерации: {data['metadata']['generated_at']}")
    text_lines.append("="*80)
    text_lines.append("")
    
    # Форматируем каждый блок
    print("\nФорматирование блоков...")
    for i, block in enumerate(data['blocks'], 1):
        print(f"  {i}/{len(data['blocks'])} - {block['name']} ({block['combinations_count']} комбинаций)")
        
        block_text = format_block_to_text(block)
        text_lines.append(block_text)
        text_lines.append("")  # Пустая строка между блоками
    
    # Добавляем футер
    text_lines.append("="*80)
    text_lines.append("КОНЕЦ ДОКУМЕНТА")
    text_lines.append("="*80)
    
    # Сохранение в файл
    print(f"\nСохранение в {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(text_lines))
    
    print(f"✓ Файл успешно сохранён")
    
    # Статистика
    print("\n" + "="*80)
    print("СТАТИСТИКА ЭКСПОРТА:")
    print("="*80)
    print(f"Всего строк в файле: {len(text_lines):,}")
    print(f"Размер файла: {output_path.stat().st_size:,} байт")
    print(f"Блоков: {len(data['blocks'])}")
    print(f"Комбинаций: {data['metadata']['final_combinations_count']}")
    print("="*80)


def main():
    """Главная функция."""
    input_file = 'categorized_combinations.json'
    output_file = 'categorized_combinations.txt'
    
    export_to_text(input_file, output_file)
    
    print("\n" + "="*80)
    print("✓ ЭКСПОРТ ЗАВЕРШЁН УСПЕШНО")
    print("="*80)


if __name__ == "__main__":
    main()
