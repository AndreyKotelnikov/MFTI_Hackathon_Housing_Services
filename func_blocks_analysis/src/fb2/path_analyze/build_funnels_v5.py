#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Построение воронок с использованием реальных данных путей пользователей.
Версия 4 (исправленная): с полями path_length и min_length
"""

import json
import re
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from collections import defaultdict


def parse_path_analysis_file(filepath: str) -> Dict:
    """
    Парсит файл path_analysis.txt и извлекает информацию о путях.
    
    Returns:
        {
            "block_name": {
                "target_action_key": {
                    "action_type": "success_actions" | "success_review",
                    "screen": str,
                    "functional": str,
                    "action": str,
                    "count": int,
                    "paths": [
                        {
                            "frequency": int,
                            "percentage": float,
                            "steps": [
                                {"screen": str, "functional": str, "action": str},
                                ...
                            ]
                        },
                        ...
                    ]
                },
                ...
            },
            ...
        }
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    result = {}
    
    # Разделяем на блоки по разделителям
    blocks = re.split(r'={80,}', content)
    
    for block_text in blocks:
        if not block_text.strip():
            continue
        
        lines = block_text.strip().split('\n')
        
        # Ищем название блока (формат: "Название блока (count):")
        block_match = re.match(r'^(.+?)\s+\([\d,]+\):$', lines[0])
        if not block_match:
            continue
        
        block_name = block_match.group(1).strip()
        result[block_name] = {}
        
        # Парсим целевые действия в этом блоке
        i = 1  # Начинаем со второй строки (после названия блока)
        while i < len(lines):
            line = lines[i]
            
            # Ищем строку с целевым действием
            # Формат: "  [success_actions] Screen => Functional => Action (count):"
            target_match = re.match(
                r'\s*\[(success_actions|success_review)\]\s+(.+?)\s+=>\s+(.+?)\s+=>\s+(.+?)\s+\(([\d,]+)\):',
                line
            )
            
            if target_match:
                action_type = target_match.group(1)
                screen = target_match.group(2).strip()
                functional = target_match.group(3).strip()
                action = target_match.group(4).strip()
                count = int(target_match.group(5).replace(',', ''))
                
                target_key = f"{screen} => {functional} => {action}"
                
                result[block_name][target_key] = {
                    "action_type": action_type,
                    "screen": screen,
                    "functional": functional,
                    "action": action,
                    "count": count,
                    "paths": []
                }
                
                # Ищем маршруты (если есть)
                i += 1
                
                # Пропускаем "Маршруты не найдены" или "TOP-N маршрутов:"
                if i < len(lines) and "Маршруты не найдены" in lines[i]:
                    i += 1
                    continue
                
                if i < len(lines) and "TOP-" in lines[i]:
                    i += 1
                    
                    # Парсим маршруты
                    while i < len(lines):
                        line = lines[i]
                        
                        # Если встретили новое целевое действие, выходим
                        if re.match(r'\s*\[(success_actions|success_review)\]', line):
                            break
                        
                        # Ищем начало маршрута (формат: "    1. Частота: 22 (15.5%)")
                        route_match = re.match(r'\s+\d+\.\s+Частота:\s+([\d,]+)\s+\(([\d.]+)%\)', line)
                        if route_match:
                            frequency = int(route_match.group(1).replace(',', ''))
                            percentage = float(route_match.group(2))
                            
                            # Следующая строка должна быть "       Маршрут:"
                            i += 1
                            if i < len(lines) and "Маршрут:" in lines[i]:
                                i += 1
                                
                                # Собираем шаги маршрута
                                steps = []
                                while i < len(lines):
                                    step_line = lines[i]
                                    
                                    # Если строка начинается с пробелов и содержит "=>", это шаг
                                    if re.match(r'\s+.+?\s+=>\s+.+?\s+=>\s+.+', step_line):
                                        # Парсим шаг: "        Screen => Functional => Action"
                                        step_match = re.match(
                                            r'\s+(.+?)\s+=>\s+(.+?)\s+=>\s+(.+)',
                                            step_line
                                        )
                                        if step_match:
                                            steps.append({
                                                "screen": step_match.group(1).strip(),
                                                "functional": step_match.group(2).strip(),
                                                "action": step_match.group(3).strip()
                                            })
                                        i += 1
                                    else:
                                        # Конец маршрута
                                        break
                                
                                if steps:
                                    result[block_name][target_key]["paths"].append({
                                        "frequency": frequency,
                                        "percentage": percentage,
                                        "steps": steps
                                    })
                        else:
                            i += 1
            else:
                i += 1
    
    return result


def build_funnel_from_paths(block_name: str, target_actions: Dict, block_screens: set) -> Dict:
    """
    Строит структуру воронки для блока на основе данных о путях.
    
    Логика:
    1. Для каждого целевого действия берём его пути
    2. Сортируем пути: сначала по длине (DESC), потом по частоте (DESC)
    3. Самый длинный и частый путь → version 1 (main)
    4. Нумеруем ВСЕ шаги пути подряд (включая шаги из других блоков)
    5. В funnel_mapping добавляем ТОЛЬКО действия из block_screens
    6. Каждое действие получает step (номер в полном пути), version=1, path_length (полная)
    7. Целевое действие получает step=0, version=0, min_length (полная)
    8. Для каждого следующего пути:
       - Нумеруем все шаги подряд
       - Добавляем в маппинг только действия из block_screens
       - Для новых действий на том же step создаём новую version
       - Обновляем min_length целевого действия, если путь короче
    
    Args:
        block_name: Название блока
        target_actions: Данные о целевых действиях и путях к ним
        block_screens: Множество screens, принадлежащих блоку
    
    Returns:
        {
            (screen, functional, action): {
                "step": int,
                "version": int,
                "path_type": "main" | "parallel" | "isolated",
                "path_length": int  # только для не-целевых действий, полная длина пути
            },
            ...
        }
    """
    funnel_mapping = {}
    
    # Для каждого целевого действия строим воронку
    for target_key, target_data in target_actions.items():
        paths = target_data["paths"]
        
        if not paths:
            # Если маршрутов нет, целевое действие становится isolated с min_length=1
            target_tuple = (
                target_data["screen"],
                target_data["functional"],
                target_data["action"]
            )
            funnel_mapping[target_tuple] = {
                "step": 0,
                "version": 0,
                "path_type": "isolated",
                "min_length": 1
            }
            continue
        
        # Сортируем пути: длинные и частые сначала
        sorted_paths = sorted(
            paths,
            key=lambda p: (len(p["steps"]), p["frequency"]),
            reverse=True
        )
        
        # Хранилище версий для каждого шага
        # step_versions[step_num][(screen, functional, action)] = [version_numbers]
        step_versions = defaultdict(lambda: defaultdict(list))
        
        # Обрабатываем первый (основной) путь
        main_path = sorted_paths[0]
        full_path_length = len(main_path["steps"])  # Полная длина пути включая шаги из других блоков
        
        # НЕ фильтруем шаги - нумеруем все подряд, но в маппинг добавляем только из block_screens
        for step_num, step_data in enumerate(main_path["steps"], start=1):
            step_tuple = (
                step_data["screen"],
                step_data["functional"],
                step_data["action"]
            )
            
            # Добавляем в маппинг ТОЛЬКО если действие из текущего блока
            if step_data["screen"] not in block_screens:
                continue
            
            funnel_mapping[step_tuple] = {
                "step": step_num,
                "version": 1,
                "path_type": "main",
                "path_length": full_path_length
            }
            
            step_versions[step_num][step_tuple].append(1)
        
        # Целевое действие
        target_tuple = (
            target_data["screen"],
            target_data["functional"],
            target_data["action"]
        )
        funnel_mapping[target_tuple] = {
            "step": 0,
            "version": 0,
            "path_type": "main",
            "min_length": full_path_length
        }
        
        # Обрабатываем остальные пути
        for path in sorted_paths[1:]:
            full_path_length = len(path["steps"])
            
            # Обновляем min_length если текущий путь короче
            if full_path_length < funnel_mapping[target_tuple]["min_length"]:
                funnel_mapping[target_tuple]["min_length"] = full_path_length
            
            # НЕ фильтруем шаги - нумеруем все подряд
            for step_num, step_data in enumerate(path["steps"], start=1):
                step_tuple = (
                    step_data["screen"],
                    step_data["functional"],
                    step_data["action"]
                )
                
                # Пропускаем действия не из текущего блока
                if step_data["screen"] not in block_screens:
                    continue
                
                # Если это действие уже есть в funnel_mapping на этом шаге, пропускаем
                if step_tuple in funnel_mapping and funnel_mapping[step_tuple]["step"] == step_num:
                    continue
                
                # Если действие новое для этого шага, добавляем новую version
                # Находим максимальную version для этого шага среди всех действий
                max_version = 1
                for existing_tuple, existing_data in funnel_mapping.items():
                    if existing_data.get("step") == step_num:
                        max_version = max(max_version, existing_data["version"])
                
                new_version = max_version + 1
                
                # Если действие вообще нет в маппинге, добавляем
                if step_tuple not in funnel_mapping:
                    funnel_mapping[step_tuple] = {
                        "step": step_num,
                        "version": new_version,
                        "path_type": "parallel",
                        "path_length": full_path_length
                    }
                    step_versions[step_num][step_tuple].append(new_version)
                # Если действие есть, но с другим step (может быть на разных этапах в разных путях)
                # В этом случае НЕ перезаписываем, т.к. оно уже обработано в более приоритетном пути
    
    return funnel_mapping


def apply_funnels_from_analysis(
    input_json_path: str,
    path_analysis_path: str,
    output_json_path: str
):
    """
    Применяет воронки к categorized_combinations на основе данных путей.
    """
    print("=" * 80)
    print("ПОСТРОЕНИЕ ВОРОНОК НА ОСНОВЕ АНАЛИЗА ПУТЕЙ")
    print("=" * 80)
    print()
    
    # Загружаем данные
    print(f"Загрузка файла: {input_json_path}")
    with open(input_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"Парсинг анализа путей: {path_analysis_path}")
    path_data = parse_path_analysis_file(path_analysis_path)
    
    print(f"\nНайдено блоков с данными о путях: {len(path_data)}")
    print()
    
    # Для каждого блока строим воронку
    for block in data["blocks"]:
        block_name = block["name"]
        
        print(f"\n{block_name}")
        
        if block_name not in path_data:
            print("  Нет данных о путях - все действия isolated")
            # Все действия получают step=0, version=0, path_type='isolated'
            for group in block["groups"]:
                for action_type in ['regular_actions', 'cancel_actions', 'exit_actions',
                                   'success_review', 'success_actions']:
                    for action_item in group.get(action_type, []):
                        action_item["step"] = 0
                        action_item["version"] = 0
                        action_item["path_type"] = "isolated"
                        # min_length не добавляем для isolated
            
            block["funnel_metadata"] = {
                "has_funnels": False,
                "main_path_length": 0,
                "parallel_paths_count": 0
            }
            continue
        
        # Собираем все screens, принадлежащие блоку
        block_screens = set()
        for group in block["groups"]:
            block_screens.add(group["screen"])
        
        # Строим маппинг воронки с учётом screens блока
        funnel_mapping = build_funnel_from_paths(
            block_name, 
            path_data[block_name],
            block_screens
        )
        
        print(f"  Действий в воронке: {len(funnel_mapping)}")
        
        # Применяем к действиям во всех группах
        for group in block["groups"]:
            # Обрабатываем все типы действий
            for action_type in ['regular_actions', 'cancel_actions', 'exit_actions', 
                               'success_review', 'success_actions']:
                actions = group.get(action_type, [])
                
                for action_item in actions:
                    combo_tuple = (
                        group["screen"],
                        group["functional"],
                        action_item["action"]
                    )
                    
                    if combo_tuple in funnel_mapping:
                        funnel_data = funnel_mapping[combo_tuple]
                        action_item["step"] = funnel_data["step"]
                        action_item["version"] = funnel_data["version"]
                        action_item["path_type"] = funnel_data["path_type"]
                        
                        # Добавляем path_length или min_length в зависимости от типа
                        if action_item["step"] == 0:  # Целевое действие
                            action_item["min_length"] = funnel_data["min_length"]
                        else:
                            action_item["path_length"] = funnel_data["path_length"]
                    else:
                        # Действие не в воронке
                        action_item["step"] = 0
                        action_item["version"] = 0
                        action_item["path_type"] = "isolated"
        
        # Собираем метаданные
        main_actions_count = 0
        parallel_versions = set()
        max_step = 0
        
        for group in block["groups"]:
            for action_type in ['regular_actions', 'cancel_actions', 'exit_actions',
                               'success_review', 'success_actions']:
                for action_item in group.get(action_type, []):
                    if action_item.get("version") == 1:
                        main_actions_count += 1
                        max_step = max(max_step, action_item.get("step", 0))
                    if action_item.get("path_type") == "parallel":
                        parallel_versions.add(action_item["version"])
        
        block["funnel_metadata"] = {
            "has_funnels": len(funnel_mapping) > 0,
            "main_path_length": max_step,
            "parallel_paths_count": len(parallel_versions)
        }
        
        print(f"  Основной путь: {max_step} шагов")
        print(f"  Параллельных версий: {len(parallel_versions)}")
    
    # Обновляем глобальные метаданные
    data["metadata"]["funnels_built_at"] = datetime.now().isoformat()
    data["metadata"]["has_step_version"] = True
    data["metadata"]["funnel_builder_version"] = "v4_fixed"
    data["metadata"]["has_path_length"] = True
    data["metadata"]["has_min_length"] = True
    
    # Сохраняем
    print()
    print("=" * 80)
    print(f"Сохранение результата: {output_json_path}")
    print("=" * 80)
    
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print()
    print("ГОТОВО!")


if __name__ == "__main__":
    input_file = "../categorized_combinations.json"
    analysis_file = "path_analysis.txt"
    output_file = "categorized_combinations_with_funnels.json"
    
    apply_funnels_from_analysis(input_file, analysis_file, output_file)
