#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для анализа маршрутов пользователей до целевых действий.

Для каждого действия в success_review и success_actions анализирует TOP-5 путей
от начала сессии до этого действия.
"""

import json
import pandas as pd
from collections import defaultdict, Counter
from typing import Dict, List, Tuple
from pathlib import Path


class PathAnalyzer:
    """Анализатор путей пользователей до целевых действий."""
    
    def __init__(self, dataset_path: str, categorization_path: str):
        """
        Инициализация анализатора.
        
        Args:
            dataset_path: Путь к CSV файлу с данными
            categorization_path: Путь к JSON с категоризацией
        """
        self.dataset_path = dataset_path
        self.categorization_path = categorization_path
        self.df = None
        self.categorization = None
        
    def load_data(self):
        """Загрузка данных."""
        print("="*80)
        print("ЗАГРУЗКА ДАННЫХ")
        print("="*80)
        
        # Загрузка датасета
        print(f"\nЗагрузка датасета из {self.dataset_path}...")
        self.df = pd.read_csv(self.dataset_path)
        print(f"✓ Загружено {len(self.df):,} записей")
        
        # Проверка наличия global_session_id
        if 'global_session_id' not in self.df.columns:
            raise ValueError("Колонка 'global_session_id' не найдена в датасете")
        
        # Загрузка категоризации
        print(f"\nЗагрузка категоризации из {self.categorization_path}...")
        with open(self.categorization_path, 'r', encoding='utf-8') as f:
            self.categorization = json.load(f)
        print(f"✓ Загружено {len(self.categorization['blocks'])} блоков")
        
        print("\n" + "="*80)
    
    def extract_path_to_action(self, session_df: pd.DataFrame, target_idx: int) -> List[Tuple[str, str, str]]:
        """
        Извлечение пути от начала сессии до целевого действия.
        
        Args:
            session_df: DataFrame сессии
            target_idx: Индекс целевого действия в сессии
            
        Returns:
            Список кортежей (Экран, Функционал, Действие)
        """
        path = []
        for idx in range(target_idx + 1):
            row = session_df.iloc[idx]
            screen = row['Экран']
            functional = row['Функционал']
            action = row['Действие']
            path.append((screen, functional, action))
        
        return path
    
    def compress_path(self, path: List[Tuple[str, str, str]]) -> List[str]:
        """
        Сжатие пути: если одинаковые Экран+Функционал с >3 действиями, сворачиваем.
        
        Args:
            path: Список кортежей (Экран, Функционал, Действие)
            
        Returns:
            Список строк в формате "Экран => Функционал => Действие" или 
            "Экран => Функционал => [N действий]"
        """
        if not path:
            return []
        
        # Группируем по (Экран, Функционал)
        groups = []
        current_group = {
            'screen': path[0][0],
            'functional': path[0][1],
            'actions': [path[0][2]]
        }
        
        for screen, functional, action in path[1:]:
            if screen == current_group['screen'] and functional == current_group['functional']:
                # Продолжаем текущую группу
                current_group['actions'].append(action)
            else:
                # Сохраняем текущую группу и начинаем новую
                groups.append(current_group)
                current_group = {
                    'screen': screen,
                    'functional': functional,
                    'actions': [action]
                }
        
        # Добавляем последнюю группу
        groups.append(current_group)
        
        # Форматируем группы
        compressed = []
        for group in groups:
            screen = group['screen']
            functional = group['functional']
            actions = group['actions']
            
            # Подсчитываем уникальные действия
            unique_actions = list(dict.fromkeys(actions))  # Сохраняет порядок
            
            if len(unique_actions) > 3:
                # Сворачиваем
                step = f"{screen} => {functional} => [{len(unique_actions)} действий]"
            else:
                # Выводим последнее действие из группы
                step = f"{screen} => {functional} => {actions[-1]}"
            
            compressed.append(step)
        
        return compressed
    
    def find_paths_to_action(self, target_screen: str, target_functional: str, 
                            target_action: str) -> Counter:
        """
        Поиск всех путей до целевого действия.
        
        Args:
            target_screen: Экран целевого действия
            target_functional: Функционал целевого действия
            target_action: Действие целевого действия
            
        Returns:
            Counter с частотами путей
        """
        paths = Counter()
        
        # Группируем по сессиям
        grouped = self.df.groupby('global_session_id')
        
        for session_id, session_df in grouped:
            # Сортируем по времени
            session_df = session_df.sort_values('Дата и время события')
            
            # Ищем целевое действие в сессии
            for idx, row in session_df.iterrows():
                if (row['Экран'] == target_screen and 
                    row['Функционал'] == target_functional and 
                    row['Действие'] == target_action):
                    
                    # Извлекаем путь
                    local_idx = session_df.index.get_loc(idx)
                    path = self.extract_path_to_action(session_df, local_idx)
                    
                    # Сжимаем путь
                    compressed = self.compress_path(path)
                    
                    # Преобразуем в строку
                    path_str = '\n        '.join(compressed)
                    
                    # Добавляем в счётчик
                    paths[path_str] += 1
        
        return paths
    
    def analyze_block(self, block: Dict) -> Dict:
        """
        Анализ путей для всех целевых действий в блоке.
        
        Args:
            block: Словарь с данными блока
            
        Returns:
            Словарь с результатами анализа
        """
        block_name = block['name']
        print(f"\n  Анализ блока: {block_name}")
        
        results = {
            'name': block_name,
            'total_count': block['total_count'],
            'target_actions': []
        }
        
        # Собираем все целевые действия
        for group in block['groups']:
            screen = group['screen']
            functional = group['functional']
            
            # success_review
            for action_data in group.get('success_review', []):
                action = action_data['action']
                count = action_data['count']
                
                print(f"    - success_review: {screen} => {functional} => {action}")
                
                # Поиск путей
                paths = self.find_paths_to_action(screen, functional, action)
                top_paths = paths.most_common(5)
                
                results['target_actions'].append({
                    'type': 'success_review',
                    'screen': screen,
                    'functional': functional,
                    'action': action,
                    'count': count,
                    'top_paths': top_paths
                })
            
            # success_actions
            for action_data in group.get('success_actions', []):
                action = action_data['action']
                count = action_data['count']
                
                print(f"    - success_actions: {screen} => {functional} => {action}")
                
                # Поиск путей
                paths = self.find_paths_to_action(screen, functional, action)
                top_paths = paths.most_common(5)
                
                results['target_actions'].append({
                    'type': 'success_actions',
                    'screen': screen,
                    'functional': functional,
                    'action': action,
                    'count': count,
                    'top_paths': top_paths
                })
        
        return results
    
    def analyze_all_blocks(self) -> List[Dict]:
        """
        Анализ путей для всех блоков.
        
        Returns:
            Список результатов по всем блокам
        """
        print("\n" + "="*80)
        print("АНАЛИЗ ПУТЕЙ ДО ЦЕЛЕВЫХ ДЕЙСТВИЙ")
        print("="*80)
        
        results = []
        
        for block in self.categorization['blocks']:
            # Проверяем, есть ли целевые действия в блоке
            has_targets = any(
                len(group.get('success_review', [])) > 0 or 
                len(group.get('success_actions', [])) > 0
                for group in block['groups']
            )
            
            if has_targets:
                block_results = self.analyze_block(block)
                if block_results['target_actions']:
                    results.append(block_results)
        
        print("\n" + "="*80)
        print(f"✓ Проанализировано блоков: {len(results)}")
        print("="*80)
        
        return results
    
    def export_to_text(self, results: List[Dict], output_path: str):
        """
        Экспорт результатов в текстовый файл.
        
        Args:
            results: Результаты анализа
            output_path: Путь к выходному файлу
        """
        print(f"\n\nЭкспорт результатов в {output_path}...")
        
        lines = []
        
        # Заголовок
        lines.append("="*80)
        lines.append("АНАЛИЗ МАРШРУТОВ ДО ЦЕЛЕВЫХ ДЕЙСТВИЙ")
        lines.append("="*80)
        lines.append(f"Всего блоков с целевыми действиями: {len(results)}")
        lines.append(f"Дата анализа: {pd.Timestamp.now().isoformat()}")
        lines.append("="*80)
        lines.append("")
        
        # Для каждого блока
        for block_result in results:
            lines.append(f"{block_result['name']} ({block_result['total_count']:,}):")
            lines.append("")
            
            # Для каждого целевого действия
            for target in block_result['target_actions']:
                action_type = target['type']
                screen = target['screen']
                functional = target['functional']
                action = target['action']
                count = target['count']
                top_paths = target['top_paths']
                
                lines.append(f"  [{action_type}] {screen} => {functional} => {action} ({count:,}):")
                
                if top_paths:
                    lines.append(f"    TOP-{len(top_paths)} маршрутов:")
                    lines.append("")
                    
                    for rank, (path_str, path_count) in enumerate(top_paths, 1):
                        percentage = (path_count / count * 100) if count > 0 else 0
                        lines.append(f"    {rank}. Частота: {path_count:,} ({percentage:.1f}%)")
                        lines.append(f"       Маршрут:")
                        lines.append(f"        {path_str}")
                        lines.append("")
                else:
                    lines.append("    Маршруты не найдены")
                    lines.append("")
            
            lines.append("="*80)
            lines.append("")
        
        # Сохранение
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        
        print(f"✓ Результаты сохранены в {output_path}")
        print(f"  Размер файла: {output_path.stat().st_size:,} байт")
    
    def run(self, output_path: str = None):
        """
        Запуск полного анализа.
        
        Args:
            output_path: Путь к выходному файлу (если None, используется по умолчанию)
        """
        # Загрузка данных
        self.load_data()
        
        # Анализ
        results = self.analyze_all_blocks()
        
        # Экспорт
        if output_path is None:
            output_path = '/home/claude/path_analysis.txt'
        
        self.export_to_text(results, output_path)
        
        print("\n" + "="*80)
        print("✓ АНАЛИЗ ЗАВЕРШЁН УСПЕШНО")
        print("="*80)


def analyze_paths(dataset_path: str, categorization_path: str, output_path: str = None):
    """
    Удобная функция для запуска анализа из другого файла.
    
    Args:
        dataset_path: Путь к CSV файлу с данными
        categorization_path: Путь к JSON с категоризацией
        output_path: Путь к выходному файлу (опционально)
    
    Example:
        >>> from path_analyzer import analyze_paths
        >>> analyze_paths(
        ...     dataset_path='data.csv',
        ...     categorization_path='categorized_combinations_v2.json',
        ...     output_path='paths.txt'
        ... )
    """
    analyzer = PathAnalyzer(dataset_path, categorization_path)
    analyzer.run(output_path)


def main():
    """Главная функция для прямого запуска скрипта."""
    # Пример использования
    dataset_path = '/mnt/user-data/uploads/events_data.csv'  # Замените на ваш путь
    categorization_path = '/home/claude/categorized_combinations_v2.json'
    output_path = '/home/claude/path_analysis.txt'
    
    analyzer = PathAnalyzer(dataset_path, categorization_path)
    analyzer.run(output_path)


if __name__ == "__main__":
    main()
