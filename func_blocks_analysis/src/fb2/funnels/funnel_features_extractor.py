"""
Оптимизированный скрипт для добавления фич по функциональным блокам в датасет
Версия 2.0 - с агрегацией по сессиям и расширенными метриками
"""

import json
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from collections import defaultdict
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


class FunnelFeaturesExtractor:
    """
    Класс для извлечения фич по функциональным блокам и агрегации данных по сессиям
    
    Создает 136 колонок (17 блоков × 8 метрик) + 1 колонка sess_dur_sec
    Оставляет только первую строку каждой сессии с агрегированными метриками
    """
    
    def __init__(self, json_path: str):
        """
        Инициализация с путем к JSON файлу с функциональными блоками
        
        Args:
            json_path: Путь к файлу categorized_combinations_with_funnels.json
        """
        self.json_path = json_path
        self.blocks_data = self._load_blocks()
        self.block_prefixes = self._generate_prefixes()
        
    def _load_blocks(self) -> List[Dict]:
        """Загрузка данных о функциональных блоках из JSON"""
        with open(self.json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data['blocks']
    
    def _generate_prefixes(self) -> Dict[str, str]:
        """
        Генерация коротких и понятных префиксов для блоков
        
        Returns:
            Словарь {название_блока: префикс}
        """
        prefixes_map = {
            'Создание заявки': 'request',
            'Просмотр и управление заявками': 'req_manage',
            'Профиль': 'profile',
            'Навигация': 'nav',
            'Уведомления': 'notif',
            'Опросы и собрания собственников': 'poll_oss',
            'Баллы и поощрения': 'rewards',
            'Мой дом': 'my_home',
            'Услуги партнеров': 'partners',
            'Управление транспортом': 'transport',
            'Просмотр объявлений': 'ann_view',
            'Умные решения': 'smart',
            'Техподдержка': 'support',
            'Гостевой доступ': 'guest',
            'Городские сервисы': 'city_serv',
            'Создание адреса': 'address',
            'Создание объявления': 'ann_create'
        }
        
        result = {}
        for block in self.blocks_data:
            block_name = block['name']
            if block_name in prefixes_map:
                result[block_name] = prefixes_map[block_name]
            else:
                result[block_name] = f'block_{len(result)}'
        
        return result
    
    def _build_action_lookup(self) -> Tuple[Dict, Dict, Dict]:
        """
        Создание справочников для быстрого поиска (оптимизация)
        
        Returns:
            Tuple из трех словарей:
            - action_to_block: {(экран, функционал, действие): (блок, step)}
            - success_actions: {(экран, функционал, действие): блок}
            - review_actions: {(экран, функционал, действие): блок}
        """
        action_to_block = {}
        success_actions = {}
        review_actions = {}
        
        for block in self.blocks_data:
            block_name = block['name']
            
            for group in block['groups']:
                screen = group['screen']
                functional = group['functional']
                
                # Все типы действий
                for action_type in ['regular_actions', 'cancel_actions', 
                                   'exit_actions', 'success_actions']:
                    for action_obj in group.get(action_type, []):
                        key = (screen, functional, action_obj['action'])
                        step = action_obj.get('step', 0)
                        action_to_block[key] = (block_name, step)
                
                # Success actions
                for action_obj in group.get('success_actions', []):
                    key = (screen, functional, action_obj['action'])
                    success_actions[key] = block_name
                
                # Review actions
                for action_obj in group.get('success_review', []):
                    key = (screen, functional, action_obj['action'])
                    review_actions[key] = block_name
        
        return action_to_block, success_actions, review_actions
    
    def _create_action_block_mapping(self, df: pd.DataFrame, 
                                    action_to_block: Dict) -> pd.Series:
        """
        ОПТИМИЗАЦИЯ: Создание mapping для всех строк датасета за один проход
        
        Args:
            df: DataFrame с событиями
            action_to_block: Справочник действий
            
        Returns:
            Series с названиями блоков для каждой строки
        """
        # Создаем tuple для каждой строки
        action_keys = list(zip(df['Экран'], df['Функционал'], df['Действие']))
        
        # Маппим на блоки
        block_mapping = [action_to_block.get(key, (None, 0))[0] for key in action_keys]
        step_mapping = [action_to_block.get(key, (None, 0))[1] for key in action_keys]
        
        return pd.Series(block_mapping, index=df.index), pd.Series(step_mapping, index=df.index)
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        ОПТИМИЗИРОВАННЫЙ метод для добавления фич и агрегации по сессиям
        
        Args:
            df: Исходный датасет с событиями
            
        Returns:
            Датасет с одной строкой на сессию и агрегированными метриками
        """
        start_time = datetime.now()
        
        logger.info("=" * 80)
        logger.info("FUNNEL FEATURES EXTRACTOR v2.0 - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ")
        logger.info("=" * 80)
        logger.info(f"Исходный датасет: {len(df):,} строк")
        logger.info(f"Уникальных сессий: {df['global_session_id'].nunique():,}")
        
        # Проверка необходимых колонок
        required_cols = ['global_session_id', 'Экран', 'Функционал', 'Действие', 
                        'Дата и время события', 'duration_seconds', 'click_count',
                        'dbl_duration_seconds', 'dbl_count']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            raise ValueError(f"Отсутствуют необходимые колонки: {missing_cols}")
        
        # ОПТИМИЗАЦИЯ 1: Создаем справочники один раз
        logger.info("\n1. Построение справочников...")
        action_to_block, success_actions, review_actions = self._build_action_lookup()
        logger.info(f"   Создано справочников: {len(action_to_block)} действий")
        
        # ОПТИМИЗАЦИЯ 2: Маппинг для всех строк за один проход
        logger.info("\n2. Создание mapping для всех строк...")
        df['_block_name'], df['_step'] = self._create_action_block_mapping(df, action_to_block)
        
        # Создаем маркеры для success и review
        action_keys = list(zip(df['Экран'], df['Функционал'], df['Действие']))
        df['_is_success'] = [key in success_actions for key in action_keys]
        df['_is_review'] = [key in review_actions for key in action_keys]
        
        logger.info(f"   Mapping создан для {len(df):,} строк")
        
        # ОПТИМИЗАЦИЯ 3: Групповая агрегация по сессиям и блокам
        logger.info("\n3. Агрегация метрик по сессиям и блокам...")
        
        # Фильтруем только строки с определенным блоком
        df_with_blocks = df[df['_block_name'].notna()].copy()
        
        # Агрегируем по сессии и блоку
        block_features = df_with_blocks.groupby(['global_session_id', '_block_name']).agg({
            '_block_name': 'count',  # count
            '_step': 'max',          # max_step
            '_is_success': 'sum',    # success_count
            '_is_review': 'sum',     # review_count
            'duration_seconds': 'sum',     # dur_sec
            'click_count': 'sum',          # click_count
            'dbl_duration_seconds': 'sum', # dbl_dur_sec
            'dbl_count': 'sum'             # dbl_count
        }).rename(columns={
            '_block_name': 'count',
            '_step': 'max_step',
            '_is_success': 'success_count',
            '_is_review': 'review_count',
            'duration_seconds': 'dur_sec',
            'dbl_duration_seconds': 'dbl_dur_sec'
        })
        
        logger.info(f"   Агрегировано {len(block_features):,} комбинаций сессия-блок")
        
        # ОПТИМИЗАЦИЯ 4: Pivot для преобразования в широкий формат
        logger.info("\n4. Преобразование в широкий формат...")
        
        # Создаем отдельные pivot для каждой метрики
        result_dfs = []
        
        for metric in ['count', 'max_step', 'success_count', 'review_count', 
                      'dur_sec', 'click_count', 'dbl_dur_sec', 'dbl_count']:
            
            pivot = block_features.reset_index().pivot(
                index='global_session_id',
                columns='_block_name',
                values=metric
            )
            
            # Переименовываем колонки с префиксами
            pivot.columns = [f'{self.block_prefixes.get(col, col)}_{metric}' 
                           for col in pivot.columns]
            
            result_dfs.append(pivot)
        
        # Объединяем все метрики
        features_df = pd.concat(result_dfs, axis=1)
        
        # Заполняем пропуски
        for col in features_df.columns:
            if col.endswith('_max_step'):
                features_df[col] = features_df[col].fillna(-1).astype('int32')
            else:
                features_df[col] = features_df[col].fillna(0).astype('int32')
        
        logger.info(f"   Создано {len(features_df.columns)} колонок funnel features")
        
        # ОПТИМИЗАЦИЯ 5: Расчет sess_dur_sec и выбор первой строки
        logger.info("\n5. Расчет длительности сессии и агрегация...")
        
        # Преобразуем дату если нужно
        if not pd.api.types.is_datetime64_any_dtype(df['Дата и время события']):
            df['Дата и время события'] = pd.to_datetime(df['Дата и время события'])
        
        # Сортируем по времени внутри каждой сессии
        df_sorted = df.sort_values(['global_session_id', 'Дата и время события'])
        
        # Список колонок для сохранения из первой строки
        cols_to_keep = [
            'Дата и время события',
            'Идентификатор устройства',
            'Номер сессии в рамках устройства',
            'Производитель устройства',
            'Модель устройства',
            'Тип устройства',
            'ОС',
            'age_back',
            'gender',
            'age_group',
            'global_session_id'
        ]
        
        # Проверяем какие колонки есть в датасете
        available_cols = [col for col in cols_to_keep if col in df_sorted.columns]
        
        # Агрегируем сессии
        agg_dict = {col: 'first' for col in available_cols if col != 'global_session_id'}
        agg_dict['duration_seconds'] = 'sum'  # Сумма duration_seconds = sess_dur_sec
        
        session_agg = df_sorted.groupby('global_session_id').agg(agg_dict)
        
        # Переименовываем duration_seconds в sess_dur_sec
        session_agg['sess_dur_sec'] = session_agg['duration_seconds'].fillna(0).astype('int32')
        session_agg = session_agg.drop(columns=['duration_seconds'])
        
        logger.info(f"   Рассчитана длительность для {len(session_agg):,} сессий")
        
        # ОПТИМИЗАЦИЯ 6: Объединяем с funnel features
        logger.info("\n6. Объединение всех метрик...")
        
        result_df = session_agg.join(features_df, on='global_session_id', how='left')
        
        # Заполняем пропуски для блоков, с которыми не было взаимодействия
        for prefix in self.block_prefixes.values():
            for metric in ['count', 'success_count', 'review_count', 
                          'dur_sec', 'click_count', 'dbl_dur_sec', 'dbl_count']:
                col = f'{prefix}_{metric}'
                if col not in result_df.columns:
                    result_df[col] = 0
                else:
                    result_df[col] = result_df[col].fillna(0).astype('int32')
            
            # max_step специальная обработка
            col = f'{prefix}_max_step'
            if col not in result_df.columns:
                result_df[col] = -1
            else:
                result_df[col] = result_df[col].fillna(-1).astype('int32')
        
        # Сбрасываем индекс
        result_df = result_df.reset_index(drop=False)
        
        # Удаляем временные колонки
        temp_cols = [col for col in result_df.columns if col.startswith('_')]
        result_df = result_df.drop(columns=temp_cols, errors='ignore')
        
        # Финальная статистика
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("\n" + "=" * 80)
        logger.info("РЕЗУЛЬТАТ ОБРАБОТКИ:")
        logger.info("=" * 80)
        logger.info(f"Исходные строки:        {len(df):,}")
        logger.info(f"Итоговые строки:        {len(result_df):,} (одна на сессию)")
        logger.info(f"Сокращение:             {len(df) - len(result_df):,} строк ({100*(1-len(result_df)/len(df)):.1f}%)")
        logger.info(f"Исходные колонки:       {len(df.columns)}")
        logger.info(f"Новые колонки:          {len(result_df.columns) - len(df.columns) + len(temp_cols)}")
        logger.info(f"Итоговые колонки:       {len(result_df.columns)}")
        logger.info(f"")
        logger.info(f"Создано funnel features: {len(self.block_prefixes) * 8} колонок")
        logger.info(f"  - {len(self.block_prefixes)} блоков × 8 метрик")
        logger.info(f"  - count, max_step, success_count, review_count")
        logger.info(f"  - dur_sec, click_count, dbl_dur_sec, dbl_count")
        logger.info(f"")
        logger.info(f"Создана колонка:         sess_dur_sec")
        logger.info(f"")
        logger.info(f"Время выполнения:       {duration:.1f} секунд ({duration/60:.1f} минут)")
        logger.info(f"Скорость:               {len(df)/duration:.0f} строк/сек")
        logger.info("=" * 80)
        
        return result_df
    
    def get_block_info(self) -> pd.DataFrame:
        """
        Получение информации о блоках и их префиксах
        
        Returns:
            DataFrame с информацией о блоках
        """
        info = []
        for block in self.blocks_data:
            block_name = block['name']
            prefix = self.block_prefixes[block_name]
            
            columns = (
                f'{prefix}_count, {prefix}_max_step, {prefix}_success_count, '
                f'{prefix}_review_count, {prefix}_dur_sec, {prefix}_click_count, '
                f'{prefix}_dbl_dur_sec, {prefix}_dbl_count'
            )
            
            info.append({
                'Название блока': block_name,
                'Префикс': prefix,
                'Всего комбинаций': block['combinations_count'],
                'Всего событий': block['total_count'],
                'Колонки (8 шт)': columns
            })
        
        return pd.DataFrame(info)


def main():
    """Пример использования"""
    # Пути к файлам
    json_path = '/mnt/user-data/uploads/1766523980935_categorized_combinations_with_funnels.json'
    
    # Инициализация экстрактора
    extractor = FunnelFeaturesExtractor(json_path)
    
    # Показываем информацию о блоках
    print("\n" + "=" * 80)
    print("ИНФОРМАЦИЯ О ФУНКЦИОНАЛЬНЫХ БЛОКАХ")
    print("=" * 80)
    blocks_info = extractor.get_block_info()
    print(blocks_info[['Название блока', 'Префикс']].to_string(index=False))
    print(f"\nВсего блоков: {len(blocks_info)}")
    print(f"Всего колонок: {len(blocks_info) * 8} + 1 (sess_dur_sec) = {len(blocks_info) * 8 + 1}")
    print("=" * 80)


if __name__ == '__main__':
    main()
