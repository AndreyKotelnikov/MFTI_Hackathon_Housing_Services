"""
Модуль для предобработки данных событий и пользователей
"""

import pandas as pd
import numpy as np
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Optional
import yaml

logger = logging.getLogger(__name__)


class DataPreprocessor:
    """
    Класс для загрузки, очистки и предобработки данных
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Инициализация препроцессора
        
        Args:
            config_path: путь к конфигурационному файлу
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.events_df: Optional[pd.DataFrame] = None
        self.users_df: Optional[pd.DataFrame] = None
        self.merged_df: Optional[pd.DataFrame] = None
        self.stats = {}
        
        logger.info("DataPreprocessor инициализирован")

    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Загрузка данных с оптимизацией памяти

        Returns:
            Tuple из двух DataFrame: events и users
        """
        logger.info("Начало загрузки данных...")

        # Пути к файлам
        raw_path = self.config['data']['raw_path']
        events_file = Path(raw_path) / self.config['data']['events_file']
        users_file = Path(raw_path) / self.config['data']['users_file']

        # Типы данных для оптимизации памяти (БЕЗ типов для даты!)
        dtype_events = {
            'Экран': 'category',
            'Функционал': 'category',
            'Действие': 'category',
            'Идентификатор устройства': 'int32',
            'Номер сессии в рамках устройства': 'int64',
            'Производитель устройства': 'category',
            'Модель устройства': 'category',
            'Тип устройства': 'category',
            'ОС': 'category'
        }

        # Загрузка событий - сначала БЕЗ парсинга даты
        try:
            logger.info("Загрузка событий...")
            self.events_df = pd.read_csv(
                events_file,
                dtype=dtype_events
            )

            # Парсинг даты вручную с правильным форматом
            logger.info("Парсинг даты...")
            # Формат: 2025-09-29T10:20:27+03:00[Europe/Moscow]
            # Убираем часовой пояс в скобках, если есть
            date_col = self.events_df['Дата и время события'].astype(str)
            # Убираем [Europe/Moscow] если есть
            date_col = date_col.str.replace(r'\[.*?\]', '', regex=True)

            # Парсим дату
            self.events_df['Дата и время события'] = pd.to_datetime(
                date_col,
                format='ISO8601',
                utc=True,
                errors='coerce'
            )

            # Проверяем сколько дат не распарсилось
            null_dates = self.events_df['Дата и время события'].isna().sum()
            if null_dates > 0:
                logger.warning(f"Не удалось распарсить {null_dates} дат")

            logger.info(f"События загружены: {len(self.events_df)} строк")

        except FileNotFoundError:
            logger.error(f"Файл не найден: {events_file}")
            raise
        except Exception as e:
            logger.error(f"Ошибка загрузки событий: {e}")
            raise

        # Загрузка пользователей (БЕЗ указания типа для age_back из-за возможных NA)
        try:
            self.users_df = pd.read_csv(
                users_file,
                dtype={
                    'number': 'int32',
                    'gender': 'category'
                }
            )

            # Преобразуем age_back в int16 с сохранением NA
            if 'age_back' in self.users_df.columns:
                self.users_df['age_back'] = self.users_df['age_back'].astype('Int16')

            logger.info(f"Пользователи загружены: {len(self.users_df)} строк")
        except FileNotFoundError:
            logger.error(f"Файл не найден: {users_file}")
            raise

        # Сохранение статистики
        self.stats['events_loaded'] = len(self.events_df)
        self.stats['users_loaded'] = len(self.users_df)

        return self.events_df, self.users_df

    def clean_data(self) -> None:
        """
        Очистка данных: удаление дублей, обработка пропусков
        """
        logger.info("Начало очистки данных...")

        if self.events_df is None or self.users_df is None:
            raise ValueError("Данные не загружены. Вызовите load_data() сначала.")

        # Удаление дублей из событий
        events_before = len(self.events_df)
        self.events_df = self.events_df.drop_duplicates().copy()
        events_removed = events_before - len(self.events_df)
        logger.info(f"Удалено дублей в events: {events_removed}")
        self.stats['events_duplicates_removed'] = events_removed

        # Удаление дублей из пользователей
        users_before = len(self.users_df)
        self.users_df = self.users_df.drop_duplicates().copy()
        users_removed = users_before - len(self.users_df)
        logger.info(f"Удалено дублей в users: {users_removed}")
        self.stats['users_duplicates_removed'] = users_removed

        # Обработка пропущенных значений в categorical колонках
        if 'Действие' in self.events_df.columns:
            # Конвертируем в object для упрощения работы
            self.events_df['Действие'] = self.events_df['Действие'].astype('object')
            self.events_df['Действие'] = self.events_df['Действие'].fillna('Не указано')
            # Конвертируем обратно в category
            self.events_df['Действие'] = self.events_df['Действие'].astype('category')

        # Удаление строк с пропущенными критичными полями
        critical_columns = ['Дата и время события', 'Идентификатор устройства']
        initial_count = len(self.events_df)
        self.events_df = self.events_df.dropna(subset=critical_columns).copy()
        removed_count = initial_count - len(self.events_df)
        if removed_count > 0:
            logger.info(f"Удалено строк с пропущенными критичными полями: {removed_count}")

        logger.info("Очистка данных завершена")

    def create_features(self) -> pd.DataFrame:
        """
        Создание новых признаков из данных

        Returns:
            DataFrame с объединенными данными и новыми признаками
        """
        logger.info("Создание новых признаков...")

        if self.events_df is None:
            raise ValueError("Events данные не загружены")

        # Проверка что данные не пустые
        if len(self.events_df) == 0:
            logger.error("DataFrame пустой после очистки!")
            raise ValueError("DataFrame пустой, невозможно создать признаки")

        # Временные признаки
        self.events_df['date'] = self.events_df['Дата и время события'].dt.date
        self.events_df['hour'] = self.events_df['Дата и время события'].dt.hour
        self.events_df['day_of_week'] = self.events_df['Дата и время события'].dt.dayofweek
        self.events_df['is_weekend'] = self.events_df['day_of_week'].isin([5, 6])
        self.events_df['month'] = self.events_df['Дата и время события'].dt.month
        self.events_df['day'] = self.events_df['Дата и время события'].dt.day

        # Время суток
        def get_time_of_day(hour):
            if pd.isna(hour):
                return 'Неизвестно'
            if 6 <= hour < 12:
                return 'Утро'
            elif 12 <= hour < 18:
                return 'День'
            elif 18 <= hour < 24:
                return 'Вечер'
            else:
                return 'Ночь'

        self.events_df['time_of_day'] = self.events_df['hour'].apply(get_time_of_day)
        self.events_df['time_of_day'] = self.events_df['time_of_day'].astype('category')

        # Объединение с пользовательскими данными
        logger.info("Объединение с данными пользователей...")
        self.merged_df = self.events_df.merge(
            self.users_df,
            left_on='Идентификатор устройства',
            right_on='number',
            how='left'
        )

        # Возрастные группы
        def get_age_group(age):
            if pd.isna(age):
                return 'Неизвестно'
            elif age < 25:
                return '18-24'
            elif age < 35:
                return '25-34'
            elif age < 45:
                return '35-44'
            elif age < 55:
                return '45-54'
            elif age < 65:
                return '55-64'
            else:
                return '65+'

        self.merged_df['age_group'] = self.merged_df['age_back'].apply(get_age_group)
        self.merged_df['age_group'] = self.merged_df['age_group'].astype('category')

        logger.info(f"Создано признаков. Итоговая форма: {self.merged_df.shape}")
        self.stats['merged_df_shape'] = self.merged_df.shape

        return self.merged_df

    def generate_profile_report(self) -> Dict:
        """
        Генерация профилей данных с экспортом в JSON
        Создаёт профиль только для объединённого датасета (merged_df)

        Returns:
            Словарь со статистикой данных
        """
        logger.info("Генерация профиля данных...")

        if self.merged_df is None or len(self.merged_df) == 0:
            logger.error("Объединённый датасет не создан или пустой. Вызовите create_features() сначала.")
            raise ValueError("merged_df не создан. Вызовите create_features() перед генерацией профиля.")

        # Создание директорий
        html_path = Path(self.config['reports']['html_path'])
        json_path = Path(self.config['reports']['json_path'])
        html_path.mkdir(parents=True, exist_ok=True)
        json_path.mkdir(parents=True, exist_ok=True)

        # Попытка использовать ydata-profiling
        try:
            from ydata_profiling import ProfileReport

            # ==========================================
            # ПРОФИЛЬ ДЛЯ MERGED_DF
            # ==========================================

            # Для больших датасетов используем выборку
            sample_size = min(4000000, len(self.merged_df))
            if len(self.merged_df) > sample_size:
                logger.info(f"Используется выборка {sample_size} строк из {len(self.merged_df)} для профилирования")
                merged_sample = self.merged_df.sample(n=sample_size, random_state=42)
            else:
                merged_sample = self.merged_df

            logger.info("Создание профиля для merged_df...")
            logger.info("Это может занять несколько минут...")

            merged_report = ProfileReport(
                merged_sample,
                title='MCD Application - Complete Data Profile',
                minimal=False,  # Полный отчёт со всеми деталями
                explorative=True,
                # Дополнительные настройки для детального анализа
                correlations={
                    "auto": {"calculate": True},
                    "pearson": {"calculate": True},
                    "spearman": {"calculate": True},
                    "kendall": {"calculate": False},  # Медленно на больших данных
                    "phi_k": {"calculate": True},
                    "cramers": {"calculate": True},
                },
                interactions={
                    "continuous": True,
                    "targets": []
                },
                missing_diagrams={
                    "heatmap": True,
                    "dendrogram": True,
                    "matrix": True,
                    "bar": True
                }
            )

            # ==========================================
            # СОХРАНЕНИЕ HTML
            # ==========================================
            logger.info("Сохранение HTML профиля...")
            merged_report.to_file(str(html_path / 'merged_data_profile.html'))
            logger.info(f"✓ HTML профиль сохранён: {html_path / 'merged_data_profile.html'}")

            # ==========================================
            # СОХРАНЕНИЕ ПОЛНОГО JSON ПРОФИЛЯ
            # ==========================================
            logger.info("Сохранение полного JSON профиля...")
            merged_json = merged_report.to_json()
            with open(json_path / 'merged_data_profile_full.json', 'w', encoding='utf-8') as f:
                f.write(merged_json)
            logger.info(f"✓ Полный JSON профиль сохранён: {json_path / 'merged_data_profile_full.json'}")

        except ImportError:
            logger.warning("ydata-profiling не установлен, пропускаем HTML и полные JSON отчеты")
            logger.info("Установите: pip install ydata-profiling")
        except Exception as e:
            logger.warning(f"Ошибка при создании профиля: {e}")
            logger.info("Продолжаем с созданием краткого summary...")

        # ==========================================
        # СОЗДАНИЕ КРАТКОГО JSON SUMMARY
        # ==========================================
        logger.info("Создание краткого JSON summary...")
        json_summary = self._create_json_summary()

        # Сохранение краткого summary
        with open(json_path / 'data_profile_summary.json', 'w', encoding='utf-8') as f:
            json.dump(json_summary, f, ensure_ascii=False, indent=2)

        logger.info(f"✓ JSON summary сохранён: {json_path / 'data_profile_summary.json'}")

        # Итоговый отчёт
        print(f"\n{'=' * 80}")
        print("ПРОФИЛИ ДАННЫХ СОЗДАНЫ:")
        print(f"{'=' * 80}")
        print(f"📊 HTML отчёт:        {html_path / 'merged_data_profile.html'}")
        print(f"📄 JSON (полный):     {json_path / 'merged_data_profile_full.json'}")
        print(f"📋 JSON (summary):    {json_path / 'data_profile_summary.json'}")
        print(f"{'=' * 80}\n")

        return json_summary
    
    def _create_json_summary(self) -> Dict:
        """
        Создание структурированного JSON отчета
        
        Returns:
            Словарь со статистикой
        """
        summary = {
            'generated_at': datetime.now().isoformat(),
            'events': {
                'total_rows': int(len(self.events_df)),
                'total_columns': int(len(self.events_df.columns)),
                'duplicates_removed': int(self.stats.get('events_duplicates_removed', 0)),
                'date_range': {
                    'min': str(self.events_df['Дата и время события'].min()) if len(self.events_df) > 0 else 'N/A',
                    'max': str(self.events_df['Дата и время события'].max()) if len(self.events_df) > 0 else 'N/A',
                    'days': int((self.events_df['Дата и время события'].max() -
                               self.events_df['Дата и время события'].min()).days) if len(self.events_df) > 0 else 0
                },
                'unique_devices': int(self.events_df['Идентификатор устройства'].nunique()),
                'unique_sessions': int(self.events_df['Номер сессии в рамках устройства'].nunique()),
                'columns_info': {},
                'top_screens': {},
                'top_functions': {},
                'device_info': {}
            },
            'users': {
                'total_rows': int(len(self.users_df)),
                'duplicates_removed': int(self.stats.get('users_duplicates_removed', 0)),
                'age_stats': {},
                'gender_distribution': {}
            }
        }
        
        # Информация по колонкам
        for col in self.events_df.columns:
            summary['events']['columns_info'][col] = {
                'dtype': str(self.events_df[col].dtype),
                'null_count': int(self.events_df[col].isna().sum()),
                'null_percentage': float(self.events_df[col].isna().sum() / len(self.events_df) * 100),
                'unique_values': int(self.events_df[col].nunique())
            }
        
        # Топ экраны и функции
        summary['events']['top_screens'] = {
            str(k): int(v) for k, v in 
            self.events_df['Экран'].value_counts().head(10).items()
        }
        
        summary['events']['top_functions'] = {
            str(k): int(v) for k, v in 
            self.events_df['Функционал'].value_counts().head(10).items()
        }
        
        # Информация об устройствах
        summary['events']['device_info'] = {
            'os_distribution': {
                str(k): int(v) for k, v in 
                self.events_df['ОС'].value_counts().items()
            },
            'device_types': {
                str(k): int(v) for k, v in 
                self.events_df['Тип устройства'].value_counts().items()
            },
            'top_manufacturers': {
                str(k): int(v) for k, v in 
                self.events_df['Производитель устройства'].value_counts().head(10).items()
            }
        }
        
        # Статистика пользователей
        summary['users']['age_stats'] = {
            'mean': float(self.users_df['age_back'].mean()),
            'median': float(self.users_df['age_back'].median()),
            'min': int(self.users_df['age_back'].min()),
            'max': int(self.users_df['age_back'].max()),
            'std': float(self.users_df['age_back'].std())
        }
        
        summary['users']['gender_distribution'] = {
            str(k): int(v) for k, v in 
            self.users_df['gender'].value_counts().items()
        }
        
        return summary

    def save_processed_data(self) -> None:
        """
        Сохранение обработанных данных в CSV
        """
        logger.info("Сохранение обработанных данных...")

        processed_path = Path(self.config['data']['processed_path'])
        processed_path.mkdir(parents=True, exist_ok=True)

        # Сохранение merged_df в CSV
        if self.merged_df is not None and len(self.merged_df) > 0:
            csv_file = processed_path / 'merged_data_test.csv'
            logger.info(f"Сохранение в {csv_file}...")

            # Конвертация date в string для CSV
            df_to_save = self.merged_df.copy()
            if 'date' in df_to_save.columns:
                df_to_save['date'] = df_to_save['date'].astype(str)

            df_to_save.to_csv(csv_file, index=False, encoding='utf-8')

            file_size = csv_file.stat().st_size / 1024 ** 2
            logger.info(f"✓ Merged_df сохранён: {csv_file} ({file_size:.2f} MB)")

        # Также сохраняем очищенные events в CSV (опционально)
        if self.events_df is not None and len(self.events_df) > 0:
            events_csv = processed_path / 'events_cleaned.csv'
            logger.info(f"Сохранение очищенных событий в {events_csv}...")

            df_to_save = self.events_df.copy()
            if 'date' in df_to_save.columns:
                df_to_save['date'] = df_to_save['date'].astype(str)

            df_to_save.to_csv(events_csv, index=False, encoding='utf-8')
            logger.info(f"✓ Очищенные события сохранены: {events_csv}")

        logger.info("Сохранение завершено")

    def load_merged_data_base(self, path: str = None) -> pd.DataFrame:
        """
        Загрузка ранее сохранённого merged_df из CSV с правильной типизацией

        Args:
            path: путь к файлу CSV (если None, используется data/processed/merged_data.csv)

        Returns:
            DataFrame с правильными типами
        """
        logger.info("Загрузка сохранённого merged_df из CSV...")

        if path is None:
            processed_path = Path(self.config['data']['processed_path'])
            path = processed_path / 'merged_data.csv'

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(
                f"Файл не найден: {path}\n"
                f"Запустите полную обработку данных сначала."
            )

        # Загрузка из CSV
        logger.info(f"Чтение файла: {path}")
        self.merged_df = pd.read_csv(path, low_memory=False)

        logger.info(f"Загружено {len(self.merged_df):,} строк, {len(self.merged_df.columns)} колонок")

        # Восстановление правильных типов данных
        logger.info("Восстановление типов данных...")

        # DateTime колонка
        if 'Дата и время события' in self.merged_df.columns:
            logger.debug("Конвертация 'Дата и время события' в datetime...")
            self.merged_df['Дата и время события'] = pd.to_datetime(
                self.merged_df['Дата и время события'],
                utc=True,
                errors='coerce'
            )

        # Categorical колонки
        categorical_cols = [
            'Экран', 'Функционал', 'Действие',
            'Производитель устройства', 'Модель устройства',
            'Тип устройства', 'ОС', 'time_of_day',
            'gender', 'age_group'
        ]

        for col in categorical_cols:
            if col in self.merged_df.columns:
                logger.debug(f"Конвертация {col} в category")
                self.merged_df[col] = self.merged_df[col].astype('category')

        # Boolean колонка
        if 'is_weekend' in self.merged_df.columns:
            self.merged_df['is_weekend'] = self.merged_df['is_weekend'].astype('bool')

        # Integer колонки с оптимизацией памяти
        int_cols = {
            'Идентификатор устройства': 'int32',
            'Номер сессии в рамках устройства': 'int64',
            'hour': 'int8',
            'day_of_week': 'int8',
            'month': 'int8',
            'day': 'int8',
            'number': 'int32'
        }

        for col, dtype in int_cols.items():
            if col in self.merged_df.columns:
                logger.debug(f"Конвертация {col} в {dtype}")
                self.merged_df[col] = self.merged_df[col].astype(dtype)

        # Nullable integer для age_back
        if 'age_back' in self.merged_df.columns:
            self.merged_df['age_back'] = pd.to_numeric(
                self.merged_df['age_back'],
                errors='coerce'
            ).astype('Int16')

        # Конвертация date обратно в date (из string)
        if 'date' in self.merged_df.columns:
            self.merged_df['date'] = pd.to_datetime(
                self.merged_df['date'],
                errors='coerce'
            ).dt.date

        # Проверка памяти
        memory_usage = self.merged_df.memory_usage(deep=True).sum() / 1024 ** 2
        logger.info(f"Использование памяти: {memory_usage:.2f} MB")

        logger.info("✓ Загрузка merged_df завершена успешно")

        return self.merged_df

    def load_merged_data(self, path: str = None) -> pd.DataFrame:
        """
        Загрузка ранее сохранённого merged_df из CSV с правильной типизацией

        Args:
            path: путь к файлу CSV (если None, используется data/processed/merged_data.csv)

        Returns:
            DataFrame с правильными типами
        """
        logger.info("Загрузка сохранённого merged_df из CSV...")

        if path is None:
            processed_path = Path(self.config['data']['processed_path'])
            path = processed_path / 'merged_data.csv'

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(
                f"Файл не найден: {path}\n"
                f"Запустите полную обработку данных сначала."
            )

        # Загрузка из CSV
        logger.info(f"Чтение файла: {path}")
        self.merged_df = pd.read_csv(path, low_memory=False)

        logger.info(f"Загружено {len(self.merged_df):,} строк, {len(self.merged_df.columns)} колонок")

        # Восстановление правильных типов данных
        logger.info("Восстановление типов данных...")

        # DateTime колонка
        if 'Дата и время события' in self.merged_df.columns:
            logger.debug("Конвертация 'Дата и время события' в datetime...")
            self.merged_df['Дата и время события'] = pd.to_datetime(
                self.merged_df['Дата и время события'],
                utc=True,
                errors='coerce'
            )

        # Categorical колонки
        categorical_cols = [
            'Экран', 'Функционал', 'Действие',
            'Производитель устройства', 'Модель устройства',
            'Тип устройства', 'ОС', 'time_of_day',
            'gender', 'age_group'
        ]

        for col in categorical_cols:
            if col in self.merged_df.columns:
                logger.debug(f"Конвертация {col} в category")
                self.merged_df[col] = self.merged_df[col].astype('category')

        # Boolean колонка
        if 'is_weekend' in self.merged_df.columns:
            self.merged_df['is_weekend'] = self.merged_df['is_weekend'].astype('bool')

        # Integer колонки с оптимизацией памяти
        int_cols = {
            # Основные колонки
            'Идентификатор устройства': 'int32',
            'Номер сессии в рамках устройства': 'int64',
            'hour': 'int8',
            'day_of_week': 'int8',
            'month': 'int8',
            'day': 'int8',
            'number': 'int32',
            'global_session_id': 'int32',  # Глобальный ID сессии (до ~1M)
            'duration_seconds': 'int32',  # Длительность события (секунды)
            'click_count': 'int16',  # Количество кликов (1-100)
            'dbl_duration_seconds': 'int32',  # Длительность удалённых дублей
            'dbl_count': 'int16'  # Количество удалённых дублей
        }

        for col, dtype in int_cols.items():
            if col in self.merged_df.columns:
                try:
                    logger.debug(f"Конвертация {col} в {dtype}")
                    self.merged_df[col] = pd.to_numeric(
                        self.merged_df[col],
                        errors='coerce'
                    ).fillna(0).astype(dtype)
                except Exception as e:
                    logger.warning(f"Не удалось конвертировать {col} в {dtype}: {e}")
                    # Fallback на более широкий тип
                    if dtype == 'int8':
                        fallback_dtype = 'int16'
                    elif dtype == 'int16':
                        fallback_dtype = 'int32'
                    elif dtype == 'int32':
                        fallback_dtype = 'int64'
                    else:
                        fallback_dtype = 'int64'

                    logger.warning(f"Использую fallback тип {fallback_dtype}")
                    self.merged_df[col] = pd.to_numeric(
                        self.merged_df[col],
                        errors='coerce'
                    ).fillna(0).astype(fallback_dtype)

        # Nullable integer для age_back
        if 'age_back' in self.merged_df.columns:
            self.merged_df['age_back'] = pd.to_numeric(
                self.merged_df['age_back'],
                errors='coerce'
            ).astype('Int16')

        # Конвертация date обратно в date (из string)
        if 'date' in self.merged_df.columns:
            self.merged_df['date'] = pd.to_datetime(
                self.merged_df['date'],
                errors='coerce'
            ).dt.date

        # ============================================================
        # СТАТУС НОВЫХ КОЛОНОК ОБРАБОТКИ
        # ============================================================

        pipeline_cols_status = {}

        if 'global_session_id' in self.merged_df.columns:
            unique_sessions = self.merged_df['global_session_id'].nunique()
            pipeline_cols_status['global_session_id'] = f"✓ {unique_sessions:,} уникальных сессий"
        else:
            pipeline_cols_status['global_session_id'] = "✗ Не найден (выполните add_global_session_id)"

        if 'duration_seconds' in self.merged_df.columns:
            avg_duration = self.merged_df['duration_seconds'].mean()
            max_duration = self.merged_df['duration_seconds'].max()
            pipeline_cols_status['duration_seconds'] = f"✓ Среднее: {avg_duration:.1f}с, Макс: {max_duration:,}с"
        else:
            pipeline_cols_status['duration_seconds'] = "✗ Не найден (выполните calculate_event_duration)"

        if 'click_count' in self.merged_df.columns:
            avg_tries = self.merged_df['click_count'].mean()
            max_tries = self.merged_df['click_count'].max()
            pipeline_cols_status['click_count'] = f"✓ Среднее: {avg_tries:.2f}, Макс: {max_tries}"
        else:
            pipeline_cols_status['click_count'] = "✗ Не найден (выполните remove_consecutive_duplicates_with_tries)"

        if 'dbl_duration_seconds' in self.merged_df.columns:
            avg_dbl_dur = self.merged_df['dbl_duration_seconds'].mean()
            sum_dbl_dur = self.merged_df['dbl_duration_seconds'].sum()
            pipeline_cols_status[
                'dbl_duration_seconds'] = f"✓ Среднее: {avg_dbl_dur:.1f}с, Сумма: {sum_dbl_dur / 3600:.1f}ч"
        else:
            pipeline_cols_status[
                'dbl_duration_seconds'] = "✗ Не найден (выполните remove_consecutive_duplicates_with_tries)"

        if 'dbl_count' in self.merged_df.columns:
            avg_dbl = self.merged_df['dbl_count'].mean()
            max_dbl = self.merged_df['dbl_count'].max()
            pipeline_cols_status['dbl_count'] = f"✓ Среднее: {avg_dbl:.2f}, Макс: {max_dbl}"
        else:
            pipeline_cols_status['dbl_count'] = "✗ Не найден (выполните remove_consecutive_duplicates_with_tries)"

        # Вывод статуса pipeline колонок
        if any('✓' in status for status in pipeline_cols_status.values()):
            logger.info(f"\n{'=' * 60}")
            logger.info("СТАТУС КОЛОНОК PIPELINE ОБРАБОТКИ:")
            logger.info(f"{'=' * 60}")
            for col_name, status in pipeline_cols_status.items():
                logger.info(f"  {col_name}: {status}")
            logger.info(f"{'=' * 60}\n")

        # Проверка памяти
        memory_usage = self.merged_df.memory_usage(deep=True).sum() / 1024 ** 2
        logger.info(f"Использование памяти: {memory_usage:.2f} MB")

        logger.info("✓ Загрузка merged_df завершена успешно")

        return self.merged_df

    def load_merged_data_funnel(self, path: str = None) -> pd.DataFrame:
        """
        Загрузка ранее сохранённого merged_df из CSV с правильной типизацией
        Поддерживает колонки funnel features (68 колонок от FunnelFeaturesExtractor)

        Args:
            path: путь к файлу CSV (если None, используется data/processed/merged_data.csv)

        Returns:
            DataFrame с правильными типами
        """
        logger.info("Загрузка сохранённого merged_df из CSV...")

        if path is None:
            processed_path = Path(self.config['data']['processed_path'])
            path = processed_path / 'merged_data.csv'

        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(
                f"Файл не найден: {path}\n"
                f"Запустите полную обработку данных сначала."
            )

        # Загрузка из CSV
        logger.info(f"Чтение файла: {path}")
        self.merged_df = pd.read_csv(path, low_memory=False)

        logger.info(f"Загружено {len(self.merged_df):,} строк, {len(self.merged_df.columns)} колонок")

        # Восстановление правильных типов данных
        logger.info("Восстановление типов данных...")

        # DateTime колонка
        if 'Дата и время события' in self.merged_df.columns:
            logger.debug("Конвертация 'Дата и время события' в datetime...")
            self.merged_df['Дата и время события'] = pd.to_datetime(
                self.merged_df['Дата и время события'],
                utc=True,
                errors='coerce'
            )

        # Categorical колонки
        categorical_cols = [
            'Экран', 'Функционал', 'Действие',
            'Производитель устройства', 'Модель устройства',
            'Тип устройства', 'ОС', 'time_of_day',
            'gender', 'age_group'
        ]

        for col in categorical_cols:
            if col in self.merged_df.columns:
                logger.debug(f"Конвертация {col} в category")
                self.merged_df[col] = self.merged_df[col].astype('category')

        # Boolean колонка
        if 'is_weekend' in self.merged_df.columns:
            self.merged_df['is_weekend'] = self.merged_df['is_weekend'].astype('bool')

        # Integer колонки с оптимизацией памяти
        int_cols = {
            # Основные колонки
            'Идентификатор устройства': 'int32',
            'Номер сессии в рамках устройства': 'int64',
            'hour': 'int8',
            'day_of_week': 'int8',
            'month': 'int8',
            'day': 'int8',
            'number': 'int32',
            'global_session_id': 'int32',  # Глобальный ID сессии (до ~1M)
            'duration_seconds': 'int32',  # Длительность события (секунды)
            'click_count': 'int16',  # Количество кликов (1-100)
            'dbl_duration_seconds': 'int32',  # Длительность удалённых дублей
            'dbl_count': 'int16'  # Количество удалённых дублей
        }

        # ============================================================
        # FUNNEL FEATURES - КОЛОНКИ ФУНКЦИОНАЛЬНЫХ БЛОКОВ (68 колонок)
        # ============================================================
        # Префиксы всех 17 функциональных блоков
        funnel_prefixes = [
            'request',  # Создание заявки
            'req_manage',  # Просмотр и управление заявками
            'profile',  # Профиль
            'nav',  # Навигация
            'notif',  # Уведомления
            'poll_oss',  # Опросы и собрания собственников
            'rewards',  # Баллы и поощрения
            'my_home',  # Мой дом
            'partners',  # Услуги партнеров
            'transport',  # Управление транспортом
            'ann_view',  # Просмотр объявлений
            'smart',  # Умные решения
            'support',  # Техподдержка
            'guest',  # Гостевой доступ
            'city_serv',  # Городские сервисы
            'address',  # Создание адреса
            'ann_create'  # Создание объявления
        ]

        # Добавляем типы для funnel features
        for prefix in funnel_prefixes:
            int_cols[f'{prefix}_count'] = 'int16'  # Количество действий (0-1000)
            int_cols[f'{prefix}_max_step'] = 'int8'  # Максимальный шаг (-1 до 50)
            int_cols[f'{prefix}_success_count'] = 'int8'  # Успешные действия (0-50)
            int_cols[f'{prefix}_review_count'] = 'int8'  # Review действия (0-50)

        # Применяем типизацию
        for col, dtype in int_cols.items():
            if col in self.merged_df.columns:
                try:
                    logger.debug(f"Конвертация {col} в {dtype}")

                    # Специальная обработка для max_step (может быть -1)
                    if col.endswith('_max_step'):
                        self.merged_df[col] = pd.to_numeric(
                            self.merged_df[col],
                            errors='coerce'
                        ).fillna(-1).astype(dtype)
                    else:
                        self.merged_df[col] = pd.to_numeric(
                            self.merged_df[col],
                            errors='coerce'
                        ).fillna(0).astype(dtype)

                except Exception as e:
                    logger.warning(f"Не удалось конвертировать {col} в {dtype}: {e}")
                    # Fallback на более широкий тип
                    if dtype == 'int8':
                        fallback_dtype = 'int16'
                    elif dtype == 'int16':
                        fallback_dtype = 'int32'
                    elif dtype == 'int32':
                        fallback_dtype = 'int64'
                    else:
                        fallback_dtype = 'int64'

                    logger.warning(f"Использую fallback тип {fallback_dtype}")

                    if col.endswith('_max_step'):
                        self.merged_df[col] = pd.to_numeric(
                            self.merged_df[col],
                            errors='coerce'
                        ).fillna(-1).astype(fallback_dtype)
                    else:
                        self.merged_df[col] = pd.to_numeric(
                            self.merged_df[col],
                            errors='coerce'
                        ).fillna(0).astype(fallback_dtype)

        # Nullable integer для age_back
        if 'age_back' in self.merged_df.columns:
            self.merged_df['age_back'] = pd.to_numeric(
                self.merged_df['age_back'],
                errors='coerce'
            ).astype('Int16')

        # Конвертация date обратно в date (из string)
        if 'date' in self.merged_df.columns:
            self.merged_df['date'] = pd.to_datetime(
                self.merged_df['date'],
                errors='coerce'
            ).dt.date

        # ============================================================
        # СТАТУС КОЛОНОК ОСНОВНОГО PIPELINE
        # ============================================================

        pipeline_cols_status = {}

        if 'global_session_id' in self.merged_df.columns:
            unique_sessions = self.merged_df['global_session_id'].nunique()
            pipeline_cols_status['global_session_id'] = f"✓ {unique_sessions:,} уникальных сессий"
        else:
            pipeline_cols_status['global_session_id'] = "✗ Не найден (выполните add_global_session_id)"

        if 'duration_seconds' in self.merged_df.columns:
            avg_duration = self.merged_df['duration_seconds'].mean()
            max_duration = self.merged_df['duration_seconds'].max()
            pipeline_cols_status['duration_seconds'] = f"✓ Среднее: {avg_duration:.1f}с, Макс: {max_duration:,}с"
        else:
            pipeline_cols_status['duration_seconds'] = "✗ Не найден (выполните calculate_event_duration)"

        if 'click_count' in self.merged_df.columns:
            avg_tries = self.merged_df['click_count'].mean()
            max_tries = self.merged_df['click_count'].max()
            pipeline_cols_status['click_count'] = f"✓ Среднее: {avg_tries:.2f}, Макс: {max_tries}"
        else:
            pipeline_cols_status['click_count'] = "✗ Не найден (выполните remove_consecutive_duplicates_with_tries)"

        if 'dbl_duration_seconds' in self.merged_df.columns:
            avg_dbl_dur = self.merged_df['dbl_duration_seconds'].mean()
            sum_dbl_dur = self.merged_df['dbl_duration_seconds'].sum()
            pipeline_cols_status[
                'dbl_duration_seconds'] = f"✓ Среднее: {avg_dbl_dur:.1f}с, Сумма: {sum_dbl_dur / 3600:.1f}ч"
        else:
            pipeline_cols_status[
                'dbl_duration_seconds'] = "✗ Не найден (выполните remove_consecutive_duplicates_with_tries)"

        if 'dbl_count' in self.merged_df.columns:
            avg_dbl = self.merged_df['dbl_count'].mean()
            max_dbl = self.merged_df['dbl_count'].max()
            pipeline_cols_status['dbl_count'] = f"✓ Среднее: {avg_dbl:.2f}, Макс: {max_dbl}"
        else:
            pipeline_cols_status['dbl_count'] = "✗ Не найден (выполните remove_consecutive_duplicates_with_tries)"

        # ============================================================
        # СТАТУС FUNNEL FEATURES КОЛОНОК
        # ============================================================

        # Проверяем наличие funnel features
        funnel_cols_found = [col for col in self.merged_df.columns
                             if any(col.startswith(f'{prefix}_') for prefix in funnel_prefixes)]

        funnel_features_status = {}

        if funnel_cols_found:
            # Подсчитываем сколько колонок найдено для каждого блока
            blocks_with_features = {}
            for prefix in funnel_prefixes:
                prefix_cols = [col for col in funnel_cols_found if col.startswith(f'{prefix}_')]
                if prefix_cols:
                    blocks_with_features[prefix] = len(prefix_cols)

            funnel_features_status['total_blocks'] = f"✓ {len(blocks_with_features)}/17 блоков"
            funnel_features_status['total_columns'] = f"✓ {len(funnel_cols_found)}/68 колонок"

            # Статистика по топ блокам
            if blocks_with_features:
                # Считаем количество сессий с взаимодействием для каждого блока
                block_stats = []
                for prefix in funnel_prefixes:
                    count_col = f'{prefix}_count'
                    if count_col in self.merged_df.columns:
                        # Берем уникальные значения по сессиям
                        sessions_with_block = (
                                self.merged_df.groupby('global_session_id')[count_col]
                                .first() > 0
                        ).sum()

                        if sessions_with_block > 0:
                            total_actions = self.merged_df.groupby('global_session_id')[count_col].first().sum()
                            block_stats.append({
                                'prefix': prefix,
                                'sessions': sessions_with_block,
                                'actions': int(total_actions)
                            })

                # Сортируем по количеству сессий
                block_stats.sort(key=lambda x: x['sessions'], reverse=True)
        else:
            funnel_features_status['status'] = (
                "✗ Не найдено (выполните FunnelFeaturesExtractor.transform())"
            )

        # ============================================================
        # ВЫВОД СТАТУСОВ
        # ============================================================

        # Вывод статуса основных pipeline колонок
        if any('✓' in status for status in pipeline_cols_status.values()):
            logger.info(f"\n{'=' * 70}")
            logger.info("СТАТУС КОЛОНОК ОСНОВНОГО PIPELINE:")
            logger.info(f"{'=' * 70}")
            for col_name, status in pipeline_cols_status.items():
                logger.info(f"  {col_name}: {status}")
            logger.info(f"{'=' * 70}")

        # Вывод статуса funnel features
        if funnel_features_status:
            logger.info(f"\n{'=' * 70}")
            logger.info("СТАТУС FUNNEL FEATURES (ФУНКЦИОНАЛЬНЫЕ БЛОКИ):")
            logger.info(f"{'=' * 70}")
            for key, status in funnel_features_status.items():
                logger.info(f"  {key}: {status}")

            # Детальная информация по блокам
            if funnel_cols_found and 'global_session_id' in self.merged_df.columns:
                logger.info(f"\n  Детализация по блокам:")

                # Группируем статистику
                block_details = []
                for prefix in funnel_prefixes:
                    count_col = f'{prefix}_count'
                    if count_col in self.merged_df.columns:
                        sessions_data = self.merged_df.groupby('global_session_id')[count_col].first()
                        sessions_with_block = (sessions_data > 0).sum()

                        if sessions_with_block > 0:
                            total_sessions = len(sessions_data)
                            percentage = 100 * sessions_with_block / total_sessions
                            total_actions = int(sessions_data.sum())
                            avg_actions = sessions_data[sessions_data > 0].mean()

                            block_details.append({
                                'prefix': prefix,
                                'sessions': sessions_with_block,
                                'percent': percentage,
                                'actions': total_actions,
                                'avg': avg_actions
                            })

                # Сортируем и показываем топ-5
                block_details.sort(key=lambda x: x['sessions'], reverse=True)
                for i, detail in enumerate(block_details[:5], 1):
                    logger.info(
                        f"    {i}. {detail['prefix']:12s}: "
                        f"{detail['sessions']:6,} сессий ({detail['percent']:4.1f}%), "
                        f"{detail['actions']:7,} действий, "
                        f"среднее: {detail['avg']:.2f}"
                    )

                if len(block_details) > 5:
                    logger.info(f"    ... и ещё {len(block_details) - 5} блоков")

            logger.info(f"{'=' * 70}\n")

        # Проверка памяти
        memory_usage = self.merged_df.memory_usage(deep=True).sum() / 1024 ** 2
        logger.info(f"Использование памяти: {memory_usage:.2f} MB")

        # Итоговая сводка
        logger.info(f"\n{'=' * 70}")
        logger.info("ИТОГОВАЯ СВОДКА ЗАГРУЖЕННОГО ДАТАСЕТА:")
        logger.info(f"{'=' * 70}")
        logger.info(f"  Строк: {len(self.merged_df):,}")
        logger.info(f"  Колонок: {len(self.merged_df.columns)}")
        logger.info(f"  Период: {self.merged_df['date'].min()} - {self.merged_df['date'].max()}")
        logger.info(f"  Память: {memory_usage:.2f} MB")

        # Считаем сколько основных и funnel колонок
        base_cols = len([c for c in self.merged_df.columns if not any(c.startswith(f'{p}_') for p in funnel_prefixes)])
        funnel_cols_count = len(funnel_cols_found)

        logger.info(f"  Базовых колонок: {base_cols}")
        if funnel_cols_count > 0:
            logger.info(f"  Funnel features: {funnel_cols_count}")
        logger.info(f"{'=' * 70}\n")

        logger.info("✓ Загрузка merged_df завершена успешно")

        return self.merged_df

    def add_global_session_id(self) -> pd.DataFrame:
        """
        Добавляет глобальный уникальный ID сессии и пересортировывает данные

        Returns:
            DataFrame с добавленным global_session_id и отсортированный
        """
        logger.info("Добавление global_session_id...")

        if self.merged_df is None or len(self.merged_df) == 0:
            raise ValueError("merged_df не загружен")

        # Создание уникальных пар и присвоение ID
        unique_sessions = self.merged_df[[
            'Идентификатор устройства',
            'Номер сессии в рамках устройства'
        ]].drop_duplicates()

        unique_sessions = unique_sessions.sort_values([
            'Идентификатор устройства',
            'Номер сессии в рамках устройства'
        ]).reset_index(drop=True)

        unique_sessions['global_session_id'] = range(1, len(unique_sessions) + 1)

        # Присоединение к основной таблице
        self.merged_df = self.merged_df.merge(
            unique_sessions,
            on=['Идентификатор устройства', 'Номер сессии в рамках устройства'],
            how='left'
        )

        logger.info(f"Создано {self.merged_df['global_session_id'].nunique():,} уникальных global_session_id")

        # Сортировка
        logger.info("Сортировка данных...")
        self.merged_df = self.merged_df.sort_values(
            by=['global_session_id', 'Дата и время события'],
            ascending=[True, True]
        ).reset_index(drop=True)

        logger.info("✓ global_session_id добавлен и данные отсортированы")

        return self.merged_df

    def calculate_event_duration(self) -> pd.DataFrame:
        """
        Расчёт длительности между событиями в рамках сессии

        Логика:
        - Добавляется колонка duration_seconds = 0 (по умолчанию)
        - В рамках каждой global_session_id:
          * Для каждой записи (кроме последней): duration_seconds = разница с ПОСЛЕДУЮЩЕЙ записью в секундах
          * Последняя запись в сессии: duration_seconds = 0

        Интерпретация: duration_seconds показывает сколько времени пользователь провёл на текущем экране
        перед переходом к следующему действию.

        Пример:
        | time      | duration_seconds | Описание                              |
        |-----------|------------------|---------------------------------------|
        | 10:00:00  | 5                | Провёл 5 секунд до следующего события |
        | 10:00:05  | 7                | Провёл 7 секунд                       |
        | 10:00:12  | 48               | Провёл 48 секунд                      |
        | 10:01:00  | 0                | Последнее событие в сессии            |

        Returns:
            DataFrame с добавленной колонкой duration_seconds
        """
        logger.info("Расчёт длительности между событиями...")

        if self.merged_df is None or len(self.merged_df) == 0:
            raise ValueError("merged_df не загружен")

        if 'global_session_id' not in self.merged_df.columns:
            raise ValueError("Требуется global_session_id (выполните add_global_session_id)")

        time_col = 'Дата и время события'

        if time_col not in self.merged_df.columns:
            raise ValueError(f"Колонка '{time_col}' не найдена")

        # Проверка что время в формате datetime
        if not pd.api.types.is_datetime64_any_dtype(self.merged_df[time_col]):
            logger.info("Конвертация времени в datetime...")
            self.merged_df[time_col] = pd.to_datetime(
                self.merged_df[time_col],
                utc=True,
                errors='coerce'
            )

        initial_rows = len(self.merged_df)
        logger.info(f"Количество записей: {initial_rows:,}")

        # Убедимся что данные отсортированы
        self.merged_df = self.merged_df.sort_values(
            by=['global_session_id', time_col],
            ascending=[True, True]
        ).reset_index(drop=True)

        logger.info("Данные отсортированы по global_session_id и времени")

        # ============================================================
        # РАСЧЁТ ДЛИТЕЛЬНОСТИ (С ПОСЛЕДУЮЩЕЙ ЗАПИСЬЮ)
        # ============================================================

        logger.info("Расчёт разницы во времени с последующим событием...")

        # Добавляем колонку duration_seconds (по умолчанию 0)
        self.merged_df['duration_seconds'] = 0

        # Получаем время ПОСЛЕДУЮЩЕГО события В ТОЙ ЖЕ СЕССИИ
        self.merged_df['next_time'] = self.merged_df.groupby(
            'global_session_id',
            sort=False
        )[time_col].shift(-1)  # ← ИЗМЕНЕНО: shift(-1) вместо shift(1)

        # Вычисляем разницу во времени (timedelta)
        time_diff = self.merged_df['next_time'] - self.merged_df[time_col]  # ← ИЗМЕНЕНО: next - current

        # Конвертируем timedelta в секунды
        # Для последних записей в сессии (где next_time = NaT) будет NaN, заменяем на 0
        self.merged_df['duration_seconds'] = time_diff.dt.total_seconds().fillna(0)

        # Конвертируем в целое число (int) для удобства
        self.merged_df['duration_seconds'] = self.merged_df['duration_seconds'].astype('int32')

        # Удаляем временную колонку
        self.merged_df = self.merged_df.drop(columns=['next_time'])

        logger.info("✓ Длительности рассчитаны")

        # ============================================================
        # СТАТИСТИКА
        # ============================================================

        logger.info(f"\n{'=' * 60}")
        logger.info("СТАТИСТИКА ДЛИТЕЛЬНОСТИ:")
        logger.info(f"{'=' * 60}")

        # Общая статистика
        total_events = len(self.merged_df)
        last_events = (self.merged_df['duration_seconds'] == 0).sum()
        non_last_events = total_events - last_events

        logger.info(f"Всего событий: {total_events:,}")
        logger.info(f"  - Последних событий в сессиях: {last_events:,} (duration=0)")
        logger.info(f"  - Промежуточных событий: {non_last_events:,}")

        # Статистика по duration_seconds (исключая последние события)
        non_zero_durations = self.merged_df[self.merged_df['duration_seconds'] > 0]['duration_seconds']

        if len(non_zero_durations) > 0:
            logger.info(f"\nСтатистика длительности (сек) для промежуточных событий:")
            logger.info(f"  Среднее: {non_zero_durations.mean():.2f} сек ({non_zero_durations.mean() / 60:.2f} мин)")
            logger.info(
                f"  Медиана: {non_zero_durations.median():.0f} сек ({non_zero_durations.median() / 60:.2f} мин)")
            logger.info(f"  Мин: {non_zero_durations.min()} сек")
            logger.info(f"  Макс: {non_zero_durations.max():,} сек ({non_zero_durations.max() / 3600:.2f} ч)")
            logger.info(f"  Стд. откл.: {non_zero_durations.std():.2f} сек")

            # Перцентили
            percentiles = non_zero_durations.quantile([0.25, 0.5, 0.75, 0.90, 0.95, 0.99])
            logger.info(f"\nПерцентили:")
            logger.info(f"  25%: {percentiles[0.25]:.0f} сек")
            logger.info(f"  50%: {percentiles[0.50]:.0f} сек")
            logger.info(f"  75%: {percentiles[0.75]:.0f} сек")
            logger.info(f"  90%: {percentiles[0.90]:.0f} сек")
            logger.info(f"  95%: {percentiles[0.95]:.0f} сек")
            logger.info(f"  99%: {percentiles[0.99]:.0f} сек")

            # Распределение по интервалам
            bins = [0, 1, 5, 10, 30, 60, 300, 600, 1800, 3600, float('inf')]
            labels = ['0-1с', '1-5с', '5-10с', '10-30с', '30с-1м', '1-5м', '5-10м', '10-30м', '30м-1ч', '>1ч']

            self.merged_df['duration_bin'] = pd.cut(
                self.merged_df['duration_seconds'],
                bins=bins,
                labels=labels,
                include_lowest=True
            )

            duration_dist = self.merged_df[self.merged_df['duration_seconds'] > 0][
                'duration_bin'].value_counts().sort_index()

            logger.info(f"\nРаспределение длительности:")
            for interval, count in duration_dist.items():
                percentage = count / non_last_events * 100
                logger.info(f"  {interval}: {count:,} ({percentage:.1f}%)")

            # Удаляем временную колонку
            self.merged_df = self.merged_df.drop(columns=['duration_bin'])

            # Аномально долгие паузы (>30 минут)
            long_pauses = (self.merged_df['duration_seconds'] > 1800).sum()
            if long_pauses > 0:
                long_pauses_pct = long_pauses / total_events * 100
                logger.info(f"\n⚠️  Событий с длительностью >30 минут: {long_pauses:,} ({long_pauses_pct:.2f}%)")
                logger.info(f"    (пользователь долго оставался на экране или вышел из приложения)")

                # Примеры долгих пауз
                long_pause_examples = self.merged_df[self.merged_df['duration_seconds'] > 1800].nlargest(5,
                                                                                                         'duration_seconds')
                logger.info(f"\n    Топ-5 самых долгих задержек на экранах:")
                for idx, row in long_pause_examples.iterrows():
                    duration_hours = row['duration_seconds'] / 3600
                    logger.info(
                        f"      {duration_hours:.2f} ч - Экран: {row.get('Экран', 'N/A')}, Функционал: {row.get('Функционал', 'N/A')}")
        else:
            logger.warning("Нет событий с duration_seconds > 0")

        logger.info(f"{'=' * 60}\n")

        # Сохранение статистики
        self.stats['event_duration'] = {
            'total_events': int(total_events),
            'last_events': int(last_events),
            'non_last_events': int(non_last_events),
            'avg_duration_sec': float(non_zero_durations.mean()) if len(non_zero_durations) > 0 else 0,
            'median_duration_sec': float(non_zero_durations.median()) if len(non_zero_durations) > 0 else 0,
            'max_duration_sec': int(non_zero_durations.max()) if len(non_zero_durations) > 0 else 0,
            'long_pauses_count': int(long_pauses) if 'long_pauses' in locals() else 0
        }

        logger.info("✓ Расчёт длительности завершён успешно")

        return self.merged_df

    def remove_consecutive_duplicates_with_clicks(self) -> pd.DataFrame:
        """
        Удаление последовательных дубликатов в рамках сессии с подсчётом кликов

        Логика:
        - В рамках каждой global_session_id сравниваем только: Экран, Функционал, Действие
        - Если подряд идут одинаковые записи по этим 3 полям, оставляем только последнюю
        - В колонку click_count записываем количество кликов, где Действие != "Не указано"
        - В колонку duration_seconds записываем СУММУ длительностей всех записей в группе
        - В колонку dbl_duration_seconds записываем сумму duration_seconds из УДАЛЁННЫХ записей
        - В колонку dbl_count записываем количество УДАЛЁННЫХ дублей

        Пример:
        Было:
        | time  | Экран | Действие     | click_count | duration_seconds | dbl_duration | dbl_count |
        |-------|-------|--------------|-----------|------------------|--------------|-----------|
        | 10:00 | Еще   | Не указано   | 0         | 3                | 0            | 0         |
        | 10:03 | Еще   | Не указано   | 0         | 2                | 0            | 0         |
        | 10:05 | Еще   | Тап на кнопку| 0         | 5                | 0            | 0         |
        | 10:10 | Еще   | Тап на кнопку| 0         | 5                | 0            | 0         |

        Стало:
        | time  | Экран | Действие     | click_count | duration_seconds | dbl_duration | dbl_count |
        |-------|-------|--------------|-----------|------------------|--------------|-----------|
        | 10:10 | Еще   | Тап на кнопку| 2         | 15               | 10           | 3         |

        Расчёт:
        - click_count = 2 (только те, где Действие != "Не указано")
        - duration_seconds = 3 + 2 + 5 + 5 = 15 (сумма всех)
        - dbl_duration_seconds = 3 + 2 + 5 = 10 (сумма удалённых)
        - dbl_count = 3 (количество удалённых)

        Returns:
            DataFrame с удалёнными дубликатами и заполненными полями
        """
        logger.info("Удаление последовательных дубликатов с подсчётом кликов...")

        if self.merged_df is None or len(self.merged_df) == 0:
            raise ValueError("merged_df не загружен")

        if 'global_session_id' not in self.merged_df.columns:
            raise ValueError("Сначала выполните add_global_session_id()")

        # Сохранение исходного количества строк
        initial_rows = len(self.merged_df)
        logger.info(f"Исходное количество записей: {initial_rows:,}")

        # Добавление новых колонок
        self.merged_df['click_count'] = 0
        self.merged_df['dbl_duration_seconds'] = 0  # ← ДОБАВЛЕНО
        self.merged_df['dbl_count'] = 0  # ← ДОБАВЛЕНО

        # Проверка наличия duration_seconds
        if 'duration_seconds' not in self.merged_df.columns:
            logger.warning("Колонка 'duration_seconds' не найдена, будет создана со значением 0")
            self.merged_df['duration_seconds'] = 0

        # ВАЖНО: Сравниваем только по 3 колонкам
        comparison_cols = ['Экран', 'Функционал', 'Действие']
        time_col = 'Дата и время события'
        action_col = 'Действие'

        logger.info(f"Сравнение дубликатов только по колонкам: {comparison_cols}")

        # Проверка наличия необходимых колонок
        for col in comparison_cols:
            if col not in self.merged_df.columns:
                raise ValueError(f"Колонка '{col}' не найдена в данных")

        # Убедимся что данные отсортированы
        self.merged_df = self.merged_df.sort_values(
            by=['global_session_id', time_col],
            ascending=[True, True]
        ).reset_index(drop=True)

        logger.info("Данные отсортированы по global_session_id и времени")

        # ============================================================
        # ВЕКТОРИЗОВАННЫЙ ПОДХОД
        # ============================================================

        logger.info("Создание ключа для группировки дубликатов...")

        # Создаём составной ключ из 3 колонок для сравнения
        key_parts = []
        for col in comparison_cols:
            col_str = self.merged_df[col].astype(str).fillna('__NA__')
            key_parts.append(col_str)

        # Объединяем в один ключ
        self.merged_df['comparison_key'] = (
                key_parts[0] + '|' + key_parts[1] + '|' + key_parts[2]
        )

        logger.info("Поиск последовательных дубликатов...")

        # Получаем ключ предыдущей записи В ТОЙ ЖЕ СЕССИИ
        self.merged_df['prev_key'] = self.merged_df.groupby(
            'global_session_id',
            sort=False
        )['comparison_key'].shift(1)

        # Проверяем, является ли текущая запись дубликатом предыдущей
        self.merged_df['is_duplicate'] = (
                self.merged_df['comparison_key'] == self.merged_df['prev_key']
        ).fillna(False)

        duplicates_count = self.merged_df['is_duplicate'].sum()
        logger.info(f"Найдено последовательных дубликатов: {duplicates_count:,}")

        # ============================================================
        # СОЗДАНИЕ ГРУПП ПОСЛЕДОВАТЕЛЬНЫХ ДУБЛИКАТОВ
        # ============================================================

        logger.info("Группировка последовательных дубликатов...")

        # Создаём новую группу когда меняется сессия ИЛИ текущая запись НЕ дубликат
        self.merged_df['new_group'] = (
                (~self.merged_df['is_duplicate']) |
                (self.merged_df['global_session_id'] != self.merged_df['global_session_id'].shift(1))
        )

        # Присваиваем ID группам
        self.merged_df['group_id'] = self.merged_df['new_group'].cumsum()

        # ============================================================
        # ПОДСЧЁТ МЕТРИК ДЛЯ КАЖДОЙ ГРУППЫ
        # ============================================================

        logger.info("Подсчёт кликов и суммирование длительности...")

        # Создаём флаг: является ли Действие значимым (не "Не указано")
        self.merged_df['is_meaningful_action'] = (
                self.merged_df[action_col] != 'Не указано'
        )

        # Агрегация по группам
        group_stats = self.merged_df.groupby('group_id').agg({
            'is_duplicate': 'first',
            'global_session_id': 'count',  # Размер группы
            'is_meaningful_action': 'sum',  # Количество значимых действий
            'duration_seconds': 'sum'  # Сумма длительностей
        }).rename(columns={
            'global_session_id': 'group_size',
            'is_meaningful_action': 'meaningful_actions_count',
            'duration_seconds': 'total_duration'
        })

        # Маппинг статистики обратно в основной DataFrame
        self.merged_df['group_size'] = self.merged_df['group_id'].map(group_stats['group_size'])
        self.merged_df['meaningful_actions_count'] = self.merged_df['group_id'].map(
            group_stats['meaningful_actions_count'])
        self.merged_df['total_duration'] = self.merged_df['group_id'].map(group_stats['total_duration'])

        # Помечаем последнюю запись в каждой группе
        self.merged_df['rank_in_group'] = self.merged_df.groupby('group_id').cumcount(ascending=False)
        self.merged_df['is_last_in_group'] = (self.merged_df['rank_in_group'] == 0)

        # ============================================================
        # ЗАПОЛНЕНИЕ ПОЛЕЙ ДЛЯ ПОСЛЕДНЕЙ ЗАПИСИ В ГРУППЕ
        # ============================================================

        logger.info("Заполнение click_count, duration_seconds, dbl_duration_seconds, dbl_count...")

        last_in_group_mask = self.merged_df['is_last_in_group']

        # click_count = количество значимых действий в группе (минимум 1)
        self.merged_df.loc[last_in_group_mask, 'click_count'] = self.merged_df.loc[
            last_in_group_mask,
            'meaningful_actions_count'
        ].clip(lower=1)

        # duration_seconds = сумма всех длительностей в группе
        self.merged_df.loc[last_in_group_mask, 'duration_seconds'] = self.merged_df.loc[
            last_in_group_mask,
            'total_duration'
        ]

        # ← НОВАЯ ЛОГИКА: dbl_duration_seconds и dbl_count
        # Для каждой группы вычисляем сумму duration_seconds удалённых записей
        # Удалённые = все кроме последней = group_size - 1

        # Создаём маску для удаляемых записей в каждой группе
        self.merged_df['is_removed'] = ~self.merged_df['is_last_in_group']

        # Для каждой группы суммируем duration_seconds удалённых записей
        removed_duration_by_group = self.merged_df[self.merged_df['is_removed']].groupby('group_id')[
            'duration_seconds'].sum()

        # Для последней записи в группе записываем эту сумму
        self.merged_df.loc[last_in_group_mask, 'dbl_duration_seconds'] = self.merged_df.loc[
            last_in_group_mask,
            'group_id'
        ].map(removed_duration_by_group).fillna(0).astype('int32')

        # dbl_count = количество удалённых = group_size - 1
        self.merged_df.loc[last_in_group_mask, 'dbl_count'] = (
                self.merged_df.loc[last_in_group_mask, 'group_size'] - 1
        )

        # ============================================================
        # УДАЛЕНИЕ ДУБЛИКАТОВ
        # ============================================================

        rows_to_keep = self.merged_df['is_last_in_group']
        rows_to_remove_count = (~rows_to_keep).sum()

        logger.info(f"Записей для удаления: {rows_to_remove_count:,}")

        # Фильтрация
        self.merged_df = self.merged_df[rows_to_keep].copy()

        # Удаление временных колонок
        temp_cols = [
            'comparison_key', 'prev_key', 'is_duplicate',
            'new_group', 'group_id', 'group_size',
            'rank_in_group', 'is_last_in_group',
            'is_meaningful_action', 'meaningful_actions_count', 'total_duration',
            'is_removed'
        ]
        self.merged_df = self.merged_df.drop(columns=temp_cols)

        # Сброс индекса
        self.merged_df = self.merged_df.reset_index(drop=True)

        # ============================================================
        # СТАТИСТИКА
        # ============================================================

        final_rows = len(self.merged_df)
        removed_rows = initial_rows - final_rows
        removal_percentage = (removed_rows / initial_rows * 100) if initial_rows > 0 else 0

        logger.info(f"\n{'=' * 60}")
        logger.info("СТАТИСТИКА ДЕДУПЛИКАЦИИ:")
        logger.info(f"{'=' * 60}")
        logger.info(f"Исходное количество записей: {initial_rows:,}")
        logger.info(f"Удалено дубликатов: {removed_rows:,} ({removal_percentage:.2f}%)")
        logger.info(f"Осталось записей: {final_rows:,}")

        # Статистика по новым полям
        logger.info(f"\nСтатистика dbl_count (количество удалённых дублей):")
        logger.info(f"  Среднее: {self.merged_df['dbl_count'].mean():.2f}")
        logger.info(f"  Медиана: {self.merged_df['dbl_count'].median():.0f}")
        logger.info(f"  Макс: {self.merged_df['dbl_count'].max()}")

        logger.info(f"\nСтатистика dbl_duration_seconds (длительность удалённых):")
        logger.info(f"  Среднее: {self.merged_df['dbl_duration_seconds'].mean():.2f} сек")
        logger.info(f"  Медиана: {self.merged_df['dbl_duration_seconds'].median():.0f} сек")
        logger.info(f"  Макс: {self.merged_df['dbl_duration_seconds'].max():,} сек")
        logger.info(
            f"  Сумма: {self.merged_df['dbl_duration_seconds'].sum():,} сек ({self.merged_df['dbl_duration_seconds'].sum() / 3600:.2f} ч)")

        # Распределение dbl_count
        dbl_count_dist = self.merged_df['dbl_count'].value_counts().sort_index()
        logger.info(f"\nРаспределение dbl_count:")
        for count, freq in dbl_count_dist.head(10).items():
            percentage = freq / final_rows * 100
            logger.info(f"  {count} удалённых: {freq:,} ({percentage:.2f}%)")

        # Статистика по click_count
        click_count_dist = self.merged_df['click_count'].value_counts().sort_index()
        logger.info(f"\nРаспределение click_count:")
        for tries, count in click_count_dist.head(15).items():
            percentage = count / final_rows * 100
            logger.info(f"  {tries} {'клик' if tries == 1 else 'кликов'}: {count:,} ({percentage:.2f}%)")

        if len(click_count_dist) > 15:
            logger.info(f"  ... (всего уникальных значений: {len(click_count_dist)})")

        # Максимальное количество кликов
        max_tries = self.merged_df['click_count'].max()
        logger.info(f"\nМаксимальное количество кликов: {max_tries}")

        # Средний click_count
        avg_tries = self.merged_df['click_count'].mean()
        logger.info(f"Средний click_count: {avg_tries:.2f}")

        # Статистика по duration_seconds
        logger.info(f"\nСтатистика duration_seconds:")
        logger.info(f"  Среднее: {self.merged_df['duration_seconds'].mean():.2f} сек")
        logger.info(f"  Медиана: {self.merged_df['duration_seconds'].median():.0f} сек")
        logger.info(f"  Макс: {self.merged_df['duration_seconds'].max():,} сек")

        logger.info(f"{'=' * 60}\n")

        # Сохранение статистики
        self.stats['deduplication'] = {
            'initial_rows': int(initial_rows),
            'removed_rows': int(removed_rows),
            'final_rows': int(final_rows),
            'removal_percentage': float(removal_percentage),
            'max_tries': int(max_tries),
            'avg_tries': float(avg_tries),
            'avg_dbl_count': float(self.merged_df['dbl_count'].mean()),
            'avg_dbl_duration': float(self.merged_df['dbl_duration_seconds'].mean()),
            'click_count_distribution': {int(k): int(v) for k, v in click_count_dist.head(20).items()}
        }

        logger.info("✓ Дедупликация завершена успешно")

        return self.merged_df

    def collapse_intermediate_screens(self) -> pd.DataFrame:
        """
        Схлопывание записей с Действие="Не указано" на одном экране

        НОВАЯ ЛОГИКА:
        - В рамках каждой global_session_id ищем последовательности записей с одинаковым Экран
        - Внутри такой последовательности оставляем только записи где Действие != "Не указано"
        - Все записи с Действие="Не указано" удаляем
        - Сумму duration_seconds из удалённых записей добавляем к первой оставшейся записи

        Пример:
        Было:
        | time  | Экран        | Действие         | duration_seconds |
        |-------|--------------|------------------|------------------|
        | 10:00 | Новая заявка | Не указано       | 5                |
        | 10:05 | Новая заявка | Не указано       | 3                |
        | 10:08 | Новая заявка | Тап на кнопку    | 10               |
        | 10:18 | Новая заявка | Выбор категории  | 5                |
        | 10:23 | Заявки       | Не указано       | 0                |

        Стало:
        | time  | Экран        | Действие         | duration_seconds |
        |-------|--------------|------------------|------------------|
        | 10:08 | Новая заявка | Тап на кнопку    | 18 (10+5+3)      |
        | 10:18 | Новая заявка | Выбор категории  | 5                |
        | 10:23 | Заявки       | Не указано       | 0                |

        Returns:
            DataFrame с удалёнными "Не указано" и обновлённым duration_seconds
        """
        logger.info("Схлопывание записей с Действие='Не указано' на одном экране...")

        if self.merged_df is None or len(self.merged_df) == 0:
            raise ValueError("merged_df не загружен")

        if 'global_session_id' not in self.merged_df.columns:
            raise ValueError("Требуется global_session_id")

        # Проверка наличия duration_seconds
        if 'duration_seconds' not in self.merged_df.columns:
            logger.warning("Колонка 'duration_seconds' не найдена, будет создана со значением 0")
            self.merged_df['duration_seconds'] = 0

        # Сохранение исходного количества строк
        initial_rows = len(self.merged_df)
        logger.info(f"Исходное количество записей: {initial_rows:,}")

        time_col = 'Дата и время события'
        screen_col = 'Экран'
        action_col = 'Действие'
        not_specified = 'Не указано'

        # Убедимся что данные отсортированы
        self.merged_df = self.merged_df.sort_values(
            by=['global_session_id', time_col],
            ascending=[True, True]
        ).reset_index(drop=True)

        logger.info(f"Удаление записей с Действие='{not_specified}' на одном экране...")

        # ============================================================
        # ГРУППИРОВКА ПО ПОСЛЕДОВАТЕЛЬНОСТЯМ ОДНОГО ЭКРАНА
        # ============================================================

        # Создаём новую группу когда меняется экран или сессия
        self.merged_df['prev_screen'] = self.merged_df.groupby(
            'global_session_id',
            sort=False
        )[screen_col].shift(1)

        self.merged_df['screen_changed'] = (
                (self.merged_df[screen_col] != self.merged_df['prev_screen']) |
                (self.merged_df['global_session_id'] != self.merged_df['global_session_id'].shift(1))
        )

        self.merged_df['screen_group_id'] = self.merged_df['screen_changed'].cumsum()

        # ============================================================
        # ОБРАБОТКА КАЖДОЙ ГРУППЫ ЭКРАНА
        # ============================================================

        logger.info("Обработка последовательностей одного экрана...")

        # Маркируем записи на удаление
        self.merged_df['to_remove'] = False

        # Для каждой группы экрана
        for group_id, group_df in self.merged_df.groupby('screen_group_id', sort=False):
            group_indices = group_df.index.tolist()

            if len(group_indices) < 2:
                # Только одна запись - ничего не делаем
                continue

            # Находим записи с "Не указано" и без
            not_specified_mask = group_df[action_col] == not_specified
            not_specified_indices = group_df[not_specified_mask].index.tolist()
            meaningful_indices = group_df[~not_specified_mask].index.tolist()

            if len(not_specified_indices) == 0:
                # Нет записей с "Не указано" - ничего не делаем
                continue

            if len(meaningful_indices) == 0:
                # Все записи с "Не указано" - оставляем как есть
                continue

            # ============================================================
            # ЛОГИКА СХЛОПЫВАНИЯ
            # ============================================================

            # Суммируем duration_seconds из записей с "Не указано"
            sum_not_specified_duration = self.merged_df.loc[not_specified_indices, 'duration_seconds'].sum()

            # Находим первую значимую запись (где Действие != "Не указано")
            first_meaningful_idx = meaningful_indices[0]

            # Добавляем накопленную длительность к первой значимой записи
            self.merged_df.loc[first_meaningful_idx, 'duration_seconds'] += sum_not_specified_duration

            # Помечаем записи с "Не указано" на удаление
            self.merged_df.loc[not_specified_indices, 'to_remove'] = True

        # ============================================================
        # УДАЛЕНИЕ ЗАПИСЕЙ
        # ============================================================

        rows_to_remove_count = self.merged_df['to_remove'].sum()
        logger.info(f"Записей для удаления (Действие='{not_specified}'): {rows_to_remove_count:,}")

        # Фильтрация
        self.merged_df = self.merged_df[~self.merged_df['to_remove']].copy()

        # Удаление временных колонок
        self.merged_df = self.merged_df.drop(columns=['prev_screen', 'screen_changed', 'screen_group_id', 'to_remove'])

        # Сброс индекса
        self.merged_df = self.merged_df.reset_index(drop=True)

        # ============================================================
        # СТАТИСТИКА
        # ============================================================

        final_rows = len(self.merged_df)
        removed_rows = initial_rows - final_rows
        removal_percentage = (removed_rows / initial_rows * 100) if initial_rows > 0 else 0

        logger.info(f"\n{'=' * 60}")
        logger.info("СТАТИСТИКА СХЛОПЫВАНИЯ:")
        logger.info(f"{'=' * 60}")
        logger.info(f"Исходное количество записей: {initial_rows:,}")
        logger.info(f"Удалено записей с Действие='{not_specified}': {removed_rows:,} ({removal_percentage:.2f}%)")
        logger.info(f"Осталось записей: {final_rows:,}")

        # Сколько "Не указано" осталось
        remaining_not_specified = (self.merged_df[action_col] == not_specified).sum()
        remaining_not_specified_pct = (remaining_not_specified / final_rows * 100) if final_rows > 0 else 0
        logger.info(
            f"\nОставшихся записей с Действие='{not_specified}': {remaining_not_specified:,} ({remaining_not_specified_pct:.2f}%)")
        logger.info(f"(это записи, где на экране не было других действий)")

        # Статистика по duration_seconds
        logger.info(f"\nСтатистика duration_seconds после схлопывания:")
        logger.info(f"  Среднее: {self.merged_df['duration_seconds'].mean():.2f} сек")
        logger.info(f"  Медиана: {self.merged_df['duration_seconds'].median():.0f} сек")
        logger.info(
            f"  Макс: {self.merged_df['duration_seconds'].max():,} сек ({self.merged_df['duration_seconds'].max() / 3600:.2f} ч)")
        logger.info(
            f"  Сумма: {self.merged_df['duration_seconds'].sum():,} сек ({self.merged_df['duration_seconds'].sum() / 3600:.2f} ч)")

        # Топ-10 экранов по среднему duration_seconds
        avg_duration_by_screen = self.merged_df.groupby(screen_col, observed=True)['duration_seconds'].mean().sort_values(
            ascending=False)
        logger.info(f"\nТоп-10 экранов по средней длительности:")
        for screen, avg_dur in avg_duration_by_screen.head(10).items():
            count = (self.merged_df[screen_col] == screen).sum()
            logger.info(f"  {screen}: {avg_dur:.1f} сек (встречается {count:,} раз)")

        logger.info(f"{'=' * 60}\n")

        # Сохранение статистики
        self.stats['collapse_screens'] = {
            'initial_rows': int(initial_rows),
            'removed_rows': int(removed_rows),
            'final_rows': int(final_rows),
            'removal_percentage': float(removal_percentage),
            'remaining_not_specified': int(remaining_not_specified),
            'avg_duration_sec': float(self.merged_df['duration_seconds'].mean())
        }

        logger.info("✓ Схлопывание записей завершено успешно")

        return self.merged_df

    def save_unique_values_json(self, output_path: str = None) -> Dict:
        """
        Сохранение уникальных значений колонок Экран, Функционал, Действие в JSON

        Создаёт JSON файл с 3 массивами уникальных значений,
        отсортированными по частоте использования (от самых частых к редким).

        Args:
            output_path: путь к выходному JSON файлу
                        (если None, используется reports/json/unique_values.json)

        Returns:
            Словарь с уникальными значениями и их частотами
        """
        logger.info("Сохранение уникальных значений в JSON...")

        if self.merged_df is None or len(self.merged_df) == 0:
            raise ValueError("merged_df не загружен")

        # Определяем путь к выходному файлу
        if output_path is None:
            json_path = Path(self.config['reports']['json_path'])
            json_path.mkdir(parents=True, exist_ok=True)
            output_path = json_path / 'unique_values.json'
        else:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

        # Колонки для анализа
        target_columns = ['Экран', 'Функционал', 'Действие']

        # Проверка наличия колонок
        for col in target_columns:
            if col not in self.merged_df.columns:
                raise ValueError(f"Колонка '{col}' не найдена в данных")

        logger.info(f"Анализ колонок: {', '.join(target_columns)}")

        # ============================================================
        # СБОР УНИКАЛЬНЫХ ЗНАЧЕНИЙ С ЧАСТОТАМИ
        # ============================================================

        result = {
            'metadata': {
                'generated_at': pd.Timestamp.now().isoformat(),
                'total_records': int(len(self.merged_df)),
                'date_range': {
                    'min': str(self.merged_df[
                                   'Дата и время события'].min()) if 'Дата и время события' in self.merged_df.columns else None,
                    'max': str(self.merged_df[
                                   'Дата и время события'].max()) if 'Дата и время события' in self.merged_df.columns else None
                }
            },
            'unique_values': {}
        }

        for col in target_columns:
            logger.info(f"Обработка колонки '{col}'...")

            # Подсчёт частот
            value_counts = self.merged_df[col].value_counts()

            # Формирование списка с частотами
            values_with_frequency = []
            for value, count in value_counts.items():
                values_with_frequency.append({
                    'value': str(value),
                    'count': int(count),
                    'percentage': round(count / len(self.merged_df) * 100, 2)
                })

            # Статистика
            total_unique = len(values_with_frequency)
            logger.info(f"  Уникальных значений: {total_unique}")
            logger.info(f"  Топ-3: {', '.join([v['value'] for v in values_with_frequency[:3]])}")

            # Сохранение в результат
            result['unique_values'][col] = {
                'total_unique': total_unique,
                'values': values_with_frequency
            }

        # ============================================================
        # ДОПОЛНИТЕЛЬНАЯ СТАТИСТИКА
        # ============================================================

        # Топ-10 комбинаций (Экран + Функционал + Действие)
        logger.info("Подсчёт топ-10 комбинаций...")

        combinations = self.merged_df.groupby(target_columns).size().reset_index(name='count')
        combinations = combinations.sort_values('count', ascending=False).head(10)

        top_combinations = []
        for _, row in combinations.iterrows():
            top_combinations.append({
                'Экран': str(row['Экран']),
                'Функционал': str(row['Функционал']),
                'Действие': str(row['Действие']),
                'count': int(row['count']),
                'percentage': round(row['count'] / len(self.merged_df) * 100, 2)
            })

        result['top_combinations'] = top_combinations

        # ============================================================
        # СОХРАНЕНИЕ В JSON
        # ============================================================

        logger.info(f"Сохранение в файл: {output_path}")

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        file_size = output_path.stat().st_size / 1024
        logger.info(f"✓ Файл сохранён: {output_path} ({file_size:.2f} KB)")

        # ============================================================
        # ВЫВОД СТАТИСТИКИ В ЛОГ
        # ============================================================

        logger.info(f"\n{'=' * 60}")
        logger.info("СТАТИСТИКА УНИКАЛЬНЫХ ЗНАЧЕНИЙ:")
        logger.info(f"{'=' * 60}")

        for col in target_columns:
            col_data = result['unique_values'][col]
            logger.info(f"\n{col}:")
            logger.info(f"  Всего уникальных: {col_data['total_unique']}")
            logger.info(f"  Топ-5 по частоте:")
            for i, item in enumerate(col_data['values'][:5], 1):
                logger.info(f"    {i}. {item['value']}: {item['count']:,} ({item['percentage']:.1f}%)")

        logger.info(f"\nТоп-3 комбинации (Экран + Функционал + Действие):")
        for i, combo in enumerate(top_combinations[:3], 1):
            logger.info(f"  {i}. {combo['Экран']} | {combo['Функционал']} | {combo['Действие']}")
            logger.info(f"     {combo['count']:,} раз ({combo['percentage']:.1f}%)")

        logger.info(f"{'=' * 60}\n")

        logger.info("✓ Сохранение уникальных значений завершено успешно")

        return result

    def save_unique_combinations_json(self, output_path: str = None) -> Dict:
        """
        Сохранение уникальных комбинаций Экран => Функционал => Действие в JSON

        Создаёт JSON файл с массивом уникальных комбинаций значений,
        отсортированных по частоте использования (от самых частых к редким).
        Комбинация представлена как единая строка с разделителем " => ".

        Args:
            output_path: путь к выходному JSON файлу
                        (если None, используется reports/json/unique_combinations.json)

        Returns:
            Словарь с уникальными комбинациями и их частотами
        """
        logger.info("Сохранение уникальных комбинаций в JSON...")

        if self.merged_df is None or len(self.merged_df) == 0:
            raise ValueError("merged_df не загружен")

        # Определяем путь к выходному файлу
        if output_path is None:
            json_path = Path(self.config['reports']['json_path'])
            json_path.mkdir(parents=True, exist_ok=True)
            output_path = json_path / 'unique_combinations.json'
        else:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

        # Колонки для анализа
        target_columns = ['Экран', 'Функционал', 'Действие']
        separator = ' => '

        # Проверка наличия колонок
        for col in target_columns:
            if col not in self.merged_df.columns:
                raise ValueError(f"Колонка '{col}' не найдена в данных")

        logger.info(f"Анализ комбинаций: {separator.join(target_columns)}")

        # ============================================================
        # СОЗДАНИЕ КОМБИНИРОВАННЫХ СТРОК
        # ============================================================

        logger.info("Создание комбинированных строк...")

        # Создаём единую строку для каждой комбинации
        self.merged_df['_combination'] = (
                self.merged_df['Экран'].astype(str) + separator +
                self.merged_df['Функционал'].astype(str) + separator +
                self.merged_df['Действие'].astype(str)
        )

        # Подсчёт частот
        combination_counts = self.merged_df['_combination'].value_counts()

        logger.info(f"Найдено уникальных комбинаций: {len(combination_counts):,}")

        # ============================================================
        # ФОРМИРОВАНИЕ РЕЗУЛЬТАТА
        # ============================================================

        total_records = len(self.merged_df)

        combinations_list = []
        for combination, count in combination_counts.items():
            combinations_list.append({
                'path': str(combination),
                'count': int(count),
                'percentage': round(count / total_records * 100, 4)
            })

        # Метаданные
        result = {
            'metadata': {
                'generated_at': pd.Timestamp.now().isoformat(),
                'total_records': int(total_records),
                'total_unique_combinations': len(combinations_list),
                'separator': separator,
                'columns': target_columns,
                'date_range': {
                    'min': str(self.merged_df[
                                   'Дата и время события'].min()) if 'Дата и время события' in self.merged_df.columns else None,
                    'max': str(self.merged_df[
                                   'Дата и время события'].max()) if 'Дата и время события' in self.merged_df.columns else None
                }
            },
            'combinations': combinations_list
        }

        # Удаляем временную колонку
        self.merged_df = self.merged_df.drop(columns=['_combination'])

        # ============================================================
        # ДОПОЛНИТЕЛЬНАЯ СТАТИСТИКА
        # ============================================================

        # Концентрация: сколько % событий покрывают топ-N комбинаций
        cumulative_percentage = 0
        coverage_stats = {}

        for threshold in [10, 20, 50, 100]:
            if threshold <= len(combinations_list):
                cumulative_percentage = sum(c['percentage'] for c in combinations_list[:threshold])
                coverage_stats[f'top_{threshold}'] = {
                    'combinations': threshold,
                    'coverage_percentage': round(cumulative_percentage, 2)
                }

        result['coverage_stats'] = coverage_stats

        # ============================================================
        # СОХРАНЕНИЕ В JSON
        # ============================================================

        logger.info(f"Сохранение в файл: {output_path}")

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        file_size = output_path.stat().st_size / 1024
        logger.info(f"✓ Файл сохранён: {output_path} ({file_size:.2f} KB)")

        # ============================================================
        # ВЫВОД СТАТИСТИКИ В ЛОГ
        # ============================================================

        logger.info(f"\n{'=' * 60}")
        logger.info("СТАТИСТИКА УНИКАЛЬНЫХ КОМБИНАЦИЙ:")
        logger.info(f"{'=' * 60}")
        logger.info(f"Всего записей: {total_records:,}")
        logger.info(f"Уникальных комбинаций: {len(combinations_list):,}")
        logger.info(f"Средняя частота комбинации: {total_records / len(combinations_list):.2f}")

        logger.info(f"\nТоп-10 комбинаций:")
        for i, combo in enumerate(combinations_list[:10], 1):
            logger.info(f"  {i:2d}. {combo['path']}")
            logger.info(f"      {combo['count']:,} раз ({combo['percentage']:.2f}%)")

        logger.info(f"\nПокрытие данных:")
        for key, stats in coverage_stats.items():
            n = stats['combinations']
            pct = stats['coverage_percentage']
            logger.info(f"  Топ-{n:3d} комбинаций покрывают {pct:5.2f}% всех событий")

        # Самые редкие комбинации
        rare_combinations = [c for c in combinations_list if c['count'] == 1]
        if len(rare_combinations) > 0:
            rare_pct = len(rare_combinations) / len(combinations_list) * 100
            logger.info(f"\nРедкие комбинации (встречаются 1 раз): {len(rare_combinations):,} ({rare_pct:.1f}%)")
            logger.info(f"  Примеры:")
            for combo in rare_combinations[:3]:
                logger.info(f"    - {combo['path']}")

        logger.info(f"{'=' * 60}\n")

        logger.info("✓ Сохранение уникальных комбинаций завершено успешно")

        return result

    def remove_trailing_empty_screens(self) -> pd.DataFrame:
        """
        Удаление последних записей "Еще → Открытие экрана → Не указано" в сессиях

        Логика:
        - В рамках каждой global_session_id проверяем последнюю запись
        - Если последняя запись имеет:
          * Экран = "Еще"
          * Функционал = "Открытие экрана"
          * Действие = "Не указано"
        - То удаляем эту запись

        Интерпретация: Пользователь открыл меню "Еще" и вышел, ничего не выбрав.
        Это "мёртвая точка" сессии без полезной информации.

        Пример:
        Было:
        | session | time  | Экран        | Функционал        | Действие    |
        |---------|-------|--------------|-------------------|-------------|
        | 1       | 10:00 | Заявки       | Просмотр списка   | Тап на заявку |
        | 1       | 10:05 | Еще          | Открытие экрана   | Не указано  | ← УДАЛИТЬ

        Стало:
        | session | time  | Экран        | Функционал        | Действие    |
        |---------|-------|--------------|-------------------|-------------|
        | 1       | 10:00 | Заявки       | Просмотр списка   | Тап на заявку |

        Returns:
            DataFrame с удалёнными бесполезными последними записями
        """
        logger.info("Удаление последних записей 'Еще → Открытие экрана → Не указано'...")

        if self.merged_df is None or len(self.merged_df) == 0:
            raise ValueError("merged_df не загружен")

        if 'global_session_id' not in self.merged_df.columns:
            raise ValueError("Требуется global_session_id (выполните add_global_session_id)")

        # Сохранение исходного количества строк
        initial_rows = len(self.merged_df)
        logger.info(f"Исходное количество записей: {initial_rows:,}")

        time_col = 'Дата и время события'
        screen_col = 'Экран'
        function_col = 'Функционал'
        action_col = 'Действие'

        target_screen = 'Еще'
        target_function = 'Открытие экрана'
        target_action = 'Не указано'

        # Проверка наличия необходимых колонок
        required_cols = [screen_col, function_col, action_col]
        for col in required_cols:
            if col not in self.merged_df.columns:
                raise ValueError(f"Колонка '{col}' не найдена в данных")

        # Убедимся что данные отсортированы
        self.merged_df = self.merged_df.sort_values(
            by=['global_session_id', time_col],
            ascending=[True, True]
        ).reset_index(drop=True)

        logger.info("Данные отсортированы по global_session_id и времени")

        # ============================================================
        # ПОИСК ПОСЛЕДНИХ ЗАПИСЕЙ С УСЛОВИЕМ
        # ============================================================

        logger.info(f"Поиск последних записей с условием:")
        logger.info(f"  Экран = '{target_screen}'")
        logger.info(f"  Функционал = '{target_function}'")
        logger.info(f"  Действие = '{target_action}'")

        # Помечаем последнюю запись в каждой сессии
        self.merged_df['is_last_in_session'] = False
        last_indices = self.merged_df.groupby('global_session_id', sort=False).tail(1).index
        self.merged_df.loc[last_indices, 'is_last_in_session'] = True

        # Проверяем условие для последних записей
        self.merged_df['to_remove'] = (
                self.merged_df['is_last_in_session'] &
                (self.merged_df[screen_col] == target_screen) &
                (self.merged_df[function_col] == target_function) &
                (self.merged_df[action_col] == target_action)
        )

        rows_to_remove_count = self.merged_df['to_remove'].sum()

        logger.info(f"Найдено записей для удаления: {rows_to_remove_count:,}")

        if rows_to_remove_count == 0:
            logger.info("⚠️  Записей для удаления не найдено")
            self.merged_df = self.merged_df.drop(columns=['is_last_in_session', 'to_remove'])
            return self.merged_df

        # ============================================================
        # СТАТИСТИКА ДО УДАЛЕНИЯ
        # ============================================================

        # Получаем информацию об удаляемых записях
        records_to_remove = self.merged_df[self.merged_df['to_remove']].copy()

        # Средняя длина сессии с таким окончанием
        sessions_with_removal = records_to_remove['global_session_id'].unique()
        session_lengths = self.merged_df[
            self.merged_df['global_session_id'].isin(sessions_with_removal)
        ].groupby('global_session_id').size()

        avg_session_length = session_lengths.mean()

        # Какие ещё экраны были в этих сессиях
        other_screens_in_affected_sessions = self.merged_df[
            (self.merged_df['global_session_id'].isin(sessions_with_removal)) &
            (~self.merged_df['to_remove'])
            ][screen_col].value_counts().head(5)

        # ============================================================
        # УДАЛЕНИЕ ЗАПИСЕЙ
        # ============================================================

        logger.info("Удаление записей...")

        # Фильтрация
        self.merged_df = self.merged_df[~self.merged_df['to_remove']].copy()

        # Удаление временных колонок
        self.merged_df = self.merged_df.drop(columns=['is_last_in_session', 'to_remove'])

        # Сброс индекса
        self.merged_df = self.merged_df.reset_index(drop=True)

        # ============================================================
        # СТАТИСТИКА ПОСЛЕ УДАЛЕНИЯ
        # ============================================================

        final_rows = len(self.merged_df)
        removed_rows = initial_rows - final_rows
        removal_percentage = (removed_rows / initial_rows * 100) if initial_rows > 0 else 0

        # Количество затронутых сессий
        affected_sessions_count = len(sessions_with_removal)
        total_sessions = self.merged_df['global_session_id'].nunique()
        affected_sessions_pct = (affected_sessions_count / (
                    total_sessions + affected_sessions_count) * 100) if total_sessions > 0 else 0

        logger.info(f"\n{'=' * 60}")
        logger.info("СТАТИСТИКА УДАЛЕНИЯ ПОСЛЕДНИХ 'ПУСТЫХ' ЗАПИСЕЙ:")
        logger.info(f"{'=' * 60}")
        logger.info(f"Исходное количество записей: {initial_rows:,}")
        logger.info(f"Удалено последних записей: {removed_rows:,} ({removal_percentage:.2f}%)")
        logger.info(f"Осталось записей: {final_rows:,}")

        logger.info(f"\nЗатронуто сессий: {affected_sessions_count:,} ({affected_sessions_pct:.2f}%)")
        logger.info(f"  Средняя длина затронутых сессий: {avg_session_length:.1f} событий")

        logger.info(f"\nИнтерпретация:")
        logger.info(f"  {affected_sessions_count:,} сессий закончились на 'Еще → Открытие → Не указано'")
        logger.info(f"  Это означает что пользователь открыл меню 'Еще' и вышел из приложения")
        logger.info(f"  Такие записи не несут полезной информации и были удалены")

        if len(other_screens_in_affected_sessions) > 0:
            logger.info(f"\nТоп-5 экранов в затронутых сессиях (до 'Еще'):")
            for screen, count in other_screens_in_affected_sessions.items():
                percentage = count / other_screens_in_affected_sessions.sum() * 100
                logger.info(f"  {screen}: {count:,} ({percentage:.1f}%)")

        # Проверка на "пустые" сессии (если после удаления осталась только 1 запись)
        session_lengths_after = self.merged_df.groupby('global_session_id').size()
        single_event_sessions = (session_lengths_after == 1).sum()

        if single_event_sessions > 0:
            single_event_pct = single_event_sessions / total_sessions * 100
            logger.info(
                f"\n⚠️  После удаления появились сессии с 1 событием: {single_event_sessions:,} ({single_event_pct:.2f}%)")
            logger.info(f"    Рекомендуется удалить такие сессии отдельным методом")

        logger.info(f"{'=' * 60}\n")

        # Сохранение статистики
        self.stats['remove_trailing_empty'] = {
            'initial_rows': int(initial_rows),
            'removed_rows': int(removed_rows),
            'final_rows': int(final_rows),
            'removal_percentage': float(removal_percentage),
            'affected_sessions': int(affected_sessions_count),
            'avg_session_length': float(avg_session_length),
            'single_event_sessions_after': int(single_event_sessions)
        }

        logger.info("✓ Удаление последних 'пустых' записей завершено успешно")

        return self.merged_df

    def fix_action_functional_typos(self) -> pd.DataFrame:
        """
        Исправление опечаток в колонках Действие и Функционал

        Логика:
        - В колонке Действие:
          * Заменить "выбор тега 1" на "Выбор тега 1"
          * Заменить "Тап на услугу партнёров" на "Тап на услугу партнеров"
          * Заменить "'Тап на кнопку 'Мои'" на "Тап на кнопку 'Мои'"
        - В колонке Функционал:
          * Заменить "Выбор услуги партнёров" на "Выбор услуги партнеров"
          * В "Переход к предоставлению доступа через +" заменить Действие "Тап на кнопку" на "Тап на кнопку '+'"
          * В "Отмена отзыва доступа" заменить Действие "Тап на кнопку 'Отмена'" на "Тап на кнопку 'Отменить'"

        Returns:
            DataFrame с исправленными опечатками
        """
        logger.info("Исправление опечаток в колонках Действие и Функционал...")

        if self.merged_df is None or len(self.merged_df) == 0:
            raise ValueError("merged_df не загружен")

        # Проверка наличия необходимых колонок
        action_col = 'Действие'
        function_col = 'Функционал'

        for col in [action_col, function_col]:
            if col not in self.merged_df.columns:
                raise ValueError(f"Колонка '{col}' не найдена в данных")

        # Сохранение исходного количества строк
        initial_rows = len(self.merged_df)
        logger.info(f"Исходное количество записей: {initial_rows:,}")

        # ============================================================
        # ИСПРАВЛЕНИЕ ОПЕЧАТОК
        # ============================================================

        corrections = [
            # В Действие
            (action_col, "выбор тега 1", "Выбор тега 1"),
            (action_col, "Тап на услугу партнёров", "Тап на услугу партнеров"),
            (action_col, "'Тап на кнопку 'Мои'", "Тап на кнопку 'Мои'"),
            # В Функционал
            (function_col, "Выбор услуги партнёров", "Выбор услуги партнеров"),
            # В Функционал с условием на Действие
            (function_col, "Переход к предоставлению доступа через +", None,
             action_col, "Тап на кнопку", "Тап на кнопку '+'"),
            (function_col, "Отмена отзыва доступа", None,
             action_col, "Тап на кнопку 'Отмена'", "Тап на кнопку 'Отменить'"),
        ]
        total_corrections = 0
        for correction in corrections:
            if len(correction) == 3:
                col, old_value, new_value = correction
                mask = self.merged_df[col] == old_value
            elif len(correction) == 6:
                col, target_function, _, action_col_cond, old_action, new_action = correction
                mask = (self.merged_df[col] == target_function) & (self.merged_df[action_col_cond] == old_action)
            else:
                continue

            count_corrections = mask.sum()
            if count_corrections > 0:
                self.merged_df.loc[mask, col] = new_value
                total_corrections += count_corrections
                logger.info(f"Исправлено {count_corrections:,} записей в колонке '{col}': '{old_value}' → '{new_value}'")
            else:
                logger.info(f"Записей для исправления в колонке '{col}' с значением '{old_value}' не найдено")
        if total_corrections == 0:
            logger.info("⚠️  Не найдено записей для исправления опечаток")
        else:
            logger.info(f"Всего исправлено записей: {total_corrections:,}")
        # ============================================================
        # СТАТИСТИКА ПОСЛЕ ИСПРАВЛЕНИЯ
        # ============================================================
        final_rows = len(self.merged_df)
        if final_rows != initial_rows:
            logger.warning("Количество записей изменилось после исправления опечаток, что не должно происходить")
        logger.info(f"\n{'=' * 60}")
        logger.info("СТАТИСТИКА ИСПРАВЛЕНИЯ ОПЧАТОК:")
        logger.info(f"{'=' * 60}")
        logger.info(f"Исходное количество записей: {initial_rows:,}")
        logger.info(f"Осталось записей: {final_rows:,}")
        logger.info(f"{'=' * 60}\n")
        # Сохранение статистики
        self.stats['fix_action_functional_typos'] = {
            'initial_rows': int(initial_rows),
            'final_rows': int(final_rows),
            'total_corrections': int(total_corrections)
        }
        logger.info("✓ Исправление опечаток завершено успешно")
        return self.merged_df

    def add_user_cohort_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Добавление колонок статуса пользователя на основе активности по месяцам

        Создает 3 boolean колонки в разрезе пользователя (Идентификатор устройства):
        - is_lost: пользователь был активен в сентябре, но НЕ был в октябре
        - is_stay: пользователь был активен И в сентябре, И в октябре (удержанные)
        - is_new: пользователь был активен ТОЛЬКО в октябре (новые)

        Логика классификации:
        ┌─────────────┬─────────┬────────┐
        │   Статус    │ Сентябрь│ Октябрь│
        ├─────────────┼─────────┼────────┤
        │ is_lost     │    ✓    │   ✗    │
        │ is_stay     │    ✓    │   ✓    │
        │ is_new      │    ✗    │   ✓    │
        └─────────────┴─────────┴────────┘

        Все строки одного пользователя получают одинаковые значения флагов.

        Args:
            df: DataFrame с колонками 'Дата и время события' и 'Идентификатор устройства'

        Returns:
            DataFrame с добавленными колонками is_lost, is_stay, is_new

        Raises:
            ValueError: если отсутствуют необходимые колонки

        Example:
            >>> df = add_user_cohort_status(df)
            >>> # Анализ оттока
            >>> churn_rate = df['is_lost'].mean()
            >>> # Анализ удержания
            >>> retention_rate = df['is_stay'].mean()
            >>> # Анализ роста
            >>> new_users_rate = df['is_new'].mean()
        """
        logger.info("=" * 70)
        logger.info("ДОБАВЛЕНИЕ КОГОРТНОГО СТАТУСА ПОЛЬЗОВАТЕЛЕЙ")
        logger.info("=" * 70)

        # Проверка входных данных
        if df is None or len(df) == 0:
            raise ValueError("DataFrame пустой или None")

        required_cols = ['Дата и время события', 'Идентификатор устройства']
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            raise ValueError(f"Отсутствуют необходимые колонки: {missing_cols}")

        logger.info(f"Исходный датасет: {len(df):,} строк")
        logger.info(f"Уникальных пользователей: {df['Идентификатор устройства'].nunique():,}")

        # Создаем копию для работы
        result_df = df.copy()

        # Преобразуем в datetime если нужно
        if not pd.api.types.is_datetime64_any_dtype(result_df['Дата и время события']):
            logger.info("Преобразование 'Дата и время события' в datetime...")
            result_df['Дата и время события'] = pd.to_datetime(
                result_df['Дата и время события'],
                utc=True,
                errors='coerce'
            )

        # Извлекаем месяц
        logger.info("Определение месяцев активности пользователей...")
        result_df['_temp_month'] = result_df['Дата и время события'].dt.month

        # Для каждого пользователя определяем в каких месяцах он был активен
        user_months = result_df.groupby('Идентификатор устройства')['_temp_month'].apply(
            lambda x: set(x.dropna().unique())
        ).to_dict()

        logger.info("Классификация пользователей по когортам...")

        # Определяем статус для каждого пользователя
        user_status = {}

        stats = {
            'lost': 0,  # Был в сентябре, нет в октябре
            'stay': 0,  # Был в сентябре и октябре
            'new': 0,  # Только в октябре
            'other': 0  # Другие случаи (например, только в августе)
        }

        for user_id, months in user_months.items():
            has_september = 9 in months
            has_october = 10 in months

            # Определяем статус
            if has_september and not has_october:
                # Потерянный пользователь
                user_status[user_id] = {
                    'is_lost': True,
                    'is_stay': False,
                    'is_new': False
                }
                stats['lost'] += 1

            elif has_september and has_october:
                # Удержанный пользователь
                user_status[user_id] = {
                    'is_lost': False,
                    'is_stay': True,
                    'is_new': False
                }
                stats['stay'] += 1

            elif not has_september and has_october:
                # Новый пользователь
                user_status[user_id] = {
                    'is_lost': False,
                    'is_stay': False,
                    'is_new': True
                }
                stats['new'] += 1

            else:
                # Другие случаи (не был ни в сентябре, ни в октябре)
                user_status[user_id] = {
                    'is_lost': False,
                    'is_stay': False,
                    'is_new': False
                }
                stats['other'] += 1

        # Применяем статусы ко всем строкам пользователя
        logger.info("Применение статусов к датасету...")

        result_df['is_lost'] = result_df['Идентификатор устройства'].map(
            lambda x: user_status.get(x, {}).get('is_lost', False)
        )
        result_df['is_stay'] = result_df['Идентификатор устройства'].map(
            lambda x: user_status.get(x, {}).get('is_stay', False)
        )
        result_df['is_new'] = result_df['Идентификатор устройства'].map(
            lambda x: user_status.get(x, {}).get('is_new', False)
        )

        # Удаляем временную колонку
        result_df.drop('_temp_month', axis=1, inplace=True)

        # Статистика
        total_users = len(user_status)

        logger.info("\n" + "=" * 70)
        logger.info("СТАТИСТИКА ПО КОГОРТАМ:")
        logger.info("=" * 70)
        logger.info(f"Всего уникальных пользователей: {total_users:,}")
        logger.info("")
        logger.info(f"  🔴 Потерянные (is_lost):   {stats['lost']:6,} ({100 * stats['lost'] / total_users:5.2f}%)")
        logger.info(f"  🟢 Удержанные (is_stay):   {stats['stay']:6,} ({100 * stats['stay'] / total_users:5.2f}%)")
        logger.info(f"  🔵 Новые (is_new):         {stats['new']:6,} ({100 * stats['new'] / total_users:5.2f}%)")

        if stats['other'] > 0:
            logger.info(f"  ⚪ Прочие:                 {stats['other']:6,} ({100 * stats['other'] / total_users:5.2f}%)")

        logger.info("")

        # Дополнительная статистика по строкам
        logger.info("РАСПРЕДЕЛЕНИЕ СТРОК ПО КОГОРТАМ:")
        logger.info("=" * 70)
        logger.info(f"  Строк потерянных:  {result_df['is_lost'].sum():8,} ({100 * result_df['is_lost'].mean():5.2f}%)")
        logger.info(f"  Строк удержанных:  {result_df['is_stay'].sum():8,} ({100 * result_df['is_stay'].mean():5.2f}%)")
        logger.info(f"  Строк новых:       {result_df['is_new'].sum():8,} ({100 * result_df['is_new'].mean():5.2f}%)")
        logger.info("=" * 70)

        # Ключевые метрики
        if stats['lost'] + stats['stay'] > 0:
            churn_rate = 100 * stats['lost'] / (stats['lost'] + stats['stay'])
            retention_rate = 100 * stats['stay'] / (stats['lost'] + stats['stay'])

            logger.info("\nКЛЮЧЕВЫЕ МЕТРИКИ:")
            logger.info("=" * 70)
            logger.info(f"  Churn Rate (отток):       {churn_rate:5.2f}%")
            logger.info(f"  Retention Rate (удержание): {retention_rate:5.2f}%")
            logger.info(f"  Growth (новые пользователи): {stats['new']:,}")
            logger.info("=" * 70)

        logger.info("\n✓ Когортные статусы успешно добавлены")
        logger.info(f"✓ Добавлено колонок: 3 (is_lost, is_stay, is_new)")
        logger.info(f"✓ Итоговый размер: {len(result_df):,} строк × {len(result_df.columns)} колонок\n")

        return result_df
