"""
Вспомогательные функции для проекта
"""

import logging
import yaml
from pathlib import Path
from datetime import datetime
from typing import Dict, Any


def setup_logging(config: dict = None) -> None:
    """
    Настройка логирования для проекта
    
    Args:
        config: конфигурация логирования
    """
    if config is None:
        config = {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        }
    
    log_level = getattr(logging, config.get('level', 'INFO'))
    log_format = config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(config.get('file', 'analysis.log'), encoding='utf-8')
        ]
    )


def load_config(config_path: str = "config.yaml") -> dict:
    """
    Загрузка конфигурации из YAML файла
    
    Args:
        config_path: путь к файлу конфигурации
        
    Returns:
        Словарь с конфигурацией
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        logging.warning(f"Файл конфигурации {config_path} не найден, используем значения по умолчанию")
        return {}


def ensure_directories(config: dict) -> None:
    """
    Создание необходимых директорий
    
    Args:
        config: конфигурация с путями
    """
    dirs_to_create = [
        config.get('data', {}).get('processed_path', 'data/processed'),
        config.get('reports', {}).get('json_path', 'reports/json'),
        config.get('reports', {}).get('html_path', 'reports/html'),
        config.get('reports', {}).get('figures_path', 'reports/figures')
    ]
    
    for dir_path in dirs_to_create:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        logging.info(f"Директория создана/проверена: {dir_path}")


def format_number(num: float, decimals: int = 2) -> str:
    """
    Форматирование числа с разделителями тысяч
    
    Args:
        num: число для форматирования
        decimals: количество десятичных знаков
        
    Returns:
        Отформатированная строка
    """
    return f"{num:,.{decimals}f}".replace(',', ' ')


def calculate_percentage(part: float, total: float, decimals: int = 2) -> float:
    """
    Расчет процента
    
    Args:
        part: часть
        total: целое
        decimals: количество десятичных знаков
        
    Returns:
        Процентное значение
    """
    if total == 0:
        return 0.0
    return round((part / total) * 100, decimals)


def get_timestamp() -> str:
    """
    Получение текущей временной метки
    
    Returns:
        Отформатированная временная метка
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def create_report_filename(base_name: str, extension: str = "json") -> str:
    """
    Создание имени файла отчета с временной меткой
    
    Args:
        base_name: базовое имя файла
        extension: расширение файла
        
    Returns:
        Имя файла с временной меткой
    """
    timestamp = get_timestamp()
    return f"{base_name}_{timestamp}.{extension}"


def summarize_dataframe(df, name: str = "DataFrame") -> Dict[str, Any]:
    """
    Создание краткой сводки по DataFrame
    
    Args:
        df: pandas DataFrame
        name: название датафрейма
        
    Returns:
        Словарь со сводной информацией
    """
    return {
        'name': name,
        'shape': df.shape,
        'columns': list(df.columns),
        'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
        'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
        'null_counts': {col: int(count) for col, count in df.isnull().sum().items() if count > 0}
    }


class Timer:
    """
    Контекстный менеджер для измерения времени выполнения
    """
    
    def __init__(self, name: str = "Operation"):
        self.name = name
        self.start_time = None
        self.logger = logging.getLogger(__name__)
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info(f"{self.name} начато...")
        return self
    
    def __exit__(self, *args):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        self.logger.info(f"{self.name} завершено за {elapsed:.2f} секунд")


def validate_data_files(config: dict) -> bool:
    """
    Проверка наличия необходимых файлов данных
    
    Args:
        config: конфигурация с путями к файлам
        
    Returns:
        True если все файлы найдены, False иначе
    """
    logger = logging.getLogger(__name__)
    
    raw_path = config.get('data', {}).get('raw_path', 'data/raw')
    events_file = Path(raw_path) / config.get('data', {}).get('events_file', 'events.csv')
    users_file = Path(raw_path) / config.get('data', {}).get('users_file', 'users.csv')
    
    files_valid = True
    
    if not events_file.exists():
        logger.error(f"Файл событий не найден: {events_file}")
        files_valid = False
    else:
        logger.info(f"Файл событий найден: {events_file}")
    
    if not users_file.exists():
        logger.error(f"Файл пользователей не найден: {users_file}")
        files_valid = False
    else:
        logger.info(f"Файл пользователей найден: {users_file}")
    
    return files_valid
