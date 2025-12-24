"""Временной анализ"""
import pandas as pd
from pathlib import Path
import logging
logger = logging.getLogger(__name__)

class TemporalAnalyzer:
    def __init__(self, df_all, df_lost, df_stay, output_dir):
        self.df_all, self.df_lost, self.df_stay = df_all, df_lost, df_stay
        self.output_dir = Path(output_dir)
    
    def run_analysis(self):
        """Запуск временного анализа"""
        # Анализ по времени суток
        if 'Дата и время события' in self.df_all.columns:
            self._hourly_analysis()
            self._daily_analysis()
        
        logger.info("  Временной анализ выполнен")
    
    def _hourly_analysis(self):
        """Анализ по часам суток"""
        results = []
        results.append("# АНАЛИЗ ПО ВРЕМЕНИ СУТОК\n\n")
        
        # Извлекаем час
        self.df_lost['hour'] = pd.to_datetime(self.df_lost['Дата и время события']).dt.hour
        self.df_stay['hour'] = pd.to_datetime(self.df_stay['Дата и время события']).dt.hour
        
        results.append("## Распределение сессий по часам\n\n")
        results.append("| Час | Lost | Stay | Всего |\n")
        results.append("|-----|------|------|-------|\n")
        
        for hour in range(24):
            lost_count = (self.df_lost['hour'] == hour).sum()
            stay_count = (self.df_stay['hour'] == hour).sum()
            total = lost_count + stay_count
            
            results.append(f"| {hour:02d}:00 | {lost_count:,} | {stay_count:,} | {total:,} |\n")
        
        # Сохранение
        output_file = self.output_dir / 'hourly_distribution.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(results)
        
        # Очистка
        self.df_lost.drop('hour', axis=1, inplace=True, errors='ignore')
        self.df_stay.drop('hour', axis=1, inplace=True, errors='ignore')
    
    def _daily_analysis(self):
        """Анализ по дням недели"""
        results = []
        results.append("# АНАЛИЗ ПО ДНЯМ НЕДЕЛИ\n\n")
        
        # Извлекаем день недели
        self.df_lost['weekday'] = pd.to_datetime(self.df_lost['Дата и время события']).dt.dayofweek
        self.df_stay['weekday'] = pd.to_datetime(self.df_stay['Дата и время события']).dt.dayofweek
        
        days = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
        
        results.append("## Распределение сессий по дням недели\n\n")
        results.append("| День | Lost | Stay | Всего |\n")
        results.append("|------|------|------|-------|\n")
        
        for day_idx, day_name in enumerate(days):
            lost_count = (self.df_lost['weekday'] == day_idx).sum()
            stay_count = (self.df_stay['weekday'] == day_idx).sum()
            total = lost_count + stay_count
            
            results.append(f"| {day_name} | {lost_count:,} | {stay_count:,} | {total:,} |\n")
        
        # Сохранение
        output_file = self.output_dir / 'weekly_patterns.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(results)
        
        # Очистка
        self.df_lost.drop('weekday', axis=1, inplace=True, errors='ignore')
        self.df_stay.drop('weekday', axis=1, inplace=True, errors='ignore')
