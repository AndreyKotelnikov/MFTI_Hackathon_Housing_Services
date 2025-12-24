"""
Модуль для базовой статистики и сравнения групп is_lost vs is_stay
"""

import pandas as pd
import numpy as np
from scipy import stats
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class BasicStatisticsAnalyzer:
    """Анализатор базовой статистики"""
    
    def __init__(self, df_all, df_lost, df_stay, output_dir, block_prefixes):
        self.df_all = df_all
        self.df_lost = df_lost
        self.df_stay = df_stay
        self.output_dir = Path(output_dir)
        self.block_prefixes = block_prefixes
        
    def run_analysis(self):
        """Запуск всех видов базового анализа"""
        # 1. Общая статистика
        self._overall_statistics()
        
        # 2. Демографический анализ
        self._demographic_analysis()
        
        # 3. Устройства и технологии
        self._device_analysis()
        
        # 4. Длительность сессий
        self._session_duration_analysis()
        
        # 5. Активность по блокам
        self._block_usage_comparison()
        
        # 6. Статистические тесты
        self._statistical_tests()
        
        logger.info("✓ Базовая статистика завершена")
    
    def _overall_statistics(self):
        """Общая статистика по группам"""
        report = []
        
        report.append("# ОБЩАЯ СТАТИСТИКА ПО ГРУППАМ\n")
        report.append(f"Дата: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        report.append("## Размеры групп\n")
        report.append(f"- Потерянные (is_lost): {len(self.df_lost):,} сессий\n")
        report.append(f"- Удержанные (is_stay): {len(self.df_stay):,} сессий\n")
        report.append(f"- Соотношение lost/stay: {len(self.df_lost)/len(self.df_stay):.3f}\n\n")
        
        # Метрики оттока
        total = len(self.df_lost) + len(self.df_stay)
        churn_rate = 100 * len(self.df_lost) / total
        retention_rate = 100 * len(self.df_stay) / total
        
        report.append("## Метрики удержания\n")
        report.append(f"- Churn Rate: {churn_rate:.2f}%\n")
        report.append(f"- Retention Rate: {retention_rate:.2f}%\n\n")
        
        # Средняя длительность сессии
        report.append("## Длительность сессий\n")
        report.append(f"- Lost: {self.df_lost['sess_dur_sec'].mean():.1f} сек (медиана: {self.df_lost['sess_dur_sec'].median():.1f})\n")
        report.append(f"- Stay: {self.df_stay['sess_dur_sec'].mean():.1f} сек (медиана: {self.df_stay['sess_dur_sec'].median():.1f})\n")
        report.append(f"- Разница: {self.df_stay['sess_dur_sec'].mean() - self.df_lost['sess_dur_sec'].mean():.1f} сек\n\n")
        
        # Количество использованных блоков
        count_cols = [f'{prefix}_count' for prefix in self.block_prefixes]
        
        lost_blocks_used = (self.df_lost[count_cols] > 0).sum(axis=1).mean()
        stay_blocks_used = (self.df_stay[count_cols] > 0).sum(axis=1).mean()
        
        report.append("## Использование блоков\n")
        report.append(f"- Lost: {lost_blocks_used:.2f} блоков в среднем\n")
        report.append(f"- Stay: {stay_blocks_used:.2f} блоков в среднем\n")
        report.append(f"- Разница: {stay_blocks_used - lost_blocks_used:.2f} блоков\n\n")
        
        # Сохранение
        output_file = self.output_dir / '01_overall_statistics.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(report)
        
        logger.info(f"  Сохранено: {output_file}")
    
    def _demographic_analysis(self):
        """Демографический анализ"""
        results = []
        
        results.append("# ДЕМОГРАФИЧЕСКИЙ АНАЛИЗ\n\n")
        
        # Анализ по полу
        if 'gender' in self.df_all.columns:
            results.append("## Распределение по полу\n\n")
            
            for group_name, df in [('Lost', self.df_lost), ('Stay', self.df_stay)]:
                gender_dist = df['gender'].value_counts(normalize=True) * 100
                results.append(f"### {group_name}:\n")
                for gender, pct in gender_dist.items():
                    results.append(f"- {gender}: {pct:.1f}%\n")
                results.append("\n")
        
        # Анализ по возрасту
        if 'age_group' in self.df_all.columns:
            results.append("## Распределение по возрастным группам\n\n")
            
            for group_name, df in [('Lost', self.df_lost), ('Stay', self.df_stay)]:
                age_dist = df['age_group'].value_counts(normalize=True) * 100
                results.append(f"### {group_name}:\n")
                for age, pct in age_dist.sort_index().items():
                    results.append(f"- {age}: {pct:.1f}%\n")
                results.append("\n")
        
        # Средний возраст
        if 'age_back' in self.df_all.columns:
            results.append("## Средний возраст\n\n")
            results.append(f"- Lost: {self.df_lost['age_back'].mean():.1f} лет\n")
            results.append(f"- Stay: {self.df_stay['age_back'].mean():.1f} лет\n")
            results.append(f"- Разница: {self.df_stay['age_back'].mean() - self.df_lost['age_back'].mean():.1f} лет\n\n")
        
        # Сохранение
        output_file = self.output_dir / '02_demographic_analysis.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(results)
        
        logger.info(f"  Сохранено: {output_file}")
    
    def _device_analysis(self):
        """Анализ устройств"""
        results = []
        
        results.append("# АНАЛИЗ УСТРОЙСТВ И ТЕХНОЛОГИЙ\n\n")
        
        # Тип устройства
        if 'Тип устройства' in self.df_all.columns:
            results.append("## Тип устройства\n\n")
            
            for group_name, df in [('Lost', self.df_lost), ('Stay', self.df_stay)]:
                device_dist = df['Тип устройства'].value_counts(normalize=True) * 100
                results.append(f"### {group_name}:\n")
                for device, pct in device_dist.items():
                    results.append(f"- {device}: {pct:.1f}%\n")
                results.append("\n")
        
        # ОС
        if 'ОС' in self.df_all.columns and 'Идентификатор устройства' in self.df_all.columns:
            results.append("## Операционная система (по пользователям)\n\n")

            for group_name, df in [('Lost', self.df_lost), ('Stay', self.df_stay)]:
                # 1) Группировка по пользователю (устройству): одна ОС на устройство
                #    Если у устройства есть несколько ОС (редко, но бывает) — берём самую частую
                os_by_device = (
                    df.dropna(subset=['Идентификатор устройства', 'ОС'])
                    .groupby('Идентификатор устройства')['ОС']
                    .agg(lambda s: s.value_counts().idxmax())
                )

                # 2) Статистика по ОС уже на уровне пользователей
                os_dist = os_by_device.value_counts(normalize=True) * 100

                results.append(f"### {group_name}:\n")
                for os, pct in os_dist.items():
                    results.append(f"- {os}: {pct:.1f}%\n")
                results.append("\n")
        
        # Производитель
        if 'Производитель устройства' in self.df_all.columns:
            results.append("## Топ-10 производителей\n\n")
            
            for group_name, df in [('Lost', self.df_lost), ('Stay', self.df_stay)]:
                brand_dist = df['Производитель устройства'].value_counts(normalize=True).head(10) * 100
                results.append(f"### {group_name}:\n")
                for brand, pct in brand_dist.items():
                    results.append(f"- {brand}: {pct:.1f}%\n")
                results.append("\n")
        
        # Сохранение
        output_file = self.output_dir / '03_device_analysis.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(results)
        
        logger.info(f"  Сохранено: {output_file}")
    
    def _session_duration_analysis(self):
        """Детальный анализ длительности сессий"""
        results = []
        
        results.append("# АНАЛИЗ ДЛИТЕЛЬНОСТИ СЕССИЙ\n\n")
        
        # Описательная статистика
        results.append("## Описательная статистика (секунды)\n\n")
        
        desc_lost = self.df_lost['sess_dur_sec'].describe()
        desc_stay = self.df_stay['sess_dur_sec'].describe()
        
        results.append("### Lost:\n")
        for stat, value in desc_lost.items():
            results.append(f"- {stat}: {value:.1f}\n")
        results.append("\n")
        
        results.append("### Stay:\n")
        for stat, value in desc_stay.items():
            results.append(f"- {stat}: {value:.1f}\n")
        results.append("\n")
        
        # Перцентили
        results.append("## Перцентили\n\n")
        percentiles = [10, 25, 50, 75, 90, 95, 99]
        
        results.append("| Перцентиль | Lost | Stay | Разница |\n")
        results.append("|------------|------|------|----------|\n")
        
        for p in percentiles:
            lost_val = self.df_lost['sess_dur_sec'].quantile(p/100)
            stay_val = self.df_stay['sess_dur_sec'].quantile(p/100)
            diff = stay_val - lost_val
            results.append(f"| {p}% | {lost_val:.1f} | {stay_val:.1f} | {diff:+.1f} |\n")
        
        results.append("\n")
        
        # Категории длительности
        results.append("## Распределение по категориям\n\n")
        
        def categorize_duration(seconds):
            if seconds < 30:
                return '0-30 сек'
            elif seconds < 60:
                return '30-60 сек'
            elif seconds < 180:
                return '1-3 мин'
            elif seconds < 300:
                return '3-5 мин'
            elif seconds < 600:
                return '5-10 мин'
            else:
                return '10+ мин'
        
        for group_name, df in [('Lost', self.df_lost), ('Stay', self.df_stay)]:
            dur_cats = df['sess_dur_sec'].apply(categorize_duration).value_counts(normalize=True) * 100
            results.append(f"### {group_name}:\n")
            
            # Сортировка по порядку
            order = ['0-30 сек', '30-60 сек', '1-3 мин', '3-5 мин', '5-10 мин', '10+ мин']
            for cat in order:
                if cat in dur_cats.index:
                    results.append(f"- {cat}: {dur_cats[cat]:.1f}%\n")
            results.append("\n")
        
        # Сохранение
        output_file = self.output_dir / '04_session_duration_analysis.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(results)
        
        logger.info(f"  Сохранено: {output_file}")
    
    def _block_usage_comparison(self):
        """Сравнение использования блоков"""
        results = []
        
        results.append("# СРАВНЕНИЕ ИСПОЛЬЗОВАНИЯ БЛОКОВ\n\n")
        
        count_cols = [f'{prefix}_count' for prefix in self.block_prefixes]
        
        # Процент пользователей, использовавших каждый блок
        results.append("## Процент пользователей, использовавших блок\n\n")
        results.append("| Блок | Lost | Stay | Разница |\n")
        results.append("|------|------|------|----------|\n")
        
        for prefix in self.block_prefixes:
            col = f'{prefix}_count'
            
            lost_pct = (self.df_lost[col] > 0).mean() * 100
            stay_pct = (self.df_stay[col] > 0).mean() * 100
            diff = stay_pct - lost_pct
            
            results.append(f"| {prefix} | {lost_pct:.1f}% | {stay_pct:.1f}% | {diff:+.1f}% |\n")
        
        results.append("\n")
        
        # Среднее количество действий в блоке (среди тех, кто использовал)
        results.append("## Среднее количество действий (среди использовавших)\n\n")
        results.append("| Блок | Lost | Stay | Разница |\n")
        results.append("|------|------|------|----------|\n")
        
        for prefix in self.block_prefixes:
            col = f'{prefix}_count'
            
            lost_mean = self.df_lost[self.df_lost[col] > 0][col].mean()
            stay_mean = self.df_stay[self.df_stay[col] > 0][col].mean()
            
            if pd.notna(lost_mean) and pd.notna(stay_mean):
                diff = stay_mean - lost_mean
                results.append(f"| {prefix} | {lost_mean:.2f} | {stay_mean:.2f} | {diff:+.2f} |\n")
        
        results.append("\n")
        
        # Сохранение
        output_file = self.output_dir / '05_block_usage_comparison.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(results)
        
        logger.info(f"  Сохранено: {output_file}")
    
    def _statistical_tests(self):
        """Статистические тесты на значимость различий"""
        results = []
        
        results.append("# СТАТИСТИЧЕСКИЕ ТЕСТЫ\n\n")
        results.append("Уровень значимости: α = 0.05\n\n")
        
        # T-test для длительности сессии
        results.append("## T-test: Длительность сессии\n\n")
        
        t_stat, p_value = stats.ttest_ind(
            self.df_lost['sess_dur_sec'],
            self.df_stay['sess_dur_sec']
        )
        
        results.append(f"- t-статистика: {t_stat:.4f}\n")
        results.append(f"- p-value: {p_value:.6f}\n")
        results.append(f"- Значимо: {'ДА' if p_value < 0.05 else 'НЕТ'}\n\n")
        
        # Mann-Whitney U test (непараметрический)
        results.append("## Mann-Whitney U test: Длительность сессии\n\n")
        
        u_stat, p_value_mw = stats.mannwhitneyu(
            self.df_lost['sess_dur_sec'],
            self.df_stay['sess_dur_sec']
        )
        
        results.append(f"- U-статистика: {u_stat:.4f}\n")
        results.append(f"- p-value: {p_value_mw:.6f}\n")
        results.append(f"- Значимо: {'ДА' if p_value_mw < 0.05 else 'НЕТ'}\n\n")
        
        # Chi-square тест для категориальных переменных
        if 'gender' in self.df_all.columns:
            results.append("## Chi-square test: Пол\n\n")
            
            contingency_table = pd.crosstab(
                self.df_all['is_lost'],
                self.df_all['gender']
            )
            
            chi2, p_value_chi, dof, expected = stats.chi2_contingency(contingency_table)
            
            results.append(f"- χ² статистика: {chi2:.4f}\n")
            results.append(f"- p-value: {p_value_chi:.6f}\n")
            results.append(f"- Степени свободы: {dof}\n")
            results.append(f"- Значимо: {'ДА' if p_value_chi < 0.05 else 'НЕТ'}\n\n")
        
        # Тесты для каждого блока
        results.append("## T-tests по блокам (count)\n\n")
        results.append("| Блок | t-stat | p-value | Значимо |\n")
        results.append("|------|--------|---------|----------|\n")
        
        significant_blocks = []
        
        for prefix in self.block_prefixes:
            col = f'{prefix}_count'
            
            try:
                t_stat, p_val = stats.ttest_ind(
                    self.df_lost[col],
                    self.df_stay[col]
                )
                
                sig = 'ДА' if p_val < 0.05 else 'нет'
                results.append(f"| {prefix} | {t_stat:.3f} | {p_val:.4f} | {sig} |\n")
                
                if p_val < 0.05:
                    significant_blocks.append((prefix, p_val))
            except:
                results.append(f"| {prefix} | - | - | - |\n")
        
        results.append("\n")
        
        # Сохранение
        output_file = self.output_dir / '06_statistical_tests.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(results)
        
        logger.info(f"  Сохранено: {output_file}")
