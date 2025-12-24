"""
Детальный анализ по функциональным блокам
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class BlockAnalyzer:
    """Анализатор функциональных блоков"""
    
    def __init__(self, df_all, df_lost, df_stay, output_dir, block_prefixes, block_names):
        self.df_all = df_all
        self.df_lost = df_lost
        self.df_stay = df_stay
        self.output_dir = Path(output_dir)
        self.block_prefixes = block_prefixes
        self.block_names = block_names
        
    def run_analysis(self):
        """Запуск анализа по блокам"""
        # 1. Детальный анализ каждого блока
        for prefix in self.block_prefixes:
            self._analyze_single_block(prefix)
        
        # 2. Сравнительная таблица всех блоков
        self._create_comparison_table()
        
        # 3. Анализ успешности
        self._success_analysis()
        
        # 4. Анализ глубины взаимодействия
        self._depth_analysis()
        
        logger.info("✓ Анализ по блокам завершен")
    
    def _analyze_single_block(self, prefix):
        """Детальный анализ одного блока"""
        results = []
        
        block_name = self.block_names.get(prefix, prefix)
        results.append(f"# АНАЛИЗ БЛОКА: {block_name} ({prefix})\n\n")
        
        # Метрики
        metrics = ['count', 'max_step', 'success_count', 'review_count', 
                  'dur_sec', 'click_count', 'dbl_dur_sec', 'dbl_count']
        
        for metric in metrics:
            col = f'{prefix}_{metric}'
            if col not in self.df_all.columns:
                continue
            
            results.append(f"## {metric.upper()}\n\n")
            
            # Процент использования
            lost_usage = (self.df_lost[col] > 0).mean() * 100
            stay_usage = (self.df_stay[col] > 0).mean() * 100
            
            results.append(f"**Процент использования:**\n")
            results.append(f"- Lost: {lost_usage:.1f}%\n")
            results.append(f"- Stay: {stay_usage:.1f}%\n")
            results.append(f"- Разница: {stay_usage - lost_usage:+.1f}%\n\n")
            
            # Среднее значение
            lost_mean = self.df_lost[col].mean()
            stay_mean = self.df_stay[col].mean()
            
            results.append(f"**Среднее значение:**\n")
            results.append(f"- Lost: {lost_mean:.2f}\n")
            results.append(f"- Stay: {stay_mean:.2f}\n")
            results.append(f"- Разница: {stay_mean - lost_mean:+.2f}\n\n")
            
            # Среди использовавших
            if lost_usage > 0 and stay_usage > 0:
                lost_mean_active = self.df_lost[self.df_lost[col] > 0][col].mean()
                stay_mean_active = self.df_stay[self.df_stay[col] > 0][col].mean()
                
                results.append(f"**Среди использовавших:**\n")
                results.append(f"- Lost: {lost_mean_active:.2f}\n")
                results.append(f"- Stay: {stay_mean_active:.2f}\n")
                results.append(f"- Разница: {stay_mean_active - lost_mean_active:+.2f}\n\n")
        
        # Сохранение
        output_file = self.output_dir / f'block_{prefix}.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(results)
    
    def _create_comparison_table(self):
        """Сводная таблица по всем блокам"""
        data = []
        
        for prefix in self.block_prefixes:
            block_name = self.block_names.get(prefix, prefix)
            count_col = f'{prefix}_count'
            
            if count_col not in self.df_all.columns:
                continue
            
            lost_usage = (self.df_lost[count_col] > 0).mean() * 100
            stay_usage = (self.df_stay[count_col] > 0).mean() * 100
            
            lost_mean = self.df_lost[count_col].mean()
            stay_mean = self.df_stay[count_col].mean()
            
            data.append({
                'Блок': block_name,
                'Префикс': prefix,
                'Lost_usage_%': lost_usage,
                'Stay_usage_%': stay_usage,
                'Diff_%': stay_usage - lost_usage,
                'Lost_mean': lost_mean,
                'Stay_mean': stay_mean,
                'Diff_mean': stay_mean - lost_mean
            })
        
        df_comparison = pd.DataFrame(data)
        df_comparison = df_comparison.sort_values('Diff_%', ascending=False)
        
        # Сохранение CSV
        output_file = self.output_dir / 'all_blocks_comparison.csv'
        df_comparison.to_csv(output_file, index=False)
        
        # Сохранение TXT
        output_file_txt = self.output_dir / 'all_blocks_comparison.txt'
        with open(output_file_txt, 'w', encoding='utf-8') as f:
            f.write("# СРАВНИТЕЛЬНАЯ ТАБЛИЦА ПО ВСЕМ БЛОКАМ\n\n")
            f.write(df_comparison.to_string(index=False))
    
    def _success_analysis(self):
        """Анализ успешности действий"""
        results = []
        
        results.append("# АНАЛИЗ УСПЕШНОСТИ\n\n")
        results.append("## Конверсия в успех (success_count / count)\n\n")
        results.append("| Блок | Lost | Stay | Разница |\n")
        results.append("|------|------|------|---------|\n")
        
        for prefix in self.block_prefixes:
            count_col = f'{prefix}_count'
            success_col = f'{prefix}_success_count'
            
            if count_col not in self.df_all.columns:
                continue
            
            # Только среди тех, кто использовал блок
            lost_conv = (self.df_lost[self.df_lost[count_col] > 0][success_col].sum() / 
                        self.df_lost[self.df_lost[count_col] > 0][count_col].sum()) * 100
            
            stay_conv = (self.df_stay[self.df_stay[count_col] > 0][success_col].sum() / 
                        self.df_stay[self.df_stay[count_col] > 0][count_col].sum()) * 100
            
            if not pd.isna(lost_conv) and not pd.isna(stay_conv):
                diff = stay_conv - lost_conv
                results.append(f"| {prefix} | {lost_conv:.1f}% | {stay_conv:.1f}% | {diff:+.1f}% |\n")
        
        # Сохранение
        output_file = self.output_dir / 'success_analysis.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(results)
    
    def _depth_analysis(self):
        """Анализ глубины взаимодействия (max_step)"""
        results = []
        
        results.append("# АНАЛИЗ ГЛУБИНЫ ВЗАИМОДЕЙСТВИЯ\n\n")
        results.append("## Средний максимальный шаг (среди использовавших)\n\n")
        results.append("| Блок | Lost | Stay | Разница |\n")
        results.append("|------|------|------|---------|\n")
        
        for prefix in self.block_prefixes:
            step_col = f'{prefix}_max_step'
            
            if step_col not in self.df_all.columns:
                continue
            
            lost_step = self.df_lost[self.df_lost[step_col] > 0][step_col].mean()
            stay_step = self.df_stay[self.df_stay[step_col] > 0][step_col].mean()
            
            if not pd.isna(lost_step) and not pd.isna(stay_step):
                diff = stay_step - lost_step
                results.append(f"| {prefix} | {lost_step:.2f} | {stay_step:.2f} | {diff:+.2f} |\n")
        
        # Сохранение
        output_file = self.output_dir / 'depth_analysis.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.writelines(results)
