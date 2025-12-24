"""Поиск паттернов"""
import pandas as pd
from pathlib import Path
import logging
logger = logging.getLogger(__name__)

class PatternAnalyzer:
    def __init__(self, df_all, df_lost, df_stay, output_dir, block_prefixes):
        self.df_all, self.df_lost, self.df_stay = df_all, df_lost, df_stay
        self.output_dir, self.block_prefixes = Path(output_dir), block_prefixes
    
    def run_analysis(self):
        results = []
        results.append("# АНАЛИЗ ПАТТЕРНОВ\n\n")
        
        # Топ комбинаций блоков
        count_cols = [f'{p}_count' for p in self.block_prefixes if f'{p}_count' in self.df_all.columns]
        
        for group_name, df in [('Lost', self.df_lost), ('Stay', self.df_stay)]:
            used_blocks = (df[count_cols] > 0).sum()
            results.append(f"## {group_name} - Топ используемых блоков:\n")
            for block, count in used_blocks.sort_values(ascending=False).head(10).items():
                results.append(f"- {block}: {count} сессий\n")
            results.append("\n")
        
        with open(self.output_dir / 'frequent_patterns.txt', 'w') as f:
            f.writelines(results)
        
        logger.info("  Анализ паттернов выполнен")
