"""Генерация визуализаций"""
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import logging
logger = logging.getLogger(__name__)

class VisualizationGenerator:
    def __init__(self, df_all, df_lost, df_stay, output_dir, block_prefixes, block_names):
        self.df_all, self.df_lost, self.df_stay = df_all, df_lost, df_stay
        self.output_dir = Path(output_dir)
        self.block_prefixes, self.block_names = block_prefixes, block_names
    
    def generate_all(self):
        plt.style.use('seaborn-v0_8-darkgrid')
        
        # 1. Сравнение длительности сессий
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.hist([self.df_lost['sess_dur_sec'], self.df_stay['sess_dur_sec']], 
                label=['Lost', 'Stay'], bins=50, alpha=0.7)
        ax.set_xlabel('Длительность сессии (сек)')
        ax.set_ylabel('Количество')
        ax.legend()
        ax.set_title('Распределение длительности сессий')
        plt.savefig(self.output_dir / 'session_duration_distribution.png', dpi=150, bbox_inches='tight')
        plt.close()
        
        # 2. Heatmap использования блоков
        count_cols = [f'{p}_count' for p in self.block_prefixes if f'{p}_count' in self.df_all.columns]
        
        if len(count_cols) > 0:
            lost_usage = (self.df_lost[count_cols] > 0).mean() * 100
            stay_usage = (self.df_stay[count_cols] > 0).mean() * 100
            
            comparison = pd.DataFrame({
                'Lost': lost_usage,
                'Stay': stay_usage
            })
            comparison.index = [col.replace('_count', '') for col in comparison.index]
            
            fig, ax = plt.subplots(figsize=(8, 10))
            sns.heatmap(comparison.T, annot=True, fmt='.1f', cmap='RdYlGn', ax=ax)
            ax.set_title('Процент использования блоков (%)')
            plt.savefig(self.output_dir / 'block_usage_heatmap.png', dpi=150, bbox_inches='tight')
            plt.close()
        
        logger.info("  Визуализации созданы")
