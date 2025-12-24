"""Анализ эффективности"""
import pandas as pd
from pathlib import Path
import logging
logger = logging.getLogger(__name__)

class EfficiencyAnalyzer:
    def __init__(self, df_all, df_lost, df_stay, output_dir, block_prefixes):
        self.df_all, self.df_lost, self.df_stay = df_all, df_lost, df_stay
        self.output_dir, self.block_prefixes = Path(output_dir), block_prefixes
    
    def run_analysis(self):
        results = []
        results.append("# АНАЛИЗ ЭФФЕКТИВНОСТИ\n\n")
        
        for prefix in self.block_prefixes:
            dur_col = f'{prefix}_dur_sec'
            dbl_dur_col = f'{prefix}_dbl_dur_sec'
            
            if dur_col in self.df_all.columns and dbl_dur_col in self.df_all.columns:
                lost_eff = 1 - (self.df_lost[dbl_dur_col].sum() / max(self.df_lost[dur_col].sum(), 1))
                stay_eff = 1 - (self.df_stay[dbl_dur_col].sum() / max(self.df_stay[dur_col].sum(), 1))
                
                results.append(f"**{prefix}**: Lost={lost_eff*100:.1f}%, Stay={stay_eff*100:.1f}%\n")
        
        with open(self.output_dir / 'efficiency_metrics.txt', 'w') as f:
            f.writelines(results)
        
        logger.info("  Анализ эффективности выполнен")
