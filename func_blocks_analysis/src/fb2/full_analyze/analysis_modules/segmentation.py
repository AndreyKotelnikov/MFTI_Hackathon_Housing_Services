"""Сегментация пользователей"""
import pandas as pd
from pathlib import Path
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import logging
logger = logging.getLogger(__name__)

class SegmentationAnalyzer:
    def __init__(self, df_all, df_lost, df_stay, output_dir, block_prefixes):
        self.df_all, self.df_lost, self.df_stay = df_all, df_lost, df_stay
        self.output_dir, self.block_prefixes = Path(output_dir), block_prefixes
    
    def run_analysis(self):
        # Кластеризация
        count_cols = [f'{p}_count' for p in self.block_prefixes if f'{p}_count' in self.df_all.columns]
        
        if len(count_cols) > 0:
            X = self.df_all[count_cols].fillna(0)
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            
            kmeans = KMeans(n_clusters=min(5, len(X)//100), random_state=42)
            clusters = kmeans.fit_predict(X_scaled)
            
            self.df_all['cluster'] = clusters
            
            # Анализ кластеров
            cluster_analysis = self.df_all.groupby('cluster').agg({
                'is_lost': 'mean',
                'sess_dur_sec': 'mean'
            })
            
            cluster_analysis.to_csv(self.output_dir / 'clusters_description.csv')
        
        logger.info("  Сегментация выполнена")
