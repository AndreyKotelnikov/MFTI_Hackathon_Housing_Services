"""
Главный скрипт для комплексного анализа поведения пользователей
Сравнение is_lost vs is_stay пользователей

Запуск: python analyze_user_behavior.py <path_to_sessions_data.csv>
"""

import pandas as pd
import numpy as np
import warnings
import os
from pathlib import Path
from datetime import datetime
import logging
import sys

warnings.filterwarnings('ignore')

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('analysis.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class UserBehaviorAnalyzer:
    """Главный класс для анализа поведения пользователей"""
    
    def __init__(self, data_path: str, output_dir: str = 'analysis_results'):
        """
        Инициализация анализатора
        
        Args:
            data_path: путь к CSV файлу с данными
            output_dir: директория для сохранения результатов
        """
        self.data_path = data_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Создание поддиректорий
        self.dirs = {
            'stats': self.output_dir / '01_statistics',
            'blocks': self.output_dir / '02_block_analysis',
            'temporal': self.output_dir / '03_temporal_analysis',
            'efficiency': self.output_dir / '04_efficiency_analysis',
            'segments': self.output_dir / '05_segmentation',
            'patterns': self.output_dir / '06_patterns',
            'visualizations': self.output_dir / '07_visualizations',
            'reports': self.output_dir / '08_final_reports'
        }
        
        for dir_path in self.dirs.values():
            dir_path.mkdir(exist_ok=True)
        
        self.df = None
        self.df_lost = None
        self.df_stay = None
        
        # Префиксы блоков
        self.block_prefixes = [
            'request', 'req_manage', 'profile', 'nav', 'notif', 
            'poll_oss', 'rewards', 'my_home', 'partners', 'transport',
            'ann_view', 'smart', 'support', 'guest', 'city_serv', 
            'address', 'ann_create'
        ]
        
        # Названия блоков
        self.block_names = {
            'request': 'Создание заявки',
            'req_manage': 'Просмотр заявок',
            'profile': 'Профиль',
            'nav': 'Навигация',
            'notif': 'Уведомления',
            'poll_oss': 'Опросы и ОСС',
            'rewards': 'Баллы и поощрения',
            'my_home': 'Мой дом',
            'partners': 'Услуги партнеров',
            'transport': 'Управление транспортом',
            'ann_view': 'Просмотр объявлений',
            'smart': 'Умные решения',
            'support': 'Техподдержка',
            'guest': 'Гостевой доступ',
            'city_serv': 'Городские сервисы',
            'address': 'Создание адреса',
            'ann_create': 'Создание объявления'
        }
        
    def load_data(self, df: pd.DataFrame = None):
        """Загрузка и первичная обработка данных"""
        logger.info("="*80)
        logger.info("ЗАГРУЗКА ДАННЫХ")
        logger.info("="*80)
        
        logger.info(f"Чтение файла: {self.data_path}")
        self.df = df
        
        logger.info(f"Загружено: {len(self.df):,} сессий")
        logger.info(f"Колонок: {len(self.df.columns)}")
        
        # Преобразование даты
        if 'Дата и время события' in self.df.columns:
            self.df['Дата и время события'] = pd.to_datetime(self.df['Дата и время события'])
        
        # Разделение на группы
        self.df_lost = self.df[self.df['is_lost'] == True].copy()
        self.df_stay = self.df[self.df['is_stay'] == True].copy()
        
        logger.info(f"\nРаспределение пользователей:")
        logger.info(f"  Потерянные (is_lost): {len(self.df_lost):,} сессий")
        logger.info(f"  Удержанные (is_stay): {len(self.df_stay):,} сессий")
        logger.info(f"  Новые (is_new): {self.df['is_new'].sum():,} сессий")
        
        # Базовая валидация
        self._validate_data()
        
    def _validate_data(self):
        """Валидация данных"""
        logger.info("\nВалидация данных...")
        
        issues = []
        
        # Проверка наличия обязательных колонок
        required_cols = ['global_session_id', 'is_lost', 'is_stay', 'sess_dur_sec']
        missing = [col for col in required_cols if col not in self.df.columns]
        if missing:
            issues.append(f"Отсутствуют колонки: {missing}")
        
        # Проверка наличия данных в группах
        if len(self.df_lost) == 0:
            issues.append("Нет данных для группы is_lost")
        if len(self.df_stay) == 0:
            issues.append("Нет данных для группы is_stay")
        
        if issues:
            logger.error("Обнаружены проблемы:")
            for issue in issues:
                logger.error(f"  - {issue}")
            raise ValueError("Валидация данных не пройдена")
        
        logger.info("✓ Валидация пройдена успешно")
    
    def run_full_analysis(self, df: pd.DataFrame = None):
        """Запуск полного анализа"""
        start_time = datetime.now()
        
        logger.info("\n" + "="*80)
        logger.info("НАЧАЛО КОМПЛЕКСНОГО АНАЛИЗА")
        logger.info("="*80)
        
        try:
            # 1. Загрузка данных
            self.load_data(df)
            
            # 2. Базовая статистика
            logger.info("\n[1/7] Базовая статистика и сравнение групп...")
            from src.fb2.full_analyze.analysis_modules.basic_statistics import BasicStatisticsAnalyzer
            stats_analyzer = BasicStatisticsAnalyzer(self.df, self.df_lost, self.df_stay, 
                                                     self.dirs['stats'], self.block_prefixes)
            stats_analyzer.run_analysis()
            
            # 3. Анализ по блокам
            logger.info("\n[2/7] Детальный анализ по функциональным блокам...")
            from src.fb2.full_analyze.analysis_modules.block_analysis import BlockAnalyzer
            block_analyzer = BlockAnalyzer(self.df, self.df_lost, self.df_stay,
                                          self.dirs['blocks'], self.block_prefixes, 
                                          self.block_names)
            block_analyzer.run_analysis()
            
            # 4. Временной анализ
            logger.info("\n[3/7] Временной анализ...")
            from src.fb2.full_analyze.analysis_modules.temporal_analysis import TemporalAnalyzer
            temporal_analyzer = TemporalAnalyzer(self.df, self.df_lost, self.df_stay,
                                                self.dirs['temporal'])
            temporal_analyzer.run_analysis()
            
            # 5. Анализ эффективности
            logger.info("\n[4/7] Анализ эффективности взаимодействия...")
            from src.fb2.full_analyze.analysis_modules.efficiency_analysis import EfficiencyAnalyzer
            efficiency_analyzer = EfficiencyAnalyzer(self.df, self.df_lost, self.df_stay,
                                                    self.dirs['efficiency'], self.block_prefixes)
            efficiency_analyzer.run_analysis()
            
            # 6. Сегментация и кластеризация
            logger.info("\n[5/7] Сегментация и кластеризация пользователей...")
            from src.fb2.full_analyze.analysis_modules.segmentation import SegmentationAnalyzer
            segment_analyzer = SegmentationAnalyzer(self.df, self.df_lost, self.df_stay,
                                                   self.dirs['segments'], self.block_prefixes)
            segment_analyzer.run_analysis()
            
            # 7. Поиск паттернов
            logger.info("\n[6/7] Поиск поведенческих паттернов...")
            from src.fb2.full_analyze.analysis_modules.pattern_mining import PatternAnalyzer
            pattern_analyzer = PatternAnalyzer(self.df, self.df_lost, self.df_stay,
                                              self.dirs['patterns'], self.block_prefixes)
            pattern_analyzer.run_analysis()
            
            # 8. Визуализация
            logger.info("\n[7/7] Генерация визуализаций...")
            from src.fb2.full_analyze.analysis_modules.visualization import VisualizationGenerator
            viz_generator = VisualizationGenerator(self.df, self.df_lost, self.df_stay,
                                                  self.dirs['visualizations'], 
                                                  self.block_prefixes, self.block_names)
            viz_generator.generate_all()
            
            # 9. Финальный отчет
            logger.info("\nГенерация итогового отчета...")
            self._generate_final_report()
            
            duration = (datetime.now() - start_time).total_seconds()
            
            logger.info("\n" + "="*80)
            logger.info("АНАЛИЗ ЗАВЕРШЕН УСПЕШНО")
            logger.info("="*80)
            logger.info(f"Время выполнения: {duration:.1f} секунд ({duration/60:.1f} минут)")
            logger.info(f"Результаты сохранены в: {self.output_dir}")
            logger.info("="*80)
            
        except Exception as e:
            logger.error(f"\n❌ Ошибка при выполнении анализа: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    def _generate_final_report(self):
        """Генерация итогового отчета со ссылками на все результаты"""
        report_path = self.dirs['reports'] / 'MASTER_REPORT.md'
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# 📊 КОМПЛЕКСНЫЙ АНАЛИЗ ПОВЕДЕНИЯ ПОЛЬЗОВАТЕЛЕЙ\n\n")
            f.write(f"**Дата анализа:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Источник данных:** {self.data_path}\n\n")
            
            f.write("---\n\n")
            
            f.write("## 📈 Основные метрики\n\n")
            f.write(f"- **Всего сессий:** {len(self.df):,}\n")
            f.write(f"- **Потерянные пользователи (is_lost):** {len(self.df_lost):,}\n")
            f.write(f"- **Удержанные пользователи (is_stay):** {len(self.df_stay):,}\n")
            f.write(f"- **Churn Rate:** {100*len(self.df_lost)/(len(self.df_lost)+len(self.df_stay)):.2f}%\n")
            f.write(f"- **Retention Rate:** {100*len(self.df_stay)/(len(self.df_lost)+len(self.df_stay)):.2f}%\n\n")
            
            f.write("---\n\n")
            
            f.write("## 📂 Структура результатов\n\n")
            
            sections = [
                ("01_statistics", "Базовая статистика и сравнение групп"),
                ("02_block_analysis", "Детальный анализ по функциональным блокам"),
                ("03_temporal_analysis", "Временной анализ и паттерны активности"),
                ("04_efficiency_analysis", "Анализ эффективности взаимодействия"),
                ("05_segmentation", "Сегментация и кластеризация"),
                ("06_patterns", "Поведенческие паттерны"),
                ("07_visualizations", "Визуализации"),
                ("08_final_reports", "Итоговые отчеты и выводы")
            ]
            
            for folder, description in sections:
                f.write(f"### {folder}\n")
                f.write(f"{description}\n\n")
                
                # Список файлов в директории
                folder_path = self.output_dir / folder
                if folder_path.exists():
                    files = sorted(folder_path.glob('*'))
                    if files:
                        for file in files:
                            f.write(f"- `{file.name}`\n")
                    else:
                        f.write("- *(файлы будут созданы после выполнения анализа)*\n")
                f.write("\n")
            
            f.write("---\n\n")
            f.write("## 🎯 Рекомендации по использованию\n\n")
            f.write("1. Начните с **01_statistics** для общего понимания\n")
            f.write("2. Изучите **02_block_analysis** для деталей по каждому блоку\n")
            f.write("3. Просмотрите **07_visualizations** для наглядного представления\n")
            f.write("4. Ознакомьтесь с итоговыми выводами в **08_final_reports**\n\n")
            
            f.write("---\n\n")
            f.write("*Анализ выполнен автоматически системой UserBehaviorAnalyzer*\n")
        
        logger.info(f"✓ Итоговый отчет сохранен: {report_path}")


def main():
    """Точка входа"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Комплексный анализ поведения пользователей (is_lost vs is_stay)'
    )
    parser.add_argument(
        'data_path',
        type=str,
        help='Путь к CSV файлу с данными сессий'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='analysis_results',
        help='Директория для сохранения результатов (по умолчанию: analysis_results)'
    )
    
    args = parser.parse_args()
    
    # Проверка существования файла
    if not os.path.exists(args.data_path):
        logger.error(f"❌ Файл не найден: {args.data_path}")
        sys.exit(1)
    
    # Запуск анализа
    analyzer = UserBehaviorAnalyzer(args.data_path, args.output)
    analyzer.run_full_analysis()


if __name__ == '__main__':
    main()
