"""
Главный файл для запуска полного pipeline анализа данных
"""
import json
import logging
import sys
from pathlib import Path

from src.fb2.full_analyze.analyze_user_behavior import UserBehaviorAnalyzer
from src.fb2.funnels.funnel_features_extractor import FunnelFeaturesExtractor

# Добавляем текущую директорию в PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent))

from src.data_preprocessing import DataPreprocessor
from src.eda_analysis import EDAAnalyzer
from src.sequence_mining import SequenceAnalyzer
from src.visualization import VisualizationEngine
from src.utils import (
    setup_logging,
    load_config,
    ensure_directories,
    Timer,
    validate_data_files
)
from src.analyze_user_journeys import analyze_user_journeys

logger = logging.getLogger(__name__)


def main():
    """
    Главная функция для запуска полного анализа
    """
    print("=" * 80)
    print("АНАЛИЗ ДАННЫХ ПОЛЬЗОВАТЕЛЬСКОГО ПОВЕДЕНИЯ - МКД ПРИЛОЖЕНИЕ")
    print("=" * 80)
    print()

    # Загрузка конфигурации
    config = load_config("config.yaml")

    # Настройка логирования
    setup_logging(config.get('logging', {}))
    logger.info("Начало выполнения анализа")

    # Создание необходимых директорий
    ensure_directories(config)

    # Проверка наличия файлов данных
    if not validate_data_files(config):
        logger.error("Необходимые файлы данных не найдены. Поместите events.csv и users.csv в data/raw/")
        print("\n❌ ОШИБКА: Файлы данных не найдены!")
        print("Поместите следующие файлы в директорию data/raw/:")
        print("  - events.csv")
        print("  - users.csv")
        return 1

    try:
        # ============================================================
        # ЭТАП 1: Загрузка/Предобработка данных
        # ============================================================
        print("\n" + "=" * 80)
        print("ЭТАП 1: ЗАГРУЗКА И ПРЕДОБРАБОТКА ДАННЫХ")
        print("=" * 80)

        preprocessor = DataPreprocessor(config_path="config.yaml")
        processed_file = Path('data/processed/merged_data.csv')

        if processed_file.exists():
            # Загрузка существующего обработанного датасета
            print(f"\n📂 Найден обработанный датасет: {processed_file}")
            print("Загрузка данных...")

            with Timer("Загрузка обработанных данных"):
                merged_df = preprocessor.load_merged_data()
                print(f"✓ Загружено строк: {len(merged_df):,}")
                print(f"✓ Колонок: {len(merged_df.columns)}")
                print(
                    f"✓ Период данных: {merged_df['Дата и время события'].min().date()} - {merged_df['Дата и время события'].max().date()}")

                # preprocessor.add_global_session_id()
                # preprocessor.calculate_event_duration()
                # preprocessor.remove_consecutive_duplicates_with_clicks()
                # preprocessor.collapse_intermediate_screens()
                # preprocessor.remove_trailing_empty_screens()
                # preprocessor.fix_action_functional_typos()
                # preprocessor.save_processed_data()

                # Путь к JSON файлу
                json_path = 'src/fb2/funnels/categorized_combinations_with_funnels.json'
                # Создание экстрактора
                extractor = FunnelFeaturesExtractor(json_path)

                # Применение funnel features
                df_transformed = extractor.transform(merged_df)

                df = preprocessor.add_user_cohort_status(df_transformed)

                # Удаляем все строки, где is_new = True
                df_transformed2 = df[df['is_new'] == False].copy()

                # Сохранение обогащенного датасета
                preprocessor.merged_df = df_transformed2
                preprocessor.save_processed_data()

                # Запуск анализа
                analyzer = UserBehaviorAnalyzer("", 'fb2_output')
                analyzer.run_full_analysis(df_transformed2)

        logger.info("Анализ завершен успешно!")
        return 0

    except Exception as e:
        logger.error(f"Критическая ошибка при выполнении анализа: {e}", exc_info=True)
        print(f"\n❌ ОШИБКА: {e}")
        print("Проверьте лог-файл analysis.log для деталей")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
