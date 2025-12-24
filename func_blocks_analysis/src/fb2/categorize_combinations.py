#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для группировки уникальных комбинаций приложения МКД по функциональным блокам.

Типы действий:
- cancel_actions: действия отмены/возврата назад
- exit_actions: выход из процесса, отмена создания
- success_review: просмотр, ознакомление, переходы к информации
- success_actions: успешные целевые действия (отправка, публикация, подтверждение)
- regular: обычные действия (не выводятся с отступом)
"""

import json
from collections import defaultdict
from typing import Dict, List, Tuple
from pathlib import Path


def determine_action_type(screen: str, functional: str, action: str) -> str:
    """
    Определение типа действия на основе ключевых слов.
    
    Returns:
        'cancel_actions', 'exit_actions', 'success_review', 'success_actions' или 'regular'
    """
    functional_lower = functional.lower()
    action_lower = action.lower()
    combined = f"{functional_lower} {action_lower}"
    
    # CANCEL ACTIONS - возврат назад
    cancel_keywords = [
        'возврат на предыдущий',
        'тап на стрелку назад'
    ]
    if any(kw in combined for kw in cancel_keywords):
        return 'cancel_actions'
    
    # EXIT ACTIONS - отмена, выход
    exit_keywords = [
        'отмена создания',
        'отмена выбора',
        'отмена подачи',
        'отмена отзыва',
        'отмена изменения',
        'отмена удаления'
    ]
    if any(kw in combined for kw in exit_keywords):
        return 'exit_actions'
    
    # SUCCESS REVIEW - просмотр, ознакомление, раскрытие, переходы для просмотра
    success_review_keywords = [
        'раскрытие вкладки',
        'открытие раздела',
        'переход в справку',
        'переход в раздел \'мои соседи\'',
        'переход к обращениям',
        'переход в раздел \'информация о доме\'',
        'переход к информации',
        'переход к чату',
        'переход в раздел \'перечень',
        'переход в раздел \'капитальный',
        'переход в раздел \'управляющая',
        'ознакомление с',
        'переход к предварительному просмотру',
        'переход к списку черновиков',
        'согласие с процессом проведения осс',
        'открытие описания варианта умного решения',
        'переход в раздел \'электронный консьерж\'',
        'переход в раздел \'база знаний\'',
        'связаться с владельцем тс => открытие экрана',
        'открытие раздела \'объявления соседей\'',
        'переход в справку по баллам',
        'переход на портал \'миллион призов\'',
        'переход к проверке баллов эд',
        'переход к правилам начисления',
        'переход в справку по функционалу',
        'переход в справку по активации',
        'поиск городского сервиса'
    ]
    if any(kw in combined for kw in success_review_keywords):
        return 'success_review'
    
    # SUCCESS ACTIONS - целевые успешные действия
    success_actions_keywords = [
        'отправка заявки',
        'публикация нового объявления',
        'подтверждение выполнения',
        'опровержение выполнения',
        'просмотр уведомления',
        'активация промокода',
        'применение промокода',
        'получение промокода',
        'переход к открытию двери',
        'звонок в диспетчерскую',
        'переход к открытию шлагбаума',
        'переход к удалению адреса',
        'включение/выключение отображения',
        'внесение контактных данных и отправка',
        'просьба убрать тс',
        'отправка сообщения автору',
        'редактирование опубликованного',
        'удаление объявления',
        'жалоба на объявление',
        'отправка заявки на подключение умного решения',
        'отправка сообщения в поддержку',
        'переход к активным гостевым доступам после завершения',
        'подтверждение отзыва доступа',
        'активация гостевого доступа',
        'отмена отзыва доступа',
        'сохранение изменения уровня',
        'отказ гостя от предоставленного',
        'отмена изменения уровня доступа',
        'добавление нового адреса',
        'прочтение инструкции',
        'прочтение справки',
        'выбор порядка формирования фонда капремонта => тап на кнопку \'далее\'',
        'добавление вопроса',
        'отправка осс на проверку',
        'загрузка файла с согласием',
        'удаление вопроса => тап на кнопку \'удалить\'',
        'скачать шаблон согласия',
        'отмена удаления вопроса'
    ]
    if any(kw in combined for kw in success_actions_keywords):
        return 'success_actions'
    
    return 'regular'


def load_combinations(filepath: str) -> Dict:
    """Загрузка данных из JSON файла."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def categorize_combination(screen: str, functional: str, action: str) -> str:
    """Определение блока для комбинации."""
    
    # 1. СОЗДАНИЕ ЗАЯВКИ
    if screen == "Новая заявка":
        return "Создание заявки"
    
    # 2. ПРОСМОТР И УПРАВЛЕНИЕ ЗАЯВКАМИ
    if screen == "Заявки":
        return "Просмотр и управление заявками"
    if screen == "Еще" and functional == "Переход в раздел 'Заявки'":
        return "Просмотр и управление заявками"
    
    # 3. ОПРОСЫ И СОБРАНИЯ СОБСТВЕННИКОВ
    if screen == "Новое ОСС":
        return "Опросы и собрания собственников"
    if screen == "Еще" and "Опросы и собрания собственников" in functional:
        return "Опросы и собрания собственников"
    
    # 4. ГОСТЕВОЙ ДОСТУП
    if screen == "Гостевой доступ":
        return "Гостевой доступ"
    if screen == "Новый адрес" and ("Гостевой доступ" in functional or "гостевого доступа" in functional.lower()):
        return "Гостевой доступ"
    if screen == "Мой дом" and "гостевым доступом" in functional.lower():
        return "Гостевой доступ"
    
    # 5. УПРАВЛЕНИЕ ТРАНСПОРТОМ
    if screen == "Связаться с владельцем ТС":
        return "Управление транспортом"
    if screen == "Еще" and "Мой транспорт" in functional:
        return "Управление транспортом"
    
    # 6. БАЛЛЫ И ПООЩРЕНИЯ
    if screen == "Мои баллы":
        return "Баллы и поощрения"
    if screen == "Еще" and "Мои баллы" in functional:
        return "Баллы и поощрения"
    
    # 7. ГОРОДСКИЕ СЕРВИСЫ
    if screen == "Услуги" and "городским сервисам" in functional.lower():
        return "Городские сервисы"
    if screen == "Услуги" and "городских сервисов" in functional.lower():
        return "Городские сервисы"
    if screen == "Услуги" and "городского сервиса" in functional.lower():
        return "Городские сервисы"
    
    # 8. УМНЫЕ РЕШЕНИЯ
    if screen == "Услуги" and "умного решения" in functional.lower():
        return "Умные решения"
    if screen == "Услуги" and "умным решениям" in functional.lower():
        return "Умные решения"
    if screen == "Мой дом" and "умного решения" in functional.lower():
        return "Умные решения"
    
    # 9. УСЛУГИ ПАРТНЕРОВ
    if screen == "Услуги":
        return "Услуги партнеров"
    if screen == "Еще" and functional == "Переход в раздел 'Услуги'":
        return "Услуги партнеров"
    
    # 10. ПРОСМОТР ОБЪЯВЛЕНИЙ
    if screen == "Объявления" and "Создание" not in functional and "Публикация" not in functional:
        return "Просмотр объявлений"
    if screen == "Еще" and functional == "Переход в раздел 'Объявления'":
        return "Просмотр объявлений"
    
    # 11. СОЗДАНИЕ ОБЪЯВЛЕНИЯ
    if screen == "Объявления" and ("Создание" in functional or "Публикация" in functional):
        return "Создание объявления"
    
    # 12. МОЙ ДОМ
    if screen == "Мой дом":
        return "Мой дом"
    
    # 13. ПРОФИЛЬ
    if screen == "Еще" and "Профиль" in functional:
        return "Профиль"
    if screen == "Еще" and "Мои платежи" in functional:
        return "Профиль"
    if screen == "Еще" and "Приборы учет" in functional:
        return "Профиль"
    if screen == "Еще" and "Настройки уведомлений" in functional:
        return "Профиль"
    
    # 14. СОЗДАНИЕ АДРЕСА
    if screen == "Новый адрес":
        return "Создание адреса"
    if screen == "Мой дом" and ("редактированию адреса" in functional.lower() or 
                                  "добавлению адреса" in functional.lower()):
        return "Создание адреса"
    
    # 15. ТЕХПОДДЕРЖКА
    if screen == "Техподдержка":
        return "Техподдержка"
    if screen == "Еще" and ("Техподдержка" in functional or 
                             "База знаний" in functional or 
                             "Электронный консьерж" in functional):
        return "Техподдержка"
    
    # 16. УВЕДОМЛЕНИЯ
    if screen == "Важное":
        return "Уведомления"
    
    # 17. НАВИГАЦИЯ
    if screen == "Еще" and functional == "Открытие экрана":
        return "Навигация"
    if screen == "Еще" and functional == "Переход в раздел 'Мои адреса'":
        return "Навигация"
    
    # По умолчанию
    if screen == "Еще":
        return "Навигация"
    
    return "Навигация"


def merge_similar_actions(actions: List[Dict]) -> List[Dict]:
    """
    Объединяет похожие действия (например, "выбор тега 1" и "Выбор тега 1").
    
    Args:
        actions: Список действий с count
        
    Returns:
        Объединенный список действий
    """
    # Группируем по нормализованному действию
    merged = defaultdict(lambda: {'count': 0, 'percentage': 0.0, 'original': None})
    
    for action in actions:
        # Нормализуем для сравнения
        normalized = action['action'].lower().strip()
        
        if merged[normalized]['original'] is None:
            # Первое вхождение - сохраняем оригинальное написание с заглавной буквы
            original_action = action['action']
            if original_action and original_action[0].islower():
                original_action = original_action[0].upper() + original_action[1:]
            merged[normalized]['original'] = original_action
        
        merged[normalized]['count'] += action['count']
        merged[normalized]['percentage'] += action['percentage']
    
    # Преобразуем обратно в список
    result = []
    for data in merged.values():
        result.append({
            'action': data['original'],
            'count': data['count'],
            'percentage': round(data['percentage'], 4)
        })
    
    # Сортируем по count
    result.sort(key=lambda x: x['count'], reverse=True)
    
    return result


def group_by_screen_functional(combinations: List[Dict], block_name: str) -> List[Dict]:
    """
    Группировка комбинаций по (Экран, Функционал) с типизацией действий.
    """
    # Группируем по (экран, функционал)
    groups = defaultdict(list)
    
    for combo in combinations:
        parts = combo['path'].split(' => ')
        screen = parts[0]
        functional = parts[1]
        action = parts[2]
        
        key = (screen, functional)
        action_type = determine_action_type(screen, functional, action)
        
        groups[key].append({
            'action': action,
            'count': combo['count'],
            'percentage': combo['percentage'],
            'action_type': action_type
        })
    
    # Формируем результат
    result = []
    for (screen, functional), actions in groups.items():
        # Объединяем похожие действия
        actions = merge_similar_actions(actions)
        
        # Определяем типы для объединенных действий заново
        for action_data in actions:
            action_data['action_type'] = determine_action_type(screen, functional, action_data['action'])
        
        # Группируем действия по типам
        regular_actions = []
        cancel_actions = []
        exit_actions = []
        success_review = []
        success_actions = []
        
        for action_data in actions:
            action_type = action_data['action_type']
            action_info = {
                'action': action_data['action'],
                'count': action_data['count'],
                'percentage': action_data['percentage']
            }
            
            if action_type == 'cancel_actions':
                cancel_actions.append(action_info)
            elif action_type == 'exit_actions':
                exit_actions.append(action_info)
            elif action_type == 'success_review':
                success_review.append(action_info)
            elif action_type == 'success_actions':
                success_actions.append(action_info)
            else:
                regular_actions.append(action_info)
        
        # Суммарный count для группы
        total_count = sum(a['count'] for a in actions)
        
        group_item = {
            'screen': screen,
            'functional': functional,
            'total_count': total_count,
            'regular_actions': regular_actions,
            'cancel_actions': cancel_actions,
            'exit_actions': exit_actions,
            'success_review': success_review,
            'success_actions': success_actions
        }
        result.append(group_item)
    
    # Сортируем группы по total_count (убывание)
    result.sort(key=lambda x: x['total_count'], reverse=True)
    
    return result


def process_combinations(input_file: str, output_file: str):
    """Основная функция обработки комбинаций."""
    print("Загрузка данных...")
    data = load_combinations(input_file)
    
    print(f"Загружено {len(data['combinations'])} уникальных комбинаций")
    
    # Группируем по блокам
    blocks_dict = {
        "Создание заявки": [],
        "Просмотр и управление заявками": [],
        "Опросы и собрания собственников": [],
        "Гостевой доступ": [],
        "Управление транспортом": [],
        "Баллы и поощрения": [],
        "Городские сервисы": [],
        "Умные решения": [],
        "Услуги партнеров": [],
        "Просмотр объявлений": [],
        "Создание объявления": [],
        "Мой дом": [],
        "Профиль": [],
        "Создание адреса": [],
        "Техподдержка": [],
        "Уведомления": [],
        "Навигация": []
    }
    
    print("\nКатегоризация комбинаций по блокам...")
    for combo in data['combinations']:
        parts = combo['path'].split(' => ')
        screen = parts[0]
        functional = parts[1]
        action = parts[2]
        
        block = categorize_combination(screen, functional, action)
        blocks_dict[block].append(combo)
    
    # Выводим статистику
    print("\n" + "="*80)
    print("СТАТИСТИКА ПО БЛОКАМ:")
    print("="*80)
    total_combinations = 0
    for block_name in sorted(blocks_dict.keys()):
        count = len(blocks_dict[block_name])
        total_combinations += count
        print(f"{block_name:<45} {count:>3} комбинаций")
    print("="*80)
    print(f"{'ИТОГО:':<45} {total_combinations:>3} комбинаций")
    print("="*80)
    
    # Группируем внутри каждого блока
    print("\nГруппировка по (Экран, Функционал) с типизацией действий...")
    result = {
        'metadata': {
            'generated_at': data['metadata']['generated_at'],
            'source_file': input_file,
            'total_records': data['metadata']['total_records'],
            'total_unique_combinations': len(data['combinations']),
            'separator': data['metadata']['separator'],
            'date_range': data['metadata']['date_range'],
            'blocks_count': len(blocks_dict)
        },
        'blocks': []
    }
    
    # Подсчитываем финальное количество комбинаций после объединения
    final_combinations_count = 0
    
    for block_name in sorted(blocks_dict.keys()):
        if len(blocks_dict[block_name]) == 0:
            continue
            
        grouped = group_by_screen_functional(blocks_dict[block_name], block_name)
        
        # Подсчитываем количество комбинаций в этом блоке
        block_combinations = sum(
            len(g['regular_actions']) + 
            len(g['cancel_actions']) + 
            len(g['exit_actions']) + 
            len(g['success_review']) + 
            len(g['success_actions']) 
            for g in grouped
        )
        final_combinations_count += block_combinations
        
        # Подсчитываем общий count блока
        total_block_count = sum(g['total_count'] for g in grouped)
        
        result['blocks'].append({
            'name': block_name,
            'total_count': total_block_count,
            'combinations_count': block_combinations,
            'groups': grouped
        })
    
    # Обновляем финальное количество комбинаций
    result['metadata']['final_combinations_count'] = final_combinations_count
    
    # Сортируем блоки по total_count
    result['blocks'].sort(key=lambda x: x['total_count'], reverse=True)
    
    # Сохраняем результат
    print(f"\nСохранение результата в {output_file}...")
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"✓ Результат сохранён в {output_file}")
    
    # Выводим краткую сводку
    print("\n" + "="*80)
    print("ИТОГОВАЯ СТАТИСТИКА:")
    print("="*80)
    print(f"Исходных комбинаций: {len(data['combinations'])}")
    print(f"Финальных комбинаций (после объединения): {final_combinations_count}")
    print(f"Объединено: {len(data['combinations']) - final_combinations_count}")
    print("="*80)
    
    for block in result['blocks']:
        print(f"{block['name']:<45} {block['combinations_count']:>3} комбинаций | {block['total_count']:>10,} событий")
    print("="*80)


def main():
    """Главная функция."""
    input_file = 'unique_combinations.json'
    output_file = 'categorized_combinations.json'
    
    process_combinations(input_file, output_file)
    
    print("\n✓ Обработка завершена успешно!")


if __name__ == "__main__":
    main()
