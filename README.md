# Школа 21 × Сбербанк

![Python](https://img.shields.io/badge/python-3.9-blue.svg)
![Pandas](https://img.shields.io/badge/pandas-1.3.5-blue)
![Scikit-learn](https://img.shields.io/badge/scikit--learn-0.23.1-green)
![n8n](https://img.shields.io/badge/n8n-latest-orange)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-orange)
![SQL](https://img.shields.io/badge/SQL-SQLite-blue)

Этот репозиторий содержит мои решения и проекты, выполненные в рамках обучения в [Школе 21](https://21-school.ru/) Сбербанка. Здесь собраны все выполненные треки и буткемпы, демонстрирующие мой прогресс и освоенные навыки.

## О школе

**Школа 21** — бесплатная школа программирования от Сбербанка, работающая по методологии peer-to-peer обучения без преподавателей и фиксированного расписания. Обучение строится на практических проектах, взаимной проверке и самостоятельном поиске решений. Каждый трек — это набор реальных задач, в ходе которых осваиваются актуальные технологии и подходы.

## Прогресс

| Проект | Тема | Статус |
|---|---|---|
| [`ds_bootcamp`](./ds_bootcamp/) | Data Science Bootcamp: от UNIX и Python до ML и ИИ-агентов | ✅ Завершён |

## Структура репозитория

```
school_21_sberbank/
└── ds_bootcamp/
    ├── AI1_Agent/
    ├── DSB1_UNIX_Command_Line/
    ├── DSB2_Syntax_Semantics/
    ├── DSB3_OOP_skills/
    ├── DSB4_Package_management/
    ├── DSB5_Efficient_code_practices/
    ├── DSB6_MovieLens_Analytics/
    ├── DSB7_Pandas/
    ├── DSB8_SQL_Pandas/
    ├── DSB9_Pandas_SQL_Data_Visualization/
    ├── DSB10_Intro_to_ML/
    ├── DSB11_ML_Advanced/
    ├── DSB12_Food_nutrition/
    ├── requirements.txt
    └── README.md
```

## Проекты

<details>
<summary><strong>DS Bootcamp — Data Science Bootcamp (Школа 21 × Сбербанк)</strong></summary>

**Описание:** Интенсив по Data Science. Первая часть охватывает создание ИИ-агента, инструменты командной строки, основы Python и ООП, заканчиваясь командным проектом по анализу датасета MovieLens. Вторая часть посвящена Pandas, SQL, визуализации данных, классическому ML и завершается командным проектом по подбору питания.

**Ключевые технологии:** Python · Pandas · NumPy · Scikit-learn · Matplotlib · Seaborn · Plotly · SQL (SQLite) · n8n · GigaChat API

**Подробное описание всех заданий** → [ds_bootcamp/README.md](./ds_bootcamp/README.md)

</details>

## Карта навыков

| Область | Технологии и инструменты |
|---|---|
| **ИИ-агенты** | n8n, GigaChat API (Cloud.ru), LLM, промпт-инжиниринг, Telegram Bot API, RAG |
| **Командная строка** | UNIX, `curl`, `jq`, `sort`, `sed` |
| **Python** | Синтаксис, ООП, генераторы, `functools`, `collections` |
| **Окружение** | `venv`, `pip`, `PyTest`, `cProfile` |
| **Анализ данных** | Pandas, NumPy, SQL (SQLite) |
| **Визуализация** | Matplotlib, Seaborn, Plotly |
| **Машинное обучение** | Scikit-learn, GridSearchCV, Pipelines, ансамбли |
| **Парсинг** | Requests, BeautifulSoup, HH.ru API, RSS |

## Диплом

Диплом об окончании DS Bootcamp — [`ds_bootcamp/graduation_diploma.pdf`](./ds_bootcamp/graduation_diploma.pdf)

## Как использовать

1.  Клонируйте репозиторий:
    ```bash
    git clone https://github.com/[Your-Username]/[Your-Repo-Name].git
    ```
2.  Перейдите в директорию нужного проекта:
    ```bash
    cd [Your-Repo-Name]/ds_bootcamp
    ```
3.  Создайте и активируйте виртуальное окружение:
    ```bash
    python -m venv venv
    source venv/bin/activate  # для macOS/Linux
    venv\Scripts\activate  # для Windows
    pip install -r requirements.txt
    ```