import time
from typing import Type, Any, Iterator

import requests

TRANSACTIONS = "transactions"
TASKS = "tasks"
CUSTOMERS = "customers"
COMPANIES = "companies"
CONTACTS = "contacts"
CUSTOM_FIELDS = "custom_fields"
LEADS = "leads"
TAGS = "tags"
PIPELINES = "pipelines"
STATUSES = "statuses"
NOTES = "notes"
EVENTS = "events"


class AmoCrmRepeatError(Exception):
    """Надо ещё раз запрос кинуть"""


class AmoCrmNoConten(Exception):
    """amocrm error not found"""


class AmoCrmSearchError(Exception):
    """amocrm error when search"""


class AmoCrmError(Exception):
    errors = {
        "301": 'Moved permanently',
        "400": 'Bad request',
        "401": 'Unauthorized',
        "403": 'Forbidden',
        "404": 'Not found',
        "500": 'Internal server error',
        "502": 'Bad gateway',
        "503": 'Service unavailable',
        "429": 'Requests Per Second Exceeded'
    }

    def __init__(self, msg=None, error_code=None):
        self.error_code = error_code
        self.error_description = self.errors.get(str(error_code), f'Undescribed error: {error_code}')
        self.msg = f"{msg or 'AmoCRM error'}, {self.error_description}"
        super().__init__(self.msg)


class AmoCrmCompaniesMoreThanOne(Exception):
    """amocrm error when found companies > 1"""


class BaseEntity:
    entity_name = None
    url = None
    custom_fields_values = None
    PATCH_FIELDS = None
    # Поля, которые не должны попадать в api_value (служебные/производные).
    _API_VALUE_EXCLUDE = frozenset({'_new_next_date', '_embedded'})

    def __str__(self):
        return f"<{type(self).__name__}: {self.name}, {self.id}>"

    def __repr__(self):
        return str(self)

    def get_field(self, field_name):
        if not self.custom_fields_values:
            return None
        field = self.custom_fields_values.get(field_name)
        if not field:
            return None
        return field.get('value')

    def set_field(self, field_name, field_value, field_id=None):
        if self.custom_fields_values is None:
            self.custom_fields_values = {}
        custom_field = self.custom_fields_values.get(field_name)
        if custom_field is None:
            if field_id is None:
                raise KeyError(f"Not found custom field with key: {field_name}")
            self.custom_fields_values[field_name] = dict(id=field_id, value=field_value)
            return
        if custom_field.get('id'):
            custom_field['value'] = field_value
        elif field_id:
            custom_field['value'] = field_value
            custom_field['id'] = field_id
        else:
            raise ValueError(f"Custom field {field_name} without id")

    def as_dict(self):
        """Сырой ответ API: собираем из атрибутов, минуя служебные.

        _embedded, contacts и companies исключены: они вынесены
        в отдельные таблицы.
        """
        skip = self._API_VALUE_EXCLUDE | {'contacts', 'companies', 'tags'}
        return {
            k: v for k, v in vars(self).items()
            if not k.startswith('_') and k not in skip
        }

    def get_url(self):
        return self.url


class Customer(BaseEntity):
    url = "api/v4/customers"
    entity_name = CUSTOMERS

    def __init__(self, id, name, next_price, next_date, responsible_user_id,
                 status_id, periodicity, created_by, updated_by, created_at,
                 updated_at, closest_task_at, is_deleted, ltv, purchases_count,
                 average_check, account_id, custom_fields_values, _embedded=None):
        self.id = id
        self.name = name
        self.next_price = next_price
        self.next_date = next_date
        self.responsible_user_id = responsible_user_id
        self.status_id = status_id
        self.periodicity = periodicity
        self.created_by = created_by
        self.updated_by = updated_by
        self.created_at = created_at
        self.updated_at = updated_at
        self.closest_task_at = closest_task_at
        self.is_deleted = is_deleted
        self.ltv = ltv
        self.purchases_count = purchases_count
        self.average_check = average_check
        self.account_id = account_id
        self.custom_fields_values = custom_fields_values
        self._embedded = _embedded

        self.companies = []
        self.contacts = []
        self._new_next_date = None
        if not _embedded:
            return
        if _embedded.get(COMPANIES):
            self.companies = _embedded.get(COMPANIES)
        if _embedded.get(CONTACTS):
            self.contacts = _embedded.get(CONTACTS)


class Lead(BaseEntity):
    url = "api/v4/leads"
    entity_name = LEADS
    PATCH_FIELDS = ("custom_fields_values", "id")

    def __init__(self, name=None, id=None, price=None, responsible_user_id=None,
                 group_id=None, status_id=None, pipeline_id=None, source_id=None,
                 created_by=None, updated_by=None, created_at=None, updated_at=None,
                 closest_task_at=None, is_deleted=None, score=None, account_id=None,
                 custom_fields_values=None, _embedded=None):
        self.id = id
        self.name = name
        self.price = price
        self.responsible_user_id = responsible_user_id
        self.group_id = group_id
        self.status_id = status_id
        self.pipeline_id = pipeline_id
        self.source_id = source_id
        self.created_by = created_by
        self.updated_by = updated_by
        self.created_at = created_at
        self.updated_at = updated_at
        self.closest_task_at = closest_task_at
        self.is_deleted = is_deleted
        self.score = score
        self.account_id = account_id
        self.custom_fields_values = custom_fields_values
        self._embedded = _embedded
        self.tags = []
        self.contacts = []
        self.companies = []


NETWORK_RETRIES = 4
NETWORK_BACKOFF = 1.0  # сек, умножается на номер попытки


_ENTITY_EXTRA_KEYS = frozenset({'tags', 'contacts', 'companies'})


def _build_entity(obj_cls: Type[BaseEntity], item: dict) -> BaseEntity:
    """Конструирует сущность из ответа API.
    Лишние ключи (например, tags у Lead) отбрасываем.
    """
    try:
        return obj_cls(**item)
    except TypeError:
        filtered = {k: v for k, v in item.items() if k not in _ENTITY_EXTRA_KEYS}
        return obj_cls(**filtered)


def amocrm_get(obj_cls: Type[BaseEntity],
               page: int = None,
               limit: int = 250,
               _filter: dict = None,
               _with: str = None,
               query: Any = None,
               custom_filter: dict = None) -> Iterator[BaseEntity]:
    params = {}
    if page:
        params['page'] = page
    if limit:
        params['limit'] = limit
    if _filter:
        for key, value in _filter.items():
            filter_key = f'filter[{key}]'
            if isinstance(value, list):
                filter_key = f'{filter_key}[]'
            params[filter_key] = value
    if _with:
        params['with'] = ",".join(_with) if isinstance(_with, list) else _with
    if query:
        params['query'] = query

    headers = {}
    base_url = ""
    url = f"{base_url}{obj_cls.url}"
    entity_name = obj_cls.entity_name

    while True:
        json_data = None
        for attempt in range(NETWORK_RETRIES):
            response = None
            try:
                response = requests.get(url, params=params, headers=headers, timeout=120)
                json_data = response_handler(response)
                break
            except AmoCrmRepeatError:
                if attempt >= NETWORK_RETRIES - 1:
                    raise
                # 429 уже поспал в response_handler; повторяем тот же запрос
                continue
            except AmoCrmNoConten:
                json_data = None
                break
            except requests.RequestException as e:
                if attempt >= NETWORK_RETRIES - 1:
                    raise AmoCrmError(msg=f"network error {e}") from e
                time.sleep(NETWORK_BACKOFF * (attempt + 1))
                continue
            except Exception as e:
                raise AmoCrmError(
                    f"[!] {response.status_code if response is not None else '<No code!>'} |"
                    f" Неожиданная AMOCRM ошибка {e} "
                ) from e

        if json_data is None:
            # 204/пустой ответ -- дальше страниц нет
            break

        counter = 0
        for item in json_data['_embedded'][entity_name]:
            counter += 1
            entity = _build_entity(obj_cls, item)
            if custom_filter:
                skip_entity = False
                for key, value in custom_filter.items():
                    if entity.get_field(key) != value:
                        skip_entity = True
                        break
                if skip_entity:
                    continue
            yield entity

        if page:
            break
        if counter < limit:
            break
        time.sleep(1)
        next_link = json_data.get('_links', {}).get('next')
        if not next_link:
            break
        url = next_link['href']
        params = {}


def response_handler(response):
    if response.status_code == 204:
        raise AmoCrmNoConten(f"no content for {response.url}, method: {response.request.method}")
    if response.status_code == 429:
        time.sleep(2)
        raise AmoCrmRepeatError()
    if response.status_code != 200:
        json_data = {}
        try:
            json_data = response.json()
        except Exception:
            pass
        raise AmoCrmError(error_code=response.status_code, msg=json_data)
    return response.json()
