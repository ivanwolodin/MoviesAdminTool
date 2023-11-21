import abc
from typing import Any
import os
import json

from constants import STATE_JSON_FILE_NAME
from logger import logger


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict[str, Any]:
        """Получить состояние из хранилища."""
        pass


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path: str = file_path
        self.json_object: dict[str, str] = {}

    def save_state(self, state: dict[str, str]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self.file_path, 'w+') as json_file:
            json.dump(state, json_file)

    def retrieve_state(self) -> dict[str, str]:
        """Получить состояние из хранилища."""
        if os.path.isfile(self.file_path):
            with open(self.file_path, 'r+') as json_file:
                try:
                    self.json_object = json.load(json_file)
                except Exception as e:
                    print(e)
        else:
            logger.info('State JSON file does not exist!')
        return self.json_object


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: JsonFileStorage) -> None:
        self.storage: JsonFileStorage = storage

    @property
    def state(self):
        """Геттер для получения текущего состояния."""
        return self.storage.retrieve_state()

    @state.setter
    def state(self, key_value_tuple: tuple[str, str]):
        """Сеттер для установки состояния."""
        key, value = key_value_tuple
        state_dict = self.storage.retrieve_state()
        state_dict[key] = value
        self.storage.save_state(state_dict)

    def get_state(self, key: str) -> str | None:
        """Получить состояние по определённому ключу."""
        return self.state.get(key, None)


json_file_storage_obj = JsonFileStorage(STATE_JSON_FILE_NAME)
state = State(json_file_storage_obj)
