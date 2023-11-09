import abc
from typing import Any, Dict
import os
import json


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        pass


class JsonFileStorage(BaseStorage):
    """Реализация хранилища, использующего локальный файл.

    Формат хранения: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self.json_object = {}

    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""
        with open(self.file_path, 'w+') as json_file:
            json.dump(state, json_file)

    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""
        if os.path.isfile(self.file_path):
            with open(self.file_path, 'r+') as json_file:
                try:
                    self.json_object = json.load(json_file)
                except Exception as e:
                    print(e)
        else:
            print('Файл не существует')
        return self.json_object


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: JsonFileStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        state_dict = self.storage.retrieve_state()
        state_dict[key] = value
        self.storage.save_state(state_dict)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        return self.storage.retrieve_state().get(key, None)


json_file_storage_obj = JsonFileStorage('state.json')
state = State(json_file_storage_obj)
