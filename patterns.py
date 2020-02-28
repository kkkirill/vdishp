import abc
import io
from functools import wraps
from pathlib import Path
from typing import Optional, Tuple, Callable, Union, Any

import pandas as pd
from ipywidgets import Widget
from pandas import DataFrame


def df_not_empty(target: DataFrame):
    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not target.empty:
                return func(*args, **kwargs)
        return wrapper
    return inner


class Command(abc.ABC):
    def __init__(self, handler: Callable[[Union[Widget, dict]], None]) -> None:
        self.handler = handler

    @abc.abstractmethod
    def __call__(self, target: Any):
        pass


class ClickCommand(Command):
    def __call__(self, button: Widget) -> None:
        if isinstance(button, Widget):
            self.handler(button)


class NotifyCommand(Command):
    def __call__(self, event: dict) -> None:
        if isinstance(event, dict):
            self.handler(event)


class CommandFactory:

    @staticmethod
    def construct(event_type: str, *args, **kwargs) -> Command:
        """Factory method"""
        cls = ClickCommand if event_type == 'click' else NotifyCommand

        return cls(*args, **kwargs)


class Publisher:
    def __init__(self) -> None:
        self.observers = dict()

    def __call__(self, event: Union[dict, Widget]) -> None:
        """Dispatch method"""
        if isinstance(event, dict):
            self.observers[event['owner'], event['type']](event)
        else:
            self.observers[(event, 'click')](event)

    def register(self, subscriber: Tuple[Widget, Callable, str]) -> None:
        cmd = CommandFactory.construct(subscriber[2], subscriber[1])

        self.observers.update({(subscriber[0], subscriber[2]): cmd})

    def unregister(self, observer: Tuple[Widget, Callable, str]) -> None:
        self.observers.pop((observer[0], observer[2]))


class FileManager:
    ALLOWED_FILE_TYPES = ['json', 'csv']

    class __FileUploader:
        """FileUploader mediator class"""

        def __call__(self, content: bytes, file_ext: str) -> DataFrame:
            df = DataFrame()

            if content:
                if file_ext == 'json':
                    df = pd.read_json(io.BytesIO(content))
                else:
                    df = pd.read_csv(io.BytesIO(content))

            return df

    class __FileDownloader:
        """FileDownloader mediator class"""

        def __call__(self, data_frame: DataFrame, filepath: Path, file_ext: str) -> Optional[str]:
            filepath.parent.mkdir(parents=True, exist_ok=True)

            if file_ext == 'json':
                return data_frame.to_json(filepath)
            return data_frame.to_csv(filepath, index=False)

    def __init__(self) -> None:
        self.__uploader = self.__FileUploader()
        self.__downloader = self.__FileDownloader()

    def load_data(self, content: bytes, metadata: dict) -> Optional[DataFrame]:
        if not content:
            return DataFrame()
        elif (file_ext := metadata.get('type').split('/')[-1]) in self.ALLOWED_FILE_TYPES:
            return self.__uploader(content, file_ext)

    def save_data(self, data_frame: DataFrame, filepath: str) -> Optional[str]:
        path_obj = Path(filepath)

        if (file_ext := path_obj.suffix[1:]) in self.ALLOWED_FILE_TYPES:
            return self.__downloader(data_frame, path_obj, file_ext)
