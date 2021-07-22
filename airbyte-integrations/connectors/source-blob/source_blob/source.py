# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


from abc import ABC, abstractmethod
from typing import Any, List, Mapping, Optional, Tuple
from traceback import format_exc
from fnmatch import fnmatch

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from .stream import IncrementalFileStreamS3


class SourceBlob(AbstractSource, ABC):

    @property
    @abstractmethod
    def stream_class(self) -> type:
        """TODO docstring"""

    def check_connection(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        """
        This method checks two things:
            - That the credentials provided in config are valid for access.
            - That the path pattern(s) provided in config are valid to be matched against.
        """
        found_a_file = False
        try:
            for filepath in self.stream_class.filepath_iterator(logger, config.get("provider")):
                found_a_file = True
                for path_pattern in config.get("path_patterns"):
                    fnmatch(filepath, path_pattern)  # test that matching on the pattern doesn't error
                break  # just need first file here to test connection and valid patterns

        except Exception as e:
            logger.error(format_exc())
            return (False, e)

        else:
            if not found_a_file:
                logger.warn("Found 0 files (but connection is valid).")
            return (True, None)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        return [self.stream_class(**config)]


class SourceBlobS3(SourceBlob):
    """TODO docstring"""

    @property
    def stream_class(self) -> type:
        return IncrementalFileStreamS3
